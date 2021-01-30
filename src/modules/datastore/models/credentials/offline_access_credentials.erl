%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Offline access credentials management model.
%%% @end
%%%-------------------------------------------------------------------
-module(offline_access_credentials).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([acquire/3, get/1, remove/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: binary().
-type record() :: #offline_access_credentials{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).

-type error() :: {error, term()}.

-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec acquire(id(), aai:subject(), auth_manager:credentials()) ->
    {ok, auth_manager:credentials()} | error().
acquire(Id, ?SUB(user, UserId), UserCredentials) ->
    case auth_manager:acquire_offline_user_access_token(UserId, UserCredentials) of
        {ok, OfflineAccessToken} ->
            Doc = #document{
                key = Id,
                value = #offline_access_credentials{
                    user_id = UserId,
                    access_token = OfflineAccessToken,
                    interface = auth_manager:get_interface(UserCredentials),
                    data_access_caveats_policy = auth_manager:get_data_access_caveats_policy(
                        UserCredentials
                    ),
                    acquirement_timestamp = global_clock:timestamp_seconds(),
                    valid_until = token_valid_until(OfflineAccessToken)
                }
            },
            datastore_model:save(?CTX, Doc);
        {error, _} = Err2 ->
            Err2
    end;
acquire(_, _, _) ->
    ?ERROR_TOKEN_SUBJECT_INVALID.


-spec get(id()) -> {ok, auth_manager:credentials()} | error().
get(Id) ->
    datastore_model:get(?CTX, Id).


-spec remove(id()) -> ok.
remove(Id) ->
    ok = datastore_model:delete(?CTX, Id).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {user_id, string},
        {access_token, string},
        {interface, atom},
        {data_access_caveats_policy, atom}
    ]}.


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec token_valid_until(tokens:serialized()) -> time:seconds().
token_valid_until(OfflineAccessTokenBin) ->
    {ok, OfflineAccessToken} = tokens:deserialize(OfflineAccessTokenBin),

    lists:foldl(fun
        (#cv_time{valid_until = ValidUntil}, undefined) -> ValidUntil;
        (#cv_time{valid_until = ValidUntil}, Acc) -> min(ValidUntil, Acc)
    end, undefined, caveats:filter([cv_time], tokens:get_caveats(OfflineAccessToken))).
