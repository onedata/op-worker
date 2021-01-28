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
-export([ensure_consumer_token_up_to_date/1]).

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
acquire(OfflineCredentialsId, ?SUB(user, UserId), UserCredentials) ->
    case auth_manager:acquire_offline_user_access_token(UserId, UserCredentials) of
        {ok, OfflineAccessToken} ->
            Doc = #document{
                key = OfflineCredentialsId,
                value = OfflineCredentials = #offline_access_credentials{
                    user_id = UserId,
                    access_token = OfflineAccessToken,
                    interface = auth_manager:get_interface(UserCredentials),
                    data_access_caveats_policy = auth_manager:get_data_access_caveats_policy(
                        UserCredentials
                    )
                }
            },
            case datastore_model:save(?CTX, Doc) of
                {ok, _} ->
                    {ok, to_token_credentials(OfflineCredentials)};
                {error, _} = Err1 ->
                    Err1
            end;
        {error, _} = Err2 ->
            Err2
    end;
acquire(_, _, _) ->
    ?ERROR_TOKEN_SUBJECT_INVALID.


-spec get(id()) -> {ok, auth_manager:credentials()} | error().
get(OfflineCredentialsId) ->
    case datastore_model:get(?CTX, OfflineCredentialsId) of
        {ok, #document{value = OfflineCredentials}} ->
            {ok, to_token_credentials(OfflineCredentials)};
        {error, _} = Error ->
            Error
    end.


-spec remove(id()) -> ok.
remove(OfflineCredentialsId) ->
    ok = datastore_model:delete(?CTX, OfflineCredentialsId).


-spec ensure_consumer_token_up_to_date(auth_manager:credentials()) ->
    auth_manager:credentials().
ensure_consumer_token_up_to_date(TokenCredentials) ->
    AccessToken = auth_manager:get_access_token(TokenCredentials),
    {ok, ProviderIdentityToken} = provider_auth:acquire_identity_token(),

    auth_manager:update_client_tokens(
        TokenCredentials,
        AccessToken,
        ProviderIdentityToken
    ).


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
-spec to_token_credentials(record()) -> auth_manager:credentials().
to_token_credentials(#offline_access_credentials{
    access_token = OfflineAccessToken,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    {ok, ProviderIdentityToken} = provider_auth:acquire_identity_token(),

    auth_manager:build_token_credentials(
        OfflineAccessToken, ProviderIdentityToken,
        undefined, Interface, DataAccessCaveatsPolicy
    ).
