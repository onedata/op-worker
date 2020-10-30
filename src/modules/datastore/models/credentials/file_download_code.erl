%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File download code record that holds a code for downloading a file via GUI.
%%% Such code is valid for certain (configurable in app.config) amount of time
%%% after which it expires and can no longer be used.
%%% @end
%%%-------------------------------------------------------------------
-module(file_download_code).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, verify/1, remove/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type code() :: binary().
-export_type([code/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(EXPIRATION_INTERVAL, application:get_env(
    ?APP_NAME, download_code_expiration_interval_seconds, timer:hours(24)
)).

-define(NOW(), clock:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(session:id(), fslogic_worker:file_guid()) -> {ok, code()} | {error, term()}.
create(SessionId, FileGuid) ->
    Ctx = ?CTX,

    ExpirationInterval = ?EXPIRATION_INTERVAL,
    CtxWithExpiration = Ctx#{expiry => ExpirationInterval},

    Doc = #document{
        value = #file_download_code{
            % Setting and checking expiration is necessary as couch will only
            % remove document from db and not from memory
            expires = ?NOW() + ExpirationInterval,
            session_id = SessionId,
            file_guid = FileGuid
        }
    },
    case datastore_model:save(CtxWithExpiration, Doc) of
        {ok, #document{key = Code}} -> {ok, Code};
        {error, _} = Error -> Error
    end.


-spec verify(code()) -> false | {true, session:id(), fslogic_worker:file_guid()}.
verify(Code) ->
    Now = ?NOW(),

    case datastore_model:get(?CTX, Code) of
        {ok, #document{value = #file_download_code{
            expires = Expires,
            session_id = SessionId,
            file_guid = FileGuid
        }}} when Now < Expires ->
            {true, SessionId, FileGuid};
        {ok, _} ->
            ok = datastore_model:delete(?CTX, Code),
            false;
        _ ->
            false
    end.


-spec remove(code()) -> ok.
remove(Code) ->
    ok = ?ok_if_not_found(datastore_model:delete(?CTX, Code)).


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
        {expires, integer},
        {session_id, string},
        {file_guid, string}
    ]}.
