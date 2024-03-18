%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Download code record that holds arguments for downloading e.g. file content
%%% or store dump via GUI.
%%% Download codes are intended to be deleted right after the first successful
%%% download. In case of a download failure, the code can still be used until
%%% the end of the expiration period, after which it becomes invalid.
%%% This approach allows resuming failed downloads at any time within the code
%%% validity.
%%
%%% NOTE: module is prefixed `file_` due to historical reason and being record
%%% stored in database.
%%% @end
%%%-------------------------------------------------------------------
-module(file_download_code).
-author("Lukasz Opiola").

-include("http/http_download.hrl").
-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/1, verify/1, remove/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type code() :: binary().
-type record() :: #file_download_code{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([code/0, record/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(EXPIRATION_INTERVAL, application:get_env(
    ?APP_NAME, download_code_expiration_interval_seconds, 86400  %% 24 hours
)).

-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(download_args:record()) -> {ok, code()} | {error, term()}.
create(DownloadArgs) ->
    Ctx = ?CTX,

    ExpirationInterval = ?EXPIRATION_INTERVAL,
    CtxWithExpiration = Ctx#{expiry => ExpirationInterval},

    Doc = #document{
        value = #file_download_code{
            % Setting and checking expiration is necessary as couch will only
            % remove document from db and not from memory
            expires = ?NOW() + ExpirationInterval,
            download_args = DownloadArgs
        }
    },
    case datastore_model:save(CtxWithExpiration, Doc) of
        {ok, #document{key = Code}} -> {ok, Code};
        {error, _} = Error -> Error
    end.


-spec verify(code()) -> false | {true, download_args:record()}.
verify(Code) ->
    Now = ?NOW(),

    case datastore_model:get(?CTX, Code) of
        {ok, #document{value = #file_download_code{
            expires = Expires,
            download_args = DownloadArgs
        }}} when Now < Expires ->
            {true, DownloadArgs};
        {ok, _} ->
            ok = datastore_model:delete(?CTX, Code),
            false;
        _ ->
            false
    end.


-spec remove(code()) -> ok.
remove(Code) ->
    ok = datastore_model:delete(?CTX, Code).


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
    4.


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
    ]};
get_record_struct(2) ->
    {record, [
        {expires, integer},
        {session_id, string},
        {file_guids, [string]} % modified field
    ]};
get_record_struct(3) ->
    {record, [
        {expires, integer},
        {session_id, string},
        {file_guids, [string]},
        {follow_symlinks, boolean}
    ]};
get_record_struct(4) ->
    {record, [
        {expires, integer},
        {download_args, {custom, string, {persistent_record, to_string, from_string, download_args}}}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, Record) ->
    {
        file_download_code, 
        Expires,
        SessionId,
        FileGuid
    } = Record,
    
    {2, {file_download_code,
        Expires,
        SessionId,
        [FileGuid]
    }};
upgrade_record(2, Record) ->
    {
        file_download_code,
        Expires,
        SessionId,
        FileGuids
    } = Record,
    
    {3, {file_download_code,
        Expires,
        SessionId,
        FileGuids,
        false
    }};
upgrade_record(3, Record) ->
    {
        file_download_code,
        Expires,
        SessionId,
        FileGuids,
        FollowSymlinks
    } = Record,

    DownloadArgs = #file_content_download_args{
        session_id = SessionId,
        file_guids = FileGuids,
        follow_symlinks = FollowSymlinks
    },
    {4, {file_download_code, Expires, DownloadArgs}}.
