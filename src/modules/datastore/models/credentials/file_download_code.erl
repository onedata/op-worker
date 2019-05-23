%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Volatile file download code record that holds a single-use code for
%%% downloading a file via GUI.
%%% @end
%%%-------------------------------------------------------------------
-module(file_download_code).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/2, consume/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type code() :: binary().
-export_type([code/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file download code based on session id and file guid.
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), fslogic_worker:file_guid()) -> {ok, code()} | {error, term()}.
create(SessionId, FileGuid) ->
    Doc = #document{
        value = #file_download_code{
            session_id = SessionId,
            file_guid = FileGuid
        }
    },
    case datastore_model:save(?CTX, Doc) of
        {ok, #document{key = Code}} -> {ok, Code};
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Verifies and consumes a file download code, upon success returning the
%% session id and file guid that were used to generated it.
%% @end
%%--------------------------------------------------------------------
-spec consume(code()) -> false | {true, session:id(), fslogic_worker:file_guid()}.
consume(Code) ->
    case datastore_model:get(?CTX, Code) of
        {ok, #document{value = #file_download_code{session_id = SessionId, file_guid = FileGuid}}} ->
            ok = datastore_model:delete(?CTX, Code),
            {true, SessionId, FileGuid};
        _ ->
            false
    end.

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