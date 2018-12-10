%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for storing of information about open files in session.
%%% @end
%%%-------------------------------------------------------------------
-module(session_open_files).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([register_open_file/2, unregister_open_file/2, invalidate_entries/1]).

-define(HELPER_HANDLES_TREE_ID, <<"helper_handles">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds open file UUId to session.
%% @end
%%--------------------------------------------------------------------
-spec register_open_file(session:id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
register_open_file(SessId, FileGuid) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:add_element(FileGuid, OpenFiles)}}
    end,

    case session:update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUId from session.
%% @end
%%--------------------------------------------------------------------
-spec unregister_open_file(session:id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
unregister_open_file(SessId, FileGuid) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:del_element(FileGuid, OpenFiles)}}
    end,

    case session:update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes all entries connected with session open files.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_entries(session:id()) -> ok | {error, term()}.
invalidate_entries(SessId) ->
    case session:get(SessId) of
        {ok, #document{key = SessId, value = #session{open_files = OpenFiles}}} ->
            lists:foreach(fun(FileGuid) ->
                FileCtx = file_ctx:new_by_guid(FileGuid),
                file_handles:invalidate_session_entry(FileCtx, SessId)
            end, sets:to_list(OpenFiles));
        Error ->
            Error
    end.