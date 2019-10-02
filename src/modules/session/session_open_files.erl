%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for storing information about open files in session.
%%% @end
%%%-------------------------------------------------------------------
-module(session_open_files).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([register/2, deregister/2, invalidate_entries/1]).
%% For RPC
-export([invalidate_local_entries/1]).

-define(OPEN_FILES_TREE_ID, <<"open_files">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds open file UUId to session.
%% @end
%%--------------------------------------------------------------------
-spec register(session:id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
register(SessId, FileGuid) ->
    case session:add_local_links(SessId, ?OPEN_FILES_TREE_ID, FileGuid, <<>>) of
        ok -> ok;
        {error, already_exists} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUId from session.
%% @end
%%--------------------------------------------------------------------
-spec deregister(session:id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
deregister(SessId, FileGuid) ->
    session:delete_local_links(SessId, ?OPEN_FILES_TREE_ID, FileGuid).

%%--------------------------------------------------------------------
%% @doc
%% Removes all entries connected with session open files.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_entries(session:id()) -> ok.
invalidate_entries(SessId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, ?MODULE, invalidate_local_entries, [SessId])
    end, Nodes).

%%--------------------------------------------------------------------
%% @doc
%% Removes all entries connected with session open files.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_local_entries(session:id()) -> ok.
invalidate_local_entries(SessId) ->
    {ok, Links} = session:fold_local_links(SessId, ?OPEN_FILES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end
    ),
    Names = lists:map(fun(#link{name = FileGuid}) ->
        FileCtx = file_ctx:new_by_guid(FileGuid),
        file_handles:invalidate_session_entry(FileCtx, SessId),
        FileGuid
    end, Links),
    session:delete_local_links(SessId, ?OPEN_FILES_TREE_ID, Names),
    ok.