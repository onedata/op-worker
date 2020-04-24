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
-export([invalidate_local_entries/1, invalidate_node_entries/3]).

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
    case session:add_local_links(SessId, ?OPEN_FILES_TREE_ID, FileGuid, <<>>, false) of
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
    FileUuid = file_id:guid_to_uuid(FileGuid),
    replica_synchronizer:cancel_transfers_of_session(FileUuid, SessId),
    session:delete_local_links(SessId, ?OPEN_FILES_TREE_ID, FileGuid, false).

%%--------------------------------------------------------------------
%% @doc
%% Removes all entries connected with session open files.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_entries(session:id()) -> ok.
invalidate_entries(SessId) ->
    {AnsList, BadNodes} =
        rpc:multicall(consistent_hashing:get_all_nodes(), ?MODULE, invalidate_local_entries, [SessId]),
    lists:foreach(fun(Ans) -> ok = Ans end, AnsList),
    lists:foreach(fun(FailedNode) ->
        [Node | _] = ha_datastore:get_backup_nodes(FailedNode),
        ok = rpc:call(Node, ?MODULE, invalidate_node_entries,
            [SessId, FailedNode, #{failed_nodes => [Node], failed_master => true}])
    end, BadNodes).

%%--------------------------------------------------------------------
%% @doc
%% @equiv invalidate_node_entries(SessId, node()).
%% @end
%%--------------------------------------------------------------------
-spec invalidate_local_entries(session:id()) -> ok.
invalidate_local_entries(SessId) ->
    invalidate_node_entries(SessId, node(), #{}).

%%--------------------------------------------------------------------
%% @doc
%% Removes all entries connected with session open files.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_node_entries(session:id(), node()) -> ok.
invalidate_node_entries(SessId, Node, CtxExtension) ->
    {ok, Links} = session:fold_local_links(SessId, ?OPEN_FILES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end, Node
    ),
    Names = lists:map(fun(#link{name = FileGuid}) ->
        FileCtx = file_ctx:new_by_guid(FileGuid),
        file_handles:invalidate_session_entry(FileCtx, SessId),
        FileGuid
    end, Links),
    session:delete_local_links(SessId, ?OPEN_FILES_TREE_ID, Names, CtxExtension#{ha_disabled => false}),
    ok.