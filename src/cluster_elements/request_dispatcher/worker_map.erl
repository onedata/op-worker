%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Worker map wraps operations on worker_map_ets. It contains mapping
%%% between worker name, and node where such worker runs.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_map).
-author("Tomasz Lichon").

-include("cluster_elements/request_dispatcher/worker_map.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, terminate/0, get_worker_node/1, get_worker_node/2, update_workers/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initialize empty worker map
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ets:new(?worker_map_ets, [bag, protected, named_table, {read_concurrency, true}]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Destroy worker map
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ets:delete(?worker_map_ets),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_worker_node(WorkerName, any).
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node(WorkerName :: atom()) -> {ok, node()} | {error, term()}.
get_worker_node(WorkerName) ->
    get_worker_node(WorkerName, ?default_worker_selection_type).

%%--------------------------------------------------------------------
%% @doc
%% Chooses one of nodes where worker is working. The second argument
%% determines selection type
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node(WorkerName :: atom(), SelectionType :: selection_type()) -> {ok, node()} | {error, term()}.
get_worker_node(WorkerName, random) ->
    get_worker_node_prefering_local(WorkerName);
get_worker_node(WorkerName, prefere_local) ->
    get_random_worker_node(WorkerName).

%%--------------------------------------------------------------------
%% @doc
%% INVOKE ONLY IN REQUEST DISPATCHER PROCESS!
%% Updates ets when new workers list appears.
%% @end
%%--------------------------------------------------------------------
-spec update_workers(WorkersList :: [{Node :: node(), WorkerName :: atom()}]) -> ok.
update_workers(WorkersList) ->
    WorkersListInverted = lists:map(fun({Node, WorkerName}) -> {WorkerName, Node} end, WorkersList),
    ModuleToNodes = cluster_manager_utils:aggregate_over_first_element(WorkersListInverted),
    lists:map(fun update_worker/1, ModuleToNodes).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses one of nodes where worker is working. Preferes local node.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node_prefering_local(WorkerName :: atom()) -> {ok, node()} | {error, term()}.
get_worker_node_prefering_local(WorkerName) ->
    MyNode = node(),
    case ets:match(?worker_map_ets, {WorkerName, MyNode}) of
        [] -> get_random_worker_node(WorkerName);
        [{_, MyNode}] -> {ok, MyNode}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses random node where worker is working
%% @end
%%--------------------------------------------------------------------
-spec get_random_worker_node(WorkerName :: atom()) -> {ok, node()} | {error, term()}.
get_random_worker_node(WorkerName) ->
    case ets:lookup(?worker_map_ets, WorkerName) of
        [] -> {error, not_found};
        Entries ->
            RandomIndex = random:uniform(length(Entries)),
            {_, Node} = lists:nth(RandomIndex, Entries),
            {ok, Node}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates location of given worker (nodes where it is running) in worker map
%% @end
%%--------------------------------------------------------------------
-spec update_worker({WorkerName :: atom(), Nodes :: [node()]}) -> ok.
update_worker({WorkerName, Nodes}) ->
    case ets:lookup(?worker_map_ets, WorkerName) of
        [] ->
            lists:foreach(fun(Node) -> true = ets:insert(?worker_map_ets, {WorkerName, Node}) end, Nodes),
            ok;
        Entries ->
            CurrentNodes =
                case cluster_manager_utils:aggregate_over_first_element(Entries) of
                    [] -> [];
                    {_, AggregatedNodes} -> AggregatedNodes
                end,
            CurrentNodesSet = sets:from_list(CurrentNodes),
            NewNodesSet = sets:from_list(Nodes),
            ToDelete = sets:to_list(sets:subtract(CurrentNodesSet, NewNodesSet)),
            ToCreate = sets:to_list(sets:subtract(NewNodesSet, CurrentNodesSet)),
            lists:foreach(fun(Node) -> true = ets:delete_object(?worker_map_ets, {WorkerName, Node}) end, ToDelete),
            lists:foreach(fun(Node) -> true = ets:insert(?worker_map_ets, {WorkerName, Node}) end, ToCreate),
            ok
    end.