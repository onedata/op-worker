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

-define(WORKER_MAP_ETS, workers_ets).

-include("cluster_elements/request_dispatcher/worker_map.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, terminate/0, get_worker_node/1, get_worker_node/2,
    get_worker_nodes/1, update_workers/1]).

-export_type([selection_type/0, worker_name/0, worker_ref/0]).

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
    ets:new(?WORKER_MAP_ETS, [bag, protected, named_table, {read_concurrency, true}]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Destroy worker map
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ets:delete(?WORKER_MAP_ETS),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_worker_node(WorkerName, any).
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node(WorkerName :: worker_name()) -> {ok, node()} | {error, term()}.
get_worker_node(WorkerName) ->
    get_worker_node(WorkerName, ?DEFAULT_WORKER_SELECTION_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Chooses one of nodes where worker is working. The second argument
%% determines selection type.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node(WorkerName :: worker_name(), SelectionType :: selection_type()) ->
    {ok, node()} | {error, term()}.
get_worker_node(WorkerName, random) ->
    get_random_worker_node(WorkerName);
get_worker_node(WorkerName, prefer_local) ->
    get_worker_node_prefering_local(WorkerName).

%%--------------------------------------------------------------------
%% @doc
%% Chooses all nodes where worker is working.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_nodes(WorkerName :: worker_name()) -> {ok, [node()]}.
get_worker_nodes(WorkerName) ->
    {_, Nodes} = lists:unzip(ets:lookup(?WORKER_MAP_ETS, WorkerName)),
    {ok, Nodes}.

%%--------------------------------------------------------------------
%% @doc
%% INVOKE ONLY IN REQUEST DISPATCHER PROCESS!
%% Updates ets when new workers list appears.
%% @end
%%--------------------------------------------------------------------
-spec update_workers(WorkersList :: [{Worker :: module(), Nodes :: [node()]}]) -> ok.
update_workers(WorkerToNodes) ->
    lists:foreach(fun update_worker/1, WorkerToNodes).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses one of nodes where worker is working. Prefers local node.
%% @end
%%--------------------------------------------------------------------
-spec get_worker_node_prefering_local(WorkerName :: worker_name()) -> {ok, node()} | {error, term()}.
get_worker_node_prefering_local(WorkerName) ->
    MyNode = node(),
    case ets:match(?WORKER_MAP_ETS, {WorkerName, MyNode}) of
        [] -> get_random_worker_node(WorkerName);
        [[]] -> {ok, MyNode}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses random node where worker is working
%% @end
%%--------------------------------------------------------------------
-spec get_random_worker_node(WorkerName :: worker_name()) -> {ok, node()} | {error, term()}.
get_random_worker_node(WorkerName) ->
    case ets:lookup(?WORKER_MAP_ETS, WorkerName) of
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
-spec update_worker({WorkerName :: worker_name(), Nodes :: [node()]}) -> ok.
update_worker({WorkerName, Nodes}) ->
    case ets:lookup(?WORKER_MAP_ETS, WorkerName) of
        [] ->
            lists:foreach(fun(Node) ->
                true = ets:insert(?WORKER_MAP_ETS, {WorkerName, Node}) end, Nodes),
            ok;
        Entries ->
            CurrentNodes =
                case utils:aggregate_over_first_element(Entries) of
                    [] -> [];
                    [{_, AggregatedNodes}] -> AggregatedNodes
                end,
            CurrentNodesSet = sets:from_list(CurrentNodes),
            NewNodesSet = sets:from_list(Nodes),
            ToDelete = sets:to_list(sets:subtract(CurrentNodesSet, NewNodesSet)),
            ToCreate = sets:to_list(sets:subtract(NewNodesSet, CurrentNodesSet)),
            lists:foreach(fun(Node) ->
                true = ets:delete_object(?WORKER_MAP_ETS, {WorkerName, Node}) end, ToDelete),
            lists:foreach(fun(Node) ->
                true = ets:insert(?WORKER_MAP_ETS, {WorkerName, Node}) end, ToCreate),
            ok
    end.