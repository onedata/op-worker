%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module handles Nagios monitoring requests.
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_http_handler).

-include("global_definitions.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/3, handle/2, terminate/3]).
-export([get_cluster_status/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no state is required
%% @end
%%--------------------------------------------------------------------
-spec init(term(), term(), term()) -> {ok, term(), []}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request producing an XML response.
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, term(), term()}.
handle(Req, State) ->
    {ok, Timeout} = application:get_env(?APP_NAME, nagios_healthcheck_timeout),

    NewReq =
        case get_cluster_status(Timeout) of
            error ->
                {ok, Req2} = opn_cowboy_bridge:apply(cowboy_req, reply, [500, Req]),
                Req2;
            {ok, {?APP_NAME, AppStatus, NodeStatuses}} ->
                MappedClusterState = lists:map(
                    fun({Node, NodeStatus, NodeComponents}) ->
                        NodeDetails = lists:map(
                            fun({Component, Status}) ->
                                {Component, [{status, atom_to_list(Status)}], []}
                            end, NodeComponents),
                        {?APP_NAME, [{name, atom_to_list(Node)}, {status, atom_to_list(NodeStatus)}], NodeDetails}
                    end, NodeStatuses),

                {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
                DateString = gui_str:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),

                % Create the reply
                Healthdata = {healthdata, [{date, DateString}, {status, atom_to_list(AppStatus)}], MappedClusterState},
                Content = lists:flatten([Healthdata]),
                Export = xmerl:export_simple(Content, xmerl_xml),
                Reply = io_lib:format("~s", [lists:flatten(Export)]),

                % Send the reply
                {ok, Req2} = opn_cowboy_bridge:apply(cowboy_req, reply,
                    [200, [{<<"content-type">>, <<"application/xml">>}], Reply, Req]),
                Req2
        end,
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed.
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% Internal Functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Contacts all components of the cluster and produces a healthcheck report
%% in following form:
%% {oneprovider_node, AppStatus, [
%%     {Node1, Node1Status, [
%%         {node_manager, NodeManStatus},
%%         {request_dispatcher, DispStatus},
%%         {Worker1, W1Status},
%%         {Worker2, W2Status},
%%         {Worker3, W3Status},
%%     ]},
%%     {Node2, Node2Status, [
%%         ...
%%     ]}
%% ]}
%% Status can be: ok | error | out_of_sync
%% @end
%%--------------------------------------------------------------------
-spec get_cluster_status(Timeout :: integer()) -> term().
get_cluster_status(Timeout) ->
    case check_ccm(Timeout) of
        error ->
            error;
        {Nodes, Workers, StateNum} ->
            try
                NodeManagerStatuses = check_node_managers(Nodes, Timeout),
                DistpatcherStatuses = check_dispatchers(Nodes, Timeout),
                WorkerStatuses = check_workers(Nodes, Workers, Timeout),
                {ok, _} = calculate_cluster_status(Nodes, StateNum, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses)
            catch
                Type:Error ->
                    ?error_stacktrace("Unexpected error during healthcheck: ~p:~p", [Type, Error]),
                    error
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a proper term expressing cluster health,
%% constructing it from statuses of all components.
%% @end
%%--------------------------------------------------------------------
-spec calculate_cluster_status(Nodes :: [Node], StateNum :: integer(), NodeManagerStatuses :: [{Node, Status}],
    DistpatcherStatuses :: [{Node, Status}], WorkerStatuses :: [{Node, [{Worker :: atom(), Status}]}]) -> term()
    when Node :: atom(), Status :: ok | out_of_sync | error.
calculate_cluster_status(Nodes, StateNum, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses) ->
    NodeStatuses =
        lists:map(
            fun(Node) ->
                % Calculate node manager and dispatcher statuses
                % ok - if the state num is the same as in the CCM
                % out_of_sync - if the state num is other than in the CCM
                % error - if the component is unreachable
                NodeManagerStatus =
                    case proplists:get_value(Node, NodeManagerStatuses) of
                        {ok, StateNum} -> ok;
                        {ok, _} -> out_of_sync;
                        _ -> error
                    end,
                RequestDistpatcherStatus =
                    case proplists:get_value(Node, DistpatcherStatuses) of
                        {ok, StateNum} -> ok;
                        {ok, _} -> out_of_sync;
                        _ -> error
                    end,
                % The list for each node contains:
                % node_manager status
                % request_dispatcher status
                % workers' statuses, alphabetically
                ComponentStatuses = [
                    {?NODE_MANAGER_NAME, NodeManagerStatus},
                    {?DISPATCHER_NAME, RequestDistpatcherStatus} |
                    lists:usort(proplists:get_value(Node, WorkerStatuses))
                ],
                % Calculate status of the whole node - it's the same as the worst status of any child
                % ok > out_of_sync > error
                NodeStatus = lists:foldl(
                    fun({_, CurrentStatus}, Acc) ->
                        case {Acc, CurrentStatus} of
                            {ok, Any} -> Any;
                            {out_of_sync, ok} -> out_of_sync;
                            {out_of_sync, Any} -> Any;
                            {error, _} -> error
                        end
                    end, ok, ComponentStatuses),
                {Node, NodeStatus, ComponentStatuses}
            end, Nodes),
    % Calculate status of the whole application - it's the same as the worst status of any node
    % ok > out_of_sync > error
    AppStatus = lists:foldl(
        fun({_, CurrentStatus, _}, Acc) ->
            case {Acc, CurrentStatus} of
                {ok, Any} -> Any;
                {out_of_sync, ok} -> out_of_sync;
                {out_of_sync, Any} -> Any;
                {error, _} -> error
            end
        end, ok, NodeStatuses),
    % Sort node statuses by node name
    {ok, {?APP_NAME, AppStatus, lists:usort(NodeStatuses)}}.


%%--------------------------------------------------------------------
%% @doc
%% Contacts CCM for healthcheck and gathers information about cluster state.
%% @end
%%--------------------------------------------------------------------
-spec check_ccm(Timeout :: integer()) -> {Nodes :: [Node], Workers :: [{Node, WorkerName :: atom()}], StateNum :: integer()} | error
    when Node :: atom().
check_ccm(Timeout) ->
    try
        {ok, {Nodes, Workers, StateNum}} = gen_server:call({global, ?CCM}, healthcheck, Timeout),
        {Nodes, Workers, StateNum}
    catch
        Type:Error ->
            ?error("CCM connection error during healthcheck: ~p:~p", [Type, Error]),
            error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Contacts node managers on given nodes for healthcheck. The check is performed in parallel (one proces per node).
%% @end
%%--------------------------------------------------------------------
-spec check_node_managers(Nodes :: [atom()], Timeout :: integer()) -> [{ok, StateNum :: integer()} | error].
check_node_managers(Nodes, Timeout) ->
    pmap(
        fun(Node) ->
            Result =
                try
                    {ok, _StateNum} = gen_server:call({?NODE_MANAGER_NAME, Node}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?NODE_MANAGER_NAME, Node, T, M]),
                    error
                end,
            {Node, Result}
        end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Contacts request dispatchers on given nodes for healthcheck. The check is performed in parallel (one proces per node).
%% @end
%%--------------------------------------------------------------------
-spec check_dispatchers(Nodes :: [atom()], Timeout :: integer()) -> [{ok, StateNum :: integer()} | error].
check_dispatchers(Nodes, Timeout) ->
    pmap(
        fun(Node) ->
            Result =
                try
                    {ok, _StateNum} = gen_server:call({?DISPATCHER_NAME, Node}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?DISPATCHER_NAME, Node, T, M]),
                    error
                end,
            {Node, Result}
        end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Contacts workers on given nodes for healthcheck. The check is performed in parallel (one proces per worker).
%% Workers are grouped into one list per each node. Nodes without workers will have empty lists.
%% @end
%%--------------------------------------------------------------------
-spec check_workers(Nodes :: [atom()], Workers :: [{Node :: atom(), WorkerName :: atom()}], Timeout :: integer()) ->
    [{Node :: atom(), [{Worker :: atom(), Status :: ok | error}]}].
check_workers(Nodes, Workers, Timeout) ->
    WorkerStatuses = pmap(
        fun({WNode, WName}) ->
            Result =
                try
                    ok = worker_proxy:call({WName, WNode}, healthcheck, Timeout)
                catch T:M ->
                    ?error("Connection error to ~p at ~p: ~p:~p", [?DISPATCHER_NAME, WNode, T, M]),
                    error
                end,
            {WNode, WName, Result}
        end, Workers),

    WorkersByNode = lists:foldl(
        fun({WNode, WName, Status}, Proplist) ->
            NewWorkerList = [{WName, Status} | proplists:get_value(WNode, Proplist, [])],
            [{WNode, NewWorkerList} | proplists:delete(WNode, Proplist)]
        end, [], WorkerStatuses),

    % If a node hosts no workers, it won't be on the WorkersByNode list, so lets add it.
    EmptyNodes = lists:foldl(
        fun(Node, Acc) ->
            case proplists:get_value(Node, WorkersByNode) of
                undefined -> [{Node, []} | Acc];
                _ -> Acc
            end
        end, [], Nodes),
    WorkersByNode ++ EmptyNodes.


%%%====================================================================
%% Paralel Map Function
%%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Works as lists:map/2, but in parallel, more explanation can be found
%% in: http://montsamu.blogspot.com/2007/02/erlang-parallel-map-and-parallel.html
%% @end
%%--------------------------------------------------------------------
-spec pmap(F :: fun((term()) -> term()), L :: list()) -> list().
pmap(F, L) ->
    S = self(),
    Pids = lists:map(fun(I) -> spawn(fun() -> pmap_f(S, F, I) end) end, L),
    pmap_gather(Pids).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather result from spawned process during pmap
%% @end
%%--------------------------------------------------------------------
-spec pmap_gather(List :: list()) -> list().
pmap_gather([H | T]) ->
    receive
        {H, Ret} -> [Ret | pmap_gather(T)]
    end;
pmap_gather([]) ->
    [].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Computes function value and sends result to parent process
%% @end
%%--------------------------------------------------------------------
-spec pmap_f(Parent :: pid(), F :: fun((term()) -> term()), I :: term()) -> term().
pmap_f(Parent, F, I) ->
    Parent ! {self(), (catch F(I))}.