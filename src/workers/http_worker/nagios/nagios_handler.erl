%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles Nagios monitoring requests.
%% @end
%% ===================================================================
-module(nagios_handler).
-behaviour(cowboy_http_handler).

-include("registered_names.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/3, handle/2, terminate/3]).
-export([get_cluster_state/1]).


%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, no state is required
-spec init(any(), term(), any()) -> {ok, term(), []}.
%% ====================================================================
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing an XML response
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->
    {ok, Timeout} = application:get_env(?APP_NAME, nagios_healthcheck_timeout),

    case get_cluster_state(Timeout) of
        error ->
            opn_cowboy_bridge:apply(cowboy_req, reply, [500, Req]);
        {ok, ClusterState} ->
            ?dump(ClusterState)
    end,

%%
%%     %check if errors occured
%%     {HealthStatus, HttpStatusCode} =
%%         case CcmConnError /= none of
%%             true ->
%%                 ErrorString = io_lib:format("~p", [{error, CcmConnError}]),
%%                 {ErrorString, 500};
%%             false ->
%%                 case contains_errors(NodesStatus) or contains_errors(WorkersStatus) of
%%                     true -> {"{error,unhealthy_member}", 500};
%%                     false -> {"ok", 200}
%%                 end
%%         end,
%%
%%     %prepare current date
%%     {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
%%     DateString = io_lib:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),
%%
%%     %parse reply
%%     Healthdata = {healthdata, [{date, DateString}, {status, HealthStatus}], NodesStatus ++ WorkersStatus},
%%     Content = lists:flatten([Healthdata]),
%%     Export = xmerl:export_simple(Content, xmerl_xml),
%%     Reply = io_lib:format("~s", [lists:flatten(Export)]),
    {ok, Req2} = opn_cowboy_bridge:apply(cowboy_req, reply, [200, [{<<"content-type">>, <<"application/json">>}], <<"juhuhu">>, Req]),
    {ok, Req2, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% Internal Functions
%% ====================================================================

%% get_cluster_state/1
%% ====================================================================
%% @doc
%% Get from ccm: nodes, workers, state number, callbak state number,
%% error (if error occured, 'none' otherwise)
%% @end
-spec get_cluster_state(Timeout :: integer()) -> term().
%% ====================================================================
get_cluster_state(Timeout) ->
    try
        {Nodes, Workers, StateNum} = gen_server:call({global, ?CCM}, healthcheck, Timeout),

        WorkerStatuses = pmap(
            fun({WNode, WName}) ->
                worker_proxy:call({WName, WNode}, healthcheck, Timeout)
            end, Workers),

        NodeManagerStatuses = pmap(
            fun(Node) ->
                gen_server:call({?NODE_MANAGER_NAME, Node}, healthcheck, timeout)
            end, Nodes),

        RequestDistpatcherStatuses = pmap(
            fun(Node) ->
                gen_server:call({?DISPATCHER_NAME, Node}, healthcheck, timeout)
            end, Nodes),
        ClusterState = {WorkerStatuses,NodeManagerStatuses,RequestDistpatcherStatuses},
%%         ClusterState =
%%             pmap(fun(Node) ->
%%                 NodeManagerStatus =
%%                     case gen_server:call({?NODE_MANAGER_NAME, Node}, healthcheck, timeout) of
%%                         {ok, StateNum} -> ok;
%%                         {ok, _} -> out_of_sync;
%%                         _ -> error
%%                     end,
%%                 RequestDistpatcherStatus =
%%                     case gen_server:call({?DISPATCHER_NAME, Node}, healthcheck, timeout) of
%%                         {ok, StateNum} -> ok;
%%                         {ok, _} -> out_of_sync;
%%                         _ -> error
%%                     end,
%%                 WorkerStatuses = [{'worker@127.0.0.1', dns_worker}, {'worker@127.0.0.1', http_worker}],
%%                 {Node, [
%%                     {node_manager, NodeManagerStatus},
%%                     {request_dispatcher, RequestDistpatcherStatus} |
%%                     WorkerStatuses
%%                 ]}
%%             end, Nodes),

%%         WorkersStatus = pmap(fun(Worker) -> worker_status(Worker, Timeout) end, Workers),
%%         NodesStatus = pmap(fun(Node) -> node_status(Node, StateNum, Timeout) end, Nodes)
        {ok, ClusterState}
    catch
        Type:Error ->
            ?error("CCM connection error during healthcheck: ~p:~p", [Type, Error]),
            error
    end.

%% node_status/5
%% ====================================================================
%% @doc Checks if callbacks num and state num on dispatcher and node manager are same as in ccm,
%% returns xmerl simple_xml output describing node health status. If state numbers don't match - assume "initializing"
%% @end
-spec node_status(Node :: atom(), CcmStateNum :: integer(), CcmCStateNum :: integer(), Timeout :: integer()) -> Result when
    Result :: {?APP_NAME, Attrs :: list(Atribute), []},
    Atribute :: {Name :: atom(), Value :: string()}.
%% ====================================================================
node_status(Node, CcmStateNum, CcmCStateNum, Timeout) ->
    ?debug("Healthcheck on node:~p", [Node]),
    try
        %get state nuber and callback number from node manager and dispatcher
        {_, DispCStateNum} = gen_server:call({?DISPATCHER_NAME, Node}, get_callbacks, Timeout),
        DispStateNum = gen_server:call({?DISPATCHER_NAME, Node}, get_state_num, Timeout),
        {ManagerCStateNum, ManagerStateNum} = gen_server:call({?NODE_MANAGER_NAME, Node}, get_callback_and_state_num, Timeout),

        %compare them with numbers from ccm and prepare result
        AllStateNumbersOk = (CcmStateNum == DispStateNum) and (CcmCStateNum == DispCStateNum) and (CcmStateNum == ManagerStateNum) and (CcmCStateNum == ManagerCStateNum),
        case AllStateNumbersOk of
            true ->
                {?APP_NAME, [{name, atom_to_list(Node)}, {status, "ok"}], []};
            false ->
                %log
                ?warning("Healthcheck on node ~p, callbacks/state number of ccm doesn't match values from node_manager", [Node]),
                ?warning("ccm_state_num: ~p, ccm_callback_num: ~p,disp_state_num: ~p, disp_callback_num: ~p,manager_state_num: ~p, manager_callback_num: ~p",
                    [CcmStateNum, CcmCStateNum, DispStateNum, DispCStateNum, ManagerStateNum, ManagerCStateNum]),
                %return
                {?APP_NAME, [{name, atom_to_list(Node)}, {status, "out_of_sync"}], []}
        end
    catch
        Type:Error ->
            ?error("Node ~p connection error: ~p:~p", [Node, Type, Error]),
            ErrorString2 = io_lib:format("~p", [{error, Error}]),
            {?APP_NAME, [{name, atom_to_list(Node)}, {status, ErrorString2}], []}
    end.

%% worker_status/2
%% ====================================================================
%% @doc Calls healthcheck method on selected worker and returns xmerl
%% simple_xml output describing worker health status
%% @end
-spec worker_status(Worker, Timeout :: integer()) -> Result when
    Worker :: {WorkerNode :: atom(), WorkerName :: atom()},
    Result :: {worker, Attrs :: list(Atribute), []},
    Atribute :: {Name :: atom(), Value :: string()}.
%% ====================================================================
worker_status(Worker, Timeout) ->
    ?debug("Healthcheck on worker: ~p", [Worker]),
    {WorkerNode, WorkerName} = Worker,
    NameString = atom_to_list(WorkerName),
    NodeString = atom_to_list(WorkerNode),
    try
        %do healthcheck
        gen_server:call({?DISPATCHER_NAME, WorkerNode}, {WorkerName, 1, self(), healthcheck}, Timeout),
        Ans = receive
                  Any -> Any
              after Timeout ->
                  {error, worker_response_timeout}
              end,

        %check healthcheck answer and prepare result
        case Ans of
            ok ->
                {worker, [{name, NameString}, {node, NodeString}, {status, "ok"}], []};
            ErrorAns ->
                ?error("Healthcheck on worker ~p failed with error: ~p", [Worker, ErrorAns]),
                ErrorAnsString = io_lib:format("~p", [ErrorAns]),
                {worker, [{name, NameString}, {node, NodeString}, {status, ErrorAnsString}], []}
        end
    catch
        Type:Error ->
            ?error("Worker ~p connection error: ~p:~p", [Worker, Type, Error]),
            ErrorString = io_lib:format("~p", [{error, Error}]),
            {worker, [{name, NameString}, {node, NodeString}, {status, ErrorString}], []}
    end.

%% contains_errors/1
%% ====================================================================
%% @doc Checks if given list of health statuses (in xmerl simple_xml format)
%% contains error status
%% @end
-spec contains_errors(StatusList :: list(Status)) -> Result when
    Status :: {Tag :: atom(), Attrs :: list(Atribute), Content :: list()},
    Atribute :: {Name :: atom(), Value :: string()},
    Result :: true | false.
%% ====================================================================
contains_errors(StatusList) ->
    Errors = [Status || {_Tag, Attrs, _Content} <- StatusList, {status, Status} <- Attrs, Status /= "ok", Status /= "out_of_sync"],
    Errors /= [].


%% ====================================================================
%% Paralel Map Function
%% ====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Works as lists:map/2, but in paralel, more explanation can be found
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