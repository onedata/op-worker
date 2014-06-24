%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received.
%% It uses very fast Erlang ping so it measures the limits of the infrastructure.
%% @end
%% ===================================================================

-module(veilcluster_driver_limit).
-export([setup/0, new/1, run/4]).

-include("basho_bench.hrl").

%% ====================================================================
%% Test driver callbacks
%% ====================================================================

%% setup/0
%% ====================================================================
%% @doc Runs once per each test node at begging of a test (before any new/1 is called)
-spec setup() -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
setup() ->
    try
        ?INFO("Test setup~n", []),

        H1 = os:cmd("hostname -f"),
        H2 = string:substr(H1, 1, length(H1) - 1),
        case net_kernel:start([list_to_atom("tester@" ++ H2), longnames]) of
            {ok, _} -> ok;
            {error, {already_started, _}} -> ok;
            Other -> throw(Other)
        end,
        true = erlang:set_cookie(node(), veil_cluster_node),
        ok
    catch
        E1:E2 ->
            ?ERROR("Setup error: ~p~n", [{E1, E2}]),
            {error, {E1, E2}}
    end.

%% new/1
%% ====================================================================
%% @doc Creates new worker with integer id
-spec new(Id :: integer()) -> Result when
    Result :: {ok, term()} | {error, Reason :: term()}.
%% ====================================================================
new(Id) ->
    try
        ?INFO("Initializing worker with id: ~p~n", [Id]),
        Hosts = basho_bench_config:get(cluster_hosts),
        Nodes = lists:map(fun(Host) ->
            st_utils:host_to_node(Host)
        end, Hosts),

        ?INFO("Worker with id: ~p initialized successfully with arguments: ~p", [Id, Nodes]),
        {ok, Nodes}
    catch
        E1:E2 ->
            ?ERROR("Initialization error for worker with id: ~p: ~p", [Id, {E1, E2}]),
            {error, {E1, E2}}
    end.


%% run/4
%% ====================================================================
%% @doc Runs an operation using one of workers
-spec run(Operation :: atom(), KeyGen :: fun(), ValueGen :: fun(), State :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term(), NewState :: term()}.
%% ====================================================================
run(_Operation, KeyGen, _ValueGen, Nodes) ->
    NewState = Nodes,
    try
        Node = lists:nth((KeyGen() rem length(Nodes)) + 1, Nodes),

        case net_adm:ping(Node) of
            pong -> {ok, NewState};
            pang -> {error, pang, NewState}
        end
    catch
        E1:E2 ->
            ?ERROR("Run error: ~p", [{E1, E2}]),
            {error, {E1, E2}, NewState}
    end.