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
-export([new/1, run/4]).

-include("basho_bench.hrl").

%% ====================================================================
%% Test driver callbacks
%% ====================================================================

%% new/1
%% ====================================================================
%% @doc Creates new worker with integer id
-spec new(Id :: integer()) -> Result when
    Result :: {ok, term()} | {error, Reason :: term()}.
%% ====================================================================
new(Id) ->
    try
        ?INFO("Initializing worker with id: ~p~n", [Id]),
        Hosts = basho_bench_config:get(cluster_erlang_nodes),

        H1 = os:cmd("hostname -f"),
        H2 = string:substr(H1, 1, length(H1) - 1),
        {ok, _} = net_kernel:start([list_to_atom("tester@" ++ H2), longnames]),
        true = erlang:set_cookie(node(), veil_cluster_node),

        ?INFO("Worker with id: ~p initialized successfully with arguments: ~p", [Id, Hosts]),
        {ok, Hosts}
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
run(_Operation, KeyGen, _ValueGen, Hosts) ->
    try
        Host = lists:nth((KeyGen() rem length(Hosts)) + 1, Hosts),
        NewState = Hosts,

        case net_adm:ping(Host) of
            pong -> {ok, NewState};
            pang -> {error, pang, NewState}
        end
    catch
        E1:E2 ->
            ?ERROR("Run error: ~p", [{E1, E2}]),
            {error, {E1, E2}, NewState}
    end.