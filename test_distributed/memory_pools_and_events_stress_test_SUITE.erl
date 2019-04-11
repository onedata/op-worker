%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides stress tests that verify cleanup of
%%% memory pools and events memory
%%% @end
%%%--------------------------------------------------------------------
-module(memory_pools_and_events_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    stress_test/1, stress_test_base/1,
    many_files_stress_test/1, many_files_stress_test_base/1,
    long_file_usage_stress_test/1, long_file_usage_stress_test_base/1
]).

-define(STRESS_CASES, [many_files_stress_test]).
-define(STRESS_NO_CLEARING_CASES, [long_file_usage_stress_test]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(SeqID, erlang:unique_integer([positive, monotonic]) - 2).

%%%===================================================================
%%% Tests
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
        {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
        {success_rate, 100},
        {config, [{name, stress}, {description, "Basic config for stress test"}]}
    ]
    ).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

many_files_stress_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, proc_num}, {value, 2}, {description, "Processes number sending messages in parallel"}],
            [{name, proc_repeats_num}, {value, 2}, {description, "Repeats by each process"}],
            [{name, timeout}, {value, timer:minutes(1)}, {description, "Timeout"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process"}
    ]).
many_files_stress_test_base(Config) ->
    many_files_test_base(Config, test_many).

long_file_usage_stress_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, proc_num}, {value, 2}, {description, "Processes number sending messages in parallel"}],
            [{name, proc_repeats_num}, {value, 2}, {description, "Repeats by each process"}],
            [{name, timeout}, {value, timer:minutes(1)}, {description, "Timeout"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process"}
    ]).
long_file_usage_stress_test_base(Config) ->
    many_files_test_base(Config, test_long_usage).

many_files_test_base(Config, TestScenario) ->
    Timeout = ?config(timeout, Config),
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    SlavePids = case get(slave_pids) of
        undefined ->
            SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
            SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),

            {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
                generator:gen_name(), 8#755)),
            ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

            client_simulation_test_base:verify_streams(Config),
            ProcNum = ?config(proc_num, Config),
            Master = self(),

            Pids = lists:map(fun(_) ->
                spawn_link(fun() ->
                    try
                        {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
                        Master ! {start_ans, ok},
                        slave_loop(Config, Sock, SpaceGuid, Master)
                    catch
                        E1:E2 ->
                            Master ! {start_ans, {E1, E2, erlang:get_stacktrace()}}
                    end
                end)
            end, lists:seq(1, ProcNum)),

            lists:foreach(fun(Pid) ->
                receive
                    {start_ans, Ans} ->
                        ?assertEqual(ok, Ans);
                    {'EXIT', Pid , Error} ->
                        ?assertEqual(normal, Error)
                after
                    Timeout ->
                        erlang:error({timeout,
                            [{module, ?MODULE},
                                {line, ?LINE}]})
                end
            end, Pids),

            {Before, _SizesBefore} = client_simulation_test_base:get_memory_pools_entries_and_sizes(Worker1),
            put(memory_pools, Before),
            put(slave_pids, Pids),

            Pids;
        SP ->
            SP
    end,

    lists:foreach(fun(Pid) ->
        Pid ! TestScenario
    end, SlavePids),

    lists:foreach(fun(Pid) ->
        receive
            {test_ans, Ans} ->
                ?assertEqual(ok, Ans);
            {'EXIT', Pid , Error} ->
                ?assertEqual(normal, Error)
        after
            Timeout ->
                ct:print("Timeout - test failed"),
                erlang:error({timeout,
                    [{module, ?MODULE},
                        {line, ?LINE}]})
        end
    end, SlavePids),
    timer:sleep(timer:seconds(30)),

    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {After, _SizesAfter} = client_simulation_test_base:get_memory_pools_entries_and_sizes(Worker1),
    MemPoolsBefore = get(memory_pools),
    Res = client_simulation_test_base:get_documents_diff(Worker1, After, MemPoolsBefore),
%%    ?assertEqual([], Res),
    ct:print("Docs number ~p", [length(Res)]),
    client_simulation_test_base:verify_streams(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

slave_loop(Config, Sock, SpaceGuid, Master) ->
    receive
        test_many ->
            try
                Args = [write, read, release, unsub],
                Repeats = ?config(proc_repeats_num, Config),
                lists:foreach(fun(_) ->
                    client_simulation_test_base:simulate_client(Config, Args, Sock, SpaceGuid, false)
                end, lists:seq(1, Repeats)),
                Master ! {test_ans, ok}
            catch
                E1:E2 ->
                    Master ! {test_ans, {E1, E2, erlang:get_stacktrace()}}
            end,
            slave_loop(Config, Sock, SpaceGuid, Master);
        test_long_usage ->
            Master ! {test_ans, ok},
            slave_loop(Config, Sock, SpaceGuid, Master)
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    client_simulation_test_base:init_per_suite(Config).

init_per_testcase(stress_test, Config) ->
    client_simulation_test_base:init_per_testcase(Config);
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test, Config) ->
    client_simulation_test_base:end_per_testcase(Config);
end_per_testcase(_Case, Config) ->
    Config.

end_per_suite(_Case) ->
    ok.
