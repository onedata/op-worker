%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for dir_stats_collector in multiprovider environment.
%%% @end
%%%--------------------------------------------------------------------
-module(large_dir_stats_multiprovider_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1, single_large_dir_creation_test/1,
    single_large_dir_creation_test_base/1]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
    single_large_dir_creation_test
]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
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

single_large_dir_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, files_num}, {value, 10000}, {description, "Numer of files in dir"}],
            [{name, proc_num}, {value, 20}, {description, "Number of precesses that create files"}],
            [{name, reps_num}, {value, 200}, {description, "Number of test function repeats"}],
            [{name, test_list}, {value, false}, {description, "Measure ls time after every 20000 files creation"}]
        ]},
        {description, "Creates files in dir using multiple processes and calculates stats for such large dir at the end"}
    ]).
single_large_dir_creation_test_base(Config) ->
    case get(stress_phase) of
        undefined ->
            Ans = files_stress_test_base:single_dir_creation_test_base(Config, false),
            RepsNum = ?config(reps_num, Config),
            case ?config(rep_num, Config) >= RepsNum of
                true ->
                    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
                    Provider1Id = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
                    Provider2Id = rpc:call(Worker2, oneprovider, get_id_or_undefined, []),
                    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),

                    ?assertEqual(ok, rpc:call(Worker1, dbsync_worker, reset_provider_stream, [SpaceId, Provider2Id])),
                    ?assertEqual(ok, rpc:call(Worker2, dbsync_worker, reset_provider_stream, [SpaceId, Provider1Id])),

                    ?assertEqual(ok, rpc:call(Worker1, dir_stats_service_state, enable, [SpaceId])),
                    put(stress_phase, calculate_stats);
                false ->
                    ok
            end,
            Ans;
        calculate_stats ->
            [Worker | _] = ?config(op_worker_nodes, Config),
            SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
            PhaseAns = files_stress_test_base:get_final_ans(0, 0, 0, 0, 0, 0, 0, 0, 0),
            case rpc:call(Worker, dir_size_stats, get_stats, [SpaceGuid]) of
                ?ERROR_DIR_STATS_NOT_READY ->
                    ct:print("Initializing stats collections"),
                    timer:sleep(5000),
                    PhaseAns;
                ?ERROR_DIR_STATS_DISABLED_FOR_SPACE ->
                    ct:print("Initializing stats collections"),
                    timer:sleep(5000),
                    PhaseAns;
                {ok, SpaceDirStats} ->
                    ExpectedDirStats = get_expected_stats(Config),
                    case SpaceDirStats of
                        ExpectedDirStats ->
                            ct:print("Collected space dir stats: ~tp", [SpaceDirStats]),
                            put(stress_phase, check_synchronization_params),
                            PhaseAns;
                        _ ->
                            ct:print("Collected space dir stats: ~tp~nExpected: ~tp", [SpaceDirStats, ExpectedDirStats]),
                            timer:sleep(5000),
                            PhaseAns
                    end
            end;
        check_synchronization_params ->
            [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
            Provider1Id = rpc:call(Worker1, oneprovider, get_id_or_undefined, []),
            Provider2Id = rpc:call(Worker2, oneprovider, get_id_or_undefined, []),
            SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
            PhaseAns = files_stress_test_base:get_final_ans(0, 0, 0, 0, 0, 0, 0, 0, 0),

            Params1 = rpc:call(Worker1, dbsync_state, get_synchronization_params, [SpaceId, Provider2Id]),
            Params2 = rpc:call(Worker2, dbsync_state, get_synchronization_params, [SpaceId, Provider1Id]),

            case {Params1, Params2} of
                {undefined, undefined} ->
                    ct:print("Resynchronization finished"),
                    [stop | PhaseAns];
                _ ->
                    ct:print("Resynchronization worker1: ~tp, worker2: ~tp", [Params1, Params2]),
                    timer:sleep(5000),
                    PhaseAns
            end
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    files_stress_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    files_stress_test_base:end_per_suite(Config).

init_per_testcase(stress_test = Case, Config) ->
    NewConfig = files_stress_test_base:init_per_testcase(Case, Config),
    [Worker | _] = ?config(op_worker_nodes, NewConfig),
    SpaceId = lfm_test_utils:get_user1_first_space_id(NewConfig),
    ?assertEqual(ok, rpc:call(Worker, dir_stats_service_state, enable, [SpaceId])),
    NewConfig;
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test = Case, Config) ->
    files_stress_test_base:end_per_testcase(Case, Config);
end_per_testcase(_Case, Config) ->
    Config.

%%%===================================================================
%%% Helper functions
%%%===================================================================

get_expected_stats(Config) ->
    StorageId = lfm_test_utils:get_user1_first_storage_id(Config, op_worker_nodes),
    RepsNum = ?config(reps_num, Config),
    FilesNum = ?config(files_num, Config),

    #{
        ?REG_FILE_AND_LINK_COUNT => FilesNum * RepsNum,
        ?DIR_COUNT => get(dirs_created),
        ?VIRTUAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?PHYSICAL_SIZE(StorageId) => 0,
        ?FILE_ERROR_COUNT => 0,
        ?DIR_ERROR_COUNT => 0
    }.