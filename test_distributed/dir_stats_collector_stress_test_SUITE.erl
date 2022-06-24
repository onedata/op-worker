%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for dir_stats_collector.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_stats_collector_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1, many_files_creation_tree_test/1,
    many_files_creation_tree_test_base/1]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
    many_files_creation_tree_test
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

many_files_creation_tree_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, spawn_beg_level}, {value, 4}, {description, "Level of tree to start spawning processes"}],
            [{name, spawn_end_level}, {value, 5}, {description, "Level of tree to stop spawning processes"}],
            [{name, dir_level}, {value, 6}, {description, "Level of last test directory"}],
            [{name, dirs_per_parent}, {value, 5}, {description, "Child directories in single dir"}],
            [{name, files_per_dir}, {value, 20}, {description, "Number of files in single directory"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process and checks statistics"}
    ]).
many_files_creation_tree_test_base(Config) ->
    case get(stress_phase) of
        undefined ->
            case files_stress_test_base:many_files_creation_tree_test_base(Config, #{cache_guids => true}) of
                [stop | PhaseAns] ->
                    put(stress_phase, verify_stats),
                    PhaseAns;
                Other ->
                    Other
            end;
        verify_stats ->
            [Worker | _] = ?config(op_worker_nodes, Config),
            SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
            {ok, SpaceDirStats} = ?assertMatch({ok, _}, rpc:call(Worker, dir_size_stats, get_stats, [SpaceGuid])),
            ExpectedDirStats = get_expected_stats(Config),
            PhaseAns = files_stress_test_base:get_final_ans_tree(Worker, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            case SpaceDirStats of
                ExpectedDirStats ->
                    ct:print("Space dir stats verified"),
                    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
                    ?assertEqual(ok, rpc:call(Worker, dir_stats_collector_config, disable, [SpaceId])),
                    ?assertEqual(ok, rpc:call(Worker, dir_stats_collector_config, enable, [SpaceId])),
                    put(stress_phase, calculate_stats),
                    PhaseAns;
                _ ->
                    ct:print("Space dir stats: ~p~nExpected: ~p", [SpaceDirStats, ExpectedDirStats]),
                    timer:sleep(5000),
                    PhaseAns
            end;
        calculate_stats ->
            [Worker | _] = ?config(op_worker_nodes, Config),
            SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
            PhaseAns = files_stress_test_base:get_final_ans_tree(Worker, 0, 0, 0, 0, 0, 0, 0, 0, 0),
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
                            [stop | PhaseAns];
                        _ ->
                            ct:print("Collected space dir stats: ~p~nExpected: ~p", [SpaceDirStats, ExpectedDirStats]),
                            timer:sleep(5000),
                            PhaseAns
                    end
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
    [Worker | _] = Workers = ?config(op_worker_nodes, NewConfig),
    SpaceId = lfm_test_utils:get_user1_first_space_id(NewConfig),
    test_utils:set_env(Workers, op_worker, dir_stats_collecting_status_for_new_spaces, enabled),
    ?assertEqual(ok, rpc:call(Worker, space_support_api, init_support_state, [SpaceId, #{}])),
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

    DirLevel = ?config(dir_level, Config),
    DirsPerParent = ?config(dirs_per_parent, Config),
    FilesPerDir = ?config(files_per_dir, Config),
    ExpectedFileCount = round(math:pow(DirsPerParent, DirLevel) * FilesPerDir), % round to change float to int
    ExpectedDirCount = round(lists:sum(lists:map(fun(Level) -> % round to change float to int
        math:pow(DirsPerParent, Level)
    end, lists:seq(1, DirLevel)))),

    #{
        ?REG_FILE_AND_LINK_COUNT => ExpectedFileCount,
        ?DIR_COUNT => ExpectedDirCount,
        ?TOTAL_SIZE => 0,
        ?SIZE_ON_STORAGE(StorageId) => 0
    }.
