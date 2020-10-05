%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains stress test of harvesting.
%%% In this test, harvesting_stream is "up to date" with the newest
%%% changes. In each repetition it processes the same number of files.
%%% @end
%%%--------------------------------------------------------------------
-module(continuous_harvesting_stress_test_SUITE).
-author("Jakub Kudzia").

-include("harvesting_stress_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).
-export([stress_test/1, stress_test_base/1, continuous_harvesting_test/1,
    continuous_harvesting_test_base/1]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [continuous_harvesting_test]).


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

continuous_harvesting_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, spawn_beg_level}, {value, 4}, {description, "Level of tree to start spawning processes"}],
            [{name, spawn_end_level}, {value, 5}, {description, "Level of tree to stop spawning processes"}],
            [{name, dir_level}, {value, 6}, {description, "Level of last test directory"}],
            [{name, dirs_per_parent}, {value, 6}, {description, "Child directories in single dir"}],
            [{name, files_per_dir}, {value, 40}, {description, "Number of files in single directory"}]
        ]},
        {description, "Harvests the same number of changes in each test repetition."}
    ]).
continuous_harvesting_test_base(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = files_stress_test_base:many_files_creation_tree_test_base(Config, false, true, true),
    RepNum = ?config(rep_num, Config),
    NewFiles = files_stress_test_base:get_param_value(files_saved, Result),
    NewDirs = files_stress_test_base:get_param_value(dirs_saved, Result),
    BaseDirs = files_stress_test_base:get_param_value(base_dirs_created, Result),
    AllFiles = case RepNum =:= 1 of
        true ->
            NewFiles + NewDirs + BaseDirs + 1;
        false ->
            NewFiles + NewDirs + BaseDirs
    end,
    Start = time_utils:timestamp_millis(),
    % start harvesting_stream
    harvesting_stress_test_utils:revise_all_spaces(Worker),
    harvesting_stress_test_utils:harvesting_receive_loop(AllFiles),
    Diff = time_utils:timestamp_millis() - Start,
    DiffSec = Diff/1000,
    AvgRate =  AllFiles /DiffSec,
    ct:print("Harvesting ~p files took ~p s.~n"
    "Average rate was ~p files per second.", [AllFiles, DiffSec, AvgRate]),
    [
        #parameter{name = total_time, description = "Total harvesting time", value = DiffSec},
        #parameter{name = avg_rate, description = "Average harvesting rate", value = AvgRate}
    ].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, ?MODULE, harvesting_stress_test_utils]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case = stress_test, Config) ->
    files_stress_test_base:init_per_testcase(Case, Config);
init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    harvesting_stress_test_utils:mock_harvesting(Worker),
    Config.

end_per_testcase(Case = stress_test, Config) ->
    files_stress_test_base:end_per_testcase(Case, Config);
end_per_testcase(Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    % stop harvesting_stream
    harvesting_stress_test_utils:mock_harvesting_stopped(Worker),
    harvesting_stress_test_utils:revise_all_spaces(Worker),
    ?assertMatch(0, harvesting_stress_test_utils:count_active_children(Worker, harvesting_stream_sup), 30),
    files_stress_test_base:end_per_testcase(Case, Config).