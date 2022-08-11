%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% This file contains tests of dir_stats_collector in single provider environment.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_test_SUITE).
-author("Michal Wrzeszcz").


%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    basic_test/1,
    enabling_for_empty_space_test/1,
    enabling_for_not_empty_space_test/1,
    enabling_large_dirs_test/1,
    enabling_during_writing_test/1,
    race_with_file_adding_test/1,
    race_with_file_writing_test/1,
    race_with_subtree_adding_test/1,
    race_with_subtree_filling_with_data_test/1,
    race_with_file_adding_to_large_dir_test/1,
    multiple_status_change_test/1,
    adding_file_when_disabled_test/1,
    restart_test/1
]).


all() -> [
    basic_test,
    enabling_for_empty_space_test,
    enabling_for_not_empty_space_test,
    enabling_large_dirs_test,
    enabling_during_writing_test,
    race_with_file_adding_test,
    race_with_file_writing_test,
    race_with_subtree_adding_test,
    race_with_subtree_filling_with_data_test,
    race_with_file_adding_to_large_dir_test,
    multiple_status_change_test,
    adding_file_when_disabled_test,
    restart_test
].


%%%====================================================================
%%% Test function
%%%====================================================================

basic_test(Config) ->
    dir_stats_collector_test_base:basic_test(Config).


enabling_for_empty_space_test(Config) ->
    dir_stats_collector_test_base:enabling_for_empty_space_test(Config).


enabling_for_not_empty_space_test(Config) ->
    dir_stats_collector_test_base:enabling_for_not_empty_space_test(Config).


enabling_large_dirs_test(Config) ->
    dir_stats_collector_test_base:enabling_large_dirs_test(Config).


enabling_during_writing_test(Config) ->
    dir_stats_collector_test_base:enabling_during_writing_test(Config).


race_with_file_adding_test(Config) ->
    dir_stats_collector_test_base:race_with_file_adding_test(Config).


race_with_file_writing_test(Config) ->
    dir_stats_collector_test_base:race_with_file_writing_test(Config).


race_with_subtree_adding_test(Config) ->
    dir_stats_collector_test_base:race_with_subtree_adding_test(Config).


race_with_subtree_filling_with_data_test(Config) ->
    dir_stats_collector_test_base:race_with_subtree_filling_with_data_test(Config).


race_with_file_adding_to_large_dir_test(Config) ->
    dir_stats_collector_test_base:race_with_file_adding_to_large_dir_test(Config).


multiple_status_change_test(Config) ->
    dir_stats_collector_test_base:multiple_status_change_test(Config).


adding_file_when_disabled_test(Config) ->
    dir_stats_collector_test_base:adding_file_when_disabled_test(Config).


restart_test(Config) ->
    dir_stats_collector_test_base:restart_test(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    lfm_files_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).


init_per_testcase(Case = basic_test, Config) ->
    dir_stats_collector_test_base:init(lfm_files_test_base:init_per_testcase(Case, Config), true);
init_per_testcase(Case, Config) ->
    dir_stats_collector_test_base:init(lfm_files_test_base:init_per_testcase(Case, Config), false).


end_per_testcase(Case, Config) ->
    dir_stats_collector_test_base:teardown(Config),
    lfm_files_test_base:end_per_testcase(Case, Config).