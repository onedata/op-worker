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


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    basic_test/1,
    hardlinks_test/1,
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
    restart_test/1,
    local_opened_file_deletion_closing_race/1,
    local_opened_many_files_deletion_closing_race/1
]).


% Spaces created by initializer from the default global setup (this suite's
% env_desc.json does not override it).
-define(SPACE_IDS, [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>]).
-define(SPACE_CLEANING_ATTEMPTS, 30).


all() -> [
    basic_test,
    hardlinks_test,
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
    restart_test,
    local_opened_file_deletion_closing_race,
    local_opened_many_files_deletion_closing_race
].


%%%====================================================================
%%% Test function
%%%====================================================================

basic_test(Config) ->
    dir_stats_collector_test_base:basic_test(Config).


hardlinks_test(Config) ->
    dir_stats_collector_test_base:hardlinks_test(Config).


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


local_opened_file_deletion_closing_race(Config) ->
    dir_stats_collector_test_base:local_opened_file_deletion_closing_race_base(Config, 1).


local_opened_many_files_deletion_closing_race(Config) ->
    dir_stats_collector_test_base:local_opened_file_deletion_closing_race_base(Config, 100).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, dir_stats_collector_test_base]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    initializer:unmock_auth_manager(Config).


init_per_testcase(basic_test, Config) ->
    dir_stats_collector_test_base:init_and_enable_for_new_space(setup_users_and_spaces(Config));
init_per_testcase(hardlinks_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Workers, op_worker, dir_stats_collector_race_preventing_time, 1000),
    dir_stats_collector_test_base:init_and_enable_for_new_space(setup_users_and_spaces(Config));
init_per_testcase(_Case, Config) ->
    dir_stats_collector_test_base:init(setup_users_and_spaces(Config)).


end_per_testcase(hardlinks_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Workers, op_worker, dir_stats_collector_race_preventing_time, 30000),
    dir_stats_collector_test_base:teardown(Config),
    clean_users_and_spaces(Config);
end_per_testcase(_Case, Config) ->
    dir_stats_collector_test_base:teardown(Config),
    clean_users_and_spaces(Config).


%% @private
setup_users_and_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


%% @private
clean_users_and_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        lfm_test_utils:clean_space(Workers, SpaceId, ?SPACE_CLEANING_ATTEMPTS)
    end, ?SPACE_IDS),
    lfm_proxy:teardown(Config),
    lfm_ct:clear_context(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).
