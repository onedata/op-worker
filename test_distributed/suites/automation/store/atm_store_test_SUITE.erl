%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of all the automation store types, exercised directly through
%%% 'atm_store_api' - creation, content updates, browsing and iteration
%%% over the matrix of item data types each store accepts.
%%%
%%% The suite covers one store type per pair of groups: the shared contract
%%% its backend imposes ('atm_store_infinite_log_based_tests' or
%%% 'atm_store_singleton_content_based_tests') and whatever the store adds on
%%% top of it. The per-store parametrisation of the shared contract, and the
%%% bodies of the store specific cases, live in the 'atm_store_*_tests'
%%% modules; 'init_per_group' picks the one a group runs against, so the very
%%% same case function serves every store type.
%%%
%%% All the store types are gathered in a single suite because they share one
%%% fixture - the '1op' deployment - and the whole family takes under two
%%% minutes to run, far less than standing that deployment up.
%%%
%%% Deliberately NOT covered here: how the stores behave as part of a running
%%% workflow execution - being iterated over to feed tasks, or receiving
%%% mapped task results - which belongs to 'atm_workflow_execution_test_SUITE';
%%% and the validation and conversion rules of the data types themselves,
%%% which belong to 'atm_value_test_SUITE'.
%%%
%%% Every case builds its own stores on a synthetic workflow execution auth,
%%% so cases share nothing but the deployment and may run in parallel. Cases
%%% that move the frozen clock are kept in sequential groups, because the
%%% freeze is global to the provider.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_test_SUITE).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_test/1,
    update_content_test/1,
    iterator_test/1,
    browse_content_test/1,
    browse_content_by_index_test/1,
    browse_content_by_offset_test/1,

    browse_by_index_test/1,
    browse_by_offset_test/1,
    browse_by_timestamp_test/1,
    expiration_test/1,
    logging_level_test/1,

    find_indices_by_trace_ids_test/1,

    iterate_in_chunks_5_with_start_10_end_50_step_2_test/1,
    iterate_in_chunks_10_with_start_1_end_2_step_10_test/1,
    iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test/1,
    iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test/1,
    iterate_in_chunks_3_with_start_10_end_10_step_2_test/1,
    reuse_iterator_test/1,

    copy_test/1,
    manage_content_test/1,
    not_supported_iteration_test/1,

    iterate_files_test/1,
    iterate_files_small_batch_test/1,
    iterate_datasets_test/1,
    iterate_datasets_small_batch_test/1,
    restart_iteration_test/1,
    restart_partial_iteration_test/1,
    iteration_with_deleted_root_test/1,
    iteration_after_restart_with_deleted_root_test/1,
    iteration_after_restart_with_new_dirs_root_test/1,
    iteration_without_permission_test/1
]).

groups() -> [
    {audit_log_infinite_log_based_tests, [parallel], [
        create_test,
        update_content_test,
        iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]},
    {audit_log_specific_tests, [sequential], [
        browse_by_timestamp_test,
        expiration_test,
        logging_level_test
    ]},

    {exception_infinite_log_based_tests, [parallel], [
        create_test,
        update_content_test,
        iterator_test,
        browse_content_by_index_test,
        browse_content_by_offset_test
    ]},
    {exception_specific_tests, [sequential], [
        find_indices_by_trace_ids_test
    ]},

    {list_infinite_log_based_tests, [parallel], [
        create_test,
        update_content_test,
        iterator_test,
        browse_content_by_index_test,
        browse_content_by_offset_test
    ]},

    {range_singleton_content_based_tests, [parallel], [
        create_test,
        update_content_test,
        browse_content_test
    ]},
    {range_specific_tests, [parallel], [
        iterate_in_chunks_5_with_start_10_end_50_step_2_test,
        iterate_in_chunks_10_with_start_1_end_2_step_10_test,
        iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test,
        iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test,
        iterate_in_chunks_3_with_start_10_end_10_step_2_test,
        reuse_iterator_test
    ]},

    {single_value_singleton_content_based_tests, [parallel], [
        create_test,
        update_content_test,
        browse_content_test
    ]},
    {single_value_specific_tests, [parallel], [
        iterator_test
    ]},

    {time_series_specific_tests, [parallel], [
        create_test,
        copy_test,
        manage_content_test,
        not_supported_iteration_test
    ]},

    {tree_forest_infinite_log_based_tests, [parallel], [
        create_test,
        update_content_test,
        browse_content_by_index_test,
        browse_content_by_offset_test
    ]},
    {tree_forest_specific_tests, [parallel], [
        iterate_files_test,
        iterate_files_small_batch_test,
        iterate_datasets_test,
        iterate_datasets_small_batch_test,
        restart_iteration_test,
        restart_partial_iteration_test,
        iteration_with_deleted_root_test,
        iteration_after_restart_with_deleted_root_test,
        iteration_after_restart_with_new_dirs_root_test,
        iteration_without_permission_test
    ]}
].

all() -> [
    {group, audit_log_infinite_log_based_tests},
    {group, audit_log_specific_tests},

    {group, exception_infinite_log_based_tests},
    {group, exception_specific_tests},

    {group, list_infinite_log_based_tests},

    {group, range_singleton_content_based_tests},
    {group, range_specific_tests},

    {group, single_value_singleton_content_based_tests},
    {group, single_value_specific_tests},

    {group, time_series_specific_tests},

    {group, tree_forest_infinite_log_based_tests},
    {group, tree_forest_specific_tests}
].


-define(STORE_TESTS_MODULE, store_tests_module).


%%%===================================================================
%%% Test cases
%%%
%%% Each case merely names the behaviour to exercise - which store type it is
%%% exercised on follows from the group it runs in.
%%%===================================================================


create_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


update_content_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterator_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_content_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_content_by_index_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_content_by_offset_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_by_index_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_by_offset_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


browse_by_timestamp_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


expiration_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


logging_level_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


find_indices_by_trace_ids_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_in_chunks_5_with_start_10_end_50_step_2_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_in_chunks_10_with_start_1_end_2_step_10_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_in_chunks_3_with_start_10_end_10_step_2_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


reuse_iterator_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


copy_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


manage_content_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


not_supported_iteration_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_files_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_files_small_batch_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_datasets_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iterate_datasets_small_batch_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


restart_iteration_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


restart_partial_iteration_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iteration_with_deleted_root_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iteration_after_restart_with_deleted_root_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iteration_after_restart_with_new_dirs_root_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


iteration_without_permission_test(Config) ->
    run_case(Config, ?FUNCTION_NAME).


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec run_case(test_config:config(), atom()) -> ok | no_return().
run_case(Config, Case) ->
    Module = ?config(?STORE_TESTS_MODULE, Config),
    Module:Case().


%% @private
%% @doc
%% The store parametrisation a group runs against, and whether it needs the
%% clock frozen. Kept as one table so that adding a group forces a decision
%% on both.
%% @end
-spec group_spec(atom()) -> {module(), frozen_clock | real_clock}.
group_spec(audit_log_infinite_log_based_tests) ->
    {atm_store_audit_log_tests, frozen_clock};
group_spec(audit_log_specific_tests) ->
    {atm_store_audit_log_tests, frozen_clock};
group_spec(exception_infinite_log_based_tests) ->
    {atm_store_exception_tests, frozen_clock};
group_spec(exception_specific_tests) ->
    {atm_store_exception_tests, frozen_clock};
group_spec(list_infinite_log_based_tests) ->
    {atm_store_list_tests, frozen_clock};
group_spec(range_singleton_content_based_tests) ->
    {atm_store_range_tests, frozen_clock};
group_spec(range_specific_tests) ->
    {atm_store_range_tests, real_clock};
group_spec(single_value_singleton_content_based_tests) ->
    {atm_store_single_value_tests, frozen_clock};
group_spec(single_value_specific_tests) ->
    {atm_store_single_value_tests, frozen_clock};
group_spec(time_series_specific_tests) ->
    {atm_store_time_series_tests, real_clock};
group_spec(tree_forest_infinite_log_based_tests) ->
    {atm_store_tree_forest_tests, frozen_clock};
group_spec(tree_forest_specific_tests) ->
    {atm_store_tree_forest_tests, real_clock}.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE,
        atm_store_test_utils,
        atm_store_infinite_log_based_tests,
        atm_store_singleton_content_based_tests,
        atm_store_audit_log_tests,
        atm_store_exception_tests,
        atm_store_list_tests,
        atm_store_range_tests,
        atm_store_single_value_tests,
        atm_store_time_series_tests,
        atm_store_tree_forest_tests
    ],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(Group, Config) ->
    {Module, ClockMode} = group_spec(Group),

    % The clock freeze is global to the provider, so a group must put the clock
    % into the state it needs rather than trust what the previous one left
    % behind. Both calls are idempotent - mock_manager unloads any previous
    % mock of 'native_node_clock' before setting up its own - so this needs no
    % guard on the current state.
    case ClockMode of
        frozen_clock -> time_test_utils:freeze_time(Config);
        real_clock -> time_test_utils:unfreeze_time(Config)
    end,

    set_up_group(Group, [{?STORE_TESTS_MODULE, Module} | Config]).


end_per_group(Group, Config) ->
    tear_down_group(Group, Config),
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.


%% @private
-spec set_up_group(atom(), test_config:config()) -> test_config:config().
set_up_group(tree_forest_specific_tests, Config) ->
    lfm_proxy:init(Config, false);
set_up_group(_Group, Config) ->
    Config.


%% @private
-spec tear_down_group(atom(), test_config:config()) -> ok.
tear_down_group(tree_forest_specific_tests, Config) ->
    lfm_proxy:teardown(Config);
tear_down_group(_Group, _Config) ->
    ok.
