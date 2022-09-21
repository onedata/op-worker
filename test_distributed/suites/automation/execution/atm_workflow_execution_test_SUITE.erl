%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation workflow execution machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_SUITE).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include_lib("ctool/include/privileges.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    schedule_atm_workflow_with_no_lanes/1,
    schedule_atm_workflow_with_empty_lane/1,
    schedule_atm_workflow_with_empty_parallel_box/1,
    schedule_incompatible_atm_workflow/1,
    schedule_atm_workflow_with_openfaas_not_configured/1,

    schedule_atm_workflow_with_invalid_initial_store_content/1,

    first_lane_run_preparation_failure_before_run_was_created/1,
    first_lane_run_preparation_failure_after_run_was_created/1,

    atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created/1,
    atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created/1,
    atm_workflow_execution_cancel_before_lane_run_preparation_failed/1,
    atm_workflow_execution_cancel_in_stopping_status_after_lane_run_preparation_failed/1,

    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4/1,

    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4/1,

    cancel_scheduled_atm_workflow_execution/1,
    cancel_enqueued_atm_workflow_execution/1,
    cancel_active_atm_workflow_execution/1,
    cancel_finishing_atm_workflow_execution/1,
    cancel_finished_atm_workflow_execution/1,

    iterate_over_list_store/1,
    iterate_over_list_store_with_some_inaccessible_items/1,
    iterate_over_list_store_with_all_items_inaccessible/1,
    iterate_over_empty_list_store/1,

    iterate_over_range_store/1,
    iterate_over_empty_range_store/1,

    iterate_over_single_value_store/1,
    iterate_over_single_value_store_with_all_items_inaccessible/1,
    iterate_over_empty_single_value_store/1,

    iterate_over_tree_forest_store/1,
    iterate_over_tree_forest_store_with_some_inaccessible_items/1,
    iterate_over_tree_forest_store_with_all_items_inaccessible/1,
    iterate_over_empty_tree_forest_store/1,

    map_results_to_audit_log_store/1,
    map_results_to_list_store/1,
    map_results_to_range_store/1,
    map_results_to_single_value_store/1,
    map_results_to_time_series_store/1,
    map_results_to_tree_forest_store/1,

    map_results_to_workflow_audit_log_store/1,
    map_results_to_task_audit_log_store/1,
    map_results_to_task_time_series_store/1,

    fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error/1,
    fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error/1,
    fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error/1,
    fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error/1,
    fail_atm_workflow_execution_due_to_job_result_store_mapping_error/1,
    fail_atm_workflow_execution_due_to_job_missing_required_results_error/1,
    fail_atm_workflow_execution_due_to_incorrect_result_type_error/1,
    fail_atm_workflow_execution_due_to_lambda_exception/1,
    fail_atm_workflow_execution_due_to_lambda_error/1,

    repeat_not_ended_atm_workflow_execution/1,
    repeat_finished_atm_lane_run_execution/1
]).

groups() -> [
    {scheduling_non_executable_workflow_schema_tests, [], [
        schedule_atm_workflow_with_no_lanes,
        schedule_atm_workflow_with_empty_lane,
        schedule_atm_workflow_with_empty_parallel_box,
        schedule_incompatible_atm_workflow,
        schedule_atm_workflow_with_openfaas_not_configured
    ]},
    {scheduling_executable_workflow_schema_with_invalid_args_tests, [], [
        schedule_atm_workflow_with_invalid_initial_store_content
    ]},
    {preparation_tests, [], [
        first_lane_run_preparation_failure_before_run_was_created,
        first_lane_run_preparation_failure_after_run_was_created,

        atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created,
        atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created,
        atm_workflow_execution_cancel_before_lane_run_preparation_failed,
        atm_workflow_execution_cancel_in_stopping_status_after_lane_run_preparation_failed,

        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4,

        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4
    ]},
    {cancellation_tests, [], [
        cancel_scheduled_atm_workflow_execution,
        cancel_enqueued_atm_workflow_execution,
        cancel_active_atm_workflow_execution,
        cancel_finishing_atm_workflow_execution,
        cancel_finished_atm_workflow_execution
    ]},
    {iteration_tests, [], [
        iterate_over_list_store,
        iterate_over_list_store_with_some_inaccessible_items,
        iterate_over_list_store_with_all_items_inaccessible,
        iterate_over_empty_list_store,

        iterate_over_range_store,
        iterate_over_empty_range_store,

        iterate_over_single_value_store,
        iterate_over_single_value_store_with_all_items_inaccessible,
        iterate_over_empty_single_value_store,

        iterate_over_tree_forest_store,
        iterate_over_tree_forest_store_with_some_inaccessible_items,
        iterate_over_tree_forest_store_with_all_items_inaccessible,
        iterate_over_empty_tree_forest_store
    ]},
    {mapping_tests, [], [
        map_results_to_audit_log_store,
        map_results_to_list_store,
        map_results_to_range_store,
        map_results_to_single_value_store,
        map_results_to_time_series_store,
        map_results_to_tree_forest_store,

        map_results_to_workflow_audit_log_store,
        map_results_to_task_audit_log_store,
        map_results_to_task_time_series_store
    ]},
    {failure_tests, [], [
        fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error,
        fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error,
        fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error,
        fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error,
        fail_atm_workflow_execution_due_to_job_result_store_mapping_error,
        fail_atm_workflow_execution_due_to_job_missing_required_results_error,
        fail_atm_workflow_execution_due_to_incorrect_result_type_error,
        fail_atm_workflow_execution_due_to_lambda_exception,
        fail_atm_workflow_execution_due_to_lambda_error
    ]},
    {repeat_tests, [], [
        repeat_not_ended_atm_workflow_execution,
        repeat_finished_atm_lane_run_execution
    ]}
].

all() -> [
    {group, scheduling_non_executable_workflow_schema_tests},
    {group, scheduling_executable_workflow_schema_with_invalid_args_tests},
    {group, preparation_tests},
    {group, cancellation_tests},
    {group, iteration_tests},
    {group, mapping_tests},
    {group, failure_tests},
    {group, repeat_tests}
].


-define(RUN_TEST(__TEST_BASE_MODULE),
    try
        __TEST_BASE_MODULE:?FUNCTION_NAME()
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

-define(RUN_SCHEDULING_TEST(), ?RUN_TEST(atm_workflow_execution_scheduling_test_base)).
-define(RUN_PREPARATION_TEST(), ?RUN_TEST(atm_workflow_execution_preparation_test_base)).
-define(RUN_CANCELLATION_TEST(), ?RUN_TEST(atm_workflow_execution_cancellation_test_base)).
-define(RUN_ITERATION_TEST(), ?RUN_TEST(atm_workflow_execution_iteration_test_base)).
-define(RUN_MAPPING_TEST(), ?RUN_TEST(atm_workflow_execution_mapping_test_base)).
-define(RUN_FAILURE_TEST(), ?RUN_TEST(atm_workflow_execution_failure_test_base)).
-define(RUN_REPEAT_TEST(), ?RUN_TEST(atm_workflow_execution_repeat_test_base)).


%%%===================================================================
%%% Test cases
%%%===================================================================


schedule_atm_workflow_with_no_lanes(_Config) ->
    ?RUN_SCHEDULING_TEST().


schedule_atm_workflow_with_empty_lane(_Config) ->
    ?RUN_SCHEDULING_TEST().


schedule_atm_workflow_with_empty_parallel_box(_Config) ->
    ?RUN_SCHEDULING_TEST().


schedule_incompatible_atm_workflow(_Config) ->
    ?RUN_SCHEDULING_TEST().


schedule_atm_workflow_with_openfaas_not_configured(_Config) ->
    ?RUN_SCHEDULING_TEST().


schedule_atm_workflow_with_invalid_initial_store_content(_Config) ->
    ?RUN_SCHEDULING_TEST().


first_lane_run_preparation_failure_before_run_was_created(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_after_run_was_created(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancel_before_lane_run_preparation_failed(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancel_in_stopping_status_after_lane_run_preparation_failed(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4(_Config) ->
    ?RUN_PREPARATION_TEST().


cancel_scheduled_atm_workflow_execution(_Config) ->
    ?RUN_CANCELLATION_TEST().


cancel_enqueued_atm_workflow_execution(_Config) ->
    ?RUN_CANCELLATION_TEST().


cancel_active_atm_workflow_execution(_Config) ->
    ?RUN_CANCELLATION_TEST().


cancel_finishing_atm_workflow_execution(_Config) ->
    ?RUN_CANCELLATION_TEST().


cancel_finished_atm_workflow_execution(_Config) ->
    ?RUN_CANCELLATION_TEST().


iterate_over_list_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_list_store_with_some_inaccessible_items(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_list_store_with_all_items_inaccessible(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_empty_list_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_range_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_empty_range_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_single_value_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_single_value_store_with_all_items_inaccessible(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_empty_single_value_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_tree_forest_store(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_tree_forest_store_with_some_inaccessible_items(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_tree_forest_store_with_all_items_inaccessible(_Config) ->
    ?RUN_ITERATION_TEST().


iterate_over_empty_tree_forest_store(_Config) ->
    ?RUN_ITERATION_TEST().


map_results_to_audit_log_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_list_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_range_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_single_value_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_time_series_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_tree_forest_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_workflow_audit_log_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_task_audit_log_store(_Config) ->
    ?RUN_MAPPING_TEST().


map_results_to_task_time_series_store(_Config) ->
    ?RUN_MAPPING_TEST().


fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_job_result_store_mapping_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_job_missing_required_results_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_incorrect_result_type_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_lambda_exception(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_lambda_error(_Config) ->
    ?RUN_FAILURE_TEST().


repeat_not_ended_atm_workflow_execution(_Config) ->
    ?RUN_REPEAT_TEST().


repeat_finished_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        atm_workflow_execution_scheduling_test_base
        | ?ATM_WORKFLOW_EXECUTION_TEST_UTILS
    ],
    oct_background:init_per_suite(
        [{?LOAD_MODULES, ModulesToLoad} | Config],
        #onenv_test_config{
            onenv_scenario = "1op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {atm_workflow_engine_slots_count, 100000},
                {atm_workflow_engine_async_calls_limit, 100000}
            ]}],
            posthook = fun(NewConfig) ->
                atm_test_inventory:init_per_suite(?PROVIDER_SELECTOR, user1),
                atm_test_inventory:add_member(?USER_SELECTOR),
                ozt_spaces:set_privileges(?SPACE_SELECTOR, ?USER_SELECTOR, [
                    ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS,
                    ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS
                    | privileges:space_member()
                ]),
                NewConfig
            end
        }
    ).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(scheduling_non_executable_workflow_schema_tests, Config) ->
    Config;

init_per_group(scheduling_executable_workflow_schema_with_invalid_args_tests, Config) ->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    Config;

init_per_group(TestGroup, Config) when
    TestGroup =:= preparation_tests;
    TestGroup =:= cancellation_tests;
    TestGroup =:= iteration_tests;
    TestGroup =:= mapping_tests;
    TestGroup =:= failure_tests;
    TestGroup =:= repeat_tests
->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    atm_workflow_execution_test_runner:init(?PROVIDER_SELECTOR),
    Config.


end_per_group(scheduling_non_executable_workflow_schema_tests, Config) ->
    Config;

end_per_group(scheduling_executable_workflow_schema_with_invalid_args_tests, Config) ->
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config;

end_per_group(TestGroup, Config) when
    TestGroup =:= preparation_tests;
    TestGroup =:= cancellation_tests;
    TestGroup =:= iteration_tests;
    TestGroup =:= mapping_tests;
    TestGroup =:= failure_tests;
    TestGroup =:= repeat_tests
->
    atm_workflow_execution_test_runner:teardown(?PROVIDER_SELECTOR),
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
