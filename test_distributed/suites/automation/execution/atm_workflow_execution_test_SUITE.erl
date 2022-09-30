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

    lane_preparing_in_advance_interruption_changes_to_failure_upon_first_lane_run_finish/1,

    cancel_scheduled_atm_workflow_execution/1,
    cancel_enqueued_atm_workflow_execution/1,

    cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results/1,
    cancel_active_atm_workflow_execution_with_uncorrelated_task_results/1,

    cancel_paused_atm_workflow_execution/1,
    cancel_interrupted_atm_workflow_execution/1,

    cancel_finishing_atm_workflow_execution/1,
    cancel_finished_atm_workflow_execution/1,
    cancel_failed_atm_workflow_execution/1,
    cancel_crashed_atm_workflow_execution/1,

    pause_scheduled_atm_workflow_execution/1,
    pause_enqueued_atm_workflow_execution/1,

    pause_active_atm_workflow_execution_with_no_uncorrelated_task_results/1,
    pause_active_atm_workflow_execution_with_uncorrelated_task_results/1,

    pause_interrupted_atm_workflow_execution/1,

    pause_finishing_atm_workflow_execution/1,
    pause_cancelled_atm_workflow_execution/1,
    pause_finished_atm_workflow_execution/1,
    pause_failed_atm_workflow_execution/1,
    pause_crashed_atm_workflow_execution/1,

    fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error/1,
    fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error/1,
    fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error/1,
    fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error/1,
    fail_atm_workflow_execution_due_to_job_timeout/1,
    fail_atm_workflow_execution_due_to_job_result_store_mapping_error/1,
    fail_atm_workflow_execution_due_to_job_missing_required_results_error/1,
    fail_atm_workflow_execution_due_to_incorrect_result_type_error/1,
    fail_atm_workflow_execution_due_to_lambda_exception/1,
    fail_atm_workflow_execution_due_to_lambda_error/1,

    interrupt_ongoing_atm_workflow_execution_due_to_expired_session/1,

    stopping_reason_failure_overrides_pause/1,
    stopping_reason_cancel_overrides_pause/1,
    stopping_reason_cancel_overrides_failure/1,

    stopping_finished_atm_workflow_execution/1,
    stopping_cancelled_atm_workflow_execution/1,

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

    map_arguments/1,

    map_results_to_audit_log_store/1,
    map_results_to_list_store/1,
    map_results_to_range_store/1,
    map_results_to_single_value_store/1,
    map_results_to_time_series_store/1,
    map_results_to_tree_forest_store/1,

    map_results_to_workflow_audit_log_store/1,
    map_results_to_task_audit_log_store/1,
    map_results_to_task_time_series_store/1,

    map_results_to_multiple_stores/1,

    repeat_not_ended_atm_workflow_execution/1,
    repeat_finished_atm_lane_run_execution/1,
    rerun_failed_iterated_atm_lane_run_execution/1,
    retry_failed_iterated_atm_lane_run_execution/1,
    repeat_failed_while_preparing_atm_lane_run_execution/1,
    repeat_failed_not_iterated_atm_lane_run_execution/1,
    repeat_cancelled_atm_lane_run_execution/1
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
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4,

        lane_preparing_in_advance_interruption_changes_to_failure_upon_first_lane_run_finish
    ]},

    {cancel_tests, [], [
        cancel_scheduled_atm_workflow_execution,
        cancel_enqueued_atm_workflow_execution,

        cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results,
        cancel_active_atm_workflow_execution_with_uncorrelated_task_results,

        cancel_paused_atm_workflow_execution,
        cancel_interrupted_atm_workflow_execution,

        cancel_finishing_atm_workflow_execution,
        cancel_finished_atm_workflow_execution,
        cancel_failed_atm_workflow_execution,
        cancel_crashed_atm_workflow_execution
    ]},

    {pause_tests, [], [
        pause_scheduled_atm_workflow_execution,
        pause_enqueued_atm_workflow_execution,

        pause_active_atm_workflow_execution_with_no_uncorrelated_task_results,
        pause_active_atm_workflow_execution_with_uncorrelated_task_results,

        pause_interrupted_atm_workflow_execution,

        pause_finishing_atm_workflow_execution,
        pause_cancelled_atm_workflow_execution,
        pause_finished_atm_workflow_execution,
        pause_failed_atm_workflow_execution,
        pause_crashed_atm_workflow_execution
    ]},

    {failure_tests, [], [
        fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error,

        fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error,
        fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error,
        fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error,

        fail_atm_workflow_execution_due_to_job_timeout,
        fail_atm_workflow_execution_due_to_job_result_store_mapping_error,
        fail_atm_workflow_execution_due_to_job_missing_required_results_error,
        fail_atm_workflow_execution_due_to_incorrect_result_type_error,
        fail_atm_workflow_execution_due_to_lambda_exception,
        fail_atm_workflow_execution_due_to_lambda_error
    ]},

    {suspend_tests, [], [
        interrupt_ongoing_atm_workflow_execution_due_to_expired_session
    ]},

    {stopping_tests, [], [
        stopping_reason_failure_overrides_pause,
        stopping_reason_cancel_overrides_pause,
        stopping_reason_cancel_overrides_failure,

        stopping_finished_atm_workflow_execution,
        stopping_cancelled_atm_workflow_execution
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
        map_arguments,

        map_results_to_audit_log_store,
        map_results_to_list_store,
        map_results_to_range_store,
        map_results_to_single_value_store,
        map_results_to_time_series_store,
        map_results_to_tree_forest_store,

        map_results_to_workflow_audit_log_store,
        map_results_to_task_audit_log_store,
        map_results_to_task_time_series_store,

        map_results_to_multiple_stores
    ]},

    {repeat_tests, [], [
        repeat_not_ended_atm_workflow_execution,
        repeat_finished_atm_lane_run_execution,
        rerun_failed_iterated_atm_lane_run_execution,
        retry_failed_iterated_atm_lane_run_execution,
        repeat_failed_while_preparing_atm_lane_run_execution,
        repeat_failed_not_iterated_atm_lane_run_execution,
        repeat_cancelled_atm_lane_run_execution
    ]}
].

all() -> [
    {group, scheduling_non_executable_workflow_schema_tests},
    {group, scheduling_executable_workflow_schema_with_invalid_args_tests},
    {group, preparation_tests},
    {group, cancel_tests},
    {group, pause_tests},
    {group, failure_tests},
    {group, suspend_tests},
    {group, stopping_tests},
    {group, iteration_tests},
    {group, mapping_tests},
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
-define(RUN_CANCEL_TEST(), ?RUN_TEST(atm_workflow_execution_cancel_test_base)).
-define(RUN_PAUSE_TEST(), ?RUN_TEST(atm_workflow_execution_pause_test_base)).
-define(RUN_FAILURE_TEST(), ?RUN_TEST(atm_workflow_execution_failure_test_base)).
-define(RUN_SUSPEND_TEST(), ?RUN_TEST(atm_workflow_execution_suspension_test_base)).
-define(RUN_STOPPING_TEST(), ?RUN_TEST(atm_workflow_execution_stopping_test_base)).
-define(RUN_ITERATION_TEST(), ?RUN_TEST(atm_workflow_execution_iteration_test_base)).
-define(RUN_MAPPING_TEST(), ?RUN_TEST(atm_workflow_execution_mapping_test_base)).
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


lane_preparing_in_advance_interruption_changes_to_failure_upon_first_lane_run_finish(_Config) ->
    ?RUN_PREPARATION_TEST().


cancel_scheduled_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_enqueued_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_active_atm_workflow_execution_with_uncorrelated_task_results(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_paused_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_interrupted_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_finishing_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_finished_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_failed_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


cancel_crashed_atm_workflow_execution(_Config) ->
    ?RUN_CANCEL_TEST().


pause_scheduled_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_enqueued_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_active_atm_workflow_execution_with_no_uncorrelated_task_results(_Config) ->
    ?RUN_PAUSE_TEST().


pause_active_atm_workflow_execution_with_uncorrelated_task_results(_Config) ->
    ?RUN_PAUSE_TEST().


pause_interrupted_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_finishing_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_cancelled_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_finished_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_failed_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


pause_crashed_atm_workflow_execution(_Config) ->
    ?RUN_PAUSE_TEST().


fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error(_Config) ->
    ?RUN_FAILURE_TEST().


fail_atm_workflow_execution_due_to_job_timeout(_Config) ->
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


interrupt_ongoing_atm_workflow_execution_due_to_expired_session(_Config) ->
    ?RUN_SUSPEND_TEST().


stopping_reason_failure_overrides_pause(_Config) ->
    ?RUN_STOPPING_TEST().


stopping_reason_cancel_overrides_pause(_Config) ->
    ?RUN_STOPPING_TEST().


stopping_reason_cancel_overrides_failure(_Config) ->
    ?RUN_STOPPING_TEST().


stopping_finished_atm_workflow_execution(_Config) ->
    ?RUN_STOPPING_TEST().


stopping_cancelled_atm_workflow_execution(_Config) ->
    ?RUN_STOPPING_TEST().


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


map_arguments(_Config) ->
    ?RUN_MAPPING_TEST().


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


map_results_to_multiple_stores(_Config) ->
    ?RUN_MAPPING_TEST().


repeat_not_ended_atm_workflow_execution(_Config) ->
    ?RUN_REPEAT_TEST().


repeat_finished_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


rerun_failed_iterated_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


retry_failed_iterated_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


repeat_failed_while_preparing_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


repeat_failed_not_iterated_atm_lane_run_execution(_Config) ->
    ?RUN_REPEAT_TEST().


repeat_cancelled_atm_lane_run_execution(_Config) ->
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
                {atm_workflow_engine_async_calls_limit, 100000},
                {atm_workflow_job_timeout_sec, 1},
                {atm_workflow_job_timeout_check_period_sec, 1}
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
    TestGroup =:= cancel_tests;
    TestGroup =:= pause_tests;
    TestGroup =:= failure_tests;
    TestGroup =:= suspend_tests;
    TestGroup =:= stopping_tests;
    TestGroup =:= iteration_tests;
    TestGroup =:= mapping_tests;
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
    TestGroup =:= cancel_tests;
    TestGroup =:= pause_tests;
    TestGroup =:= failure_tests;
    TestGroup =:= suspend_tests;
    TestGroup =:= stopping_tests;
    TestGroup =:= iteration_tests;
    TestGroup =:= mapping_tests;
    TestGroup =:= repeat_tests
->
    atm_workflow_execution_test_runner:teardown(?PROVIDER_SELECTOR),
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
