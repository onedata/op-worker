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

-include("atm_workflow_exeuction_test.hrl").
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
    atm_workflow_with_no_lanes_scheduling_should_fail_test/1,
    atm_workflow_with_empty_lane_scheduling_should_fail_test/1,
    atm_workflow_with_empty_parallel_box_scheduling_should_fail_test/1,
    atm_workflow_scheduling_with_openfaas_not_configured_should_fail_test/1,

    atm_workflow_with_invalid_initial_store_content_scheduling_should_fail_test/1,

    atm_workflow_execution_cancelled_in_scheduled_status_test/1,

    first_lane_run_preparation_failure_before_run_was_created_test/1,
    first_lane_run_preparation_failure_after_run_was_created_test/1,

    atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created_test/1,
    atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created_test/1,
    atm_workflow_execution_cancel_in_aborting_status_after_lane_run_preparation_failed_test/1,

    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1_test/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2_test/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3_test/1,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4_test/1,

    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1_test/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2_test/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3_test/1,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4_test/1
]).

groups() -> [
    {non_executable_workflow_schema_scheduling, [parallel], [
        atm_workflow_with_no_lanes_scheduling_should_fail_test,
        atm_workflow_with_empty_lane_scheduling_should_fail_test,
        atm_workflow_with_empty_parallel_box_scheduling_should_fail_test,
        atm_workflow_scheduling_with_openfaas_not_configured_should_fail_test
    ]},
    {executable_workflow_schema_scheduling_with_invalid_args, [parallel], [
        atm_workflow_with_invalid_initial_store_content_scheduling_should_fail_test
    ]},
    {execution_tests, [parallel], [
        atm_workflow_execution_cancelled_in_scheduled_status_test,

        first_lane_run_preparation_failure_before_run_was_created_test,
        first_lane_run_preparation_failure_after_run_was_created_test,

        atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created_test,
        atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created_test,
        atm_workflow_execution_cancel_in_aborting_status_after_lane_run_preparation_failed_test,

        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1_test,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2_test,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3_test,
        first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4_test,

        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1_test,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2_test,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3_test,
        first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4_test
    ]}
].

all() -> [
    {group, non_executable_workflow_schema_scheduling},
    {group, executable_workflow_schema_scheduling_with_invalid_args},
    {group, execution_tests}
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


%%%===================================================================
%%% Test cases
%%%===================================================================


atm_workflow_with_no_lanes_scheduling_should_fail_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


atm_workflow_with_empty_lane_scheduling_should_fail_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


atm_workflow_with_empty_parallel_box_scheduling_should_fail_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


atm_workflow_scheduling_with_openfaas_not_configured_should_fail_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


atm_workflow_with_invalid_initial_store_content_scheduling_should_fail_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


atm_workflow_execution_cancelled_in_scheduled_status_test(_Config) ->
    ?RUN_SCHEDULING_TEST().


first_lane_run_preparation_failure_before_run_was_created_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_after_run_was_created_test(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created_test(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created_test(_Config) ->
    ?RUN_PREPARATION_TEST().


atm_workflow_execution_cancel_in_aborting_status_after_lane_run_preparation_failed_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3_test(_Config) ->
    ?RUN_PREPARATION_TEST().


first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4_test(_Config) ->
    ?RUN_PREPARATION_TEST().


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
            envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
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


init_per_group(non_executable_workflow_schema_scheduling, Config) ->
    Config;

init_per_group(executable_workflow_schema_scheduling_with_invalid_args, Config) ->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    Config;

init_per_group(execution_tests, Config) ->
    atm_openfaas_task_executor_mock:init(?PROVIDER_SELECTOR, atm_openfaas_docker_mock),
    atm_workflow_execution_test_runner:init(?PROVIDER_SELECTOR),
    Config.


end_per_group(non_executable_workflow_schema_scheduling, Config) ->
    Config;

end_per_group(executable_workflow_schema_scheduling_with_invalid_args, Config) ->
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config;

end_per_group(execution_tests, Config) ->
    atm_workflow_execution_test_runner:teardown(?PROVIDER_SELECTOR),
    atm_openfaas_task_executor_mock:teardown(?PROVIDER_SELECTOR),
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
