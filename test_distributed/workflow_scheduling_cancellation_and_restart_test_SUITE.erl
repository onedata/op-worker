%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test of workflow scheduling cancellation.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_scheduling_cancellation_and_restart_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    sync_workflow_only_task_of_lane_cancel_test/1,
    sync_workflow_cancel_test/1,
    async_workflow_with_prepare_in_advance_cancel_test/1,
    async_workflow_cancel_by_handle_callback_test/1,
    async_workflow_cancel_during_result_processing_test/1,
    cancel_by_sync_job_error_test/1,
    cancel_by_async_job_error_test/1,
    cancel_by_async_job_timeout_test/1,
    cancel_by_async_result_processing_error_test/1,
    cancel_during_lane_prepare_test/1,
    cancel_during_lane_prepare_in_advance_test/1,
    async_job_timeout_before_prepare_in_advance_finish_test/1,

    restart_callback_failure_test/1
]).

all() ->
    ?ALL([
        % TODO VFS-7784 - test cancellation when next lane prepare in advance fails
        sync_workflow_only_task_of_lane_cancel_test,
        sync_workflow_cancel_test,
        async_workflow_with_prepare_in_advance_cancel_test,
        async_workflow_cancel_by_handle_callback_test,
        async_workflow_cancel_during_result_processing_test,
        cancel_by_sync_job_error_test,
        cancel_by_async_job_error_test,
        cancel_by_async_job_timeout_test,
        cancel_by_async_result_processing_error_test,
        cancel_during_lane_prepare_test,
        cancel_during_lane_prepare_in_advance_test,
        async_job_timeout_before_prepare_in_advance_finish_test,

        restart_callback_failure_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

sync_workflow_only_task_of_lane_cancel_test(Config) ->
    cancel_test_base(Config, sync, false, <<"1">>, {cancel_execution, process_item, <<"1_1_1">>}).

sync_workflow_cancel_test(Config) ->
    cancel_test_base(Config, sync, false, <<"3">>, {cancel_execution, process_item, <<"3_3_1">>}).

async_workflow_with_prepare_in_advance_cancel_test(Config) ->
    cancel_test_base(Config, async, true, <<"3">>, {cancel_execution, process_item, <<"3_3_2">>}).

async_workflow_cancel_by_handle_callback_test(Config) ->
    cancel_test_base(Config, async, true, <<"1">>, {cancel_execution, handle_callback, <<"1_1_1">>}).

async_workflow_cancel_during_result_processing_test(Config) ->
    cancel_test_base(Config, async, false, <<"3">>, {cancel_execution, process_result, <<"3_2_1">>}).

cancel_by_sync_job_error_test(Config) ->
    cancel_test_base(Config, sync, false, <<"3">>, {fail_job, <<"3_3_2">>}).

cancel_by_async_job_error_test(Config) ->
    cancel_test_base(Config, async, false, <<"3">>, {fail_job, <<"3_3_2">>}).

cancel_by_async_job_timeout_test(Config) ->
    cancel_test_base(Config, async, false, <<"1">>, {timeout, <<"1_1_1">>}).

cancel_by_async_result_processing_error_test(Config) ->
    cancel_test_base(Config, async, true, <<"3">>, {fail_result_processing, <<"3_3_1">>}).

cancel_during_lane_prepare_test(Config) ->
    cancel_test_base(Config, sync, false, <<"3">>, {cancel_execution, prepare_lane, <<"3">>}).

cancel_during_lane_prepare_in_advance_test(Config) ->
    cancel_test_base(Config, sync, true, <<"2">>, {cancel_execution, prepare_lane, <<"3">>}).

async_job_timeout_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"2">>}, true),
    cancel_test_base(Config, async, true, <<"1">>, {timeout, <<"1_1_1">>}).

%%%===================================================================

restart_callback_failure_test(Config) ->
    InitialKeys = workflow_scheduling_test_utils:get_all_keys(Config),
    WorkflowType = sync,
    PrepareInAdvance = false,
    LaneId = <<"3">>,

    [Worker | _] = ?config(op_worker_nodes, Config),
    #{id := ExecutionId} = WorkflowExecutionSpec =
        workflow_scheduling_test_utils:gen_workflow_execution_spec(
            WorkflowType, PrepareInAdvance, #{lane_options => #{failure_count_to_abort => 1}}),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_job, {<<"3_1_1">>, <<"100">>}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    workflow_scheduling_test_utils:verify_execution_history(
        WorkflowExecutionSpec, ExecutionHistory, #{stop_on_lane => LaneId}),
    workflow_scheduling_test_utils:verify_memory(Config, InitialKeys, true),

    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, LaneId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec])),
    #{execution_history := ExecutionHistory2} = workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_empty_lane(ExecutionHistory2, LaneId),

    WorkflowExecutionSpec2 = workflow_scheduling_test_utils:gen_workflow_execution_spec(
        WorkflowType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow restarted"),

    #{execution_history := ExecutionHistory3} = ExtendedHistoryStats3 =
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_execution_history_stats(
        ExtendedHistoryStats3, WorkflowType, #{restart => true}),
    workflow_scheduling_test_utils:verify_execution_history(
        WorkflowExecutionSpec2, ExecutionHistory3, #{restart_lane => LaneId}),

    workflow_scheduling_test_utils:verify_memory(Config, InitialKeys).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

cancel_test_base(Config, WorkflowType, PrepareInAdvance, LaneId, GathererOption) ->
    InitialKeys = workflow_scheduling_test_utils:get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    LaneOptions = case GathererOption of
        {cancel_execution, _, _} -> #{};
        _ -> #{failure_count_to_abort => 1}
    end,
    #{id := ExecutionId} = WorkflowExecutionSpec = workflow_scheduling_test_utils:gen_workflow_execution_spec(
        WorkflowType, PrepareInAdvance, #{lane_options => LaneOptions}),
    {GathererOptionKey, GathererOptionValue} = case GathererOption of
        {cancel_execution, prepare_lane, LaneIdToCancel} ->
            {cancel_execution, {prepare_lane, LaneIdToCancel}};
        {cancel_execution, Function, Item} ->
            {cancel_execution, {Function, Item, <<"100">>}};
        {Key, TaskId} ->
            {Key, {TaskId, <<"100">>}}
    end,
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, GathererOptionKey, GathererOptionValue),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    case GathererOption of
        cancel_execution -> ?assertMatch(#{cancel_ans := ok}, ExtendedHistoryStats);
        _ -> ok
    end,
    workflow_scheduling_test_utils:verify_execution_history_stats(
        ExtendedHistoryStats, WorkflowType, #{ignore_async_slots_check => true}),

    case GathererOption of
        {cancel_execution, prepare_lane, LaneId} ->
            workflow_scheduling_test_utils:verify_execution_history(
                WorkflowExecutionSpec, ExecutionHistory, #{expect_lane_finish => LaneId});
        _ ->
            workflow_scheduling_test_utils:verify_execution_history(
                WorkflowExecutionSpec, ExecutionHistory, #{stop_on_lane => LaneId})
    end,

    workflow_scheduling_test_utils:verify_memory(Config, InitialKeys, true),

    WorkflowExecutionSpec2 = workflow_scheduling_test_utils:gen_workflow_execution_spec(
        WorkflowType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow restarted"),

    #{execution_history := ExecutionHistory2} = ExtendedHistoryStats2 =
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_execution_history_stats(
        ExtendedHistoryStats2, WorkflowType, #{restart => true}),
    workflow_scheduling_test_utils:verify_execution_history(
        WorkflowExecutionSpec2, ExecutionHistory2, #{restart_lane => LaneId}),

    workflow_scheduling_test_utils:verify_memory(Config, InitialKeys).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    workflow_scheduling_test_utils:init_per_suite(Config).

end_per_suite(Config) ->
    workflow_scheduling_test_utils:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    workflow_scheduling_test_utils:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    workflow_scheduling_test_utils:end_per_testcase(Case, Config).