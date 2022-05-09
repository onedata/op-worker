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
    sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test/1,
    sync_workflow_external_cancel_test/1,
    async_workflow_with_prepare_in_advance_external_cancel_test/1,
    async_workflow_external_cancel_during_handle_callback_test/1,
    async_workflow_external_cancel_during_result_processing_test/1,
    internal_cancel_caused_by_sync_job_error_test/1,
    internal_cancel_caused_by_async_job_error_test/1,
    internal_cancel_caused_by_async_job_timeout_test/1,
    internal_cancel_caused_by_result_processing_timeout_test/1,
    external_cancel_during_lane_prepare_test/1,
    external_cancel_during_lane_prepare_in_advance_test/1,
    internal_cancel_caused_by_async_job_timeout_before_prepare_in_advance_finish_test/1,
    async_workflow_with_streams_external_cancel/1,
    async_workflow_with_streams_and_prepare_in_advance_external_cancel/1,
    async_workflow_with_streams_and_long_lasting_prepare_in_advance_external_cancel/1,
    internal_cancel_of_workflow_with_stream_caused_by_async_job_error_test/1,
    internal_cancel_caused_by_stream_data_processing_error_test/1,
    internal_cancel_caused_by_stream_closing_error_test/1,

    restart_callback_failure_test/1
]).

all() ->
    ?ALL([
        % TODO VFS-7784 - test cancellation when next lane prepare in advance fails
        sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test,
        sync_workflow_external_cancel_test,
        async_workflow_with_prepare_in_advance_external_cancel_test,
        async_workflow_external_cancel_during_handle_callback_test,
        async_workflow_external_cancel_during_result_processing_test,
        internal_cancel_caused_by_sync_job_error_test,
        internal_cancel_caused_by_async_job_error_test,
        internal_cancel_caused_by_async_job_timeout_test,
        internal_cancel_caused_by_result_processing_timeout_test,
        external_cancel_during_lane_prepare_test,
        external_cancel_during_lane_prepare_in_advance_test,
        internal_cancel_caused_by_async_job_timeout_before_prepare_in_advance_finish_test,
        async_workflow_with_streams_external_cancel,
        async_workflow_with_streams_and_prepare_in_advance_external_cancel,
        async_workflow_with_streams_and_long_lasting_prepare_in_advance_external_cancel,
        internal_cancel_of_workflow_with_stream_caused_by_async_job_error_test,
        internal_cancel_caused_by_stream_data_processing_error_test,
        internal_cancel_caused_by_stream_closing_error_test,

        restart_callback_failure_test
    ]).


-record(test_config, {
    task_type = sync :: sync | async,
    prepare_in_advance = false :: boolean(),
    lane_id :: workflow_engine:lane_id(),
    test_execution_manager_option ::
        {workflow_scheduling_test_common:test_manager_task_failure_key(), workflow_engine:task_id()} |
        {cancel_execution, prepare_lane, workflow_engine:lane_id()} |
        {cancel_execution, process_item | handle_callback | process_result, workflow_engine:task_id()},
    generator_options = #{} :: workflow_test_handler:test_execution_context()
}).


%%%===================================================================
%%% Test functions
%%%===================================================================

sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        lane_id = <<"1">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"1_1_1">>}
    }).

sync_workflow_external_cancel_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"3_3_1">>}
    }).

async_workflow_with_prepare_in_advance_external_cancel_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"3_3_2">>}
    }).

async_workflow_external_cancel_during_handle_callback_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"1">>,
        test_execution_manager_option = {cancel_execution, handle_callback, <<"1_1_1">>}
    }).

async_workflow_external_cancel_during_result_processing_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_result, <<"3_2_1">>}
    }).

internal_cancel_caused_by_sync_job_error_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>}
    }).

internal_cancel_caused_by_async_job_error_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>}
    }).

internal_cancel_caused_by_async_job_timeout_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_result_processing, <<"3_3_1">>}
    }).

internal_cancel_caused_by_result_processing_timeout_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_result_processing, <<"3_3_1">>}
    }).

external_cancel_during_lane_prepare_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, prepare_lane, <<"3">>}
    }).

external_cancel_during_lane_prepare_in_advance_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        prepare_in_advance = true,
        lane_id = <<"2">>,
        test_execution_manager_option = {cancel_execution, prepare_lane, <<"3">>}
    }).

internal_cancel_caused_by_async_job_timeout_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, {delay_lane_preparation, <<"2">>}, true),
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"1">>,
        test_execution_manager_option = {timeout, <<"1_1_1">>}
    }).

async_workflow_with_streams_external_cancel(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"3_2_1">>},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,2} => [{stream_termination_callback, 5}],
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,2} => [<<"100">>, stream_termination_callback],
                {3,3} => []
            }
        }}
    }).

async_workflow_with_streams_and_prepare_in_advance_external_cancel(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"3_2_1">>},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,2} => [{stream_termination_callback, 5}],
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,2} => [<<"100">>, stream_termination_callback],
                {3,3} => []
            }
        }}
    }).

async_workflow_with_streams_and_long_lasting_prepare_in_advance_external_cancel(Config) ->
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, {delay_lane_preparation, <<"4">>}, true),
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_item, <<"3_2_1">>},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,2} => [{stream_termination_callback, 5}],
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,2} => [<<"100">>, stream_termination_callback],
                {3,3} => []
            }
        }}
    }).

internal_cancel_of_workflow_with_stream_caused_by_async_job_error_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,2} => [{stream_termination_callback, 5}],
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,2} => [<<"10">>, <<"100">>],
                {3,3} => []
            }
        }}
    }).

internal_cancel_caused_by_stream_data_processing_error_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_data_processing, <<"3_3_2">>},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,2} => [{stream_termination_callback, 5}],
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,2} => [<<"10">>, <<"100">>],
                {3,3} => []
            }
        }}
    }).

internal_cancel_caused_by_stream_closing_error_test(Config) ->
    cancel_and_restart_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_stream_termination, {<<"3_2_2">>, stream_termination_callback}},
        generator_options = #{task_streams => #{
            3 => #{
                {1,1} => [<<"1">>],
                {2,1} => [{stream_termination_callback, 5}],
                {2,2} => [<<"10">>, <<"100">>, termination_error], % termination_error does not  trigger anything but
                                                                   % for workflow execution check (otherwise there will
                                                                   % be one more callback execution than expected)
                {3,1} => [<<"1">>,{<<"2">>, 10},<<"3">>,<<"4">>,<<"100">>,<<"150">>],
                {3,3} => []
            }
        }}
    }).

%%%===================================================================

restart_callback_failure_test(Config) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),
    TaskType = sync,
    PrepareInAdvance = false,
    LaneId = <<"3">>,

    [Worker | _] = ?config(op_worker_nodes, Config),
    #{id := ExecutionId} = WorkflowExecutionSpec =
        workflow_scheduling_test_common:gen_workflow_execution_spec(
            TaskType, PrepareInAdvance, #{lane_options => #{failure_count_to_cancel => 1}}),
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, fail_job, {<<"3_1_1">>, <<"100">>}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(ExtendedHistoryStats, TaskType),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec, ExecutionHistory, #{stop_on_lane => LaneId}),
    workflow_scheduling_test_common:verify_memory(Config, InitialKeys, true),

    % Same callback is used to restart and prepare lane so set lane preparation to be failed
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, fail_lane_preparation, LaneId),
    % execute_workflow always return ok - failure of restart results in immediate end of workflow execution
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),
    #{execution_history := ExecutionHistory2} = workflow_scheduling_test_common:get_task_execution_history(Config),
    % If restart fails, no task should be executed
    workflow_scheduling_test_common:verify_empty_lane(ExecutionHistory2, LaneId),

    WorkflowExecutionSpec2 = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow restarted"),

    #{execution_history := ExecutionHistory3} = ExtendedHistoryStats3 =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(
        ExtendedHistoryStats3, TaskType, #{restart => true}),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec2, ExecutionHistory3, #{restart_lane => LaneId}),

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

cancel_and_restart_test_base(Config, #test_config{
    task_type = TaskType,
    prepare_in_advance = PrepareInAdvance,
    lane_id = LaneId,
    test_execution_manager_option = TestExecutionManagerOption,
    generator_options = GeneratorOptions
}) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    LaneOptions = case TestExecutionManagerOption of
        {cancel_execution, _, _} -> #{};
        _ -> #{failure_count_to_cancel => 1}
    end,
    #{id := ExecutionId} = WorkflowExecutionSpec = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, GeneratorOptions#{lane_options => LaneOptions}),
    {TestExecutionManagerOptionKey, TestExecutionManagerOptionValue} = case TestExecutionManagerOption of
        {cancel_execution, prepare_lane, LaneIdToCancel} ->
            {cancel_execution, {prepare_lane, LaneIdToCancel}};
        {cancel_execution, Function, Item} ->
            {cancel_execution, {Function, Item, <<"100">>}};
        {_Key, {_TaskId, _Itme}} ->
            TestExecutionManagerOption;
        {Key, TaskId} ->
            {Key, {TaskId, <<"100">>}}
    end,
    workflow_scheduling_test_common:set_test_execution_manager_option(
        Config, TestExecutionManagerOptionKey, TestExecutionManagerOptionValue),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    case TestExecutionManagerOption of
        cancel_execution -> ?assertMatch(#{cancel_ans := ok}, ExtendedHistoryStats);
        _ -> ok
    end,
    workflow_scheduling_test_common:verify_execution_history_stats(
        ExtendedHistoryStats, TaskType, #{ignore_async_slots_check => true}),

    case TestExecutionManagerOption of
        {cancel_execution, prepare_lane, LaneId} ->
            workflow_scheduling_test_common:verify_execution_history(
                WorkflowExecutionSpec, ExecutionHistory, #{expect_lane_finish => LaneId});
        {cancel_execution, _, _} ->
            workflow_scheduling_test_common:verify_execution_history(
                WorkflowExecutionSpec, ExecutionHistory, #{stop_on_lane => LaneId});
        {FailureType, {FailedTaskId, FailedItem}} ->
            workflow_scheduling_test_common:verify_execution_history(WorkflowExecutionSpec, ExecutionHistory,
                #{stop_on_lane => LaneId, FailureType => {LaneId, FailedTaskId, FailedItem}});
        {FailureType, FailedTaskId} ->
            workflow_scheduling_test_common:verify_execution_history(WorkflowExecutionSpec, ExecutionHistory,
                #{stop_on_lane => LaneId, FailureType => {LaneId, FailedTaskId, <<"100">>}})
    end,

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys, true),

    WorkflowExecutionSpec2 = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow restarted"),

    #{execution_history := ExecutionHistory2} = ExtendedHistoryStats2 =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(
        ExtendedHistoryStats2, TaskType, #{restart => true}),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec2, ExecutionHistory2, #{restart_lane => LaneId}),

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys).


%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    workflow_scheduling_test_common:init_per_suite(Config).

end_per_suite(Config) ->
    workflow_scheduling_test_common:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    workflow_scheduling_test_common:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    workflow_scheduling_test_common:end_per_testcase(Case, Config).