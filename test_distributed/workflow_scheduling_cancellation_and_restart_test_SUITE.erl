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

-include("workflow_engine.hrl").
-include("workflow_scheduling_test_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test/1,
    sync_workflow_external_cancel_test/1,
    async_workflow_with_prepare_in_advance_external_cancel_test/1,
    async_workflow_external_cancel_during_report_async_task_result_test/1,
    async_workflow_external_cancel_during_result_processing_test/1,
    async_workflow_last_item_last_result_external_cancel/1,
    sync_workflow_last_item_middle_job_external_cancel/1,
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

    multiple_parallel_cancels_during_run_task_test/1,
    multiple_parallel_cancels_during_report_result_test/1,
    cancel_interrupted_by_exception_during_run_task_test/1,
    cancel_interrupted_by_exception_during_processing_result_test/1,

    resume_callback_failure_test/1
]).

all() ->
    ?ALL([
        % TODO VFS-7784 - test cancellation when next lane prepare in advance fails
        sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test,
        sync_workflow_external_cancel_test,
        async_workflow_with_prepare_in_advance_external_cancel_test,
        async_workflow_external_cancel_during_report_async_task_result_test,
        async_workflow_external_cancel_during_result_processing_test,
        async_workflow_last_item_last_result_external_cancel,
        sync_workflow_last_item_middle_job_external_cancel,
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

        multiple_parallel_cancels_during_run_task_test,
        multiple_parallel_cancels_during_report_result_test,
        cancel_interrupted_by_exception_during_run_task_test,
        cancel_interrupted_by_exception_during_processing_result_test,

        resume_callback_failure_test
    ]).
% TODO VFS-9993 - check resume with cancel after lane_ended callback (verify iterator snapshot)
% TODO VFS-9993 - check resume after prepare_lane fail (verify iterator snapshot)
% TODO VFS-9993 - check resume after cancel when all items are processed (verify iterator snapshot)
% TODO VFS-9993 - multiple resume tests (including cancel right after or during resume_lane callback)
% TODO VFS-9993 - test abandon

-record(test_config, {
    task_type = sync :: sync | async,
    prepare_in_advance = false :: boolean(),
    lane_id :: workflow_engine:lane_id(),
    test_execution_manager_option ::
        {workflow_scheduling_test_common:test_manager_task_failure_key(), workflow_engine:task_id()} |
        {cancel_execution, prepare_lane, workflow_engine:lane_id()} |
        {cancel_execution, run_task_for_item | report_async_task_result |
        process_task_result_for_item, workflow_engine:task_id()} |
        {init_cancel_procedure, run_task_for_item | report_async_task_result |
        process_task_result_for_item, workflow_engine:task_id(), CallsNum :: non_neg_integer()} |
        {fail_stream_termination, {workflow_engine:task_id(), handle_task_results_processed_for_all_items}},
    generator_options = #{} :: workflow_test_handler:generator_options(),
    verify_history_options = #{} :: #{workflow_scheduling_test_common:lane_history_check_key() => workflow_engine:lane_id()}
}).


%%%===================================================================
%%% Test functions
%%%===================================================================

sync_workflow_external_cancel_during_execution_of_the_only_task_of_lane_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        lane_id = <<"1">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"1_1_1">>}
    }).

sync_workflow_external_cancel_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"3_3_1">>}
    }).

async_workflow_with_prepare_in_advance_external_cancel_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"3_3_2">>}
    }).

async_workflow_external_cancel_during_report_async_task_result_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"1">>,
        test_execution_manager_option = {cancel_execution, report_async_task_result, <<"1_1_1">>}
    }).

async_workflow_external_cancel_during_result_processing_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, process_task_result_for_item, <<"3_2_1">>}
    }).

async_workflow_last_item_last_result_external_cancel(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        % There are 3 parallel tasks in last parallel box - sleep before cancel to allow 2 other tasks end
        test_execution_manager_option = {sleep_and_cancel_execution, process_task_result_for_item, <<"3_3_3">>, <<"200">>, 5000}
    }).

sync_workflow_last_item_middle_job_external_cancel(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        lane_id = <<"3">>,
        % Sleep before cancel to allow other parallel items end
        test_execution_manager_option = {sleep_and_cancel_execution, run_task_for_item, <<"3_2_2">>, <<"200">>, 5000},
        generator_options = ?EXEMPLARY_STREAMS
    }).

internal_cancel_caused_by_sync_job_error_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>}
    }).

internal_cancel_caused_by_async_job_error_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>}
    }).

internal_cancel_caused_by_async_job_timeout_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_result_processing, <<"3_3_1">>}
    }).

internal_cancel_caused_by_result_processing_timeout_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_result_processing, <<"3_3_1">>}
    }).

external_cancel_during_lane_prepare_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, prepare_lane, <<"3">>}
    }).

external_cancel_during_lane_prepare_in_advance_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        prepare_in_advance = true,
        lane_id = <<"2">>,
        test_execution_manager_option = {cancel_execution, prepare_lane, <<"3">>}
    }).

internal_cancel_caused_by_async_job_timeout_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, {delay_lane_preparation, <<"2">>}, true),
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"1">>,
        test_execution_manager_option = {timeout, <<"1_1_1">>}
    }).

async_workflow_with_streams_external_cancel(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"3_2_1">>},
        generator_options = ?EXEMPLARY_STREAMS
    }).

async_workflow_with_streams_and_prepare_in_advance_external_cancel(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"3_2_1">>},
        generator_options = ?EXEMPLARY_STREAMS
    }).

async_workflow_with_streams_and_long_lasting_prepare_in_advance_external_cancel(Config) ->
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, {delay_lane_preparation, <<"4">>}, true),
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        lane_id = <<"3">>,
        test_execution_manager_option = {cancel_execution, run_task_for_item, <<"3_2_1">>},
        generator_options = ?EXEMPLARY_STREAMS
    }).

internal_cancel_of_workflow_with_stream_caused_by_async_job_error_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_job, <<"3_3_2">>},
        generator_options = ?EXEMPLARY_STREAMS2
    }).

internal_cancel_caused_by_stream_data_processing_error_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {fail_task_data_processing, <<"3_3_2">>},
        generator_options = ?EXEMPLARY_STREAMS2
    }).

internal_cancel_caused_by_stream_closing_error_test(Config) ->
    cancel_and_resume_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option =
            {fail_stream_termination, {<<"3_2_1">>, handle_task_results_processed_for_all_items}},
        generator_options = ?EXEMPLARY_STREAMS_WITH_TERMINATION_ERROR
    }).

%%%===================================================================

multiple_parallel_cancels_during_run_task_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    AfterFirstExecutionCallback = fun(ExecutionId) ->
        rpc:call(Worker, workflow_engine, finish_cancel_procedure, [ExecutionId])
    end,
    multiple_parallel_cancels_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {init_cancel_procedure, {run_task_for_item, <<"3_3_2">>, <<"100">>, 3}},
        verify_history_options = #{stop_on_lane => <<"3">>}
    }, AfterFirstExecutionCallback).

multiple_parallel_cancels_during_report_result_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    AfterFirstExecutionCallback = fun(ExecutionId) ->
        rpc:call(Worker, workflow_engine, finish_cancel_procedure, [ExecutionId])
    end,
    multiple_parallel_cancels_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {init_cancel_procedure, {report_async_task_result, <<"3_3_2">>, <<"100">>, 3}},
        prepare_in_advance = true,
        generator_options = ?EXEMPLARY_STREAMS,
        verify_history_options = #{stop_on_lane => <<"3">>}
    }, AfterFirstExecutionCallback).


cancel_interrupted_by_exception_during_run_task_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    AfterFirstExecutionCallback = fun(ExecutionId) ->
        rpc:call(Worker, workflow_engine, init_cancel_procedure, [ExecutionId])
    end,
    multiple_parallel_cancels_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {init_cancel_procedure_and_throw, {run_task_for_item, <<"3_3_2">>, <<"100">>}},
        generator_options = ?EXEMPLARY_STREAMS,
        verify_history_options = #{expect_exception => <<"3">>}
    }, AfterFirstExecutionCallback).


cancel_interrupted_by_exception_during_processing_result_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    AfterFirstExecutionCallback = fun(ExecutionId) ->
        rpc:call(Worker, workflow_engine, init_cancel_procedure, [ExecutionId])
    end,
    multiple_parallel_cancels_test_base(Config, #test_config{
        task_type = async,
        lane_id = <<"3">>,
        test_execution_manager_option = {init_cancel_procedure_and_throw, {process_task_result_for_item, <<"3_3_2">>, <<"100">>}},
        generator_options = ?EXEMPLARY_STREAMS,
        verify_history_options = #{expect_exception => <<"3">>}
    }, AfterFirstExecutionCallback).


%%%===================================================================

resume_callback_failure_test(Config) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),
    TaskType = sync,
    PrepareInAdvance = false,
    LaneId = <<"3">>,

    [Worker | _] = ?config(op_worker_nodes, Config),
    #{id := ExecutionId} = WorkflowExecutionSpec =
        workflow_scheduling_test_common:gen_workflow_execution_spec(TaskType, PrepareInAdvance,
            #{lane_options => #{failure_count_to_cancel => 1}, progress_data_persistence => save_progress}),
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, fail_job, {<<"3_1_1">>, <<"100">>}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(ExtendedHistoryStats, TaskType),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec, ExecutionHistory, #{stop_on_lane => LaneId}),
    workflow_scheduling_test_common:verify_memory(Config, InitialKeys, true),

    % Same callback is used to resume and prepare lane so set lane preparation to be failed
    workflow_scheduling_test_common:set_test_execution_manager_option(Config, fail_lane_preparation, LaneId),
    % execute_workflow always return ok - failure of resume results in immediate end of workflow execution
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),
    #{execution_history := ExecutionHistory2} = workflow_scheduling_test_common:get_task_execution_history(Config),
    % If resume fails, no task should be executed
    workflow_scheduling_test_common:verify_empty_lane(ExecutionHistory2, LaneId),

    WorkflowExecutionSpec2 = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow resumed"),

    #{execution_history := ExecutionHistory3} = ExtendedHistoryStats3 =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(ExtendedHistoryStats3, TaskType),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec2, ExecutionHistory3, #{resume_lane => LaneId}),

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

cancel_and_resume_test_base(Config, TestConfig) ->
    ct:print("Test resume from iterator"),
    cancel_and_resume_test_base(Config, TestConfig, from_iterator),
    ct:print("Test resume from dump"),
    cancel_and_resume_test_base(Config, TestConfig, from_dump).

cancel_and_resume_test_base(Config, #test_config{
    task_type = TaskType,
    prepare_in_advance = PrepareInAdvance,
    lane_id = LaneId,
    test_execution_manager_option = TestExecutionManagerOption,
    generator_options = GeneratorOptions
}, ResumeType) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    LaneOptions = case TestExecutionManagerOption of
        {cancel_execution, _, _} -> #{};
        {sleep_and_cancel_execution, _, _, _, _} -> #{};
        _ -> #{failure_count_to_cancel => 1}
    end,
    {SnapshotMode, DataPersistence} = case ResumeType of
        from_dump -> {?ALL_ITEMS, save_progress};
        from_iterator -> {?UNTIL_FIRST_FAILURE, save_iterator}
    end,
    #{id := ExecutionId} = WorkflowExecutionSpec = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType,
        PrepareInAdvance,
        GeneratorOptions#{
            lane_options => LaneOptions, progress_data_persistence => DataPersistence, snapshot_mode => SnapshotMode
        }
    ),
    {TestExecutionManagerOptionKey, TestExecutionManagerOptionValue} = case TestExecutionManagerOption of
        {cancel_execution, prepare_lane, LaneIdToCancel} ->
            {cancel_execution, {prepare_lane, LaneIdToCancel}};
        {cancel_execution, Function, Task} ->
            {cancel_execution, {Function, Task, <<"100">>}};
        {sleep_and_cancel_execution, Function, Task, Item, SleepTime} ->
            {sleep_and_cancel_execution, {Function, Task, Item, SleepTime}};
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
        sleep_and_cancel_execution -> ?assertMatch(#{cancel_ans := ok}, ExtendedHistoryStats);
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
        {sleep_and_cancel_execution, _, _, _, _} ->
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

    ResumeWorkflowExecutionSpec = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, GeneratorOptions#{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), ResumeWorkflowExecutionSpec])),
    ct:print("Workflow resumed"),

    #{execution_history := ExecutionHistoryAfterResume} = ExtendedHistoryStatsAfterResume =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(ExtendedHistoryStatsAfterResume, TaskType),
    workflow_scheduling_test_common:verify_execution_history(
        ResumeWorkflowExecutionSpec, ExecutionHistoryAfterResume, #{resume_lane => LaneId}),

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys),

    case ResumeType of
        from_dump ->
            ct:print("Verifying combined history"),
            FilteredExecutionHistory = workflow_scheduling_test_common:filter_finish_and_exception_handlers(
                ExecutionHistory, LaneId),
            FilteredExecutionHistory2 = workflow_scheduling_test_common:filter_prepare_in_adnave_handler(
                FilteredExecutionHistory, LaneId, PrepareInAdvance),
            FilteredExecutionHistoryAfterResume = workflow_scheduling_test_common:check_prepare_lane_in_head_and_filter(
                ExecutionHistoryAfterResume, LaneId),
            FinalVerifyOptions = case {TestExecutionManagerOption, TaskType} of
                {{fail_job, TId}, async} -> GeneratorOptions#{fail_and_resume_job => {LaneId, TId, <<"100">>}};
                _ -> GeneratorOptions#{}
            end,
            MergedExecutionHistory  = workflow_scheduling_test_common:filter_repeated_stream_callbacks(
                FilteredExecutionHistory2 ++ FilteredExecutionHistoryAfterResume, LaneId, GeneratorOptions),
            workflow_scheduling_test_common:verify_execution_history(
                WorkflowExecutionSpec, MergedExecutionHistory, FinalVerifyOptions);
        from_iterator ->
            ok
    end.


multiple_parallel_cancels_test_base(Config, #test_config{
    task_type = TaskType,
    prepare_in_advance = PrepareInAdvance,
    lane_id = LaneId,
    test_execution_manager_option = TestExecutionManagerOption,
    generator_options = GeneratorOptions,
    verify_history_options = VerifyHistoryOptions
}, AfterFirstExecutionCallback) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),
    [Worker | _] = ?config(op_worker_nodes, Config),

    #{id := ExecutionId} = WorkflowExecutionSpec = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, GeneratorOptions#{progress_data_persistence => save_progress}),
    {TestExecutionManagerOptionKey, TestExecutionManagerOptionValue} = TestExecutionManagerOption,
    workflow_scheduling_test_common:set_test_execution_manager_option(
        Config, TestExecutionManagerOptionKey, TestExecutionManagerOptionValue),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    ?assertMatch(#{cancel_ans := ok}, ExtendedHistoryStats),
    workflow_scheduling_test_common:verify_execution_history_stats(
        ExtendedHistoryStats, TaskType, #{ignore_async_slots_check => true}),
    ?assertNot(workflow_scheduling_test_common:has_any_finish_callback_for_lane(ExecutionHistory, LaneId)),
    HasExceptionCallback = case VerifyHistoryOptions of
        #{expect_exception := _} -> true;
        _ -> false
    end,
    ?assertEqual(HasExceptionCallback, workflow_scheduling_test_common:has_exception_callback(ExecutionHistory)),

    AfterFirstExecutionCallback(ExecutionId),
    rpc:call(Worker, workflow_engine, finish_cancel_procedure, [ExecutionId]),
    #{execution_history := ExecutionHistory2} = workflow_scheduling_test_common:get_task_execution_history(Config),
    ?assertEqual([], ExecutionHistory2),

    rpc:call(Worker, workflow_engine, finish_cancel_procedure, [ExecutionId]),
    #{execution_history := ExecutionHistory3} =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history(WorkflowExecutionSpec,
        ExecutionHistory ++ ExecutionHistory2 ++ ExecutionHistory3, VerifyHistoryOptions),
    workflow_scheduling_test_common:verify_memory(Config, InitialKeys, true),

    WorkflowExecutionSpec2 = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, #{first_lane_id => LaneId}, ExecutionId),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow,
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec2])),
    ct:print("Workflow resumed"),

    #{execution_history := FinalExecutionHistory} = FinalExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(FinalExtendedHistoryStats, TaskType),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec2, FinalExecutionHistory, #{resume_lane => LaneId}),

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