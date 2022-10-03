%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test of workflow scheduling.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_scheduling_test_SUITE).
-author("Michal Wrzeszcz").

-include("workflow_scheduling_test_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    empty_workflow_execution_test/1,
    empty_async_workflow_with_prepare_in_advance_test/1,
    empty_workflow_with_stream_execution_test/1,
    empty_async_workflow_with_stream_and_prepare_in_advance_test/1,

    single_sync_workflow_execution_test/1,
    single_async_workflow_execution_test/1,
    single_async_workflow_with_empty_streams_execution_test/1,
    single_async_workflow_with_streams_execution_test/1,
    prepare_in_advance_test/1,
    heartbeat_test/1,
    async_task_enqueuing_test/1,
    long_prepare_in_advance_test/1,

    fail_the_only_task_in_lane_test/1,
    fail_the_only_task_in_box_test/1,
    fail_one_of_many_async_tasks_in_box_test/1,
    fail_one_of_many_async_tasks_in_workflow_with_streams_test/1,
    fail_one_of_many_async_tasks_in_single_item_workflow_test/1,
    async_task_timeout_test/1,
    fail_result_processing_test/1,
    fail_task_before_prepare_in_advance_finish_test/1,
    fail_task_before_prepare_in_advance_fail_test/1,
    fail_iteration_test/1,
    fail_iteration_with_prepare_in_advance_test/1,
    fail_iteration_with_stream_test/1,
    fail_first_item_iteration_test/1,
    fail_first_item_iteration_with_prepare_in_advance_test/1,
    fail_first_item_iteration_with_stream_test/1,

    exception_during_run_task_test/1,
    exception_during_processing_result_test/1,

    lane_preparation_failure_test/1,
    lane_preparation_in_advance_failure_test/1,
    fail_lane_preparation_before_prepare_in_advance_finish_test/1,
    long_lasting_lane_preparation_of_two_lanes_test/1,
    lane_execution_ended_handler_failure_test/1,
    lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test/1,
    lane_preparation_exception_test/1,
    lane_preparation_in_advance_exception_test/1,
    
    execute_other_lane_than_the_one_prepared_in_advance_test/1,
    reuse_already_prepared_lane_test/1,
    retry_lane_test/1,
    retry_and_execute_other_lane_than_the_one_prepared_in_advance_test/1,
    execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test/1,
    prepare_lane_too_early_with_long_callback_execution_test/1,
    retry_lane_with_long_lasting_lane_preparation_test/1,
    retry_and_execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test/1,
    execute_other_lane_than_the_one_prepared_in_advance_with_preparation_error_test/1,
    prepare_lane_too_early_with_preparation_error_test/1,
    execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_failed_lane_preparation_test/1,
    prepare_lane_too_early_with_long_failed_callback_execution_test/1
]).

all() ->
    ?ALL([
        empty_workflow_execution_test,
        empty_async_workflow_with_prepare_in_advance_test,
        empty_workflow_with_stream_execution_test,
        empty_async_workflow_with_stream_and_prepare_in_advance_test,

        single_sync_workflow_execution_test,
        single_async_workflow_execution_test,
        single_async_workflow_with_empty_streams_execution_test,
        single_async_workflow_with_streams_execution_test,
        prepare_in_advance_test,
        heartbeat_test,
        async_task_enqueuing_test,
        long_prepare_in_advance_test,

        fail_the_only_task_in_lane_test,
        fail_the_only_task_in_box_test,
        fail_one_of_many_async_tasks_in_box_test,
        fail_one_of_many_async_tasks_in_workflow_with_streams_test,
        fail_one_of_many_async_tasks_in_single_item_workflow_test,
        async_task_timeout_test,
        fail_result_processing_test,
        fail_task_before_prepare_in_advance_finish_test,
        fail_task_before_prepare_in_advance_fail_test,
        fail_iteration_test,
        fail_iteration_with_prepare_in_advance_test,
        fail_iteration_with_stream_test,
        fail_first_item_iteration_test,
        fail_first_item_iteration_with_prepare_in_advance_test,
        fail_first_item_iteration_with_stream_test,

        exception_during_run_task_test,
        exception_during_processing_result_test,

        lane_preparation_failure_test,
        lane_preparation_in_advance_failure_test,
        fail_lane_preparation_before_prepare_in_advance_finish_test,
        long_lasting_lane_preparation_of_two_lanes_test,
        lane_execution_ended_handler_failure_test,
        lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test,
        lane_preparation_exception_test,
        lane_preparation_in_advance_exception_test,

        % TODO VFS-7784 - add test when lane is set to be prepared in advance twice
        % (callback should be called only once - test successful and failed execution)
        execute_other_lane_than_the_one_prepared_in_advance_test,
        reuse_already_prepared_lane_test,
        retry_lane_test,
        retry_and_execute_other_lane_than_the_one_prepared_in_advance_test,
        execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test,
        prepare_lane_too_early_with_long_callback_execution_test,
        retry_lane_with_long_lasting_lane_preparation_test,
        retry_and_execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test,
        execute_other_lane_than_the_one_prepared_in_advance_with_preparation_error_test,
        prepare_lane_too_early_with_preparation_error_test,
        execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_failed_lane_preparation_test,
        prepare_lane_too_early_with_long_failed_callback_execution_test
    ]).


-record(test_config, {
    task_type = sync :: sync | async,
    prepare_in_advance = false :: boolean(),
    test_manager_failure_key = undefined :: workflow_scheduling_test_common:test_manager_task_failure_key(),
    test_execution_manager_options = [] :: {fail_lane_preparation, workflow_engine:lane_id()} |
        {{delay_lane_preparation, workflow_engine:lane_id()}, boolean()} |
        {delay_call, {workflow_engine:task_id(), iterator:item()}} | {sleep_on_preparation, non_neg_integer()},
    generator_options = #{} :: workflow_test_handler:generator_options(),
    verify_statistics_options = #{} :: #{is_empty => boolean()},
    verify_history_options = #{} :: #{
        delay_and_fail_lane_preparation_in_advance => workflow_engine:lane_id(),
        workflow_scheduling_test_common:test_manager_task_failure_key() =>
            {workflow_engine:lane_id(), workflow_engine:task_id(), iterator:item()},
        workflow_scheduling_test_common:lane_history_check_key() => workflow_engine:lane_id()
    },
    restart_doc_present = false :: boolean()
}).



%%%===================================================================
%%% Test functions
%%%===================================================================

empty_workflow_execution_test(Config) ->
    empty_workflow_execution_test_base(Config, #test_config{}).

empty_async_workflow_with_prepare_in_advance_test(Config) ->
    empty_workflow_execution_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        test_execution_manager_options = [{sleep_on_preparation, 500}] % sleep to allow start preparation in advance
    }).


empty_workflow_with_stream_execution_test(Config) ->
    empty_workflow_execution_test_base(Config, #test_config{
        generator_options = ?EXEMPLARY_EMPTY_STREAM
    }).

empty_async_workflow_with_stream_and_prepare_in_advance_test(Config) ->
    empty_workflow_execution_test_base(Config, #test_config{
        task_type = async,
        generator_options = ?EXEMPLARY_EMPTY_STREAM,
        prepare_in_advance = true,
        test_execution_manager_options = [{sleep_on_preparation, 500}] % sleep to allow start preparation in advance
    }).

%%%===================================================================

single_sync_workflow_execution_test(Config) ->
    single_execution_test_base(Config, #test_config{}).

single_async_workflow_execution_test(Config) ->
    single_execution_test_base(Config, #test_config{task_type = async}).


single_async_workflow_with_empty_streams_execution_test(Config) ->
    single_execution_test_base(Config, #test_config{
        task_type = async,
        generator_options = ?EXEMPLARY_EMPTY_STREAMS
    }).

single_async_workflow_with_streams_execution_test(Config) ->
    single_execution_test_base(Config, #test_config{
        task_type = async,
        generator_options = ?EXEMPLARY_STREAMS
    }).

prepare_in_advance_test(Config) ->
    single_execution_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true
    }).

heartbeat_test(Config) ->
    single_execution_test_base(Config, #test_config{
        task_type = async,
        test_execution_manager_options = [{delay_call, {<<"3_2_2">>, <<"100">>}}]
    }).

async_task_enqueuing_test(Config) ->
    single_execution_test_base(Config, #test_config{
        task_type = async,
        % First heartbeat for task <<"3_2_2">> for item <<"100">> will appear after 8 seconds while keepalive_timeout
        % is 5 seconds - the workflow should not fail because of set_enqueuing_timeout set to 20 seconds
        % in init_per_testcase
        test_execution_manager_options = [{delay_call, {<<"3_2_2">>, <<"100">>, timer:seconds(8)}}]
    }).

long_prepare_in_advance_test(Config) ->
    single_execution_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_execution_manager_options = [{{delay_lane_preparation, <<"3">>}, true}]
    }).

%%%===================================================================

fail_the_only_task_in_lane_test(Config) ->
    failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_job
    }, <<"1">>, <<"1_1_1">>).

fail_the_only_task_in_box_test(Config) ->
    failure_test_base(Config, #test_config{test_manager_failure_key = fail_job}, <<"3">>, <<"3_1_1">>).

fail_one_of_many_async_tasks_in_box_test(Config) ->
    failure_test_base(Config, #test_config{
        task_type = async,
        test_manager_failure_key = fail_job
    }, <<"3">>, <<"3_3_2">>).

fail_one_of_many_async_tasks_in_workflow_with_streams_test(Config) ->
    failure_test_base(Config, #test_config{
        task_type = async,
        test_manager_failure_key = fail_job,
        generator_options = ?EXEMPLARY_STREAMS2
    }, <<"3">>, <<"3_3_2">>).

fail_one_of_many_async_tasks_in_single_item_workflow_test(Config) ->
    failure_test_base(Config, #test_config{
        task_type = async,
        test_manager_failure_key = fail_job,
        generator_options = #{item_count => 1},
        verify_statistics_options = #{ignore_max_slots_check => true}
    }, <<"3">>, <<"3_2_1">>, <<"1">>).

async_task_timeout_test(Config) ->
    failure_test_base(Config, #test_config{
        task_type = async,
        test_manager_failure_key = timeout
    }, <<"3">>, <<"3_3_1">>).

fail_result_processing_test(Config) ->
    failure_test_base(Config, #test_config{
        task_type = async,
        test_manager_failure_key = fail_result_processing
    }, <<"3">>, <<"3_2_1">>).

fail_task_before_prepare_in_advance_finish_test(Config) ->
    failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_job,
        test_execution_manager_options = [{{delay_lane_preparation, <<"4">>}, true}]
    }, <<"3">>, <<"3_1_1">>).

fail_task_before_prepare_in_advance_fail_test(Config) ->
    failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_job,
        test_execution_manager_options = [{{delay_lane_preparation, <<"3">>}, true}, {fail_lane_preparation, <<"3">>}]
    }, <<"2">>, <<"2_1_1">>).

fail_iteration_test(Config) ->
    iteration_failure_test_base(Config, #test_config{}, <<"1">>, 5).

fail_iteration_with_prepare_in_advance_test(Config) ->
    iteration_failure_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true
    }, <<"1">>, 5).

fail_iteration_with_stream_test(Config) ->
    iteration_failure_test_base(Config, #test_config{
        task_type = async,
        generator_options = #{task_streams => #{
            1 => #{
                {1,1} => [<<"2">>, <<"4">>]
            }
        }}
    }, <<"1">>, 5).

fail_first_item_iteration_test(Config) ->
    iteration_failure_test_base(Config, #test_config{}, <<"1">>, 1).

fail_first_item_iteration_with_prepare_in_advance_test(Config) ->
    iteration_failure_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true
    }, <<"1">>, 1).

fail_first_item_iteration_with_stream_test(Config) ->
    iteration_failure_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = true,
        generator_options = ?EXEMPLARY_EMPTY_STREAM
    }, <<"1">>, 1).


%%%===================================================================

exception_during_run_task_test(Config) ->
    exception_test_base(Config, run_task_for_item).

exception_during_processing_result_test(Config) ->
    exception_test_base(Config, process_task_result_for_item).

%%%===================================================================

lane_preparation_failure_test(Config) ->
    lane_failure_test_base(Config,
        #test_config{test_manager_failure_key = fail_lane_preparation}, expect_empty_items_list).

lane_preparation_in_advance_failure_test(Config) ->
    lane_failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_lane_preparation
    }, fail_lane_preparation_in_advance).

fail_lane_preparation_before_prepare_in_advance_finish_test(Config) ->
    lane_failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_lane_preparation,
        test_execution_manager_options = [{{delay_lane_preparation, <<"3">>}, true}]
    }, delay_and_fail_lane_preparation_in_advance).

long_lasting_lane_preparation_of_two_lanes_test(Config) ->
    % TODO VFS-7784 - change prepare of lane 3 to be sync (not in advanced) - otherwise prepare of lane 4 does not start
    lane_failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_lane_preparation,
        test_execution_manager_options = [
            {{delay_lane_preparation, <<"3">>}, true},
            {{delay_lane_preparation, <<"4">>}, true}
        ]
    }, delay_and_fail_lane_preparation_in_advance).

lane_execution_ended_handler_failure_test(Config) ->
    % TODO VFS-7784 - do not skip items check when execution_ended_handler fails
    lane_failure_test_base(Config, #test_config{test_manager_failure_key = fail_execution_ended_handler}, stop_on_lane).

lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test(Config) ->
    lane_failure_test_base(Config, #test_config{
        prepare_in_advance = true,
        test_manager_failure_key = fail_execution_ended_handler,
        test_execution_manager_options = [{{delay_lane_preparation, <<"4">>}, true}]
    }, stop_on_lane).

lane_preparation_exception_test(Config) ->
    lane_preparation_exception_test_base(Config, <<"3">>, false).

lane_preparation_in_advance_exception_test(Config) ->
    lane_preparation_exception_test_base(Config, <<"4">>, true).

%%%===================================================================

execute_other_lane_than_the_one_prepared_in_advance_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        generator_options = #{prepare_ignored_lane_in_advance => true}
    }).

reuse_already_prepared_lane_test(Config) ->
    % Test verifies if lane that was prepared in advance and than scheduled for
    % preparation in advance second time executes prepare_lane callback only once
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        generator_options = #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}}
    }).

retry_lane_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        generator_options = #{lane_to_retry => <<"2">>}
    }).

retry_and_execute_other_lane_than_the_one_prepared_in_advance_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        generator_options = #{lane_to_retry => <<"2">>, prepare_ignored_lane_in_advance => true}
    }).

execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test(Config) ->
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [{{delay_lane_preparation, IgnoredLaneId}, true}],
        generator_options = #{prepare_ignored_lane_in_advance => true}
    }).

prepare_lane_too_early_with_long_callback_execution_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [{{delay_lane_preparation, <<"4">>}, true}],
        generator_options = #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}}
    }).

retry_lane_with_long_lasting_lane_preparation_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [{{delay_lane_preparation, <<"3">>}, true}],
        generator_options = #{lane_to_retry => <<"2">>}
    }).

retry_and_execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_lane_preparation_test(Config) ->
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [
            {{delay_lane_preparation, <<"3">>}, true},
            {{delay_lane_preparation, IgnoredLaneId}, true}
        ],
        generator_options = #{lane_to_retry => <<"2">>, prepare_ignored_lane_in_advance => true}
    }).

execute_other_lane_than_the_one_prepared_in_advance_with_preparation_error_test(Config) ->
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [{fail_lane_preparation, IgnoredLaneId}],
        generator_options = #{prepare_ignored_lane_in_advance => true}
    }).

prepare_lane_too_early_with_preparation_error_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [{fail_lane_preparation, <<"4">>}],
        generator_options = #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}},
        verify_history_options = #{delay_and_fail_lane_preparation_in_advance => <<"4">>}
    }).

execute_other_lane_than_the_one_prepared_in_advance_with_long_lasting_failed_lane_preparation_test(Config) ->
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [
            {fail_lane_preparation, IgnoredLaneId},
            {{delay_lane_preparation, IgnoredLaneId}, true}
        ],
        generator_options = #{prepare_ignored_lane_in_advance => true}
    }).

prepare_lane_too_early_with_long_failed_callback_execution_test(Config) ->
    execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, #test_config{
        test_execution_manager_options = [
            {fail_lane_preparation, <<"4">>},
            {{delay_lane_preparation, <<"4">>}, true}
        ],
        generator_options = #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}},
        verify_history_options = #{delay_and_fail_lane_preparation_in_advance => <<"4">>}
    }).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

empty_workflow_execution_test_base(Config, BasicConfig) ->
    single_execution_test_base(Config, BasicConfig#test_config{
        generator_options = #{item_count => 0},
        verify_statistics_options = #{is_empty => true}
    }).

failure_test_base(Config, TestConfig, LaneId, TaskId) ->
    failure_test_base(Config, TestConfig, LaneId, TaskId, <<"100">>).

failure_test_base(Config, #test_config{
    test_manager_failure_key = ManagerKey,
    test_execution_manager_options = ManagerOptions,
    generator_options = GeneratorOptions
} = BasicConfig, LaneId, TaskId, Item) ->
    single_execution_test_base(Config, BasicConfig#test_config{
        test_execution_manager_options = [{ManagerKey, {TaskId, Item}} | ManagerOptions],
        generator_options = GeneratorOptions#{finish_on_lane => LaneId},
        verify_history_options = #{ManagerKey => {LaneId, TaskId, Item}}
    }).

iteration_failure_test_base(Config, #test_config{
    verify_statistics_options = VerifyStatsOptions,
    generator_options = GeneratorOptions,
    verify_history_options = VerifyHistoryOptions
} = BasicConfig, LaneId, ItemNum) ->
    single_execution_test_base(Config, BasicConfig#test_config{
        verify_statistics_options = VerifyStatsOptions#{ignore_max_slots_check => true},
        generator_options = GeneratorOptions#{fail_iteration => ItemNum, finish_on_lane => LaneId},
        verify_history_options = VerifyHistoryOptions#{expect_exception => LaneId},
        restart_doc_present = true
    }).

exception_test_base(Config, CallbackToThrow) ->
    ExecutionHistory = single_execution_test_base(Config, #test_config{
        task_type = async,
        generator_options = ?EXEMPLARY_STREAMS,
        test_execution_manager_options = [{throw_error, {CallbackToThrow, <<"3_3_2">>, <<"100">>}}],
        verify_statistics_options = #{ignore_async_slots_check => true},
        verify_history_options = #{expect_exception => <<"3">>},
        restart_doc_present = true
    }),
% TODO - sprawdzic resume'a
    ?assertNot(workflow_scheduling_test_common:has_finish_callbacks_for_lane(ExecutionHistory, <<"3">>)).

lane_failure_test_base(Config, #test_config{
    test_manager_failure_key = ManagerKey,
    test_execution_manager_options = ManagerOptions
} = BasicConfig, VerifyOptionKey) ->
    LaneId = <<"3">>,
    single_execution_test_base(Config, BasicConfig#test_config{
        test_execution_manager_options = [{ManagerKey, LaneId} | ManagerOptions],
        verify_history_options = #{VerifyOptionKey => LaneId}
    }).

lane_preparation_exception_test_base(Config, LineToThrow, PrepareInAdvance) ->
    ExecutionHistory = single_execution_test_base(Config, #test_config{
        task_type = async,
        prepare_in_advance = PrepareInAdvance,
        generator_options = ?EXEMPLARY_STREAMS,
        test_execution_manager_options = [{throw_error, LineToThrow}],
        verify_statistics_options = #{ignore_async_slots_check => true},
        verify_history_options = #{expect_exception => <<"3">>},
        restart_doc_present = true
    }),

    ?assertNot(workflow_scheduling_test_common:has_finish_callbacks_for_lane(ExecutionHistory, <<"3">>)).

execute_other_lane_than_the_one_prepared_in_advance_test_base(Config, BasicConfig) ->
    single_execution_test_base(Config, BasicConfig#test_config{prepare_in_advance = true}).

single_execution_test_base(Config, #test_config{
    task_type = TaskType,
    prepare_in_advance = PrepareInAdvance,
    test_execution_manager_options = ManagerOptions,
    generator_options = GeneratorOptions,
    verify_statistics_options = VerifyStatsOptions,
    verify_history_options = VerifyHistoryOptions,
    restart_doc_present = RestartDocPresent
}) ->
    workflow_scheduling_test_common:set_test_execution_manager_options(Config, ManagerOptions),
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowExecutionSpec = workflow_scheduling_test_common:gen_workflow_execution_spec(
        TaskType, PrepareInAdvance, GeneratorOptions),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, 
        [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = 
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(
        ExtendedHistoryStats, TaskType, VerifyStatsOptions),
    workflow_scheduling_test_common:verify_execution_history(
        WorkflowExecutionSpec, ExecutionHistory, VerifyHistoryOptions),

    workflow_scheduling_test_common:verify_memory(Config, InitialKeys, RestartDocPresent),
    ExecutionHistory.


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