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

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    empty_workflow_execution_test/1,
    empty_async_workflow_with_prepare_in_advance_test/1,

    single_sync_workflow_execution_test/1,
    single_async_workflow_execution_test/1,
    prepare_in_advance_test/1,
    heartbeat_test/1,
    long_prepare_in_advance_test/1,

    fail_only_task_in_lane_test/1,
    fail_only_task_in_box_test/1,
    fail_one_of_many_async_tasks_in_box_test/1,
    timeout_test/1,
    fail_result_processing_test/1,
    fail_task_before_prepare_in_advance_finish_test/1,
    fail_task_before_prepare_in_advance_fail_test/1,

    lane_preparation_failure_test/1,
    lane_preparation_in_advance_failure_test/1,
    fail_lane_preparation_before_prepare_in_advance_finish_test/1,
    delay_two_lanes_preparation_test/1,
    lane_execution_ended_handler_failure_test/1,
    lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test/1,
    
    change_prepared_in_advance_lane_test/1,
    prepare_lane_too_early_test/1,
    repeat_lane_test/1,
    repeat_and_change_prepared_in_advance_lane_test/1,
    change_prepared_in_advance_lane_with_delayed_lane_preparation_test/1,
    prepare_lane_too_early_with_long_callback_execution_test/1,
    repeat_lane_with_delayed_lane_preparation_test/1,
    repeat_and_change_prepared_in_advance_lane_with_delayed_lane_preparation_test/1,
    change_prepared_in_advance_lane_with_preparation_error_test/1,
    prepare_lane_too_early_with_preparation_error_test/1,
    change_prepared_in_advance_lane_with_delayed_failed_lane_preparation_test/1,
    prepare_lane_too_early_with_long_failed_callback_execution_test/1
]).

all() ->
    ?ALL([
        empty_workflow_execution_test,
        empty_async_workflow_with_prepare_in_advance_test,

        single_sync_workflow_execution_test,
        single_async_workflow_execution_test,
        prepare_in_advance_test,
        heartbeat_test,
        long_prepare_in_advance_test,

        fail_only_task_in_lane_test,
        fail_only_task_in_box_test,
        fail_one_of_many_async_tasks_in_box_test,
        timeout_test,
        fail_result_processing_test,
        fail_task_before_prepare_in_advance_finish_test,
        fail_task_before_prepare_in_advance_fail_test,

        lane_preparation_failure_test,
        lane_preparation_in_advance_failure_test,
        fail_lane_preparation_before_prepare_in_advance_finish_test,
        delay_two_lanes_preparation_test,
        lane_execution_ended_handler_failure_test,
        lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test,

        % TODO VFS-7784 - add test when lane is set to be prepared in advance twice
        % (callback should be called only once - test successful and failed execution)
        change_prepared_in_advance_lane_test,
        prepare_lane_too_early_test,
        repeat_lane_test,
        repeat_and_change_prepared_in_advance_lane_test,
        change_prepared_in_advance_lane_with_delayed_lane_preparation_test,
        prepare_lane_too_early_with_long_callback_execution_test,
        repeat_lane_with_delayed_lane_preparation_test,
        repeat_and_change_prepared_in_advance_lane_with_delayed_lane_preparation_test,
        change_prepared_in_advance_lane_with_preparation_error_test,
        prepare_lane_too_early_with_preparation_error_test,
        change_prepared_in_advance_lane_with_delayed_failed_lane_preparation_test,
        prepare_lane_too_early_with_long_failed_callback_execution_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

empty_workflow_execution_test(Config) ->
    empty_workflow_execution_test_base(Config, sync, false).

empty_async_workflow_with_prepare_in_advance_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, sleep_on_preparation, 500), % sleep to allow start preparation in advance
    empty_workflow_execution_test_base(Config, async, true).

%%%===================================================================

single_sync_workflow_execution_test(Config) ->
    single_full_execution_test_base(Config, sync, false).

single_async_workflow_execution_test(Config) ->
    single_full_execution_test_base(Config, async, false).

prepare_in_advance_test(Config) ->
    single_full_execution_test_base(Config, async, true).

heartbeat_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, delay_call, {<<"3_2_2">>, <<"100">>}),
    single_full_execution_test_base(Config, async, false).

long_prepare_in_advance_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    single_full_execution_test_base(Config, sync, true).

%%%===================================================================

fail_only_task_in_lane_test(Config) ->
    failure_test_base(Config, sync, true, <<"1">>, <<"1_1_1">>, fail_job).

fail_only_task_in_box_test(Config) ->
    failure_test_base(Config, sync, false, <<"3">>, <<"3_1_1">>, fail_job).

fail_one_of_many_async_tasks_in_box_test(Config) ->
    failure_test_base(Config, async, false, <<"3">>, <<"3_3_2">>, fail_job).

timeout_test(Config) ->
    failure_test_base(Config, async, false, <<"3">>, <<"3_3_1">>, timeout).

fail_result_processing_test(Config) ->
    failure_test_base(Config, async, false, <<"3">>, <<"3_2_1">>, fail_result_processing).

fail_task_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"4">>}, true),
    failure_test_base(Config, sync, true, <<"3">>, <<"3_1_1">>, fail_job).

fail_task_before_prepare_in_advance_fail_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, <<"3">>),
    failure_test_base(Config, sync, true, <<"2">>, <<"2_1_1">>, fail_job).

%%%===================================================================

lane_preparation_failure_test(Config) ->
    lane_failure_test_base(Config, false, fail_lane_preparation, expect_empty_items_list).

lane_preparation_in_advance_failure_test(Config) ->
    lane_failure_test_base(Config, true, fail_lane_preparation, fail_lane_preparation_in_advance).

fail_lane_preparation_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    lane_failure_test_base(Config, true, fail_lane_preparation, fail_delayed_lane_preparation_in_advance).

delay_two_lanes_preparation_test(Config) ->
    % TODO VFS-7784 - change prepare of lane 3 to be sync (not in advanced) - otherwise prepare of lane 4 does not start
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"4">>}, true),
    lane_failure_test_base(Config, true, fail_lane_preparation, fail_delayed_lane_preparation_in_advance).

lane_execution_ended_handler_failure_test(Config) ->
    % TODO VFS-7784 - do not skip items check when execution_ended_handler fails
    lane_failure_test_base(Config, false, fail_execution_ended_handler, stop_on_lane).

lane_execution_ended_handler_failure_before_prepare_in_advance_finish_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"4">>}, true),
    lane_failure_test_base(Config, true, fail_execution_ended_handler, stop_on_lane).

%%%===================================================================

change_prepared_in_advance_lane_test(Config) ->
    prepared_in_advance_lane_change_test_base(Config, #{prepare_ignored_lane_in_advance => true}, #{}).

prepare_lane_too_early_test(Config) ->
    prepared_in_advance_lane_change_test_base(Config, #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}}, #{}).

repeat_lane_test(Config) ->
    prepared_in_advance_lane_change_test_base(Config, #{repeat_lane => <<"2">>}, #{}).

repeat_and_change_prepared_in_advance_lane_test(Config) ->
    prepared_in_advance_lane_change_test_base(Config, #{repeat_lane_and_change_next => <<"2">>}, #{}).

change_prepared_in_advance_lane_with_delayed_lane_preparation_test(Config) ->
    % Note: prepare for ignored lane is executed in advance and only then lane is ignored
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(
        Config, {delay_lane_preparation, IgnoredLaneId}, true),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_ignored_lane_in_advance => true}, #{}).

prepare_lane_too_early_with_long_callback_execution_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"4">>}, true),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}}, #{}).

repeat_lane_with_delayed_lane_preparation_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    prepared_in_advance_lane_change_test_base(Config, #{repeat_lane => <<"2">>}, #{}).

repeat_and_change_prepared_in_advance_lane_with_delayed_lane_preparation_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"3">>}, true),
    % Note: prepare for ignored lane is executed in advance and only then lane is ignored
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(
        Config, {delay_lane_preparation, IgnoredLaneId}, true),
    prepared_in_advance_lane_change_test_base(Config, #{repeat_lane_and_change_next => <<"2">>}, #{}).

change_prepared_in_advance_lane_with_preparation_error_test(Config) ->
    % Note: prepare for ignored lane is executed in advance and only then lane is ignored
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, IgnoredLaneId),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_ignored_lane_in_advance => true}, #{}).

prepare_lane_too_early_with_preparation_error_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, <<"4">>),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}},
        #{fail_delayed_lane_preparation_in_advance => <<"4">>}).

change_prepared_in_advance_lane_with_delayed_failed_lane_preparation_test(Config) ->
    % Note: prepare for ignored lane is executed in advance and only then lane is ignored
    IgnoredLaneId = workflow_test_handler:get_ignored_lane_id(),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, IgnoredLaneId),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(
        Config, {delay_lane_preparation, IgnoredLaneId}, true),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_ignored_lane_in_advance => true}, #{}).

prepare_lane_too_early_with_long_failed_callback_execution_test(Config) ->
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, fail_lane_preparation, <<"4">>),
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, {delay_lane_preparation, <<"4">>}, true),
    prepared_in_advance_lane_change_test_base(Config, #{prepare_in_advance_out_of_order => {<<"2">>, <<"4">>}},
        #{fail_delayed_lane_preparation_in_advance => <<"4">>}).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

empty_workflow_execution_test_base(Config, WorkflowType, PrepareInAdvance) ->
    single_execution_test_base(Config, WorkflowType, PrepareInAdvance, #{items_count => 0}, #{is_empty => true}, #{}).

single_full_execution_test_base(Config, WorkflowType, PrepareInAdvance) ->
    single_execution_test_base(Config, WorkflowType, PrepareInAdvance, #{}, #{}, #{}).

failure_test_base(Config, WorkflowType, PrepareInAdvance, LaneId, TaskId, FailureOption) ->
    Item = <<"100">>,
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, FailureOption, {TaskId, Item}),
    single_execution_test_base(Config, WorkflowType, PrepareInAdvance,
        #{finish_on_lane => LaneId}, #{}, #{FailureOption => {LaneId, TaskId, Item}}).

lane_failure_test_base(Config, PrepareInAdvance, GathererOption, VerifyOptionKey) ->
    LaneId = <<"3">>,
    workflow_scheduling_test_utils:set_task_execution_gatherer_option(Config, GathererOption, LaneId),
    single_execution_test_base(Config, sync, PrepareInAdvance, #{}, #{}, #{VerifyOptionKey => LaneId}).

prepared_in_advance_lane_change_test_base(Config, GathererOptions, VerifyOptions) ->
    single_execution_test_base(Config, sync, true, GathererOptions, #{}, VerifyOptions).

single_execution_test_base(Config, WorkflowType, PrepareInAdvance, 
    GeneratorOptions, VerifyStatsOptions, VerifyHistoryOptions) ->
    InitialKeys = workflow_scheduling_test_utils:get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowExecutionSpec = workflow_scheduling_test_utils:gen_workflow_execution_spec(
        WorkflowType, PrepareInAdvance, GeneratorOptions),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, 
        [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec])),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = 
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_execution_history_stats(
        ExtendedHistoryStats, WorkflowType, VerifyStatsOptions),
    workflow_scheduling_test_utils:verify_execution_history(
        WorkflowExecutionSpec, ExecutionHistory, VerifyHistoryOptions),

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