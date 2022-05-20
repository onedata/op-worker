%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test of multiple workflows parallel scheduling. Each test starts many executions of workflow in parallel.
%%% The tests use different types of tasks (sync vs async) and differently prepare lanes.
%%% @end
%%%-------------------------------------------------------------------
-module(multiple_workflow_scheduling_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    sync_task_execution_test/1,
    async_task_execution_test/1,
    sync_task_with_lanes_prepare_in_advance_execution_test/1
]).

all() ->
    ?ALL([
        sync_task_execution_test,
        async_task_execution_test,
        sync_task_with_lanes_prepare_in_advance_execution_test
    ]).


-record(test_config, {
    task_type = sync :: sync | async,
    prepare_in_advance = false :: boolean(),
    item_count :: non_neg_integer()
}).


%%%===================================================================
%%% Test functions
%%%===================================================================

sync_task_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, #test_config{item_count = 200}).

async_task_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, #test_config{
        task_type = async,
        item_count = 1000
    }).

sync_task_with_lanes_prepare_in_advance_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, #test_config{
        prepare_in_advance = true,
        item_count = 100
    }).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

multiple_workflow_execution_test_base(Config, #test_config{
    task_type = TaskType,
    prepare_in_advance = PrepareInAdvance,
    item_count = ItemCount
}) ->
    InitialKeys = workflow_scheduling_test_common:get_all_workflow_related_datastore_keys(Config),

    WorkflowExecutionSpecs = lists:map(fun(_) ->
        #{id := Id} = Spec = workflow_scheduling_test_common:gen_workflow_execution_spec(
            TaskType, PrepareInAdvance, #{item_count => ItemCount}),
        {Id, Spec}
    end, lists:seq(1, 5)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    Master = self(),
    lists:foreach(fun({_, WorkflowExecutionSpec}) ->
        spawn(fun() ->
            Master ! {start_ans, rpc:call(Worker, workflow_engine, execute_workflow,
                [workflow_scheduling_test_common:get_engine_id(), WorkflowExecutionSpec])}
        end)
    end, WorkflowExecutionSpecs),
    workflow_scheduling_test_common:verify_executions_started(length(WorkflowExecutionSpecs)),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_common:get_task_execution_history(Config),
    workflow_scheduling_test_common:verify_execution_history_stats(ExtendedHistoryStats, TaskType),

    ReversedHandlerCallsPerExecution =
        workflow_scheduling_test_common:group_handler_calls_by_execution_id(ExecutionHistory),

    lists:foreach(fun({ExecutionId, ReversedSingleExecutionHistory}) ->
        workflow_scheduling_test_common:verify_execution_history(
            proplists:get_value(ExecutionId, WorkflowExecutionSpecs),
            lists:reverse(ReversedSingleExecutionHistory)
        )
    end, maps:to_list(ReversedHandlerCallsPerExecution)),

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