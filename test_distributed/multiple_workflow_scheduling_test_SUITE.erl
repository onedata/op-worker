%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test of multiple workflows parallel scheduling.
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
    multiple_sync_workflow_execution_test/1,
    multiple_async_workflow_execution_test/1,
    prepare_in_advance_multiple_workflow_test/1
]).

all() ->
    ?ALL([
        multiple_sync_workflow_execution_test,
        multiple_async_workflow_execution_test,
        prepare_in_advance_multiple_workflow_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

multiple_sync_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, sync, false, 200).

multiple_async_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, async, false, 1000).

prepare_in_advance_multiple_workflow_test(Config) ->
    multiple_workflow_execution_test_base(Config, sync, true, 100).


%%%===================================================================
%%% Test skeletons
%%%===================================================================

multiple_workflow_execution_test_base(Config, WorkflowType, PrepareInAdvance, ItemsCount) ->
    InitialKeys = workflow_scheduling_test_utils:get_all_keys(Config),

    WorkflowExecutionSpecs = lists:map(fun(_) ->
        #{id := Id} = Spec = workflow_scheduling_test_utils:gen_workflow_execution_spec(
            WorkflowType, PrepareInAdvance, #{items_count => ItemsCount}),
        {Id, Spec}
    end, lists:seq(1, 5)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    Master = self(),
    lists:foreach(fun({_, WorkflowExecutionSpec}) ->
        spawn(fun() ->
            Master ! {start_ans, rpc:call(Worker, workflow_engine, execute_workflow,
                [workflow_scheduling_test_utils:get_engine_id(), WorkflowExecutionSpec])}
        end)
    end, WorkflowExecutionSpecs),
    workflow_scheduling_test_utils:verify_executions_started(length(WorkflowExecutionSpecs)),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats =
        workflow_scheduling_test_utils:get_task_execution_history(Config),
    workflow_scheduling_test_utils:verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),

    ReversedHandlerCallsPerExecution =
        workflow_scheduling_test_utils:group_handler_calls_by_execution_id(ExecutionHistory),

    lists:foreach(fun({ExecutionId, ReversedSingleExecutionHistory}) ->
        workflow_scheduling_test_utils:verify_execution_history(
            proplists:get_value(ExecutionId, WorkflowExecutionSpecs),
            lists:reverse(ReversedSingleExecutionHistory)
        )
    end, maps:to_list(ReversedHandlerCallsPerExecution)),

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