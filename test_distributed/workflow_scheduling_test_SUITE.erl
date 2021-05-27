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
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    single_sync_workflow_execution_test/1,
    single_async_workflow_execution_test/1,
    multiple_sync_workflow_execution_test/1,
    multiple_async_workflow_execution_test/1
]).

all() ->
    ?ALL([
        single_sync_workflow_execution_test,
        single_async_workflow_execution_test,
        multiple_sync_workflow_execution_test,
        multiple_async_workflow_execution_test
    ]).

-define(ENGINE_ID, <<"test_engine">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

single_sync_workflow_execution_test(Config) ->
    single_workflow_execution_test_base(Config, sync).

single_async_workflow_execution_test(Config) ->
    single_workflow_execution_test_base(Config, async).

single_workflow_execution_test_base(Config, WorkflowType) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Id = case WorkflowType of
        sync -> <<"test_workflow">>;
        async -> <<"async_test_workflow">>
    end,
    Workflow = workflow_definition:get(Id),


    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [
        ?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order(Workflow, Id),
    ExecutionHistory = get_task_execution_history(Config),
    ?assertNotEqual(timeout, ExecutionHistory),

    verify_task_execution_history(Expected, ExecutionHistory, WorkflowType),
    ok.

multiple_sync_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, sync).

multiple_async_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, async).

multiple_workflow_execution_test_base(Config, WorkflowType) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ExecutionIdsBase = [<<"wf1">>, <<"wf2">>, <<"wf3">>, <<"wf4">>, <<"wf5">>],
    Ids = case WorkflowType of
        sync -> ExecutionIdsBase;
        async -> lists:map(fun(Id) -> <<"async_", Id/binary>> end, ExecutionIdsBase)
    end,
    Workflows = lists:map(fun(ExecutionId) -> workflow_definition:get(ExecutionId) end, Ids),

    Master = self(),
    lists:foreach(fun(Workflow) ->
        spawn(fun() ->
            Master ! {start_ans, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])}
        end)
    end, Workflows),

    verify_executions_started(length(Workflows)),

    ExecutionHistory = get_task_execution_history(Config),
    ?assertNotEqual(timeout, ExecutionHistory),

    % Different id sizes - TODO VFS-7551 - add workflow id to history element
    ExecutionHistoryMap = case WorkflowType of
        sync ->
            lists:foldl(fun
                ({<<"result_", Id:3/binary, _/binary>>, _} = Element, Acc) ->
                    ElementsPerExecutionId = maps:get(Id, Acc),
                    Acc#{Id => [Element | ElementsPerExecutionId]};
                ({<<Id:3/binary, _/binary>>, _} = Element, Acc) ->
                    ElementsPerExecutionId = maps:get(Id, Acc),
                    Acc#{Id => [Element | ElementsPerExecutionId]}
            end, maps:from_list(lists:map(fun(ExecutionId) -> {ExecutionId, []} end, Ids)), ExecutionHistory);
        async ->
            lists:foldl(fun
                ({<<"result_", Id:9/binary, _/binary>>, _} = Element, Acc) ->
                    ElementsPerExecutionId = maps:get(Id, Acc),
                    Acc#{Id => [Element | ElementsPerExecutionId]};
                ({<<Id:9/binary, _/binary>>, _} = Element, Acc) ->
                    ElementsPerExecutionId = maps:get(Id, Acc),
                    Acc#{Id => [Element | ElementsPerExecutionId]}
            end, maps:from_list(lists:map(fun(ExecutionId) -> {ExecutionId, []} end, Ids)), ExecutionHistory)
    end,

    lists:map(fun({ExecutionId, Workflow}) ->
        LanesDefinitions = get_expected_task_execution_order(Workflow, ExecutionId),
        WorkflowExecutionHistory = maps:get(ExecutionId, ExecutionHistoryMap),
        verify_task_execution_history(LanesDefinitions, lists:reverse(WorkflowExecutionHistory), WorkflowType)
    end, lists:zip(Ids, Workflows)),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        ok = rpc:call(Worker, workflow_engine, init, [?ENGINE_ID]),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Master = spawn(fun start_task_execution_gatherer/0),
    test_utils:mock_new(Workers, [task_executor]),
    
    test_utils:mock_expect(Workers, task_executor, process_item, fun(EngineId, ExecutionId, Task, Item) ->
        Master ! {task_executed, self(), Task, Item},
        receive
            history_saved -> ok
        end,
        meck:passthrough([EngineId, ExecutionId, Task, Item])
    end),
    
    test_utils:mock_expect(Workers, task_executor, process_async_result, fun(EngineId, ExecutionId, Task, Item, Ref) ->
        Master ! {task_executed, self(), <<"result_", Task/binary>>, Item},
        receive
            history_saved -> ok
        end,
        meck:passthrough([EngineId, ExecutionId, Task, Item, Ref])
    end),

    [{task_execution_gatherer, Master} | Config].

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [task_executor]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_task_execution_gatherer() ->
    task_execution_gatherer_loop([], undefined).

task_execution_gatherer_loop(Acc, ProcWaitingForAns) ->
    receive
        {task_executed, Sender, Task, Item} ->
            Sender ! history_saved,
            case ProcWaitingForAns of
                undefined -> ok;
                _ -> ProcWaitingForAns ! gathering_task_execution_history
            end,
            task_execution_gatherer_loop([{Task, Item} | Acc], ProcWaitingForAns);
        {get_task_execution_history, Sender} ->
            task_execution_gatherer_loop(Acc, Sender)
    after
        15000 ->
            case ProcWaitingForAns of
                undefined -> task_execution_gatherer_loop(Acc, ProcWaitingForAns);
                _ -> ProcWaitingForAns ! {task_execution_history, lists:reverse(Acc)}
            end
    end.

get_task_execution_history(Config) ->
    ?config(task_execution_gatherer, Config) ! {get_task_execution_history, self()},
    receive
        gathering_task_execution_history -> get_task_execution_history(Config);
        {task_execution_history, History} -> History
    after
        30000 -> timeout
    end.

get_expected_task_execution_order(Workflow, ExecutionId) ->
    lists:map(fun(LaneIndex) ->
        #{parallel_boxes := Boxes, iterator := Iterator} =
            workflow_definition:get_lane(ExecutionId, LaneIndex),
        Items = get_items(Iterator),
        Tasks = maps:map(fun(_BoxNum, TasksMap) ->
            maps:values(TasksMap)
        end, Boxes),
        {Tasks, Items}
    end, lists:seq(1, maps:get(lanes_count, Workflow))).

get_items(Iterator) ->
    case workflow_store:get_next(Iterator) of
        {ok, NextItem, _TaskToStart, NextIterator} -> [NextItem | get_items(NextIterator)];
        none -> []
    end.

% This function verifies if gathered execution history contains all expected elements
verify_task_execution_history([], [], _WorkflowType) ->
    ok;
verify_task_execution_history([{ExpectedTasks, ExpectedItems} | ExpectedTail], Gathered, WorkflowType) ->
    TasksCount = maps:fold(fun(_, TasksList, Acc) -> length(TasksList) + Acc end, 0, ExpectedTasks),
    LaneElementsCount = case WorkflowType of
        sync -> TasksCount * length(ExpectedItems);
        async -> 2 * TasksCount * length(ExpectedItems)
    end,

    ct:print("Verify ~p history elements", [LaneElementsCount]),
    GatheredForLane = lists:sublist(Gathered, LaneElementsCount),

    Remaining = lists:foldl(fun(Item, Acc) ->
        Filtered = lists:filtermap(fun({_Task, GatheredItem}) -> GatheredItem =:= Item end, Acc),
        verify_item_execution_history(Item, 1, ExpectedTasks, Filtered, WorkflowType),
        Acc -- Filtered
    end, GatheredForLane, ExpectedItems),

    ?assertEqual([], Remaining),

    verify_task_execution_history(ExpectedTail,
        lists:sublist(Gathered, LaneElementsCount + 1, length(Gathered) - LaneElementsCount), WorkflowType).

% Helper function for verify_task_execution_history/3 that verifies history for single item
verify_item_execution_history(_Item, _BoxIndex, ExpectedTasks, [], _WorkflowType) ->
    ?assertEqual(0, maps:size(ExpectedTasks));
verify_item_execution_history(Item, BoxIndex, ExpectedTasks, [{Task, Item} | Gathered], WorkflowType) ->
    TasksInBox = maps:get(BoxIndex, ExpectedTasks),
    ?assert(lists:member(Task, TasksInBox)),

    NewTasksInBox = case {Task, WorkflowType} of
        {<<"result_", _/binary>>, _} -> TasksInBox;
        {_, sync} -> TasksInBox;
        {_, async} -> [<<"result_", Task/binary>> | TasksInBox]
    end,

    case lists:delete(Task, NewTasksInBox) of
        [] ->
            verify_item_execution_history(
                Item, BoxIndex + 1, maps:remove(BoxIndex, ExpectedTasks), Gathered, WorkflowType);
        FinalTasksInBox ->
            verify_item_execution_history(
                Item, BoxIndex, ExpectedTasks#{BoxIndex => FinalTasksInBox}, Gathered, WorkflowType)
    end.

verify_executions_started(0) ->
    ok;
verify_executions_started(Count) ->
    Check = receive
        {start_ans, Ans} -> Ans
    after
        5000 -> timeout
    end,
    ?assertEqual(ok, Check),
    verify_executions_started(Count - 1).