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

-include("workflow_engine.hrl").
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
    multiple_async_workflow_execution_test/1,
    job_failure_test/1
]).

all() ->
    ?ALL([
        single_sync_workflow_execution_test,
        single_async_workflow_execution_test,
        multiple_sync_workflow_execution_test,
        multiple_async_workflow_execution_test,
        job_failure_test
    ]).

-define(ENGINE_ID, <<"test_engine">>).
-define(ASYNC_CALL_POOL_ID, <<"test_call_pool">>).

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
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order(Workflow),
    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),

    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistoryWithoutPrepare, WorkflowType),
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
    Workflows = lists:map(fun(ExecutionId) ->
        #{
            id => ExecutionId,
            workflow_handler => workflow_test_handler,
            execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
        }
    end, Ids),

    Master = self(),
    lists:foreach(fun(Workflow) ->
        spawn(fun() ->
            Master ! {start_ans, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])}
        end)
    end, Workflows),

    verify_executions_started(length(Workflows)),

    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),

    % Different id sizes - TODO VFS-7784 - add workflow id to history element
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
        LanesDefinitions = get_expected_task_execution_order(Workflow),
        WorkflowExecutionHistory = maps:get(ExecutionId, ExecutionHistoryMap),
        ExecutionHistoryWithoutPrepare = verify_preparation_phase(ExecutionId, lists:reverse(WorkflowExecutionHistory)),
        verify_execution_history(LanesDefinitions, ExecutionHistoryWithoutPrepare, WorkflowType)
    end, lists:zip(Ids, Workflows)),
    ok.

job_failure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowType = sync,
    Id = <<"job_failure_test_workflow">>,
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    TaskToFail = <<"job_failure_test_workflow_task3_3_2">>,
    ItemToFail = <<"100">>,
    set_task_execution_gatherer_option(Config, fail_job, {TaskToFail, ItemToFail}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order(Workflow, 3, 3, ItemToFail, TaskToFail),
    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),

    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistoryWithoutPrepare, WorkflowType),

    unset_task_execution_gatherer_option(Config, fail_job),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        ok = rpc:call(Worker, workflow_engine, init, [?ENGINE_ID,
            #{workflow_async_call_pools_to_use => [{?ASYNC_CALL_POOL_ID, 60}]}]),
        NewConfig
    end,
    [
        % TODO VFS-7784 - uncomment when workflow_test_handler is moved to test directory
        % {?LOAD_MODULES, [workflow_test_handler]},
        {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Master = spawn(fun start_task_execution_gatherer/0),
    test_utils:mock_new(Workers, [workflow_test_handler, workflow_engine_callback_handler]),

    test_utils:mock_expect(Workers, workflow_test_handler, prepare, fun(ExecutionId, Context) ->
        Master ! {task_processing, self(), <<ExecutionId/binary, "_prepare">>, undefined},
        meck:passthrough([ExecutionId, Context])
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, process_item,
        fun(ExecutionId, Context, TaskId, Item, FinishCallback, HeartbeatCallback) ->
            Master ! {task_processing, self(), TaskId, Item},
            receive
                history_saved -> meck:passthrough(
                    [ExecutionId, Context, TaskId, Item, FinishCallback, HeartbeatCallback]);
                fail_job -> error
            end
        end
    ),
    
    test_utils:mock_expect(Workers, workflow_engine_callback_handler, handle_callback, fun(CallbackId, Result) ->
        {_CallbackType, ExecutionId, _EngineId, JobIdentifier, _CallPools} =
            workflow_engine_callback_handler:decode_callback_id(CallbackId),
        {TaskId, ItemId} = get_task_and_item_ids(ExecutionId, JobIdentifier),
        Master ! {task_processing, self(), <<"result_", TaskId/binary>>, ItemId},
        receive
            history_saved -> ok
        end,
        % Warning: do not use meck:passthrough as it does not work when 2 mocks work within one process
        apply(meck_util:original_name(workflow_engine_callback_handler), handle_callback, [CallbackId, Result])
    end),

    [{task_execution_gatherer, Master} | Config].

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [workflow_test_handler, workflow_engine_callback_handler]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_task_execution_gatherer() ->
    task_execution_gatherer_loop(#{execution_history => []}, undefined, #{}).

task_execution_gatherer_loop(#{execution_history := History} = Acc, ProcWaitingForAns, Options) ->
    receive
        {task_processing, Sender, Task, Item} ->
            Acc2 = update_slots_usage_statistics(Acc, async_slots_used_stats, rpc:call(node(Sender),
                workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID])),
            Acc3 = update_slots_usage_statistics(Acc2, pool_slots_used_stats, rpc:call(node(Sender),
                workflow_engine_state, get_slots_used, [?ENGINE_ID])),
            Acc4 = case Options of
                #{fail_job := {Task, Item}} ->
                    Sender ! fail_job,
                    Acc3;
                _ ->
                    Sender ! history_saved,
                    Acc3#{execution_history => [{Task, Item} | History]}
            end,
            case ProcWaitingForAns of
                undefined -> ok;
                _ -> ProcWaitingForAns ! gathering_task_execution_history
            end,
            task_execution_gatherer_loop(Acc4, ProcWaitingForAns, Options);
        {get_task_execution_history, Sender} ->
            task_execution_gatherer_loop(Acc, Sender, Options);
        {set_option, Key, Value} ->
            task_execution_gatherer_loop(Acc, ProcWaitingForAns, Options#{Key => Value});
        {unset_option, Key} ->
            task_execution_gatherer_loop(Acc, ProcWaitingForAns, maps:remove(Key, Options))
    after
        15000 ->
            case ProcWaitingForAns of
                undefined -> task_execution_gatherer_loop(Acc, ProcWaitingForAns, Options);
                _ -> ProcWaitingForAns ! {task_execution_history, Acc#{execution_history => lists:reverse(History)}}
            end
    end.

update_slots_usage_statistics(Acc, Key, NewValue) ->
    case maps:get(Key, Acc, undefined) of
        undefined -> Acc#{Key => {NewValue, NewValue}};
        {Min, Max} -> Acc#{Key => {min(Min, NewValue), max(Max, NewValue)}}
    end.

get_task_execution_history(Config) ->
    ?config(task_execution_gatherer, Config) ! {get_task_execution_history, self()},
    receive
        gathering_task_execution_history ->
            get_task_execution_history(Config);
        {task_execution_history, HistoryAcc} ->
            [Worker | _] = ?config(op_worker_nodes, Config),
            AsyncSlotsUsed = rpc:call(Worker, workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID]),
            EngineSlotsUsed = rpc:call(Worker, workflow_engine_state, get_slots_used, [?ENGINE_ID]),
            HistoryAcc#{final_async_slots_used => AsyncSlotsUsed, final_pool_slots_used => EngineSlotsUsed}
    after
        30000 -> timeout
    end.

% TODO VFS-7784 - uncomment checks in this function and fix tests
verify_execution_history_stats(Acc, WorkflowType) ->
    ?assertEqual(0, maps:get(final_async_slots_used, Acc)),
%%    ?assertEqual(0, maps:get(final_pool_slots_used, Acc)),

    {MinAsyncSlots, MaxAsyncSlots} = maps:get(async_slots_used_stats, Acc),
    {MinPoolSlots, MaxPoolSlots} = maps:get(pool_slots_used_stats, Acc),
    case WorkflowType of
        sync ->
            ?assertEqual(0, MinAsyncSlots),
            ?assertEqual(0, MaxAsyncSlots),
            % Task processing is initialized after pool slots count is incremented
            % and it is finished before pool slots count is decremented so '0' should not appear in history
%%            ?assertEqual(1, MinPoolSlots);
            ok;
        async ->
            % Task processing is initialized after async slots count is incremented
            % and it is finished before async slots count is decremented so '0' should not appear in history
%%            ?assertEqual(1, MinAsyncSlots),
            ?assertEqual(60, MaxAsyncSlots),
            % '0' should appear in history because slots count is decremented after async processing is scheduled
%%            ?assertEqual(0, MinPoolSlots)
            ok
    end,

    ?assertEqual(20, MaxPoolSlots).

set_task_execution_gatherer_option(Config, Key, Value) ->
    ?config(task_execution_gatherer, Config) ! {set_option, Key, Value}.

unset_task_execution_gatherer_option(Config, Key) ->
    ?config(task_execution_gatherer, Config) ! {unset_option, Key}.

get_expected_task_execution_order(Workflow) ->
    get_expected_task_execution_order(Workflow, undefined, undefined, undefined, undefined).

get_expected_task_execution_order(#{id := ExecutionId, execution_context := Context},
    LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail) ->
    get_expected_task_execution_order(
        1, ExecutionId, Context,
        LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail
    ).

get_expected_task_execution_order(LaneIndex, ExecutionId, Context,
    LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail) ->
    {ok, #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        is_last := IsLast
    }} = workflow_test_handler:get_lane_spec(ExecutionId, Context, LaneIndex),
    Items = get_items(Context, Iterator),

    LaneSpec = case LaneIndex of
        LaneWithErrorIndex ->
            TasksForFailedItem = lists:sublist(Boxes, BoxWithErrorIndex - 1) ++
                [maps:remove(TaskToFail, lists:nth(BoxWithErrorIndex, Boxes))],
            {Boxes, Items, ItemToFail, TasksForFailedItem};
        _ ->
            {Boxes, Items, undefined, []}
    end,

    case IsLast of
        true ->
            [LaneSpec];
        false ->
            [LaneSpec | get_expected_task_execution_order(LaneIndex + 1, ExecutionId, Context,
                LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail)]
    end.

get_items(Context, Iterator) ->
    case iterator:get_next(Context, Iterator) of
        {ok, NextItem, NextIterator} -> [NextItem | get_items(Context, NextIterator)];
        stop -> []
    end.

verify_preparation_phase(ExecutionId, Gathered) ->
    ?assertNotEqual([], Gathered),
    [{GatheredHeadTask, _} = _GatheredHead | GatheredTail] = Gathered,
    ExpectedHeadTask = <<ExecutionId/binary, "_prepare">>,
    ?assertEqual(ExpectedHeadTask, GatheredHeadTask),
    GatheredTail.

% This function verifies if gathered execution history contains all expected elements
verify_execution_history([], [], _WorkflowType) ->
    ok;
verify_execution_history([{ExpectedTasks, ExpectedItems, ItemToFail, TasksForFailedItem} = LineExpected | ExpectedTail],
    Gathered, WorkflowType) ->
    TasksCount = calculate_tasks_count(LineExpected),
    LaneElementsCount = case WorkflowType of
        sync -> TasksCount;
        async -> 2 * TasksCount
    end,
    
    ct:print("Verify ~p history elements", [LaneElementsCount]),
    GatheredForLane = lists:sublist(Gathered, LaneElementsCount),

    Remaining = lists:foldl(fun(Item, Acc) ->
        Filtered = lists:filtermap(fun({_Task, GatheredItem}) -> GatheredItem =:= Item end, Acc),
        ExpectedTasksForItem = case Item of
            ItemToFail -> TasksForFailedItem;
            _ -> ExpectedTasks
        end,
        verify_item_execution_history(Item, ExpectedTasksForItem, Filtered, WorkflowType),
        Acc -- Filtered
    end, GatheredForLane, ExpectedItems),

    ?assertEqual([], Remaining),

    verify_execution_history(ExpectedTail,
        lists:sublist(Gathered, LaneElementsCount + 1, length(Gathered) - LaneElementsCount), WorkflowType).

calculate_tasks_count({ExpectedTasks, ExpectedItems, undefined, []}) ->
    TasksTypesCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, ExpectedTasks),
    TasksTypesCount * length(ExpectedItems);
calculate_tasks_count({ExpectedTasks, ExpectedItems, _ItemToFail, TasksForFailedItem} ) ->
    TasksTypesCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, ExpectedTasks),
    TasksForFailedItemCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, TasksForFailedItem),
    TasksTypesCount * (length(ExpectedItems) - 1) + TasksForFailedItemCount.

% Helper function for verify_execution_history/3 that verifies history for single item
verify_item_execution_history(_Item, ExpectedTasks, [], _WorkflowType) ->
    ?assertEqual([], ExpectedTasks);
verify_item_execution_history(Item, [TasksInBox | ExpectedTasks], [{Task, Item} | Gathered], WorkflowType) ->
    ?assert(maps:is_key(Task, TasksInBox)),

    NewTasksInBox = case {Task, WorkflowType} of
        {<<"result_", _/binary>>, _} -> TasksInBox;
        {_, sync} -> TasksInBox;
        {_, async} -> TasksInBox#{<<"result_", Task/binary>> => undefined}
    end,
    FinalTasksInBox = maps:remove(Task, NewTasksInBox),

    case maps:size(FinalTasksInBox) of
        0 ->
            verify_item_execution_history(Item, ExpectedTasks, Gathered, WorkflowType);
        _ ->
            verify_item_execution_history(Item, [FinalTasksInBox | ExpectedTasks], Gathered, WorkflowType)
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

get_task_and_item_ids(ExecutionId, {_, ItemIndex, BoxIndex, TaskIndex}) ->
    {ok, LaneIndex} = workflow_execution_state:get_lane_index(ExecutionId),
    TaskId = <<ExecutionId/binary, "_task", (integer_to_binary(LaneIndex))/binary, "_",
        (integer_to_binary(BoxIndex))/binary, "_", (integer_to_binary(TaskIndex))/binary>>,
    ItemId = integer_to_binary(ItemIndex),
    {TaskId, ItemId}.

