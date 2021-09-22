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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    single_sync_workflow_execution_test/1
]).

all() ->
    ?ALL([
        single_sync_workflow_execution_test
    ]).

-define(ENGINE_ID, <<"test_engine">>).
-define(ASYNC_CALL_POOL_ID, <<"test_call_pool">>).

-record(handler_call, {
    function :: atom(),
    execution_id :: workflow_engine:execution_id(),
    context :: workflow_engine:execution_context(),
    lane_id :: workflow_engine:lane_id(),
    task_id :: workflow_engine:task_id(),
    item :: iterator:iterator()
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

single_sync_workflow_execution_test(Config) ->
    single_workflow_execution_test_base(Config, sync).

single_workflow_execution_test_base(Config, WorkflowType) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    Workflow = #{
        id => generator:gen_name(),
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]},
        first_lane_id => <<"1">>
    },
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order(Workflow),
    #{execution_history := ExecutionHistory%, lane_finish_log := LaneFinishLog
    } = ExtendedHistoryStats =
        get_task_execution_history(Config),
%%    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistory),

    verify_memory(Config, InitialKeys),
    ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = Workers = ?config(op_worker_nodes, NewConfig),
        test_utils:mock_new(Workers, [oneprovider]),
        test_utils:mock_expect(Workers, oneprovider, get_domain, fun() ->
            atom_to_binary(?GET_DOMAIN(node()), utf8)
        end),
        ok = rpc:call(Worker, workflow_engine, init, [?ENGINE_ID,
            #{
                workflow_async_call_pools_to_use => [{?ASYNC_CALL_POOL_ID, 60}],
                init_workflow_timeout_server => {true, 2}
            }
        ]),
        NewConfig
    end,
    [
        % TODO VFS-7784 - uncomment when workflow_test_handler is moved to test directory
        % {?LOAD_MODULES, [workflow_test_handler]},
        {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [oneprovider]).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Gatherer = spawn(fun start_task_execution_gatherer/0),
    % TODO VFS-7784 - mock iterator and check if forget_before and mark_exhausted after iterators are not needed anymore
    % TODO VFS-7784 - test iteration failure
    mock_handlers(Workers, Gatherer),
    [{task_execution_gatherer, Gatherer} | Config].

end_per_testcase(_, Config) ->
    ?config(task_execution_gatherer, Config) ! stop,
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [workflow_test_handler, workflow_engine_callback_handler]).

%%%===================================================================
%%% Task execution gatherer helper functions
%%%===================================================================

start_task_execution_gatherer() ->
    task_execution_gatherer_loop(#{execution_history => []}, undefined, #{}).

task_execution_gatherer_loop(#{execution_history := History} = Acc, ProcWaitingForAns, Options) ->
    receive
        {handler_call, Sender, HandlerCallReport} ->
            Acc2 = update_slots_usage_statistics(Acc, async_slots_used_stats, rpc:call(node(Sender),
                workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID])),
            Acc3 = update_slots_usage_statistics(Acc2, pool_slots_used_stats, rpc:call(node(Sender),
                workflow_engine_state, get_slots_used, [?ENGINE_ID])),
            Acc4 = Acc3#{execution_history => [HandlerCallReport | History]},
            case ProcWaitingForAns of
                undefined -> ok;
                _ -> ProcWaitingForAns ! gathering_task_execution_history
            end,
            Sender ! history_saved,
            task_execution_gatherer_loop(Acc4, ProcWaitingForAns, Options);
        {get_task_execution_history, Sender} ->
            task_execution_gatherer_loop(Acc, Sender, Options);
        {set_option, Key, Value} ->
            task_execution_gatherer_loop(Acc, ProcWaitingForAns, Options#{Key => Value});
        {unset_option, Key} ->
            task_execution_gatherer_loop(Acc, ProcWaitingForAns, maps:remove(Key, Options));
        stop ->
            ok
    after
        15000 ->
            case ProcWaitingForAns of
                undefined ->
                    task_execution_gatherer_loop(Acc, ProcWaitingForAns, Options);
                _ ->
                    ProcWaitingForAns ! {task_execution_history, Acc#{execution_history => lists:reverse(History)}},
                    task_execution_gatherer_loop(#{execution_history => []}, undefined, #{})
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

set_task_execution_gatherer_option(Config, Key, Value) ->
    ?config(task_execution_gatherer, Config) ! {set_option, Key, Value}.

unset_task_execution_gatherer_option(Config, Key) ->
    ?config(task_execution_gatherer, Config) ! {unset_option, Key}.


mock_handlers(Workers, Gatherer) ->
    test_utils:mock_new(Workers, [workflow_test_handler, workflow_engine_callback_handler]),

    MockTemplate = fun(HandlerCallReport, PassthroughArgs) ->
        Gatherer ! {handler_call, self(), HandlerCallReport},
        receive
            history_saved -> meck:passthrough(PassthroughArgs);
            fail_call-> error
        end
    end,

    test_utils:mock_expect(Workers, workflow_test_handler, prepare_lane, fun(ExecutionId, Context, LaneId) ->
        MockTemplate(
            #handler_call{
                function = prepare_lane,
                execution_id = ExecutionId,
                context =  Context,
                lane_id = LaneId
            },
            [ExecutionId, Context, LaneId]
        )
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, restart_lane, fun(ExecutionId, Context, LaneId) ->
        MockTemplate(
            #handler_call{
                function = prepare_lane,
                execution_id = ExecutionId,
                context =  Context,
                lane_id = LaneId
            },
            [ExecutionId, Context, LaneId]
        )
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, process_item,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId, Item, FinishCallback, HeartbeatCallback) ->
            MockTemplate(
                #handler_call{
                    function = process_item,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = Item
                },
                [ExecutionId, Context, TaskId, Item, FinishCallback, HeartbeatCallback]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, process_result,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId, Item, Result) ->
            MockTemplate(
                #handler_call{
                    function = process_result,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = Item
                },
                [ExecutionId, Context, TaskId, Item, Result]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, handle_task_execution_ended,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId) ->
            MockTemplate(
                #handler_call{
                    function = handle_task_execution_ended,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId
                },
                [ExecutionId, Context, TaskId]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, handle_lane_execution_ended,
        fun(ExecutionId, Context, LaneId) ->
            MockTemplate(
                #handler_call{
                    function = handle_lane_execution_ended,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId
                },
                [ExecutionId, Context, LaneId]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, handle_workflow_execution_ended,
        fun(ExecutionId, Context) ->
            MockTemplate(
                #handler_call{
                    function = handle_workflow_execution_ended,
                    execution_id = ExecutionId,
                    context =  Context
                },
                [ExecutionId, Context]
            )
        end),

    test_utils:mock_expect(Workers, workflow_engine_callback_handler, handle_callback, fun(CallbackId, Result) ->
        {_CallbackType, ExecutionId, EngineId, JobIdentifier} =
            workflow_engine_callback_handler:decode_callback_id(CallbackId),
        {_, _, TaskId} = workflow_execution_state:get_result_processing_data(ExecutionId, JobIdentifier),
        Item = workflow_cached_item:get_item(workflow_execution_state:get_item_id(ExecutionId, JobIdentifier)),
        #{lane_id := LaneId} = workflow_execution_state:get_current_lane_context(ExecutionId),
        Gatherer ! {handler_call, self(), #handler_call{
            function = handle_callback,
            execution_id = ExecutionId,
            lane_id = LaneId,
            task_id = TaskId,
            item = Item
        }},

        receive
            history_saved ->
                % Warning: do not use meck:passthrough as it does not work when 2 mocks work within one process
                apply(meck_util:original_name(workflow_engine_callback_handler), handle_callback, [CallbackId, Result]);
            delay_call ->
                spawn(fun() ->
                    lists:foreach(fun(_) ->
                        % Warning: do not use meck:passthrough as we are in spawned process
                        HeartbeatCallbackId = apply(meck_util:original_name(workflow_engine_callback_handler),
                            prepare_heartbeat_callback_id, [ExecutionId, EngineId, JobIdentifier]),
                        apply(meck_util:original_name(workflow_engine_callback_handler),
                            handle_callback, [HeartbeatCallbackId, undefined]),
                        timer:sleep(timer:seconds(3))
                    end, lists:seq(1,10))
                end),
                timer:sleep(timer:seconds(20)),
                % Warning: do not use meck:passthrough as it does not work when 2 mocks work within one process
                apply(meck_util:original_name(workflow_engine_callback_handler), handle_callback, [CallbackId, Result]);
            fail_call ->
                ok
        end
    end).


%%%===================================================================
%%% Helper functions verifying execution history
%%%===================================================================

get_expected_task_execution_order(#{id := ExecutionId, execution_context := InitialContext}) ->
    get_expected_task_execution_order(<<"1">>, ExecutionId, InitialContext).

get_expected_task_execution_order(LaneId, ExecutionId, InitialContext) ->
    {ok, #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        execution_context := LaneExecutionContext
    }} = workflow_test_handler:prepare_lane(ExecutionId, InitialContext, LaneId),
    Items = get_items(InitialContext, Iterator),

    TaskIds = lists:map(fun(TasksList) -> sets:from_list(maps:keys(TasksList)) end, Boxes),
    ExpectedForLane = {TaskIds, Items, LaneExecutionContext},
    case workflow_test_handler:handle_lane_execution_ended(ExecutionId, LaneExecutionContext, LaneId) of
        ?FINISH_EXECUTION ->
            [ExpectedForLane];
        ?CONTINUE(NextLaneId, _) ->
            [ExpectedForLane | get_expected_task_execution_order(NextLaneId, ExecutionId, InitialContext)]
    end.

get_items(Context, Iterator) ->
    case iterator:get_next(Context, Iterator) of
        {ok, NextItem, NextIterator} -> [NextItem | get_items(Context, NextIterator)];
        stop -> []
    end.

% This function verifies if gathered execution history contains all expected elements
verify_execution_history(Expected, [HandlerCall]) ->
    ?assertEqual(handle_workflow_execution_ended, HandlerCall#handler_call.function),
    ?assertEqual([], Expected);
verify_execution_history([{TaskIds, ExpectedItems, LaneExecutionContext} | ExpectedTail], Gathered) ->
    #{type := WorkflowType, lane_id := LaneId} = LaneExecutionContext,
    TasksPerItemCount = lists:foldl(fun(TasksForBox, Acc) -> sets:size(TasksForBox) + Acc end, 0, TaskIds),
    TasksCount = TasksPerItemCount * length(ExpectedItems),
    NotificationsCount = TasksPerItemCount + 2, % Notification for each task + prepare_lane + handle_lane_execution_ended
    LaneElementsCount = case WorkflowType of
        sync-> TasksCount + NotificationsCount;
        async -> 3 * TasksCount + NotificationsCount
    end,

    ct:print("Verify ~p history elements", [LaneElementsCount]),
    [FirstForLane | GatheredForLane] = lists:sublist(Gathered, LaneElementsCount),
    ?assertEqual(prepare_lane, FirstForLane#handler_call.function),
    ?assertEqual(LaneId, FirstForLane#handler_call.lane_id),

    [LastForLane | GatheredForLane2] = lists:reverse(GatheredForLane),
    ?assertEqual(handle_lane_execution_ended, LastForLane#handler_call.function),
    ?assertEqual(LaneId, LastForLane#handler_call.lane_id),

    TaskIdsList = lists:foldl(fun(CallsForBox, Acc) -> sets:to_list(CallsForBox) ++ Acc end, [], TaskIds),
    RemainingTaskIdsList = lists:foldl(fun
        (#handler_call{function = handle_task_execution_ended, task_id = TaskId}, Acc) ->
            ?assert(lists:member(TaskId, Acc)),
            Acc -- [TaskId];
        (#handler_call{task_id = TaskId}, Acc) ->
            ?assertNot(lists:member(TaskId, Acc)),
            Acc
    end, TaskIdsList, GatheredForLane2),
    ?assertEqual([], RemainingTaskIdsList),
    GatheredForLane3 = lists:reverse(lists:filter(fun(#handler_call{function = Fun}) ->
        Fun =/= handle_task_execution_ended
    end, GatheredForLane2)),

    Remaining = lists:foldl(fun(Item, Acc) ->
        Filtered = lists:filtermap(fun(HandlerCall) -> HandlerCall#handler_call.item =:= Item end, Acc),
        verify_item_execution_history(Item, TaskIds, Filtered, LaneExecutionContext),
        Acc -- Filtered
    end, GatheredForLane3, ExpectedItems),

    ?assertEqual([], Remaining),
    verify_execution_history(ExpectedTail,
        lists:sublist(Gathered, LaneElementsCount + 1, length(Gathered) - LaneElementsCount)).

% Helper function for verify_execution_history/3 that verifies history for single item
verify_item_execution_history(_Item, ExpectedCalls, [], _LaneExecutionContext) ->
    ?assertEqual([], ExpectedCalls);
verify_item_execution_history(Item, [CallsForBox | ExpectedCalls], [HandlerCall | Gathered], LaneExecutionContext) ->
    #{type := WorkflowType, lane_id := ExpectedLaneId} = LaneExecutionContext,
    #handler_call{function = Function, lane_id = LaneId, task_id = TaskId, item = Item} = HandlerCall,
    ?assertEqual(ExpectedLaneId, LaneId),
    ?assertEqual(Item, Item),
    SetElement = case Function of
        process_item -> TaskId;
        _ -> {Function, TaskId}
    end,
    ?assert(sets:is_element(SetElement, CallsForBox)),

    NewCallsForBox = case {WorkflowType, Function} of
        {async, process_item} -> sets:add_element({handle_callback, TaskId}, CallsForBox);
        {async, handle_callback} -> sets:add_element({process_result, TaskId}, CallsForBox);
        _ -> CallsForBox
    end,
    FinalCallsForBox = sets:del_element(SetElement, NewCallsForBox),

    case sets:is_empty(FinalCallsForBox) of
        true ->
            verify_item_execution_history(Item, ExpectedCalls, Gathered, LaneExecutionContext);
        false ->
            verify_item_execution_history(Item, [FinalCallsForBox | ExpectedCalls], Gathered, LaneExecutionContext)
    end.


%%%===================================================================
%%% Memory verification helper functions
%%%===================================================================

verify_memory(Config, InitialKeys) ->
    verify_memory(Config, InitialKeys, false).

verify_memory(Config, InitialKeys, RestartDocPresent) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    ?assertEqual([], rpc:call(Worker, workflow_engine_state, get_execution_ids, [?ENGINE_ID])),
    ?assertEqual(0, rpc:call(Worker, workflow_engine_state, get_slots_used, [?ENGINE_ID])),

    lists:foreach(fun({Model, Keys}) ->
        case RestartDocPresent andalso Model =:= workflow_iterator_snapshot of
            true -> ?assertMatch([_], Keys -- proplists:get_value(Model, InitialKeys));
            false -> ?assertEqual([], Keys -- proplists:get_value(Model, InitialKeys))
        end
    end, get_all_keys(Config)).

get_all_keys(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    Models = [workflow_cached_item, workflow_cached_async_result, workflow_iterator_snapshot, workflow_execution_state],
    lists:map(fun(Model) ->
        Ctx = datastore_model_default:set_defaults(datastore_model_default:get_ctx(Model)),
        #{memory_driver := MemoryDriver, memory_driver_ctx := MemoryDriverCtx} = Ctx,
        {Model, get_keys(Worker, MemoryDriver, MemoryDriverCtx)}
    end, Models).


get_keys(Worker, ets_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ lists:filtermap(fun
            ({_Key, #document{deleted = true}}) -> false;
            ({Key, #document{deleted = false}}) -> {true, Key}
        end, rpc:call(Worker, ets, tab2list, [Table]))
    end, [], rpc:call(Worker, datastore_multiplier, get_names, [MemoryDriverCtx]));
get_keys(Worker, mnesia_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ mnesia:async_dirty(fun() ->
            rpc:call(Worker, mnesia, foldl, [fun
                ({entry, _Key, #document{deleted = true}}, Acc) -> Acc;
                ({entry, Key, #document{deleted = false}}, Acc) -> [Key | Acc]
            end, [], Table])
        end)
    end, [], rpc:call(Worker, datastore_multiplier, get_names, [MemoryDriverCtx])).