%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test utils for workflow scheduling tests.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_scheduling_test_common).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% Init/teardown functions
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% Test execution manager helper functions
-export([get_task_execution_history/1, set_test_execution_manager_option/3, set_test_execution_manager_options/2,
    group_handler_calls_by_execution_id/1]).
%% Helper functions verifying execution history
-export([verify_execution_history/2, verify_execution_history/3, verify_empty_lane/2]).
%% Helper functions history statistics
-export([verify_execution_history_stats/2, verify_execution_history_stats/3]).
%% Memory verification helper functions
-export([verify_memory/2, verify_memory/3, get_all_workflow_related_datastore_keys/1]).
%% Generic helper functions
-export([gen_workflow_execution_spec/3, gen_workflow_execution_spec/4, verify_executions_started/1, get_engine_id/0]).

-define(ENGINE_ID, <<"test_engine">>).
-define(ASYNC_CALL_POOL_ID, <<"test_call_pool">>).

-record(handler_call, {
    function :: atom(),
    execution_id :: workflow_engine:execution_id(),
    context :: workflow_engine:execution_context(),
    lane_id :: workflow_engine:lane_id(),
    task_id :: workflow_engine:task_id(),
    item :: iterator:iterator(),
    result :: term()
}).

-type test_manager_task_failure_key() :: fail_job | fail_result_processing | timeout |
    fail_lane_preparation | fail_execution_ended_handler.
-type lane_history_check_key() :: expect_empty_items_list | stop_on_lane |
    delay_and_fail_lane_preparation_in_advance | fail_lane_preparation_in_advance.
-export_type([test_manager_task_failure_key/0, lane_history_check_key/0]).

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
        % {?LOAD_MODULES, [?MODULE, workflow_test_handler]},
        {?LOAD_MODULES, [?MODULE]},
        {?ENV_UP_POSTHOOK, Posthook} | Config
    ].

end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [oneprovider]).

init_per_testcase(async_task_enqueuing_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    rpc:call(Worker, workflow_engine, set_enqueuing_timeout, [?ENGINE_ID, 20]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Manager = spawn(fun start_test_execution_manager/0),
    % TODO VFS-7784 - mock iterator and check if forget_before and mark_exhausted after iterators are not needed anymore
    % TODO VFS-7784 - test iteration failure
    mock_handlers(Workers, Manager),
    [{test_execution_manager, Manager} | Config].

end_per_testcase(async_task_enqueuing_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    rpc:call(Worker, workflow_engine, set_enqueuing_timeout, [?ENGINE_ID, undefined]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_, Config) ->
    ?config(test_execution_manager, Config) ! stop,
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [workflow_test_handler, workflow_engine]).

%%%===================================================================
%%% Test execution manager helper functions
%%%===================================================================

start_test_execution_manager() ->
    test_execution_manager_loop(#{execution_history => []}, undefined, #{}).

test_execution_manager_loop(#{execution_history := History} = Acc, ProcWaitingForAns, Options) ->
    receive
        {handler_call, Sender, #handler_call{} = HandlerCallReport} ->
            Acc2 = update_slots_usage_statistics(Acc, async_slots_used_stats, rpc:call(node(Sender),
                workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID])),
            Acc3 = update_slots_usage_statistics(Acc2, pool_slots_used_stats, rpc:call(node(Sender),
                workflow_engine_state, get_slots_used, [?ENGINE_ID])),
            Acc4 = Acc3#{execution_history => [HandlerCallReport | History]},

            case ProcWaitingForAns of
                undefined -> ok;
                _ -> ProcWaitingForAns ! gathering_task_execution_history
            end,

            Acc5 = reply_to_handler_mock(Sender, Acc4, Options, HandlerCallReport),
            test_execution_manager_loop(Acc5, ProcWaitingForAns, Options);
        {get_task_execution_history, Sender} ->
            test_execution_manager_loop(Acc, Sender, Options);
        {set_option, Key, Value} ->
            test_execution_manager_loop(Acc, ProcWaitingForAns, Options#{Key => Value});
        stop ->
            ok
    after
        20000 ->
            case ProcWaitingForAns of
                undefined ->
                    test_execution_manager_loop(Acc, ProcWaitingForAns, Options);
                _ ->
                    ProcWaitingForAns ! {task_execution_history, Acc#{execution_history => lists:reverse(History)}},
                    test_execution_manager_loop(#{execution_history => []}, undefined, #{})
            end
    end.

update_slots_usage_statistics(Acc, Key, NewValue) ->
    case maps:get(Key, Acc, undefined) of
        undefined -> Acc#{Key => {NewValue, NewValue}};
        {Min, Max} -> Acc#{Key => {min(Min, NewValue), max(Max, NewValue)}}
    end.

reply_to_handler_mock(Sender, ManagerAcc, Options, #handler_call{
    function = Function, execution_id = ExecutionId, lane_id = LaneId, task_id = TaskId, item = Item
}) ->
    case {Function, Options} of
        {run_task_for_item, #{fail_job := {TaskId, Item}}} ->
            Sender ! fail_call,
            ManagerAcc;
        {handle_callback, #{timeout := {TaskId, Item}}} ->
            Sender ! fail_call,
            ManagerAcc;
        {handle_callback, #{delay_call := {TaskId, Item}}} ->
            Sender ! {delay_call, 0},
            ManagerAcc;
        {handle_callback, #{delay_call := {TaskId, Item, InitialSleepTime}}} ->
            Sender ! {delay_call, InitialSleepTime},
            ManagerAcc;
        {process_task_result_for_item, #{fail_result_processing := {TaskId, Item}}} ->
            Sender ! fail_call,
            ManagerAcc;
        {prepare_lane, #{fail_lane_preparation := LaneId, {delay_lane_preparation, LaneId} := true}} ->
            Sender ! delay_and_fail_call,
            ManagerAcc;
        {prepare_lane, #{fail_lane_preparation := LaneId}} ->
            Sender ! fail_call,
            ManagerAcc;
        {prepare_lane, #{{delay_lane_preparation, LaneId} := true}} ->
            Sender ! delay_call,
            ManagerAcc;
        {prepare_lane, #{sleep_on_preparation := Value}} ->
            Sender ! {sleep, Value},
            ManagerAcc;
        {process_streamed_task_data, #{fail_task_data_processing := {TaskId, Item}}} ->
            Sender ! fail_call,
            ManagerAcc;
        {handle_task_results_processed_for_all_items, #{fail_stream_termination := {TaskId, Item}}} ->
            Sender ! fail_call,
            ManagerAcc;
        {Fun, #{cancel_execution := {Fun, TaskId, Item}}} ->
            CancelAns = rpc:call(node(Sender), workflow_engine, cancel_execution, [ExecutionId]),
            Sender ! history_saved,
            ManagerAcc#{cancel_ans => CancelAns};
        {Fun, #{cancel_execution := {Fun, LaneId}}} ->
            CancelAns = rpc:call(node(Sender), workflow_engine, cancel_execution, [ExecutionId]),
            Sender ! history_saved,
            ManagerAcc#{cancel_ans => CancelAns};
        {handle_lane_execution_ended, #{fail_execution_ended_handler := LaneId}} ->
            Sender ! throw_error,
            ManagerAcc;
        _ ->
            Sender ! history_saved,
            ManagerAcc
    end.

get_task_execution_history(Config) ->
    ?config(test_execution_manager, Config) ! {get_task_execution_history, self()},
    FinalAns = receive
        gathering_task_execution_history ->
            get_task_execution_history(Config);
        {task_execution_history, HistoryAcc} ->
            [Worker | _] = ?config(op_worker_nodes, Config),
            AsyncSlotsUsed = rpc:call(Worker, workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID]),
            EngineSlotsUsed = rpc:call(Worker, workflow_engine_state, get_slots_used, [?ENGINE_ID]),
            HistoryAcc#{final_async_slots_used => AsyncSlotsUsed, final_pool_slots_used => EngineSlotsUsed}
    after
        30000 -> timeout
    end,

    ?assertNotEqual(timeout, FinalAns),
    FinalAns.

set_test_execution_manager_option(Config, Key, Value) ->
    ?config(test_execution_manager, Config) ! {set_option, Key, Value}.


set_test_execution_manager_options(Config, Options) ->
    lists:foreach(fun({Key, Value}) ->
        ?config(test_execution_manager, Config) ! {set_option, Key, Value}
    end, Options).


mock_handlers(Workers, Manager) ->
    test_utils:mock_new(Workers, [workflow_test_handler, workflow_engine]),

    MockTemplateWithDelayOrFail = fun(HandlerCallReport, PassthroughArgs, DelayFun, OnFailFun) ->
        Manager ! {handler_call, self(), HandlerCallReport},
        receive
            history_saved ->
                meck:passthrough(PassthroughArgs);
            fail_call ->
                OnFailFun(),
                error;
            delay_call ->
                DelayFun(),
                meck:passthrough(PassthroughArgs);
            {sleep, Value} ->
                timer:sleep(Value),
                meck:passthrough(PassthroughArgs);
            delay_and_fail_call ->
                DelayFun(),
                OnFailFun(),
                error;
            throw_error ->
                meck:passthrough(PassthroughArgs),
                throw(some_error)
        end
    end,

    MockTemplate = fun(HandlerCallReport, PassthroughArgs) ->
        MockTemplateWithDelayOrFail(HandlerCallReport, PassthroughArgs, fun() -> ok end, fun() -> ok end)
    end,

    test_utils:mock_expect(Workers, workflow_test_handler, prepare_lane, fun
        (_ExecutionId, #{lane_id := _} = _Context, _LaneId) ->
            % Context with lane_id defined cannot be used in prepare_lane handler
            % (wrong type of context is used by caller)
            throw(wrong_context);
        (ExecutionId, Context, LaneId) ->
            MockTemplateWithDelayOrFail(
                #handler_call{
                    function = prepare_lane,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId
                },
                [ExecutionId, Context, LaneId],
                fun() ->
                    case apply(meck_util:original_name(workflow_test_handler), get_ignored_lane_id, []) of
                        LaneId ->
                            % Note: prepare for ignored lane is executed in advance and only then lane is ignored
                            PredecessorId = apply(
                                meck_util:original_name(workflow_test_handler), get_ignored_lane_predecessor_id, []),
                            wait_for_lane_finish(ExecutionId, PredecessorId);
                        _ ->
                            wait_for_lane_finish(ExecutionId, integer_to_binary(binary_to_integer(LaneId) - 1))
                    end
                end,
                fun() ->
                    op_worker:set_env({lane_finished, ExecutionId, LaneId}, true)
                end
            )
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, restart_lane, fun
        (_ExecutionId, #{lane_id := _} = _Context, _LaneId) ->
            % Context with lane_id defined cannot be used in restart_lane handler
            % (wrong type of context is used by caller)
            throw(wrong_context);
        (ExecutionId, Context, LaneId) ->
            MockTemplate(
                #handler_call{
                    function = prepare_lane, % currently restart_lane and prepare_lane callbacks works identically
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId
                },
                [ExecutionId, Context, LaneId]
            )
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, run_task_for_item,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId, Item, FinishCallback, HeartbeatCallback) ->
            maybe_stream_data(ExecutionId, TaskId, Context, Item),
            MockTemplate(
                #handler_call{
                    function = run_task_for_item,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = Item
                },
                [ExecutionId, Context, TaskId, Item, FinishCallback, HeartbeatCallback]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, process_task_result_for_item,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId, Item, Result) ->
            MockTemplate(
                #handler_call{
                    function = process_task_result_for_item,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = Item,
                    result = Result
                },
                [ExecutionId, Context, TaskId, Item, Result]
            )
        end),


    test_utils:mock_expect(Workers, workflow_test_handler, report_item_error,
        fun(ExecutionId, #{lane_id := LaneId} = Context, Item) ->
            MockTemplate(
                #handler_call{
                    function = report_item_error,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    item = Item
                },
                [ExecutionId, Context, Item]
            )
        end),


    test_utils:mock_expect(Workers, workflow_test_handler, handle_task_results_processed_for_all_items,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId) ->
            Ans = MockTemplate(
                #handler_call{
                    function = handle_task_results_processed_for_all_items,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = handle_task_results_processed_for_all_items
                },
                [ExecutionId, Context, TaskId]
            ),

            maybe_stream_data(ExecutionId, TaskId, Context, handle_task_results_processed_for_all_items),
            case Ans of
                ok -> workflow_engine:report_task_data_streaming_concluded(ExecutionId, TaskId, success);
                _ -> workflow_engine:report_task_data_streaming_concluded(ExecutionId, TaskId, {failure, Ans})
            end,
            Ans
        end),


    test_utils:mock_expect(Workers, workflow_test_handler, process_streamed_task_data,
        fun(ExecutionId, #{lane_id := LaneId} = Context, TaskId, Data) ->
            MockTemplate(
                #handler_call{
                    function = process_streamed_task_data,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    task_id = TaskId,
                    item = Data % item_id is used as data during test
                },
                [ExecutionId, Context, TaskId, Data]
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
        fun(ExecutionId, #{lane_index := LaneIndex} = Context, LaneId) ->
            op_worker:set_env({lane_finished, ExecutionId, LaneId}, true),
            MockTemplate(
                #handler_call{
                    function = handle_lane_execution_ended,
                    execution_id = ExecutionId,
                    context =  Context,
                    lane_id = LaneId,
                    result = workflow_execution_state:is_finished_and_cleaned(ExecutionId, LaneIndex)
                },
                [ExecutionId, Context, LaneId]
            )
        end),

    test_utils:mock_expect(Workers, workflow_test_handler, handle_workflow_execution_ended, fun
        (_ExecutionId, #{lane_id := _} = _Context) ->
            % Context with lane_id defined cannot be used in handle_workflow_execution_ended handler
            % (wrong type of context is used by caller)
            throw(wrong_context);
        (ExecutionId, Context) ->
            MockTemplate(
                #handler_call{
                    function = handle_workflow_execution_ended,
                    execution_id = ExecutionId,
                    context =  Context
                },
                [ExecutionId, Context]
            )
    end),

    % Warning: do not use MockTemplate as meck:passthrough does not work when 2 mocks work within one process
    % (it is possible for handle_callback mock)
    test_utils:mock_expect(Workers, workflow_engine, report_async_task_result, fun(ExecutionId, EncodedJobIdentifier, Result) ->
        JobIdentifier = workflow_jobs:decode_job_identifier(EncodedJobIdentifier),
        {_, _, TaskId} = workflow_execution_state:get_result_processing_data(ExecutionId, JobIdentifier),
        Item = workflow_cached_item:get_item(workflow_execution_state:get_item_id(ExecutionId, JobIdentifier)),
        #{lane_id := LaneId} = workflow_execution_state:get_current_lane_context(ExecutionId),
        Manager ! {handler_call, self(), #handler_call{
            function = handle_callback,
            execution_id = ExecutionId,
            lane_id = LaneId,
            task_id = TaskId,
            item = Item,
            result = Result
        }},

        receive
            history_saved ->
                apply(meck_util:original_name(workflow_engine), report_async_task_result,
                    [ExecutionId, EncodedJobIdentifier, Result]);
            {delay_call, InitialSleepTime} ->
                spawn(fun() ->
                    timer:sleep(InitialSleepTime),
                    lists:foreach(fun(_) ->
                        apply(meck_util:original_name(workflow_engine),
                            report_async_task_heartbeat, [ExecutionId, EncodedJobIdentifier]),
                        timer:sleep(timer:seconds(3))
                    end, lists:seq(1,6))
                end),
                timer:sleep(timer:seconds(15)),
                apply(meck_util:original_name(workflow_engine), report_async_task_result,
                    [ExecutionId, EncodedJobIdentifier, Result]);
            fail_call ->
                ok
        end
    end).

wait_for_lane_finish(ExecutionId, LaneId) ->
    case op_worker:get_env({lane_finished, ExecutionId, LaneId}, undefined) of
        undefined ->
            timer:sleep(timer:seconds(1)),
            wait_for_lane_finish(ExecutionId, LaneId);
        _ ->
            ok
    end.

maybe_stream_data(ExecutionId, TaskId, Context, StreamElementKey) ->
    {LaneIndex, BoxIndex, TaskIndex} =
        apply(meck_util:original_name(workflow_test_handler), decode_task_id, [TaskId]),
    TaskStreams = kv_utils:get([task_streams, LaneIndex, {BoxIndex, TaskIndex}], Context, []),
    % Task streams are specified as list of elements to be executed. Element can be specified using Id or
    % tuple {Id, NumberOfCallsToBeExecuted}.
    case lists:member(StreamElementKey, TaskStreams) of
        true ->
            workflow_engine:stream_task_data(ExecutionId, TaskId, StreamElementKey);
        false ->
            Repeats = proplists:get_value(StreamElementKey, TaskStreams, 0),
            lists:foreach(fun(_) ->
                workflow_engine:stream_task_data(ExecutionId, TaskId, StreamElementKey)
            end, lists:seq(1, Repeats))
    end.

group_handler_calls_by_execution_id(ExecutionHistory) ->
    lists:foldl(fun(#handler_call{execution_id = ExecutionId} = HandlerCall, Acc) ->
        ChosenExecutionCalls = maps:get(ExecutionId, Acc, []),
        Acc#{ExecutionId => [HandlerCall | ChosenExecutionCalls]}
    end, #{}, ExecutionHistory).

%%%===================================================================
%%% Helper functions verifying execution history
%%%===================================================================

verify_execution_history(WorkflowExecutionSpec, Gathered) ->
    verify_execution_history(WorkflowExecutionSpec, Gathered, #{}).

% This function verifies if gathered execution history contains all expected elements
verify_execution_history(#{
    id := ExecutionId,
    first_lane_id := FirstLaneId,
    execution_context := InitialContext,
    next_lane_id := NextLaneId
}, Gathered, Options) ->
    Expected = get_expected(FirstLaneId, undefined, ExecutionId, InitialContext, NextLaneId, false),
    verify_lanes_execution_history(Expected, Gathered, Options).

get_expected(LaneId, PreparedInAdvanceLaneId, ExecutionId, InitialContext, LaneIdToBePreparedInAdvance, IsLanePrepared) ->
    {ok, #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        execution_context := LaneExecutionContext
    }} = workflow_test_handler:prepare_lane(ExecutionId, InitialContext, LaneId),
    Items = get_items(InitialContext, Iterator),

    TaskIds = lists:map(fun(TasksList) -> sets:from_list(maps:keys(TasksList)) end, Boxes),
    IsLastLaneId = workflow_test_handler:is_last_lane(LaneId),
    ExtendedLaneExecutionContext = LaneExecutionContext#{
        lane_id_to_be_prepared_in_advance => LaneIdToBePreparedInAdvance,
        is_lane_prepared => IsLanePrepared,
        should_prepare_next_lane => not IsLastLaneId andalso LaneIdToBePreparedInAdvance =/= PreparedInAdvanceLaneId
    },

    ExpectedForLane = {TaskIds, Items, ExtendedLaneExecutionContext},
    case workflow_test_handler:handle_lane_execution_ended(ExecutionId, LaneExecutionContext, LaneId) of
        ?END_EXECUTION ->
            [ExpectedForLane];
        ?CONTINUE(NextLaneId, NextLaneIdToBePreparedInAdvance) ->
            NextIsLanePrepared = NextLaneId =:= LaneIdToBePreparedInAdvance,
            [ExpectedForLane | get_expected(NextLaneId, LaneIdToBePreparedInAdvance, ExecutionId,
                InitialContext, NextLaneIdToBePreparedInAdvance, NextIsLanePrepared)]
    end.

get_items(Context, Iterator) ->
    case catch iterator:get_next(Context, Iterator) of
        {ok, NextItem, NextIterator} -> [NextItem | get_items(Context, NextIterator)];
        _ -> []
    end.

verify_lanes_execution_history([], Gathered, _Options) ->
    ?assertMatch([#handler_call{function = handle_workflow_execution_ended}], Gathered);
verify_lanes_execution_history([{_, _, #{lane_id := LaneId}} | _], Gathered, #{fail_lane_preparation_in_advance := LaneId}) ->
    ?assertMatch([#handler_call{function = handle_workflow_execution_ended}], Gathered);
verify_lanes_execution_history([{_, _, #{lane_index := LaneIndex, lane_id := LaneId}} | _], Gathered,
    #{delay_and_fail_lane_preparation_in_advance := LaneId}) ->
    % It is possible (but not guaranteed) that next lane preparation in advance started
    case Gathered of
        [_] ->
            ?assertMatch([#handler_call{function = handle_workflow_execution_ended}], Gathered);
        _ ->
            NextLaneId = integer_to_binary(LaneIndex + 1),
            ?assertMatch([#handler_call{function = prepare_lane, lane_id = NextLaneId},
                #handler_call{function = handle_workflow_execution_ended}], Gathered)
    end;
verify_lanes_execution_history([{TaskIds, ExpectedItems, LaneExecutionContext} | ExpectedTail],
    Gathered, Options) ->
    #{
        lane_index := LaneIndex,
        lane_id := LaneId
    } = LaneExecutionContext,
    TaskStreams = kv_utils:get([task_streams, LaneIndex], LaneExecutionContext, #{}),

    VerificationType = case Options of
        #{stop_on_lane := LaneId} -> skip_items_verification;
        #{restart_lane := LaneId} -> skip_items_verification;
        #{expect_empty_items_list := LaneId} -> expect_empty_items_list;
        #{expect_lane_finish := LaneId} -> expect_lane_finish;
        _ -> verify_all
    end,

    LaneElementsCount = count_lane_elements(LaneExecutionContext, TaskIds, ExpectedItems, Options),
    ct:print("Verify ~p history elements", [LaneElementsCount]),

    GatheredForLane = verify_prepare_lane_handler_calls_history(Gathered, LaneElementsCount, LaneExecutionContext),

    case VerificationType of
        verify_all ->
            [LastForLane | GatheredForLane2] = lists:reverse(GatheredForLane),
            ?assertEqual(handle_lane_execution_ended, LastForLane#handler_call.function),
            ?assert(LastForLane#handler_call.result),
            ?assertEqual(LaneId, LastForLane#handler_call.lane_id),

            {NewExpected, CheckItemFailedReport} = case Options of
                #{fail_job := {LaneId, _, Item}} -> {[], {true, Item}};
                #{timeout := {LaneId, _, Item}} -> {[], {true, Item}};
                #{fail_result_processing := {LaneId, _, Item}} -> {[], {true, Item}};
                _ -> {ExpectedTail, false}
            end,

            GatheredForLane3 = case CheckItemFailedReport of
                {true, ExpectedItem} ->
                    ItemFailedReport = lists:foldl(fun
                        (#handler_call{function = report_item_error, lane_id = Id} = Call, Acc)
                            when Id =:= LaneId ->
                            ?assertEqual(undefined, Acc),
                            Call;
                        (_, Acc) ->
                            Acc
                    end, undefined, GatheredForLane2),
                    ?assertNotEqual(undefined, ItemFailedReport),
                    ?assertEqual(ExpectedItem, ItemFailedReport#handler_call.item),
                    GatheredForLane2 -- [ItemFailedReport];
                false ->
                    GatheredForLane2
            end,
            GatheredForLane4 = verify_task_handlers(LaneIndex, lists:reverse(GatheredForLane3), TaskIds, TaskStreams),
            GatheredForLane5 = verify_stream_processing(LaneIndex, GatheredForLane4, TaskStreams),

            Remaining = lists:foldl(fun(Item, Acc) ->
                Filtered = lists:filtermap(fun(HandlerCall) -> HandlerCall#handler_call.item =:= Item end, Acc),
                verify_item_execution_history(Item, TaskIds, Filtered, LaneExecutionContext, Options),
                Acc -- Filtered
            end, GatheredForLane5, ExpectedItems),
            ?assertEqual([], Remaining),

            verify_lanes_execution_history(NewExpected,
                lists:sublist(Gathered, LaneElementsCount + 1, length(Gathered) - LaneElementsCount), Options);
        skip_items_verification ->
            GatheredForLane2 = verify_task_handlers(LaneIndex, GatheredForLane, TaskIds, TaskStreams),
            [FirstNotFiltered | _] = lists:dropwhile(fun
                (#handler_call{lane_id = Id, function = Function}) when Id =:= LaneId ->
                    Function =/= handle_lane_execution_ended;
                (_) ->
                    false
            end, GatheredForLane2),
            ?assertEqual(handle_lane_execution_ended, FirstNotFiltered#handler_call.function),
            ?assert(FirstNotFiltered#handler_call.result),
            ?assertEqual(LaneId, FirstNotFiltered#handler_call.lane_id),

            NewExpected = case Options of
                #{stop_on_lane := LaneId} -> [];
                #{restart_lane := LaneId} -> ExpectedTail
            end,
            [_ | NewGathered] = lists:dropwhile(fun(HandlerCall) ->
                    HandlerCall =/= FirstNotFiltered
            end, Gathered),
            verify_lanes_execution_history(NewExpected, NewGathered, Options);
        expect_lane_finish ->
            GatheredForLane2 = verify_task_handlers(LaneIndex, GatheredForLane, TaskIds, TaskStreams),
            ?assertMatch([
                #handler_call{function = handle_lane_execution_ended, lane_id = LaneId, result = true},
                #handler_call{function = handle_workflow_execution_ended}
            ], GatheredForLane2);
        expect_empty_items_list ->
            ?assertMatch([#handler_call{function = handle_workflow_execution_ended}], GatheredForLane)
    end.

verify_prepare_lane_handler_calls_history(Gathered, LaneElementsCount, #{
    prepare_in_advance := true,
    lane_id := LaneId,
    is_lane_prepared := IsLanePrepared,
    should_prepare_next_lane := ShouldPrepareNextLane,
    lane_id_to_be_prepared_in_advance := NextLaneId
}) ->
    GatheredForLane = lists:sublist(Gathered, LaneElementsCount),

    {PrepareForLane, PrepareNextLane} = lists:foldl(fun
        (#handler_call{function = prepare_lane, lane_id = Id} = Call, {PrepareLaneAcc, PrepareNextLaneAcc})
            when Id =:= LaneId ->
            ?assertEqual(undefined, PrepareLaneAcc),
            {Call, PrepareNextLaneAcc};
        (#handler_call{function = prepare_lane, lane_id = Id} = Call, {PrepareLaneAcc, PrepareNextLaneAcc})
            when Id =:= NextLaneId ->
            ?assertEqual(undefined, PrepareNextLaneAcc),
            {PrepareLaneAcc, Call};
        (_, Acc) ->
            Acc
    end, {undefined, undefined}, GatheredForLane),

    case ShouldPrepareNextLane of
        true ->
            ?assertNotEqual(undefined, PrepareNextLane);
        false ->
            ?assertEqual(undefined, PrepareNextLane)
    end,
    GatheredForLane2 = GatheredForLane -- [PrepareNextLane],

    case IsLanePrepared of
        true ->
            ?assertEqual(undefined, PrepareForLane),
            GatheredForLane2;
        false ->
            [FirstForLane | GatheredForLane3] = GatheredForLane2,
            ?assertEqual(PrepareForLane, FirstForLane),
            GatheredForLane3
    end;
verify_prepare_lane_handler_calls_history(Gathered, LaneElementsCount, #{lane_id := LaneId}) ->
    [FirstForLane | GatheredForLane] = lists:sublist(Gathered, LaneElementsCount),
    ?assertEqual(prepare_lane, FirstForLane#handler_call.function),
    ?assertEqual(LaneId, FirstForLane#handler_call.lane_id),
    GatheredForLane.

verify_task_handlers(LaneIndex, GatheredForLane, TaskIds, TaskStreams) ->
    ReversedGatheredForLane = lists:reverse(GatheredForLane),
    TaskIdsList = lists:foldl(fun(CallsForBox, Acc) -> sets:to_list(CallsForBox) ++ Acc end, [], TaskIds),
    StreamIds = lists:map(fun({BoxIndex, TaskIndex}) ->
        workflow_test_handler:pack_task_id(LaneIndex, BoxIndex, TaskIndex)
    end, maps:keys(TaskStreams)),
    InitialAcc = #{
        task_ids => TaskIdsList,
        stream_ids => StreamIds
    },
    #{task_ids := RemainingTaskIdsList, stream_ids := RemainingStreamIds} = lists:foldl(fun
        (#handler_call{function = handle_task_execution_ended, task_id = TaskId}, #{task_ids := TaskIdsListAcc} = Acc) ->
            ?assert(lists:member(TaskId, TaskIdsListAcc)),
            Acc#{task_ids => TaskIdsListAcc -- [TaskId]};
        (#handler_call{function = handle_task_results_processed_for_all_items, task_id = TaskId}, #{
            task_ids := TaskIdsListAcc,
            stream_ids := StreamIds
        } = Acc) ->
            ?assertNot(lists:member(TaskId, TaskIdsListAcc)),
            ?assert(lists:member(TaskId, StreamIds)),
            Acc#{stream_ids => StreamIds -- [TaskId]};
        (#handler_call{function = process_streamed_task_data, task_id = TaskId}, #{
            task_ids := TaskIdsListAcc
        } = Acc) ->
            ?assertNot(lists:member(TaskId, TaskIdsListAcc)),
            Acc;
        (#handler_call{task_id = TaskId}, #{task_ids := TaskIdsListAcc, stream_ids := StreamIds} = Acc) ->
            ?assertNot(lists:member(TaskId, TaskIdsListAcc)),
            ?assertNot(lists:member(TaskId, StreamIds)),
            Acc
    end, InitialAcc, ReversedGatheredForLane),
    ?assertEqual([], RemainingTaskIdsList),
    ?assertEqual([], RemainingStreamIds),

    lists:filter(fun(#handler_call{function = Fun}) ->
        Fun =/= handle_task_execution_ended andalso Fun =/= handle_task_results_processed_for_all_items
    end, GatheredForLane).

verify_stream_processing(LaneIndex, GatheredForLane, TaskStreams) ->
    DataProcessingCallbackCallCount = maps:filtermap(fun
        (_, []) ->
            false;
        (_, CallbackCalls) ->
            {true, lists:foldl(fun
                ({_, CallsCount}, Acc) -> Acc + CallsCount;
                (_, Acc) -> Acc + 1
            end, 0, CallbackCalls)}
    end, TaskStreams),

    RemainingDataProcessingCallbackCallCount = lists:foldl(fun
        (#handler_call{function = process_streamed_task_data, task_id = TaskId}, Acc) ->
            {LaneIndex, BoxIndex, TaskIndex} = workflow_test_handler:decode_task_id(TaskId),
            TaskCallsCount = maps:get({BoxIndex, TaskIndex}, Acc, 0),
            ?assert(TaskCallsCount > 0),
            case TaskCallsCount of
                1 -> maps:remove({BoxIndex, TaskIndex}, Acc);
                _ -> Acc#{{BoxIndex, TaskIndex} => TaskCallsCount - 1}
            end;
        (_, Acc) ->
            Acc
    end, DataProcessingCallbackCallCount, GatheredForLane),
    ?assertEqual(0, maps:size(RemainingDataProcessingCallbackCallCount)),

    lists:filter(fun(#handler_call{function = Fun}) -> Fun =/= process_streamed_task_data end, GatheredForLane).

% Helper function for verify_lanes_execution_history/3 that verifies history for single item
verify_item_execution_history(_Item, ExpectedCalls, [], _LaneExecutionContext, _Options) ->
    ?assertEqual([], ExpectedCalls);
verify_item_execution_history(Item, [CallsForBox | ExpectedCalls], [HandlerCall | Gathered],
    LaneExecutionContext, Options) ->
    #{task_type := WorkflowType, lane_id := ExpectedLaneId} = LaneExecutionContext,
    #handler_call{function = Function, lane_id = LaneId, task_id = TaskId, item = Item} = HandlerCall,
    ?assertEqual(ExpectedLaneId, LaneId),
    ?assertEqual(Item, Item),
    SetElement = case Function of
        run_task_for_item -> TaskId;
        _ -> {Function, TaskId}
    end,
    ?assert(sets:is_element(SetElement, CallsForBox)),

    Ignore = case Options of
        #{fail_job := {LaneId, TaskId, Item}} -> ignore_callback_call;
        #{timeout := {LaneId, TaskId, Item}} -> ignore_next_box;
        #{fail_result_processing := {LaneId, TaskId, Item}} -> ignore_next_box;
        _ -> ignore_nothing
    end,

    NewCallsForBox = case {WorkflowType, Function} of
        {async, run_task_for_item} when Ignore =/= ignore_callback_call ->
            sets:add_element({handle_callback, TaskId}, CallsForBox);
        {async, handle_callback} ->
            sets:add_element({process_task_result_for_item, TaskId}, CallsForBox);
        _ -> CallsForBox
    end,
    FinalCallsForBox = sets:del_element(SetElement, NewCallsForBox),

    FinalExpectedCalls = case Ignore of
        ignore_nothing -> ExpectedCalls;
        _ -> []
    end,

    case sets:is_empty(FinalCallsForBox) of
        true ->
            verify_item_execution_history(Item, FinalExpectedCalls, Gathered, LaneExecutionContext, Options);
        false ->
            verify_item_execution_history(
                Item, [FinalCallsForBox | FinalExpectedCalls], Gathered, LaneExecutionContext, Options)
    end.

verify_empty_lane(ExecutionHistory, LaneId) ->
    ?assertMatch([#handler_call{function = prepare_lane, lane_id = LaneId},
        #handler_call{function = handle_workflow_execution_ended}], ExecutionHistory).

%%%===================================================================
%%% Helper functions history statistics
%%%===================================================================

verify_execution_history_stats(Acc, WorkflowType) ->
    verify_execution_history_stats(Acc, WorkflowType, #{}).

verify_execution_history_stats(Acc, WorkflowType, Options) ->
    ?assertEqual(0, maps:get(final_async_slots_used, Acc)),
    ?assertEqual(0, maps:get(final_pool_slots_used, Acc)),

    {MinAsyncSlots, MaxAsyncSlots} = maps:get(async_slots_used_stats, Acc),
    {MinPoolSlots, MaxPoolSlots} = maps:get(pool_slots_used_stats, Acc),
    ?assertEqual(0, MinAsyncSlots),
    case {Options, WorkflowType} of
        {#{is_empty := true}, _} ->
            ok;
        {#{restart := true}, sync} ->
            ?assertEqual(0, MaxAsyncSlots),
            % Restart callback is called before any pool slot is used
            ?assertEqual(0, MinPoolSlots),
            ?assertEqual(20, MaxPoolSlots);
        {#{ignore_max_slots_check := true}, sync} ->
            ?assertEqual(0, MaxAsyncSlots),
            ?assertNotEqual(0, MinPoolSlots);
        {_, sync} ->
            ?assertEqual(0, MaxAsyncSlots),
            % Task processing is initialized after pool slots count is incremented
            % and it is finished before pool slots count is decremented so '0' should not appear in history
            ?assertNotEqual(0, MinPoolSlots),
            ?assertEqual(20, MaxPoolSlots);
        {#{ignore_async_slots_check := true}, async} -> 
            ?assertEqual(20, MaxPoolSlots);
        {#{ignore_max_slots_check := true}, async} ->
            ok;
        {_, async} ->
            ?assertEqual(60, MaxAsyncSlots),
            % Do not check MinPoolSlots as any value is possible ('0' can appear in history because slots count is
            % decremented after async processing is scheduled, but async result can appear before decrementation
            % and each such race results in higher MinPoolSlots)
            ?assertEqual(20, MaxPoolSlots)
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
    end, get_all_workflow_related_datastore_keys(Config)).

get_all_workflow_related_datastore_keys(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    % TODO VFS-7788 - fix race between workflow_iterator_snapshot:cleanup and workflow_iterator_snapshot:save
    % (snapshot can be restored)
%%    Models = [workflow_cached_item, workflow_cached_async_result, workflow_iterator_snapshot, workflow_execution_state],
    Models = [workflow_cached_item, workflow_cached_async_result, workflow_execution_state],
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


%%%===================================================================
%%% Generic helper functions
%%%===================================================================

gen_workflow_execution_spec(WorkflowType, PrepareInAdvance, ContextBase) ->
    gen_workflow_execution_spec(WorkflowType, PrepareInAdvance, ContextBase, generator:gen_name()).

gen_workflow_execution_spec(WorkflowType, PrepareInAdvance, ContextBase, Id) ->
    FirstLaneId = maps:get(first_lane_id, ContextBase, <<"1">>),
    NextLaneId = case PrepareInAdvance of
        true when FirstLaneId =/= undefined ->
            FirstLaneIndex = binary_to_integer(FirstLaneId),
            integer_to_binary(FirstLaneIndex + 1);
        true ->
            <<"2">>;
        false ->
            undefined
    end,
    #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => ContextBase#{
            task_type => WorkflowType,
            prepare_in_advance => PrepareInAdvance,
            async_call_pools => [?ASYNC_CALL_POOL_ID]
        },
        first_lane_id => FirstLaneId,
        next_lane_id => NextLaneId
    }.

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

count_lane_elements(#{
    task_type := WorkflowType,
    lane_index := LaneIndex,
    lane_id := LaneId,
    prepare_in_advance := PrepareInAdvance,
    is_lane_prepared := IsLanePrepared,
    should_prepare_next_lane := ShouldPrepareNextLane
} = LaneExecutionContext, TaskIds, ExpectedItems, Options) ->
    TasksPerItemCount = count_tasks(TaskIds),
    TasksCount = TasksPerItemCount * length(ExpectedItems),

    TaskStreams = kv_utils:get([task_streams, LaneIndex], LaneExecutionContext, #{}),
    TaskStreamCount = maps:size(TaskStreams),

    PrepareCallbacksCount = case {PrepareInAdvance, IsLanePrepared, ShouldPrepareNextLane} of
        {true, true, false} -> 0;
        {true, false, true} -> 2;
        _ -> 1
    end,
    NotificationsCount = TasksPerItemCount + TaskStreamCount + PrepareCallbacksCount + 1, % Notification for each task + prepare_lane
                                                                                          % callbacks + handle_lane_execution_ended

    DataProcessingCallbackCallCount = maps:fold(fun(_, CallbackCalls, Acc) ->
        lists:foldl(fun
            ({_, CallsCount}, InternalAcc) -> InternalAcc + CallsCount;
            (_, InternalAcc) -> InternalAcc + 1
        end, Acc, CallbackCalls)
    end, 0, TaskStreams),

    BasicLaneElementsCount = case WorkflowType of
        sync -> TasksCount + NotificationsCount + DataProcessingCallbackCallCount;
        async -> 3 * TasksCount + NotificationsCount + DataProcessingCallbackCallCount
    end,

    case {Options, WorkflowType} of
        {#{fail_job := {LaneId, FailedTask, _}}, sync} ->
            BasicLaneElementsCount - count_not_executed_tasks(TaskIds, FailedTask) + 1;
        {#{fail_job := {LaneId, FailedTask, _}}, async} ->
            BasicLaneElementsCount - 3 * count_not_executed_tasks(TaskIds, FailedTask) - 1;
        {#{timeout := {LaneId, FailedTask, _}}, async} ->
            BasicLaneElementsCount - 3 * count_not_executed_tasks(TaskIds, FailedTask) + 1;
        {#{fail_result_processing := {LaneId, FailedTask, _}}, async} ->
            BasicLaneElementsCount - 3 * count_not_executed_tasks(TaskIds, FailedTask) + 1;
        _ ->
            BasicLaneElementsCount
    end.

count_tasks(Tasks) ->
    lists:foldl(fun(TasksForBox, Acc) -> sets:size(TasksForBox) + Acc end, 0, Tasks).

count_not_executed_tasks(TaskIds, FailedTask) ->
    [_ | IgnoredTaskIds] = lists:dropwhile(fun(TasksForBox) ->
        not sets:is_element(FailedTask, TasksForBox)
    end, TaskIds),
    count_tasks(IgnoredTaskIds).

get_engine_id() ->
    ?ENGINE_ID.