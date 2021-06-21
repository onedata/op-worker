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
    single_sync_workflow_execution_test/1,
    single_async_workflow_execution_test/1,
    multiple_sync_workflow_execution_test/1,
    multiple_async_workflow_execution_test/1,
    fail_one_of_many_task_in_box_test/1,
    fail_only_task_in_box_test/1,
    fail_only_task_in_lane_test/1,
    timeout_test/1,
    heartbeat_test/1,
    preparation_failure_test/1,
    lane_preparation_failure_test/1
]).

all() ->
    ?ALL([
        single_sync_workflow_execution_test,
        single_async_workflow_execution_test,
        multiple_sync_workflow_execution_test,
        multiple_async_workflow_execution_test,
        fail_one_of_many_task_in_box_test,
        fail_only_task_in_box_test,
        fail_only_task_in_lane_test,
        timeout_test,
        heartbeat_test,
        preparation_failure_test,
        lane_preparation_failure_test
    ]).

-define(ENGINE_ID, <<"test_engine">>).
-define(ASYNC_CALL_POOL_ID, <<"test_call_pool">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

single_sync_workflow_execution_test(Config) ->
    single_workflow_execution_test_base(Config, sync, <<"test_workflow">>).

single_async_workflow_execution_test(Config) ->
    single_workflow_execution_test_base(Config, async, <<"async_test_workflow">>).

single_workflow_execution_test_base(Config, WorkflowType, Id) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
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
    verify_memory(Config, InitialKeys),
    ok.

multiple_sync_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, sync).

multiple_async_workflow_execution_test(Config) ->
    multiple_workflow_execution_test_base(Config, async).

multiple_workflow_execution_test_base(Config, WorkflowType) ->
    InitialKeys = get_all_keys(Config),

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
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType, length(Workflows), false),
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

    verify_memory(Config, InitialKeys),
    ok.

fail_one_of_many_task_in_box_test(Config) ->
    failure_test_base(Config, <<"fail_one_test_workflow">>, <<"fail_one_test_workflow_task3_3_2">>, 3, 3).

fail_only_task_in_box_test(Config) ->
    failure_test_base(Config, <<"fail_only_task_in_box_test_workflow">>,
        <<"fail_only_task_in_box_test_workflow_task3_1_1">>, 3, 1).

fail_only_task_in_lane_test(Config) ->
    failure_test_base(Config, <<"fail_only_task_in_lane_test_workflow">>,
        <<"fail_only_task_in_lane_test_workflow_task1_1_1">>, 1, 1).

failure_test_base(Config, Id, TaskToFail, LaneWithErrorIndex, BoxWithErrorIndex) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowType = sync,
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    ItemToFail = <<"100">>,
    set_task_execution_gatherer_option(Config, fail_job, {TaskToFail, ItemToFail}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order_with_error(
        Workflow, LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail),
    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),
    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistoryWithoutPrepare, WorkflowType),
    verify_memory(Config, InitialKeys, true),
    ct:print("Execution with error verified"),

    unset_task_execution_gatherer_option(Config, fail_job),
    ExpectedAfterRestart = get_expected_task_execution_order_after_restart(Workflow, LaneWithErrorIndex, ItemToFail),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),
    #{execution_history := ExecutionHistoryAfterRestart} = ExtendedHistoryStatsAfterRestart =
        get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStatsAfterRestart, WorkflowType, 1, true),
    ?assertNotEqual(timeout, ExecutionHistoryAfterRestart),
    verify_execution_history(ExpectedAfterRestart, ExecutionHistoryAfterRestart, WorkflowType),
    verify_memory(Config, InitialKeys),
    ok.

timeout_test(Config) ->
    timeout_test(Config, <<"async_timeouts_workflow_task3_3_2">>, 3, 3).

timeout_test(Config, TaskToFail, LaneWithErrorIndex, BoxWithErrorIndex) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowType = async,
    Id = <<"async_timeouts_workflow">>,
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    ItemToFail = <<"100">>,
    ResultToFail = <<"result_", TaskToFail/binary>>,
    set_task_execution_gatherer_option(Config, fail_job, {ResultToFail, ItemToFail}),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order_with_timeout(
        Workflow, LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail),
    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),
    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistoryWithoutPrepare, WorkflowType),
    verify_memory(Config, InitialKeys, true),
    ct:print("Execution with error verified"),

    unset_task_execution_gatherer_option(Config, fail_job),
    ExpectedAfterRestart = get_expected_task_execution_order_after_restart(Workflow, LaneWithErrorIndex, ItemToFail),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),
    #{execution_history := ExecutionHistoryAfterRestart} = ExtendedHistoryStatsAfterRestart =
        get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStatsAfterRestart, WorkflowType, 1, true),
    ?assertNotEqual(timeout, ExecutionHistoryAfterRestart),
    verify_execution_history(ExpectedAfterRestart, ExecutionHistoryAfterRestart, WorkflowType),
    verify_memory(Config, InitialKeys),
    ok.

heartbeat_test(Config) ->
    ResultToDelay = <<"result_async_test_workflow_task3_3_2">>,
    ItemToDelay = <<"100">>,
    set_task_execution_gatherer_option(Config, delay_execution, {ResultToDelay, ItemToDelay}),
    single_workflow_execution_test_base(Config, async, <<"async_heartbeat_test_workflow">>),
    unset_task_execution_gatherer_option(Config, delay_execution).

preparation_failure_test(Config) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowType = sync,
    Id = <<"preparation_failure_test_workflow">>,
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    set_task_execution_gatherer_option(Config, fail_preparation, Id),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    #{execution_history := ExecutionHistory} = get_task_execution_history(Config),
    ?assertNotEqual(timeout, ExecutionHistory),
    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    ?assertEqual([], ExecutionHistoryWithoutPrepare),
    verify_memory(Config, InitialKeys),
    ct:print("Execution with error verified"),

    unset_task_execution_gatherer_option(Config, fail_preparation),
    ExpectedAfterRestart = get_expected_task_execution_order(Workflow),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),
    #{execution_history := ExecutionHistoryAfterRestart} = ExtendedHistoryStatsAfterRestart =
        get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStatsAfterRestart, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistoryAfterRestart),
    ExecutionHistoryAfterRestartWithoutPrepare = verify_preparation_phase(Id, ExecutionHistoryAfterRestart),

    verify_execution_history(ExpectedAfterRestart, ExecutionHistoryAfterRestartWithoutPrepare, WorkflowType),
    verify_memory(Config, InitialKeys),
    ok.

lane_preparation_failure_test(Config) ->
    InitialKeys = get_all_keys(Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    WorkflowType = sync,
    Id = <<"lane_preparation_failure_test_workflow">>,
    Workflow = #{
        id => Id,
        workflow_handler => workflow_test_handler,
        execution_context => #{type => WorkflowType, async_call_pools => [?ASYNC_CALL_POOL_ID]}
    },

    set_task_execution_gatherer_option(Config, fail_lane_preparation, 1),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    #{execution_history := ExecutionHistoryWithoutTasks} = get_task_execution_history(Config),
    ?assertNotEqual(timeout, ExecutionHistoryWithoutTasks),
    ExecutionHistoryWithoutTasksAndPrepare = verify_preparation_phase(Id, ExecutionHistoryWithoutTasks),
    ?assertEqual([], ExecutionHistoryWithoutTasksAndPrepare),
    verify_memory(Config, InitialKeys),
    ct:print("Execution with error verified"),

    set_task_execution_gatherer_option(Config, fail_lane_preparation, 3),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),

    Expected = get_expected_task_execution_order_with_lane_preparation_error(Workflow, 3),
    #{execution_history := ExecutionHistory} = ExtendedHistoryStats = get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStats, WorkflowType),
    ?assertNotEqual(timeout, ExecutionHistory),
    ExecutionHistoryWithoutPrepare = verify_preparation_phase(Id, ExecutionHistory),
    verify_execution_history(Expected, ExecutionHistoryWithoutPrepare, WorkflowType),
    verify_memory(Config, InitialKeys, true),
    ct:print("Execution with secoond error verified"),

    unset_task_execution_gatherer_option(Config, fail_lane_preparation),
    ExpectedAfterRestart = get_expected_task_execution_order_after_restart(Workflow, 3, <<"0">>),
    ?assertEqual(ok, rpc:call(Worker, workflow_engine, execute_workflow, [?ENGINE_ID, Workflow])),
    #{execution_history := ExecutionHistoryAfterRestart} = ExtendedHistoryStatsAfterRestart =
        get_task_execution_history(Config),
    verify_execution_history_stats(ExtendedHistoryStatsAfterRestart, WorkflowType, 1, true),
    ?assertNotEqual(timeout, ExecutionHistoryAfterRestart),
    verify_execution_history(ExpectedAfterRestart, ExecutionHistoryAfterRestart, WorkflowType),
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
    Master = spawn(fun start_task_execution_gatherer/0),
    test_utils:mock_new(Workers, [workflow_test_handler, workflow_engine_callback_handler]),

    test_utils:mock_expect(Workers, workflow_test_handler, prepare, fun(ExecutionId, Context) ->
        Master ! {preparation, self(), <<ExecutionId/binary, "_prepare">>},
        receive
            history_saved -> meck:passthrough([ExecutionId, Context]);
            fail_preparation -> error
        end
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, get_lane_spec, fun(ExecutionId, Context, LaneIndex) ->
        Master ! {lane_preparation, self(), LaneIndex},
        receive
            history_saved -> meck:passthrough([ExecutionId, Context, LaneIndex]);
            fail_preparation -> error
        end
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
        {_CallbackType, ExecutionId, EngineId, JobIdentifier, _CallPools} =
            workflow_engine_callback_handler:decode_callback_id(CallbackId),
        {_, _, TaskId} = workflow_execution_state:get_result_processing_data(ExecutionId, JobIdentifier),
        Item = workflow_cached_item:get_item(workflow_execution_state:get_item_id(ExecutionId, JobIdentifier)),
        Master ! {task_processing, self(), <<"result_", TaskId/binary>>, Item},
        receive
            history_saved ->
                % Warning: do not use meck:passthrough as it does not work when 2 mocks work within one process
                apply(meck_util:original_name(workflow_engine_callback_handler), handle_callback, [CallbackId, Result]);
            delay_execution ->
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
            fail_job ->
                ok
        end
    end),

    test_utils:mock_expect(Workers, workflow_test_handler, handle_lane_execution_ended,
        fun(ExecutionId, Context, LaneIndex) ->
            Master ! {lane_ended, LaneIndex, workflow_execution_state:is_finished_and_cleaned(ExecutionId, LaneIndex)},
            meck:passthrough([ExecutionId, Context, LaneIndex])
        end
    ),

    [{task_execution_gatherer, Master} | Config].

end_per_testcase(_, Config) ->
    ?config(task_execution_gatherer, Config) ! stop,
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
                #{delay_execution := {Task, Item}} ->
                    Sender ! delay_execution,
                    Acc3#{execution_history => [{Task, Item} | History]};
                _ ->
                    Sender ! history_saved,
                    Acc3#{execution_history => [{Task, Item} | History]}
            end,
            case ProcWaitingForAns of
                undefined -> ok;
                _ -> ProcWaitingForAns ! gathering_task_execution_history
            end,
            task_execution_gatherer_loop(Acc4, ProcWaitingForAns, Options);
        {preparation, Sender, Log} ->
            Acc2 = update_slots_usage_statistics(Acc, async_slots_used_stats, rpc:call(node(Sender),
                workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID])),
            Acc3 = update_slots_usage_statistics(Acc2, pool_slots_used_stats, rpc:call(node(Sender),
                workflow_engine_state, get_slots_used, [?ENGINE_ID])),
            case Options of
                #{fail_preparation := _} ->
                    Sender ! fail_preparation;
                _ ->
                    Sender ! history_saved
            end,
            Acc4 = Acc3#{execution_history => [{Log, undefined} | History]},
            task_execution_gatherer_loop(Acc4, ProcWaitingForAns, Options);
        {lane_preparation, Sender, LaneIndex} ->
            Acc2 = update_slots_usage_statistics(Acc, async_slots_used_stats, rpc:call(node(Sender),
                workflow_async_call_pool, get_slot_usage, [?ASYNC_CALL_POOL_ID])),
            case Options of
                #{fail_lane_preparation := LaneIndex} ->
                    Sender ! fail_preparation;
                _ ->
                    Sender ! history_saved
            end,
            task_execution_gatherer_loop(Acc2, ProcWaitingForAns, Options);
        {lane_ended, LaneIndex, IsFinished} ->
            IsFinishedList = maps:get(is_finished_and_cleaned, Acc, []),
            Acc2 = Acc#{is_finished_and_cleaned => [{LaneIndex, IsFinished} | IsFinishedList]},
            task_execution_gatherer_loop(Acc2, ProcWaitingForAns, Options);
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

verify_execution_history_stats(Acc, WorkflowType) ->
    verify_execution_history_stats(Acc, WorkflowType, 1, false).

% TODO VFS-7784 - uncomment checks in this function and fix tests
verify_execution_history_stats(Acc, WorkflowType, WorkflowNumber, IsPrepared) ->
    lists:foreach(fun(IsFinished) ->
        ?assertMatch({_, true}, IsFinished)
    end, maps:get(is_finished_and_cleaned, Acc, [])),

    ?assertEqual(0, maps:get(final_async_slots_used, Acc)),
    ?assertEqual(0, maps:get(final_pool_slots_used, Acc)),

    {MinAsyncSlots, MaxAsyncSlots} = maps:get(async_slots_used_stats, Acc),
    {MinPoolSlots, MaxPoolSlots} = maps:get(pool_slots_used_stats, Acc),
    ?assertEqual(0, MinAsyncSlots),
    case WorkflowType of
        sync ->
            ?assertEqual(0, MaxAsyncSlots),
            % Task processing is initialized after pool slots count is incremented
            % and it is finished before pool slots count is decremented so '0' should not appear in history
            ?assertNotEqual(0, MinPoolSlots),

            case IsPrepared of
                false ->
                    % Each workflow can block only 2 slots for preparation
                    % (one for preparation callback, one to check that nothing more can be executed)
                    ?assert(MinPoolSlots =< 2 * WorkflowNumber);
                true ->
                    ok
            end;
        async ->
            ?assertEqual(60, MaxAsyncSlots),
            % '0' should appear in history because slots count is decremented after async processing is scheduled
            ?assertEqual(0, MinPoolSlots)
    end,
    ?assertEqual(20, MaxPoolSlots).

set_task_execution_gatherer_option(Config, Key, Value) ->
    ?config(task_execution_gatherer, Config) ! {set_option, Key, Value}.

unset_task_execution_gatherer_option(Config, Key) ->
    ?config(task_execution_gatherer, Config) ! {unset_option, Key}.

get_expected_task_execution_order(#{id := ExecutionId, execution_context := Context}) ->
    get_expected_task_execution_order(1, ExecutionId, Context, full_execution).

get_expected_task_execution_order_with_error(#{id := ExecutionId, execution_context := Context},
    LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail) ->
    get_expected_task_execution_order(
        1, ExecutionId, Context,
        {expected_failure, LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail}
    ).

get_expected_task_execution_order_with_timeout(#{id := ExecutionId, execution_context := Context},
    LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail) ->
    get_expected_task_execution_order(
        1, ExecutionId, Context,
        {expected_timeout, LaneWithErrorIndex, BoxWithErrorIndex, ItemToFail, TaskToFail}
    ).

get_expected_task_execution_order_with_lane_preparation_error(#{id := ExecutionId, execution_context := Context},
    FailedLaneIndex) ->
    get_expected_task_execution_order(1, ExecutionId, Context, {expect_lane_failure, FailedLaneIndex}).

get_expected_task_execution_order_after_restart(#{id := ExecutionId, execution_context := Context},
    RestartedLaneIndex, RestartedItem) ->
    get_expected_task_execution_order(1, ExecutionId, Context, {restarted_from, RestartedLaneIndex, RestartedItem}).

get_expected_task_execution_order(LaneIndex, ExecutionId, Context, Description) ->
    {ok, #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        is_last := IsLast
    }} = workflow_test_handler:get_lane_spec(ExecutionId, Context, LaneIndex),
    Items = get_items(Context, Iterator),

    {LaneSpec, ShouldFinish} = case Description of
        {expected_failure, LaneIndex, BoxWithErrorIndex, ItemToFail, TaskToFail} ->
            TasksForFailedBox = maps:remove(TaskToFail, lists:nth(BoxWithErrorIndex, Boxes)),
            TasksForFailedItem = case maps:size(TasksForFailedBox) of
                0 -> lists:sublist(Boxes, BoxWithErrorIndex - 1);
                _ -> lists:sublist(Boxes, BoxWithErrorIndex - 1) ++ [TasksForFailedBox]
            end,
            {{Boxes, Items, ItemToFail, undefined, TasksForFailedItem}, true};
        {expected_timeout, LaneIndex, BoxWithErrorIndex, ItemToFail, TaskToFail} ->
            {{Boxes, Items, ItemToFail, TaskToFail, lists:sublist(Boxes, BoxWithErrorIndex)}, true};
        {expect_lane_failure, FailedLaneIndex} when LaneIndex =:= FailedLaneIndex ->
            {undefined, true};
        {restarted_from, RestartedLaneIndex, _RestartedItem} when LaneIndex < RestartedLaneIndex ->
            {undefined, IsLast};
        {restarted_from, LaneIndex, RestartedItem} ->
            FilteredItems = lists:filter(fun(Item) ->
                binary_to_integer(Item) >= binary_to_integer(RestartedItem)
            end, Items),
            {{Boxes, FilteredItems, undefined, undefined, []}, IsLast};
        _ ->
            {{Boxes, Items, undefined, undefined, []}, IsLast}
    end,

    case {ShouldFinish, LaneSpec} of
        {true, undefined} ->
            [];
        {true, _} ->
            [LaneSpec];
        {false, undefined} ->
            get_expected_task_execution_order(LaneIndex + 1, ExecutionId, Context, Description);
        {false, _} ->
            [LaneSpec | get_expected_task_execution_order(LaneIndex + 1, ExecutionId, Context, Description)]
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
verify_execution_history(
    [{ExpectedTasks, ExpectedItems, ItemToFail, TaskToIgnoreResult, TasksForFailedItem} = LaneExpected | ExpectedTail],
    Gathered, WorkflowType) ->
    TasksCount = calculate_tasks_count(LaneExpected),
    LaneElementsCount = case {WorkflowType, TaskToIgnoreResult} of
        {sync, _} -> TasksCount;
        {async, undefined} -> 2 * TasksCount;
        {async, _} -> 2 * TasksCount - 1
    end,
    
    ct:print("Verify ~p history elements", [LaneElementsCount]),
    GatheredForLane = lists:sublist(Gathered, LaneElementsCount),

    Remaining = lists:foldl(fun(Item, Acc) ->
        Filtered = lists:filtermap(fun({_Task, GatheredItem}) -> GatheredItem =:= Item end, Acc),
        case Item of
            ItemToFail ->
                verify_item_execution_history(Item, TasksForFailedItem, Filtered, WorkflowType, TaskToIgnoreResult);
            _ ->
                verify_item_execution_history(Item, ExpectedTasks, Filtered, WorkflowType, undefined)
        end,
        Acc -- Filtered
    end, GatheredForLane, ExpectedItems),

    ?assertEqual([], Remaining),

    verify_execution_history(ExpectedTail,
        lists:sublist(Gathered, LaneElementsCount + 1, length(Gathered) - LaneElementsCount), WorkflowType).

calculate_tasks_count({ExpectedTasks, ExpectedItems, undefined, undefined, []}) ->
    TasksTypesCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, ExpectedTasks),
    TasksTypesCount * length(ExpectedItems);
calculate_tasks_count({ExpectedTasks, ExpectedItems, _ItemToFail, _TaskToIgnoreResult, TasksForFailedItem} ) ->
    TasksTypesCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, ExpectedTasks),
    TasksForFailedItemCount = lists:foldl(fun(TasksList, Acc) -> maps:size(TasksList) + Acc end, 0, TasksForFailedItem),
    TasksTypesCount * (length(ExpectedItems) - 1) + TasksForFailedItemCount.

% Helper function for verify_execution_history/3 that verifies history for single item
verify_item_execution_history(_Item, ExpectedTasks, [], _WorkflowType, _TaskToIgnoreResult) ->
    ?assertEqual([], ExpectedTasks);
verify_item_execution_history(Item, [TasksInBox | ExpectedTasks], [{Task, Item} | Gathered], WorkflowType, TaskToIgnoreResult) ->
    ?assert(maps:is_key(Task, TasksInBox)),

    NewTasksInBox = case {Task, WorkflowType, Task =:= TaskToIgnoreResult} of
        {<<"result_", _/binary>>, _, _} -> TasksInBox;
        {_, sync, _} -> TasksInBox;
        {_, async, true} -> TasksInBox;
        {_, async, false} -> TasksInBox#{<<"result_", Task/binary>> => undefined}
    end,
    FinalTasksInBox = maps:remove(Task, NewTasksInBox),

    case maps:size(FinalTasksInBox) of
        0 ->
            verify_item_execution_history(Item, ExpectedTasks, Gathered, WorkflowType, TaskToIgnoreResult);
        _ ->
            verify_item_execution_history(Item, [FinalTasksInBox | ExpectedTasks], Gathered, WorkflowType, TaskToIgnoreResult)
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

    Models = [workflow_cached_item, workflow_iterator_snapshot, workflow_execution_state],
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