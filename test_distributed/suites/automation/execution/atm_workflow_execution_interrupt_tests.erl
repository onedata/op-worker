%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning interrupt of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_interrupt_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    interrupt_scheduled_atm_workflow_execution_due_to_internal_exception/0,
    interrupt_scheduled_atm_workflow_execution_due_to_external_abandon/0,

    interrupt_enqueued_atm_workflow_execution_due_to_internal_exception/0,
    interrupt_enqueued_atm_workflow_execution_due_to_external_abandon/0,

    interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_internal_exception/0,
    interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_internal_exception/0,
    interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_external_abandon/0,
    interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_external_abandon/0,

    interrupt_paused_atm_workflow_execution/0
]).


-define(ECHO_ATM_TASK_SCHEMA_DRAFT(__ID, __TARGET_STORE_SCHEMA_ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = __TARGET_STORE_SCHEMA_ID,
        store_content_update_options = #atm_list_store_content_update_options{function = append}
    }]
}).

-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __ITEMS_COUNT, __RELAY_METHOD), #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(__TESTCASE),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(<<"iterated_store">>, lists:seq(1, __ITEMS_COUNT)),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(<<"task1_dst_store">>),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(<<"task2_dst_store">>),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(<<"task3_dst_store">>),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(<<"task4_dst_store">>)
        ],
        lanes = [
            #atm_lane_schema_draft{
                id = <<"lane1">>,
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task1">>, <<"task1_dst_store">>),
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task2">>, <<"task2_dst_store">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task3">>, <<"task3_dst_store">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"iterated_store">>},

                % Check that paused executions are not retried automatically
                max_retries = ?RAND_INT(3, 6)
            },
            % Check what happens to lane run preparing in advance (in various stages of preparation)
            % when previous lane run is paused
            #atm_lane_schema_draft{
                id = <<"lane2">>,
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb3">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task4">>, <<"task4_dst_store">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"iterated_store">>},
                max_retries = ?RAND_INT(3, 6)
            }
        ]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ATM_INTEGER_DATA_SPEC, __RELAY_METHOD)
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, 5, return_value)).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).
-define(TASK2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task2">>}).
-define(TASK3_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task3">>}).

-define(PB1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>}).
-define(PB2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>}).

-type interrupt_type() :: internal_exception | external_abandon.


%%%===================================================================
%%% Tests
%%%===================================================================


interrupt_scheduled_atm_workflow_execution_due_to_internal_exception() ->
    interrupt_scheduled_atm_workflow_execution_test_base(internal_exception).


interrupt_scheduled_atm_workflow_execution_due_to_external_abandon() ->
    interrupt_scheduled_atm_workflow_execution_test_base(external_abandon).


%% @private
-spec interrupt_scheduled_atm_workflow_execution_test_base(interrupt_type()) ->
    ok.
interrupt_scheduled_atm_workflow_execution_test_base(InterruptType) ->
    StoppingExecutionExpStateDiff = [
        {lane_run, {1, 1}, stopping},
        workflow_stopping
    ],

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        before_step_hook = get_interrupt_hook(InterruptType),
                        % State is changed immediately only by external interrupt procedure
                        before_step_exp_state_diff = case InterruptType of
                            internal_exception -> default;
                            external_abandon -> StoppingExecutionExpStateDiff
                        end,
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, interrupted}]
                    }
                },
                ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, before_step, {1, 1}})
            ],
            % Callback called only in case of interrupt caused by exception
            handle_exception = #atm_step_mock_spec{after_step_exp_state_diff = case InterruptType of
                internal_exception -> StoppingExecutionExpStateDiff;
                external_abandon -> default
            end},
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_interrupted]
            },
            after_hook = fun assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


interrupt_enqueued_atm_workflow_execution_due_to_internal_exception() ->
    interrupt_enqueued_atm_workflow_execution_test_base(internal_exception).


interrupt_enqueued_atm_workflow_execution_due_to_external_abandon() ->
    interrupt_enqueued_atm_workflow_execution_test_base(external_abandon).


%% @private
-spec interrupt_enqueued_atm_workflow_execution_test_base(interrupt_type()) ->
    ok.
interrupt_enqueued_atm_workflow_execution_test_base(InterruptType) ->
    StoppingExecutionExpStateDiff = [
        {all_tasks, {1, 1}, interrupted},
        {lane_run, {1, 1}, stopping},
        workflow_stopping
    ],

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Await for next lane run to prepare in advance
                        defer_after = {prepare_lane, after_step, {2, 1}}
                    },
                    run_task_for_item = #atm_step_mock_spec{
                        before_step_hook = get_interrupt_hook(InterruptType),
                        % State is changed immediately only by external interrupt procedure
                        before_step_exp_state_diff = case InterruptType of
                            internal_exception -> default;
                            external_abandon -> StoppingExecutionExpStateDiff
                        end,
                        after_step_exp_state_diff = no_diff
                    },
                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, interrupted}]
                    }
                },
                ?INTERRUPTED_LANE_RUN_PREPARED_IN_ADVANCE_TEST_SPEC({2, 1})
            ],
            % Callback called only in case of interrupt caused by exception
            handle_exception = #atm_step_mock_spec{after_step_exp_state_diff = case InterruptType of
                internal_exception -> StoppingExecutionExpStateDiff;
                external_abandon -> default
            end},
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    workflow_interrupted
                ]
            },
            after_hook = fun assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_internal_exception() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, internal_exception, return_value).


interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_internal_exception() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, internal_exception, file_pipe).


interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_external_abandon() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, external_abandon, return_value).


interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_external_abandon() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, external_abandon, file_pipe).


%% @private
interrupt_active_atm_workflow_execution_test_base(Testcase, InterruptType, RelayMethod) ->
    ItemCount = 20,

    ExpFinalStatus = case RelayMethod of
        return_value -> interrupted;
        file_pipe -> failed
    end,
    InferTaskStatusAfterInterruptFun = fun(ExpTaskState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpTaskState#{<<"status">> => case IIP + IP of
            0 -> <<"interrupted">>;
            _ -> <<"stopping">>
        end}
    end,
    InferStoppedTaskStatusFun = fun
        (ExpTaskState = #{<<"status">> := <<"stopping">>}) ->
            atm_workflow_execution_exp_state_builder:adjust_abruptly_stopped_task_stats(ExpTaskState#{
                <<"status">> => str_utils:to_binary(ExpFinalStatus)
            });
        (ExpTaskState = #{<<"status">> := <<"interrupted">>}) ->
            ExpTaskState
    end,

    StoppingExecutionExpStateDiff = [
        % task1 or task2 transitions to 'stopping' status as for at least one of them some
        % items were scheduled
        {task, ?TASK1_SELECTOR({1, 1}), InferTaskStatusAfterInterruptFun},
        {task, ?TASK2_SELECTOR({1, 1}), InferTaskStatusAfterInterruptFun},
        {parallel_box, ?PB1_SELECTOR({1, 1}), stopping},

        % task3 immediately transitions to 'interrupted' as definitely no item was scheduled for it
        {task, ?TASK3_SELECTOR({1, 1}), interrupted},
        {parallel_box, ?PB2_SELECTOR({1, 1}), interrupted},

        {lane_run, {1, 1}, stopping},
        workflow_stopping
    ],

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(Testcase, ItemCount, RelayMethod),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Await lane run in advance prepared and enqueued
                        defer_after = {prepare_lane, after_step, {2, 1}}
                    },
                    run_task_for_item = #atm_step_mock_spec{
                        % Delay execution of last batch to ensure it happens after execution is paused
                        strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, _, ItemBatch]}) ->
                            case lists:member(ItemCount, ItemBatch) of
                                true -> {passthrough_with_delay, timer:seconds(2)};
                                false -> passthrough
                            end
                        end
                    },
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = get_interrupt_hook(InterruptType),
                        % State is changed immediately only by external interrupt procedure
                        before_step_exp_state_diff = case InterruptType of
                            internal_exception -> default;
                            external_abandon -> StoppingExecutionExpStateDiff
                        end
                    },
                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            assert_exception_store_is_empty({1, 1}, AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = [
                            {task, ?TASK1_SELECTOR({1, 1}), InferStoppedTaskStatusFun},
                            {task, ?TASK2_SELECTOR({1, 1}), InferStoppedTaskStatusFun},
                            {parallel_box, ?PB1_SELECTOR({1, 1}), ExpFinalStatus},
                            {lane_run, {1, 1}, ExpFinalStatus}
                        ]
                    }
                },
                ?INTERRUPTED_LANE_RUN_PREPARED_IN_ADVANCE_TEST_SPEC({2, 1})
            ],
            % Callback called only in case of interrupt caused by exception
            handle_exception = #atm_step_mock_spec{after_step_exp_state_diff = case InterruptType of
                internal_exception -> StoppingExecutionExpStateDiff;
                external_abandon -> default
            end},
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = case ExpFinalStatus of
                    interrupted ->
                        [
                            {lane_run, {2, 1}, removed},
                            workflow_interrupted
                        ];
                    failed ->
                        [
                            {lane_run, {2, 1}, removed},
                            {lane_runs, [{1, 1}], rerunable},
                            workflow_failed
                        ]
                end
            },
            after_hook = case ExpFinalStatus of
                interrupted -> fun assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated/1;
                failed -> fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
            end
        }]
    }).


interrupt_paused_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = [
                            {all_tasks, {2, 1}, stopping},
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ]
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task4">> => [
                                {task, ?TASK_ID_PLACEHOLDER, paused},
                                {parallel_box, ?PB_SELECTOR_PLACEHOLDER, paused}
                            ]
                        })
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {2, 1}, paused}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_paused]
            },
            after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                ?assertEqual(
                    ?ERROR_ATM_INVALID_STATUS_TRANSITION(?PAUSED_STATUS, ?STOPPING_STATUS),
                    atm_workflow_execution_test_utils:interrupt_workflow_execution(AtmMockCallCtx)
                ),
                atm_workflow_execution_test_utils:assert_not_ended_workflow_execution_can_not_be_repeated(
                    {1, 1}, AtmMockCallCtx
                ),
                ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0))
            end
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_interrupt_hook(interrupt_type()) -> atm_workflow_execution_test_runner:hook().
get_interrupt_hook(internal_exception) -> fun atm_workflow_execution_test_utils:delete_offline_session/1;
get_interrupt_hook(external_abandon) -> fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1.


%% @private
assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    ?assertThrow(
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(?INTERRUPTED_STATUS, ?STOPPING_STATUS),
        atm_workflow_execution_test_utils:pause_workflow_execution(AtmMockCallCtx)
    ),
    atm_workflow_execution_test_utils:assert_not_ended_workflow_execution_can_not_be_repeated(
        {1, 1}, AtmMockCallCtx
    ),
    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0)).


%% @private
assert_exception_store_is_empty(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertEqual([], lists:sort(atm_workflow_execution_test_utils:get_exception_store_content(
        AtmLaneRunSelector, AtmMockCallCtx
    ))).
