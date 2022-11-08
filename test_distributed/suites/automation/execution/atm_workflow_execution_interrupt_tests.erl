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
    interrupt_scheduled_atm_workflow_execution_due_to_expired_session/0,
    interrupt_scheduled_atm_workflow_execution_due_to_openfaas_down/0,

    interrupt_enqueued_atm_workflow_execution_due_to_expired_session/0,
    interrupt_enqueued_atm_workflow_execution_due_to_openfaas_down/0,

    interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_expired_session/0,
    interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_expired_session/0
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


%%%===================================================================
%%% Tests
%%%===================================================================


interrupt_scheduled_atm_workflow_execution_due_to_expired_session() ->
    interrupt_scheduled_atm_workflow_execution_test_base(
        exception,
        fun atm_workflow_execution_test_utils:delete_offline_session/1
    ).


interrupt_scheduled_atm_workflow_execution_due_to_openfaas_down() ->
    interrupt_scheduled_atm_workflow_execution_test_base(
        external,
        fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1
    ).


%% @private
interrupt_scheduled_atm_workflow_execution_test_base(InterruptType, InterruptHook) ->
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
                        before_step_hook = InterruptHook,
                        % State is changed immediately only by external interrupt procedure
                        before_step_exp_state_diff = case InterruptType of
                            exception -> default;
                            external -> StoppingExecutionExpStateDiff
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
                exception -> StoppingExecutionExpStateDiff;
                external -> default
            end},
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_interrupted]
            },
            after_hook = fun assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


interrupt_enqueued_atm_workflow_execution_due_to_expired_session() ->
    interrupt_enqueued_atm_workflow_execution_test_base(
        exception,
        fun atm_workflow_execution_test_utils:delete_offline_session/1
    ).


interrupt_enqueued_atm_workflow_execution_due_to_openfaas_down() ->
    interrupt_enqueued_atm_workflow_execution_test_base(
        external,
        fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1
    ).


%% @private
interrupt_enqueued_atm_workflow_execution_test_base(InterruptType, InterruptHook) ->
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
                        before_step_hook = InterruptHook,
                        % State is changed immediately only by external interrupt procedure
                        before_step_exp_state_diff = case InterruptType of
                            exception -> default;
                            external -> StoppingExecutionExpStateDiff
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
                exception -> StoppingExecutionExpStateDiff;
                external -> default
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


interrupt_active_atm_workflow_execution_with_no_uncorrelated_task_results_due_to_expired_session() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, return_value).


interrupt_active_atm_workflow_execution_with_uncorrelated_task_results_due_to_expired_session() ->
    interrupt_active_atm_workflow_execution_test_base(?FUNCTION_NAME, file_pipe).


%% @private
interrupt_active_atm_workflow_execution_test_base(Testcase, RelayMethod) ->
    ItemCount = 20,

    UpdateTaskStatusAfterPauseFun = fun(ExpTaskState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpTaskState#{<<"status">> => case IIP + IP of
            0 -> <<"interrupted">>;
            _ -> <<"stopping">>
        end}
    end,

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
                        before_step_hook = fun atm_workflow_execution_test_utils:delete_offline_session/1
                    },
                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            assert_exception_store_is_empty({1, 1}, AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = [
                            {all_tasks, {1, 1}, abruptly, interrupted},
                            {lane_run, {1, 1}, interrupted}
                        ]
                    }
                },
                ?INTERRUPTED_LANE_RUN_PREPARED_IN_ADVANCE_TEST_SPEC({2, 1})
            ],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = lists:flatten([
                    % task1 or task2 transitions to 'stopping' status as for at least one of them some
                    % items were scheduled
                    {task, ?TASK1_SELECTOR({1, 1}), UpdateTaskStatusAfterPauseFun},
                    {task, ?TASK2_SELECTOR({1, 1}), UpdateTaskStatusAfterPauseFun},
                    {parallel_box, ?PB1_SELECTOR({1, 1}), stopping},

                    % task3 immediately transitions to 'interrupted' as definitely no item was scheduled for it
                    {task, ?TASK3_SELECTOR({1, 1}), interrupted},
                    {parallel_box, ?PB2_SELECTOR({1, 1}), interrupted},

                    {lane_run, {1, 1}, stopping},
                    workflow_stopping
                ])
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    workflow_interrupted
                ]
            },
            after_hook = fun assert_interrupted_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
