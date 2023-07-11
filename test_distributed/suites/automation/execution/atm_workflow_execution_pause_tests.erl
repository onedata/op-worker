%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning pause of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_pause_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    pause_scheduled_atm_workflow_execution/0,
    pause_enqueued_atm_workflow_execution/0,

    pause_active_atm_workflow_execution_with_no_uncorrelated_task_results/0,
    pause_active_atm_workflow_execution_with_uncorrelated_task_results/0,

    pause_interrupted_atm_workflow_execution/0,
    pause_resuming_interrupted_atm_workflow_execution/0
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
        ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ATM_NUMBER_DATA_SPEC, __RELAY_METHOD)
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, 5, return_value)).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).
-define(TASK2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task2">>}).
-define(TASK3_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task3">>}).


%%%===================================================================
%%% Tests
%%%===================================================================


pause_scheduled_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    }
                },
                ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_paused]
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


pause_enqueued_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Await for next lane run to prepare in advance
                        defer_after = {prepare_lane, after_step, {2, 1}},

                        after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        after_step_exp_state_diff = [
                            {all_tasks, {1, 1}, paused},
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ]
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                    }
                },
                ?INTERRUPTED_LANE_RUN_PREPARED_IN_ADVANCE_TEST_SPEC({2, 1})
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    workflow_paused
                ]
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


pause_active_atm_workflow_execution_with_no_uncorrelated_task_results() ->
    pause_active_atm_workflow_execution_test_base(?FUNCTION_NAME, return_value).


pause_active_atm_workflow_execution_with_uncorrelated_task_results() ->
    pause_active_atm_workflow_execution_test_base(?FUNCTION_NAME, file_pipe).


%% @private
pause_active_atm_workflow_execution_test_base(Testcase, RelayMethod) ->
    ItemCount = 100,

    UpdateTaskStatusAfterPauseFun = fun(ExpTaskState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpTaskState#{<<"status">> => case IIP + IP of
            0 -> <<"paused">>;
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
                            Values = atm_workflow_execution_test_utils:get_values_batch(ItemBatch),
                            case lists:member(ItemCount, Values) of
                                true -> {passthrough_with_delay, timer:seconds(2)};
                                false -> passthrough
                            end
                        end
                    },
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = lists:flatten([
                            % task1 or task2 transitions to 'stopping' status as for at least one of them some
                            % items were scheduled
                            {task, ?TASK1_SELECTOR({1, 1}), UpdateTaskStatusAfterPauseFun},
                            {task, ?TASK2_SELECTOR({1, 1}), UpdateTaskStatusAfterPauseFun},

                            % task3 immediately transitions to 'paused' as definitely no item was scheduled for it
                            {task, ?TASK3_SELECTOR({1, 1}), paused},

                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Assert not all items were processed by tasks
                        before_step_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            {_, _, ExpItemsProcessed} = atm_workflow_execution_exp_state_builder:get_task_stats(
                                AtmTaskExecutionId, ExpState
                            ),
                            ?assert(ExpItemsProcessed < ItemCount),

                            AtmTaskSchemaId = atm_workflow_execution_exp_state_builder:get_task_schema_id(
                                AtmTaskExecutionId, ExpState
                            ),
                            #{<<"items">> := StDstItems} = atm_workflow_execution_test_utils:browse_store(
                                <<AtmTaskSchemaId/binary, "_dst_store">>, AtmMockCallCtx
                            ),
                            ?assertEqual(ExpItemsProcessed, length(StDstItems))
                        end,
                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            [<<"task1">>, <<"task2">>] => [{task, ?TASK_ID_PLACEHOLDER, paused}],
                            <<"task3">> => no_diff
                        })
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            assert_exception_store_is_empty({1, 1}, AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                    }
                },
                ?INTERRUPTED_LANE_RUN_PREPARED_IN_ADVANCE_TEST_SPEC({2, 1})
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    workflow_paused
                ]
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
        }]
    }).


pause_interrupted_atm_workflow_execution() ->
    InterruptedIncarnation = build_interrupted_active_incarnation_test_spec(),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [InterruptedIncarnation#atm_workflow_execution_incarnation_test_spec{
            after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                ?assertThrow(
                    ?ERROR_ATM_INVALID_STATUS_TRANSITION(?INTERRUPTED_STATUS, ?STOPPING_STATUS),
                    atm_workflow_execution_test_utils:pause_workflow_execution(AtmMockCallCtx)
                ),
                atm_workflow_execution_test_utils:assert_not_ended_workflow_execution_can_not_be_repeated(
                    {1, 1}, AtmMockCallCtx
                ),
                ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0))
            end
        }]
    }).


pause_resuming_interrupted_atm_workflow_execution() ->
    InterruptedIncarnation = build_interrupted_active_incarnation_test_spec(),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [
            InterruptedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpInterruptedState}) ->
                    atm_workflow_execution_test_utils:resume_workflow_execution(AtmMockCallCtx),

                    ExpResumingState = atm_workflow_execution_exp_state_builder:expect(ExpInterruptedState, [
                        {lane_run, {2, 1}, resuming},
                        workflow_resuming
                    ]),
                    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpResumingState))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        resume_lane = #atm_step_mock_spec{
                            before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                            before_step_exp_state_diff = lists:flatten([
                                {all_tasks, {2, 1}, paused},
                                {lane_run, {2, 1}, stopping},
                                workflow_stopping
                            ]),
                            after_step_exp_state_diff = no_diff
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{lane_run, {2, 1}, paused}]
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [workflow_paused]
                },
                after_hook = fun assert_paused_atm_workflow_execution_can_be_neither_paused_nor_repeated/1
            }
        ]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
build_interrupted_active_incarnation_test_spec() ->
    #atm_workflow_execution_incarnation_test_spec{
        incarnation_num = 1,
        lane_runs = [
            #atm_lane_run_execution_test_spec{
                selector = {1, 1}
            },
            #atm_lane_run_execution_test_spec{
                selector = {2, 1},
                process_task_result_for_item = #atm_step_mock_spec{
                    before_step_hook = fun atm_workflow_execution_test_utils:delete_offline_session/1,
                    before_step_exp_state_diff = no_diff
                },

                % This is called as part of `handle_workflow_abruptly_stopped`
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {all_tasks, {2, 1}, abruptly, interrupted},
                        {lane_run, {2, 1}, interrupted}
                    ]
                }
            }
        ],
        handle_exception = #atm_step_mock_spec{
            after_step_exp_state_diff = [
                {all_tasks, {2, 1}, stopping},
                {lane_run, {2, 1}, stopping},
                workflow_stopping
            ]
        },
        handle_workflow_abruptly_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_interrupted]
        }
    }.


%% @private
assert_paused_atm_workflow_execution_can_be_neither_paused_nor_repeated(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    ?assertThrow(
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(?PAUSED_STATUS, ?STOPPING_STATUS),
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
