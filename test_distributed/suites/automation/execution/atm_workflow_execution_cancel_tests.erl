%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning cancel of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_cancel_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    cancel_scheduled_atm_workflow_execution/0,
    cancel_enqueued_atm_workflow_execution/0,

    cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results/0,
    cancel_active_atm_workflow_execution_with_uncorrelated_task_results/0,

    cancel_paused_atm_workflow_execution/0,
    cancel_interrupted_atm_workflow_execution/0,

    cancel_resuming_paused_atm_workflow_execution/0,
    cancel_resuming_interrupted_atm_workflow_execution/0
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
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"iterated_store">>, lists:seq(1, __ITEMS_COUNT)),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"task1_dst_store">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"task2_dst_store">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"task3_dst_store">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"task4_dst_store">>)
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

                % Check that cancelled executions are not retried automatically
                max_retries = ?RAND_INT(3, 6)
            },
            % Check what happens to lane run preparing in advance (in various stages of preparation)
            % when previous lane run is cancelled
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
        ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(#atm_data_spec{type = atm_integer_type}, __RELAY_METHOD)
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(
    ?FUNCTION_NAME, ?RAND_INT(5, 10), return_value)
).


%%%===================================================================
%%% Tests
%%%===================================================================


cancel_scheduled_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{after_step_exp_state_diff = no_diff}
                },
                ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


cancel_enqueued_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % await for next lane run to prepare in advance
                        defer_after = {prepare_lane, after_step, {2, 1}},

                        after_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        after_step_exp_state_diff = [
                            {all_tasks, {1, 1}, cancelled},
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ]
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, stopping}
                        ],
                        after_step_exp_state_diff = [{lane_run, {2, 1}, interrupted}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results() ->
    cancel_active_atm_workflow_execution_test_base(?FUNCTION_NAME, return_value).


cancel_active_atm_workflow_execution_with_uncorrelated_task_results() ->
    cancel_active_atm_workflow_execution_test_base(?FUNCTION_NAME, file_pipe).


%% @private
cancel_active_atm_workflow_execution_test_base(Testcase, RelayMethod) ->
    ItemCount = 40,

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
                        % Delay execution of last batch to ensure it happens after execution is cancelled
                        strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, _, ItemBatch]}) ->
                            case lists:member(ItemCount, ItemBatch) of
                                true -> {passthrough_with_delay, timer:seconds(2)};
                                false -> passthrough
                            end
                        end
                    },
                    process_task_result_for_item = #atm_step_mock_spec{
                        % Cancel happens before processing results for first item executed for either
                        % task1 or task2 (more items could have returned results but it is also possible
                        % that no item was processed for one of the tasks). This should result in:
                        % - task1 or task2 transitions to 'stopping' status (one of them may immediately
                        %   transition to 'cancelled' if no item was scheduled).
                        % - task3 transitions to 'cancelled' as definitely no item was scheduled for it.
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        before_step_exp_state_diff = [
                            {all_tasks, {1, 1}, stopping_due_to, cancel},  %% TODO VFS-9917 expectation per task ?
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ]
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
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            case atm_workflow_execution_exp_state_builder:get_task_schema_id(
                                AtmTaskExecutionId, ExpState
                            ) of
                                <<"task3">> ->
                                    false;
                                _ ->
                                    {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                                        {task, AtmTaskExecutionId, cancelled},
                                        {task, AtmTaskExecutionId, parallel_box_transitioned_to_inferred_status, fun
                                            (<<"stopping">>, [<<"cancelled">>, <<"stopping">>]) -> <<"stopping">>;
                                            (<<"stopping">>, [<<"cancelled">>]) -> <<"cancelled">>
                                        end}
                                    ])}
                            end
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            assert_exception_store_is_empty({1, 1}, AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        % Previously enqueued (in advance) lane run is changed to interrupted
                        after_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, interrupted}
                        ]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


cancel_paused_atm_workflow_execution() ->
    PausedIncarnation = build_paused_active_incarnation_test_spec(),
    cancel_suspended_atm_workflow_execution_test_base(?FUNCTION_NAME, PausedIncarnation).


cancel_interrupted_atm_workflow_execution() ->
    InterruptedIncarnation = build_interrupted_active_incarnation_test_spec(),
    cancel_suspended_atm_workflow_execution_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
cancel_suspended_atm_workflow_execution_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, ?RAND_INT(5, 10), return_value
        ),
        incarnations = [SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
            after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpSuspendedState}) ->
                atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx),

                ExpCancelledState = atm_workflow_execution_exp_state_builder:expect(ExpSuspendedState, [
                    workflow_stopping,
                    {all_tasks, {2, 1}, cancelled},
                    {lane_run, {2, 1}, cancelled},
                    {lane_runs, [{1, 1}, {2, 1}], rerunable},
                    workflow_cancelled
                ]),
                ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpCancelledState)),

                atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed(
                    AtmMockCallCtx#atm_mock_call_ctx{workflow_execution_exp_state = ExpCancelledState}
                )
            end
        }]
    }).


cancel_resuming_paused_atm_workflow_execution() ->
    PausedIncarnation = build_paused_active_incarnation_test_spec(),
    cancel_resuming_atm_workflow_execution_test_base(?FUNCTION_NAME, PausedIncarnation).


cancel_resuming_interrupted_atm_workflow_execution() ->
    InterruptedIncarnation = build_interrupted_active_incarnation_test_spec(),
    cancel_resuming_atm_workflow_execution_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
cancel_resuming_atm_workflow_execution_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, ?RAND_INT(5, 10), return_value
        ),
        incarnations = [
            SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpSuspendedState}) ->
                    atm_workflow_execution_test_utils:resume_workflow_execution(AtmMockCallCtx),

                    ExpResumingState = atm_workflow_execution_exp_state_builder:expect(ExpSuspendedState, [
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
                            before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                            before_step_exp_state_diff = [
                                {all_tasks, {2, 1}, cancelled},
                                {lane_run, {2, 1}, stopping},
                                workflow_stopping
                            ]
                        },
                        % This is called from within 'resume_lane'
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{lane_run, {2, 1}, cancelled}]
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_cancelled
                    ]
                },
                after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
            }
        ]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
build_paused_active_incarnation_test_spec() ->
    #atm_workflow_execution_incarnation_test_spec{
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
                    after_step_exp_state_diff = [{all_tasks, {2, 1}, paused}]
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [{lane_run, {2, 1}, paused}]
                }
            }
        ],
        handle_workflow_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_paused]
        }
    }.


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
                    after_step_exp_state_diff = no_diff
                },

                % this is called as part of `handle_workflow_abruptly_stopped`
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
assert_exception_store_is_empty(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertEqual([], lists:sort(atm_workflow_execution_test_utils:get_exception_store_content(
        AtmLaneRunSelector, AtmMockCallCtx
    ))).
