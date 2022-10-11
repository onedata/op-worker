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
    cancel_interrupted_atm_workflow_execution/0
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
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_1">>, lists:seq(1, __ITEMS_COUNT)),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_2">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_3">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_4">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_devnull">>)
        ],
        lanes = [
            #atm_lane_schema_draft{
                id = <<"lane1">>,
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task1">>, <<"st_2">>),
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task2">>, <<"st_3">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task3">>, <<"st_4">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_1">>},

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
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task4">>, <<"st_devnull">>)]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb4">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task5">>, <<"st_devnull">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_4">>},
                max_retries = ?RAND_INT(3, 6)
            }
        ]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(#atm_data_spec{type = atm_integer_type}, __RELAY_METHOD)
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, 5, return_value)).


%%%===================================================================
%%% Tests
%%%===================================================================


cancel_scheduled_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {1, 1}},
                        after_step_exp_state_diff = no_diff
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            },
            % assert cancelled execution can not be cancelled again
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_cancelled/1
        }]
    }).


cancel_enqueued_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, before_step, {2, 1}},
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_started_preparing_in_advance(
                                {2, 1}, ExpState
                            )}
                        end,

                        after_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_cancelled({1, 1}, ExpState1),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                        end
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    prepare_lane = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {1, 1}},
                        before_step_exp_state_diff = no_diff,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            % Lane run creation began before execution was cancelled and as such all components
                            % should be created
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_created({2, 1}, ExpState)}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted({2, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({2, 1}, ExpState)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_removed({2, 1}, ExpState1),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState2)}
                end
            },
            % assert cancelled execution can not be cancelled again
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_cancelled/1
        }]
    }).


cancel_active_atm_workflow_execution_with_no_uncorrelated_task_results() ->
    cancel_active_atm_workflow_execution_test_base(?FUNCTION_NAME, return_value).


cancel_active_atm_workflow_execution_with_uncorrelated_task_results() ->
    cancel_active_atm_workflow_execution_test_base(?FUNCTION_NAME, file_pipe).


%% @private
cancel_active_atm_workflow_execution_test_base(Testcase, RelayMethod) ->
    ItemCount = 100,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(Testcase, ItemCount, RelayMethod),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {2, 1}}
                    },
                    run_task_for_item = #atm_step_mock_spec{
                        strategy = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            call_args = [_, _, _, _, ItemBatch]
                        }) ->
                            case 'get task selector for run_task_for_item step call'(AtmMockCallCtx) of
                                {_, <<"pb1">>, _} ->
                                    case lists:member(ItemCount, ItemBatch) of
                                        true ->
                                            % Delay execution of last batch to ensure it happens
                                            % after execution is cancelled
                                            {passthrough_with_delay, timer:seconds(2)};
                                        false ->
                                            passthrough
                                    end;
                                {_, <<"pb2">>, _} ->
                                    passthrough
                            end
                        end,
                        before_step_hook = fun(AtmMockCallCtx) ->
                            case 'get task selector for run_task_for_item step call'(AtmMockCallCtx) of
                                {_, <<"pb1">>, _} ->
                                    ok;
                                {_, <<"pb2">>, _} ->
                                    atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                            end
                        end,
                        before_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0
                        }) ->
                            case 'get task selector for run_task_for_item step call'(AtmMockCallCtx) of
                                {_, <<"pb1">>, _} ->
                                    false;
                                {_, <<"pb2">>, _} ->
                                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping(
                                        {1, 1}, ExpState0
                                    ),
                                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping(
                                        {1, 1}, cancel, ExpState1
                                    ),
                                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(
                                        ExpState2
                                    )}
                            end
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            ExpTaskStats = atm_workflow_execution_exp_state_builder:get_task_stats(
                                AtmTaskExecutionId, ExpState
                            ),
                            ExpItemsProcessed = element(3, ExpTaskStats),
                            % cancel blocks scheduling execution of leftover items
                            ?assert(ExpItemsProcessed < ItemCount),

                            DstAtmStoreSchemaId = case atm_workflow_execution_exp_state_builder:get_task_selector(
                                AtmTaskExecutionId, ExpState
                            ) of
                                {_, _, <<"task1">>} -> <<"st_2">>;
                                {_, _, <<"task2">>} -> <<"st_3">>;
                                {_, _, <<"task3">>} -> <<"st_4">>
                            end,
                            % assert all processed items were mapped to destination store
                            #{<<"items">> := StDstItems} = atm_workflow_execution_test_runner:browse_store(
                                DstAtmStoreSchemaId, AtmMockCallCtx
                            ),
                            ?assertEqual(ExpItemsProcessed, length(StDstItems)),

                            {true, ExpState}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_cancelled(
                                AtmTaskExecutionId, ExpState0
                            ),
                            InferStatusFun = fun
                                % task task1 and task2 parallel box transition possible combinations
                                (<<"stopping">>, [<<"cancelled">>, <<"stopping">>]) -> <<"stopping">>;
                                (<<"stopping">>, [<<"cancelled">>]) -> <<"cancelled">>;

                                % task task3 parallel box transition
                                (<<"cancelled">>, [<<"cancelled">>]) -> <<"cancelled">>
                            end,
                            {true, atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
                                AtmTaskExecutionId, InferStatusFun, ExpState1
                            )}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                        end
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            % Previously enqueued lane is changed to interrupted
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted(
                                {2, 1}, atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted(
                                    {2, 1}, ExpState
                                )
                            )}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_removed({2, 1}, ExpState1),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState2)}
                end
            },
            % assert cancelled execution can not be cancelled again
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_cancelled/1
        }]
    }).


cancel_paused_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, pause)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            {true, expect_lane2_pb_stopped(<<"paused">>, AtmTaskExecutionId, ExpState0)}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({2, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                end
            },
            after_hook = fun cancel_suspended_atm_workflow_execution/1
        }]
    }).


cancel_interrupted_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:delete_offline_session/1,
                        before_step_exp_state_diff = no_diff
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_execution_stopping_while_processing_lane2(ExpState0, interrupt),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted({2, 1}, ExpState1),
                    ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({2, 1}, ExpState2),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState3)}
                end
            },
            after_hook = fun cancel_suspended_atm_workflow_execution/1
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
'get task selector for run_task_for_item step call'(#atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState,
    call_args = [
        _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
        _AtmJobBatchId, _ItemBatch
    ]
}) ->
    atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState).


%% @private
expect_execution_stopping_while_processing_lane2(ExpState0, Reason) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({2, 1}, Reason, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
expect_lane2_pb_stopped(ExpectTaskFinalStatus, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        % lane2 parallel boxes have only single task and as such their final status
        % will be the same as that of their respective task
        fun(_, _) -> ExpectTaskFinalStatus end,
        expect_lane2_task_stopped(ExpectTaskFinalStatus, AtmTaskExecutionId, ExpState0)
    ).


%% @private
expect_lane2_task_stopped(<<"paused">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_paused(AtmTaskExecutionId, ExpState0);

expect_lane2_task_stopped(<<"interrupted">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_interrupted(AtmTaskExecutionId, ExpState0);

expect_lane2_task_stopped(<<"cancelled">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_cancelled(AtmTaskExecutionId, ExpState0).


%% @private
cancel_suspended_atm_workflow_execution(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    ?assertEqual(ok, atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)),

    ExpState1 = atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_cancelled({2, 1}, ExpState1),
    ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({2, 1}, ExpState2),
    ExpState4 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState3),
    ExpState5 = atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState4),

    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState5, 0)).


%% @private
assert_ended_atm_workflow_execution_can_not_be_cancelled(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    ?assertThrow(
        ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED,
        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
    ),
    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0)).


%% @private
expect_lane_runs_rerunable(AtmLaneRunSelectors, ExpState) ->
    lists:foldl(fun(AtmLaneRunSelector, ExpStateAcc) ->
        atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(AtmLaneRunSelector, ExpStateAcc)
    end, ExpState, AtmLaneRunSelectors).
