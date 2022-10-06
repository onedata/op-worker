%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for automation workflow execution pause tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_pause_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    pause_scheduled_atm_workflow_execution/0,
    pause_enqueued_atm_workflow_execution/0,

    pause_active_atm_workflow_execution_with_no_uncorrelated_task_results/0,
    pause_active_atm_workflow_execution_with_uncorrelated_task_results/0,

    pause_interrupted_atm_workflow_execution/0
]).


-define(ECHO_ATM_TASK_SCHEMA__DRAFT(__ID, __TARGET_STORE_SCHEMA_ID), #atm_task_schema_draft{
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
                            ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"task1">>, <<"st_2">>),
                            ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"task2">>, <<"st_3">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"task3">>, <<"st_4">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_1">>},

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
                        tasks = [?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"task4">>, <<"st_devnull">>)]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb4">>,
                        tasks = [?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"task5">>, <<"st_devnull">>)]
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


pause_scheduled_atm_workflow_execution() ->
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
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState)}
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
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                end
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_not_be_paused/1
        }]
    }).


pause_enqueued_atm_workflow_execution() ->
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

                        after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_paused({1, 1}, ExpState1),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState)}
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
                            % Lane run creation began before execution was paused and as such all components
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_removed({2, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState1)}
                end
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_not_be_paused/1
        }]
    }).


pause_active_atm_workflow_execution_with_no_uncorrelated_task_results() ->
    pause_active_atm_workflow_execution_test_base(?FUNCTION_NAME, return_value).


pause_active_atm_workflow_execution_with_uncorrelated_task_results() ->
    pause_active_atm_workflow_execution_test_base(?FUNCTION_NAME, file_pipe).


%% @private
pause_active_atm_workflow_execution_test_base(Testcase, RelayMethod) ->
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
                                            % after execution is paused
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
                                    atm_workflow_execution_test_runner:pause_workflow_execution(AtmMockCallCtx)
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
                                        {1, 1}, pause, ExpState1
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
                            % pause blocks scheduling execution of leftover items
                            % but the ones already scheduled should be finished
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
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_paused(
                                AtmTaskExecutionId, ExpState0
                            ),
                            InferStatusFun = fun
                            % task task1 and task2 parallel box transition possible combinations
                                (<<"stopping">>, [<<"paused">>, <<"stopping">>]) -> <<"stopping">>;
                                (<<"stopping">>, [<<"paused">>]) -> <<"paused">>;

                                % task task3 parallel box transition
                                (<<"paused">>, [<<"paused">>]) -> <<"paused">>
                            end,
                            {true, atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
                                AtmTaskExecutionId, InferStatusFun, ExpState1
                            )}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState)}
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_removed({2, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState1)}
                end
            },
            after_hook = fun assert_paused_atm_workflow_execution_can_not_be_paused/1
        }]
    }).


pause_interrupted_atm_workflow_execution() ->
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
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted({2, 1}, ExpState1),
                    ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({2, 1}, ExpState2),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState3)}
                end
            },
            after_hook = fun(AtmMockCallCtx = #atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                ?assertThrow(
                    ?ERROR_ATM_INVALID_STATUS_TRANSITION(?INTERRUPTED_STATUS, ?STOPPING_STATUS),
                    atm_workflow_execution_test_runner:pause_workflow_execution(AtmMockCallCtx)
                ),
                ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0))
            end
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
assert_paused_atm_workflow_execution_can_not_be_paused(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    ?assertThrow(
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(?PAUSED_STATUS, ?STOPPING_STATUS),
        atm_workflow_execution_test_runner:pause_workflow_execution(AtmMockCallCtx)
    ),
    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0)).
