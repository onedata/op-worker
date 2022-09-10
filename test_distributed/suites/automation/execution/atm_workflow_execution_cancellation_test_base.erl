%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning cancellation of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_cancellation_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    cancel_scheduled_atm_workflow_execution/0,
    cancel_enqueued_atm_workflow_execution/0,
    cancel_active_atm_workflow_execution/0,
    cancel_finishing_atm_workflow_execution/0,
    cancel_finished_atm_workflow_execution/0
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

-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(__ITEMS_COUNT), #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_1">>, lists:seq(1, __ITEMS_COUNT)),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_2">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_3">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_4">>),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_5">>)
        ],
        lanes = [
            #atm_lane_schema_draft{
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pbox_first">>,
                        tasks = [
                            ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"t1">>, <<"st_2">>),
                            ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"t2">>, <<"st_3">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pbox_last">>,
                        tasks = [
                            ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"t3">>, <<"st_4">>)
                        ]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_1">>},

                % Check that cancelled executions are not retried automatically
                max_retries = ?RAND_INT(3, 6)
            },
            % Check what happens to lane run preparing in advance (in various stages of preparation)
            % when previous lane run is cancelled
            #atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                    ?ECHO_ATM_TASK_SCHEMA__DRAFT(<<"t10">>, <<"st_5">>)
                ]}],
                store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_4">>},
                max_retries = ?RAND_INT(3, 6)
            }
        ]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?INTEGER_ECHO_LAMBDA_DRAFT
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(5)).


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
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
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
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState0)}
                end
            }
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

                        after_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_skipped(
                                AtmTaskExecutionId, ExpState0
                            ),
                            {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState1) of
                                {_, <<"pbox_first">>, _} ->
                                    % parallel box with 2 tasks - it should transition to:
                                    % - active when first task ended
                                    % - skipped after second task ended
                                    InferStatusFun = fun
                                        (<<"pending">>, [<<"pending">>, <<"skipped">>]) -> <<"active">>;
                                        (<<"active">>, [<<"skipped">>]) -> <<"skipped">>
                                    end,
                                    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
                                        AtmTaskExecutionId, InferStatusFun, ExpState1
                                    );
                                {_, <<"pbox_last">>, _} ->
                                    % parallel box with only 1 task - should transition to skipped status
                                    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
                                        AtmTaskExecutionId, fun(_, _) -> <<"skipped">> end, ExpState1
                                    )
                            end}
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
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({2, 1}, ExpState)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_skipped(
                                {2, 1}, ExpState0
                            ),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({2, 1}, ExpState1)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_num_set(
                        {2, 1}, 1, ExpState0
                    ),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            }
        }]
    }).


cancel_active_atm_workflow_execution() ->
    ItemCount = 100,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(ItemCount),
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
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
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
                            % but the ones already scheduled should be finished
                            ?assert(ExpItemsProcessed < ItemCount),

                            DstAtmStoreSchemaId = case atm_workflow_execution_exp_state_builder:get_task_selector(
                                AtmTaskExecutionId, ExpState
                            ) of
                                {_, _, <<"t1">>} -> <<"st_2">>;
                                {_, _, <<"t2">>} -> <<"st_3">>;
                                {_, _, <<"t3">>} -> <<"st_4">>
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
                            EndTaskFun = case atm_workflow_execution_exp_state_builder:get_task_stats(
                                AtmTaskExecutionId, ExpState0
                            ) of
                                {0, 0, 0} -> fun atm_workflow_execution_exp_state_builder:expect_task_skipped/2;
                                _ -> fun atm_workflow_execution_exp_state_builder:expect_task_finished/2
                            end,
                            ExpState1 = EndTaskFun(AtmTaskExecutionId, ExpState0),

                            InferStatusFun = fun
                                % task t1 and t2 parallel box transition possible combinations
                                (<<"active">>, [<<"active">>, <<"finished">>]) -> <<"active">>;
                                (<<"active">>, [<<"finished">>, <<"pending">>]) -> <<"active">>;
                                (<<"active">>, [<<"finished">>]) -> <<"finished">>;
                                (<<"active">>, [<<"finished">>, <<"skipped">>]) -> <<"finished">>;

                                % task t3 parallel box transition
                                (<<"pending">>, [<<"skipped">>]) -> <<"skipped">>
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
                                {2, 1}, atm_workflow_execution_exp_state_builder:expect_all_tasks_skipped(
                                    {2, 1}, ExpState
                                )
                            )}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_num_set(
                        {2, 1}, 1, ExpState0
                    ),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            }
        }]
    }).


cancel_finishing_atm_workflow_execution() ->
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
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            % While atm workflow execution as whole has not yet transition to finished status
                            % (last step remaining) the current lane run did. At this point cancel
                            % is no longer possible (execution is treated as successfully ended)
                            ?assertThrow(
                                ?ERROR_ATM_INVALID_STATUS_TRANSITION(?FINISHED_STATUS, ?STOPPING_STATUS),
                                atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                            )
                        end
                    }
                }
            ]
        }]
    }).


cancel_finished_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{selector = {1, 1}},
                #atm_lane_run_execution_test_spec{selector = {2, 1}}
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_hook = fun(AtmMockCallCtx) ->
                    ?assertThrow(
                        ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED,
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    )
                end
            }
        }]
    }).
