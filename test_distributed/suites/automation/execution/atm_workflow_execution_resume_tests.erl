%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of resuming atm workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_resume_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    resume_atm_workflow_execution_paused_while_scheduled/0,
    resume_atm_workflow_execution_interrupted_while_scheduled/0,

    resume_atm_workflow_execution_paused_while_preparing/0,
    resume_atm_workflow_execution_interrupted_while_preparing/0,

    resume_atm_workflow_execution_paused_while_active/0,
    resume_atm_workflow_execution_interrupted_while_active/0,

    resume_atm_workflow_execution_paused_after_all_tasks_finished/0,
    resume_atm_workflow_execution_interrupted_after_all_tasks_finished/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).

-define(LAMBDA_DRAFT(__DOCKER_IMAGE), #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = __DOCKER_IMAGE
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = ?INTEGER_DATA_SPEC,
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = ?INTEGER_DATA_SPEC,
        relay_method = ?RAND_ELEMENT([return_value, file_pipe])
    }]
}).

-define(ECHO_ATM_TASK_SCHEMA_DRAFT(__ID, __LAMBDA_ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = __LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = ?TARGET_STORE_SCHEMA_ID,
        store_content_update_options = #atm_list_store_content_update_options{function = append}
    }]
}).

-define(ITERATOR_SPEC_DRAFT, #atm_store_iterator_spec_draft{
    store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
    max_batch_size = 1
}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __ITERATED_CONTENT, __LAMBDA2_DOCKER_IMAGE_ID),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, __ITERATED_CONTENT),
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [
                        #atm_parallel_box_schema_draft{
                            id = <<"pb1">>,
                            tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task1">>, <<"lambda1">>)]
                        },
                        #atm_parallel_box_schema_draft{
                            id = <<"pb2">>,
                            tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task2">>, <<"lambda2">>)]
                        }
                    ],
                    store_iterator_spec = ?ITERATOR_SPEC_DRAFT,
                    max_retries = 2
                },
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task3">>, <<"lambda1">>)]
                    }],
                    store_iterator_spec = ?ITERATOR_SPEC_DRAFT
                }
            ]
        },
        supplementary_lambdas = #{
            <<"lambda1">> => #{?ECHO_LAMBDA_REVISION_NUM => ?LAMBDA_DRAFT(?ECHO_DOCKER_IMAGE_ID)},
            <<"lambda2">> => #{?ECHO_LAMBDA_REVISION_NUM => ?LAMBDA_DRAFT(__LAMBDA2_DOCKER_IMAGE_ID)}
        }
    }
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT, ?ATM_WORKFLOW_SCHEMA_DRAFT(
    ?FUNCTION_NAME, [?RAND_INT(100)], ?ECHO_DOCKER_IMAGE_ID
)).


%%%===================================================================
%%% Tests
%%%===================================================================


resume_atm_workflow_execution_paused_while_scheduled() ->
    PausedIncarnation = #atm_workflow_execution_incarnation_test_spec{
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
        }
    },
    resume_atm_workflow_execution_suspended_while_scheduled_test_base(?FUNCTION_NAME, PausedIncarnation).


resume_atm_workflow_execution_interrupted_while_scheduled() ->
    InterruptedIncarnation = #atm_workflow_execution_incarnation_test_spec{
        incarnation_num = 1,
        lane_runs = [
            #atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    before_step_hook = fun atm_workflow_execution_test_runner:delete_offline_session/1,
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    % this is called as part of `handle_workflow_abruptly_stopped`
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({1, 1}, ExpState0)}
                    end
                }
            },
            #atm_lane_run_execution_test_spec{
                selector = {2, 1},
                prepare_lane = #atm_step_mock_spec{
                    defer_after = {handle_exception, after_step, 1},
                    after_step_exp_state_diff = no_diff
                }
            }
        ],
        handle_exception = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                {true, expect_execution_stopping_while_processing_lane1(ExpState0, interrupt)}
            end
        },
        handle_workflow_abruptly_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState0)}
            end
        }
    },
    resume_atm_workflow_execution_suspended_while_scheduled_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
resume_atm_workflow_execution_suspended_while_scheduled_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [?RAND_INT(100)], ?ECHO_DOCKER_IMAGE_ID
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{defer_after = {prepare_lane, after_step, {1, 1}}}
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(
                            [{1, 1}, {2, 1}], ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                    end
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_while_preparing() ->
    PausedIncarnation = #atm_workflow_execution_incarnation_test_spec{
        incarnation_num = 1,
        lane_runs = [
            #atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = no_diff
                },
                create_run = #atm_step_mock_spec{
                    after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_created({1, 1}, ExpState0),
                        ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState1),
                        ExpState3 = atm_workflow_execution_exp_state_builder:expect_all_tasks_paused({1, 1}, ExpState2),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState3)}
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
        }
    },
    resume_atm_workflow_execution_suspended_while_preparing_test_base(?FUNCTION_NAME, PausedIncarnation).


resume_atm_workflow_execution_interrupted_while_preparing() ->
    InterruptedIncarnation = #atm_workflow_execution_incarnation_test_spec{
        incarnation_num = 1,
        lane_runs = [
            #atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = no_diff
                },
                create_run = #atm_step_mock_spec{
                    after_step_hook = fun atm_workflow_execution_test_runner:report_openfaas_unhealthy/1,
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_created({1, 1}, ExpState0),
                        {true, expect_execution_stopping_while_processing_lane1(ExpState1, interrupt)}
                    end
                },
                % this is called as part of `handle_workflow_abruptly_stopped`
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({1, 1}, ExpState)}
                    end
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
        handle_workflow_abruptly_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState0)}
            end
        }
    },
    resume_atm_workflow_execution_suspended_while_preparing_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
resume_atm_workflow_execution_suspended_while_preparing_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [?RAND_INT(100)], ?ECHO_DOCKER_IMAGE_ID
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_pending({1, 1}, ExpState0),
                                ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_enqueued({1, 1}, ExpState1),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_active(ExpState2)}
                            end
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{defer_after = {prepare_lane, after_step, {1, 1}}}
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(
                            [{1, 1}, {2, 1}], ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                    end
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_while_active() ->
    resume_atm_workflow_execution_suspended_while_active_test_base(?FUNCTION_NAME, pause).


resume_atm_workflow_execution_interrupted_while_active() ->
    resume_atm_workflow_execution_suspended_while_active_test_base(?FUNCTION_NAME, interrupt).


%% @private
resume_atm_workflow_execution_suspended_while_active_test_base(Testcase, SuspendMethod) ->
    ResumedLaneRunBaseTestSpec = build_failed_atm_lane1_run_execution_test_spec(
        {1, 1}, {0, 0, 6}, {0, 3, 6}, [2, 4, 6], false
    ),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase,
            [1, 2, 3, 4, 5, 6],
            ?ECHO_WITH_EXCEPTION_ON_EVEN_NUMBERS
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        run_task_for_item = #atm_step_mock_spec{
                            strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, _, [Item]]}) ->
                                case lists:member(Item, [4, 5, 6]) of
                                    % Delay execution of last batch to ensure it happens after execution is suspended
                                    true -> {passthrough_with_delay, timer:seconds(1)};
                                    false -> passthrough
                                end
                            end
                        },
                        process_task_result_for_item = 'build mock spec for process_task_result_for_item step'(),
                        handle_task_execution_stopped = #atm_step_mock_spec{
                            before_step_hook = get_suspend_hook(SuspendMethod),

                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{
                                workflow_execution_exp_state = ExpState0,
                                call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                            }) ->
                                assert_lane1_tasks_stats(AtmTaskExecutionId, {0, 0, 6}, {0, 1, 3}, ExpState0),
                                {true, expect_execution_stopping_while_processing_lane1(ExpState0, SuspendMethod)}
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                                workflow_execution_exp_state = ExpState0,
                                call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                            }) ->
                                ExpAtmTaskExecutionFinalStatus = case SuspendMethod of
                                    pause -> <<"paused">>;
                                    interrupt -> <<"interrupted">>
                                end,
                                {true, expect_lane1_pb_stopped(ExpAtmTaskExecutionFinalStatus, AtmTaskExecutionId, ExpState0)}
                            end
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                                workflow_execution_exp_state = ExpState
                            }) ->
                                ?assertEqual([2], lists:sort(get_exception_store_content({1, 1}, AtmMockCallCtx))),
                                {true, expect_lane_run_suspended(SuspendMethod, {1, 1}, ExpState)}
                            end
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {handle_lane_execution_stopped, after_step, {1, 1}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, expect_workflow_execution_suspended(SuspendMethod, ExpState0)}
                    end
                },
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_active({1, 1}, ExpState0),
                                ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_active({1, 1}, ExpState1),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_active(ExpState2)}
                            end
                        }
                    },
                    build_failed_atm_lane1_run_execution_test_spec({1, 2}, {0, 0, 3}, {0, 3, 3}, [2, 4, 6], false),
                    build_failed_atm_lane1_run_execution_test_spec({1, 3}, {0, 0, 3}, {0, 3, 3}, [2, 4, 6], true),

                    #atm_lane_run_execution_test_spec{
                        selector = {2, 3},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {handle_lane_execution_stopped, after_step, {1, 3}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_repeatable(
                            [{1, 1}, {1, 2}, {1, 3}], true, true, ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                    end
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_after_all_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(?FUNCTION_NAME, pause).


resume_atm_workflow_execution_interrupted_after_all_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(?FUNCTION_NAME, interrupt).


%% @private
resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(Testcase, SuspendMethod) ->
    ResumedLaneRunBaseTestSpec = build_failed_atm_lane1_run_execution_test_spec(
        {1, 2}, {0, 0, 2}, {0, 2, 2}, [22, 44], false
    ),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase,
            [11, 22, 44, 55],
            ?ECHO_WITH_EXCEPTION_ON_EVEN_NUMBERS
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    build_failed_atm_lane1_run_execution_test_spec(
                        {1, 1}, {0, 0, 4}, {0, 2, 4}, [22, 44], false
                    ),
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            before_step_hook = get_suspend_hook(SuspendMethod),
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = expect_lane_run_suspended(SuspendMethod, {1, 2}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                            end
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {handle_lane_execution_stopped, after_step, {1, 2}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, expect_workflow_execution_suspended(SuspendMethod, ExpState0)}
                    end
                },
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 2}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                                ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_active({1, 2}, ExpState1),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_active(ExpState2)}
                            end
                        }
                    },
                    build_failed_atm_lane1_run_execution_test_spec({1, 3}, {0, 0, 2}, {0, 2, 2}, [22, 44], true),

                    #atm_lane_run_execution_test_spec{
                        selector = {2, 3},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {handle_lane_execution_stopped, after_step, {1, 3}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_repeatable(
                            [{1, 1}, {1, 2}, {1, 3}], true, true, ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                    end
                }
            }
        ]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
interrupt_workflow_execution(AtmMockCallCtx) ->
    atm_workflow_execution_test_runner:stop_workflow_execution(interrupt, AtmMockCallCtx).


%% @private
get_suspend_hook(pause) -> fun atm_workflow_execution_test_runner:pause_workflow_execution/1;
get_suspend_hook(interrupt) -> fun interrupt_workflow_execution/1.


%% @private
expect_lane_run_suspended(pause, AtmLaneRunSelector, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_lane_run_paused(AtmLaneRunSelector, ExpState);
expect_lane_run_suspended(interrupt, AtmLaneRunSelector, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted(AtmLaneRunSelector, ExpState).


%% @private
expect_workflow_execution_suspended(pause, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState);
expect_workflow_execution_suspended(interrupt, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState).


%% @private
build_failed_atm_lane1_run_execution_test_spec(
    AtmLaneRunSelector, ExpTask1FinalStats, ExpTask2FinalStats, ExpExceptionStoreContent, IsLastExpLaneRun
) ->
    #atm_lane_run_execution_test_spec{
        selector = AtmLaneRunSelector,
        process_task_result_for_item = 'build mock spec for process_task_result_for_item step'(),
        handle_task_execution_stopped = 'build mock for handle_task_execution_stopped step'(
            ExpTask1FinalStats, ExpTask2FinalStats
        ),
        handle_lane_execution_stopped = 'build mock for handle_lane_execution_stopped step'(
            AtmLaneRunSelector, ExpExceptionStoreContent, IsLastExpLaneRun
        )
    }.


%% @private
'build mock spec for process_task_result_for_item step'() ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [
                _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
                ItemBatch, _JobBatchResult
            ]
        }) ->
            ItemCount = length(ItemBatch),

            case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
                {_, _, <<"task1">>} ->
                    {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                        AtmTaskExecutionId, ItemCount, ExpState0
                    )};
                {_, _, <<"task2">>} ->
                    FailedItemCount = lists:sum(lists:map(fun(Item) -> 1 - (Item rem 2) end, ItemBatch)),
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_failed_and_processed(
                        AtmTaskExecutionId, FailedItemCount, ExpState0
                    ),
                    {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                        AtmTaskExecutionId, ItemCount - FailedItemCount, ExpState1
                    )}
            end
        end
    }.


%% @private
'build mock for handle_task_execution_stopped step'(ExpTask1FinalStats, ExpTask2FinalStats) ->
    #atm_step_mock_spec{
        before_step_hook = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
        }) ->
            assert_lane1_tasks_stats(AtmTaskExecutionId, ExpTask1FinalStats, ExpTask2FinalStats, ExpState0)
        end,
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState,
            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
        }) ->
            {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState) of
                {_, _, <<"task1">>} ->
                    expect_lane1_pb_stopped(<<"finished">>, AtmTaskExecutionId, ExpState);
                {_, _, <<"task2">>} ->
                    expect_lane1_pb_stopped(<<"failed">>, AtmTaskExecutionId, ExpState)
            end}
        end
    }.


%% @private
assert_lane1_tasks_stats(AtmTaskExecutionId, ExpTask1Stats, ExpTask2Stats, ExpState0) ->
    ExpTaskStats = case atm_workflow_execution_exp_state_builder:get_task_selector(
        AtmTaskExecutionId, ExpState0
    ) of
        {_, _, <<"task1">>} -> ExpTask1Stats;
        _ -> ExpTask2Stats
    end,
    ?assertEqual(ExpTaskStats, atm_workflow_execution_exp_state_builder:get_task_stats(
        AtmTaskExecutionId, ExpState0
    )).


%% @private
'build mock for handle_lane_execution_stopped step'(AtmLaneRunSelector, ExpExceptionStoreContent, IsLastExpLaneRun) ->
    #atm_step_mock_spec{
        after_step_hook = fun(AtmMockCallCtx) ->
            ?assertEqual(
                lists:sort(ExpExceptionStoreContent),
                lists:sort(get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx))
            )
        end,
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0
        }) ->
            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_failed(
                AtmLaneRunSelector, ExpState0
            ),
            {true, case IsLastExpLaneRun of
                true ->
                    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1);
                false ->
                    {AtmLaneSelector, RunNum} = AtmLaneRunSelector,

                    atm_workflow_execution_exp_state_builder:expect_lane_run_automatic_retry_scheduled(
                        {AtmLaneSelector, RunNum + 1}, ExpState1
                    )
            end}
        end
    }.


%% @private
expect_execution_stopping_while_processing_lane1(ExpState0, Reason) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({1, 1}, Reason, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
expect_lane1_pb_stopped(ExpectTaskFinalStatus, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        % lane2 parallel boxes have only single task and as such their final status
        % will be the same as that of their respective task
        fun(_, _) -> ExpectTaskFinalStatus end,
        expect_lane1_task_stopped(ExpectTaskFinalStatus, AtmTaskExecutionId, ExpState0)
    ).


%% @private
expect_lane1_task_stopped(<<"finished">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_finished(AtmTaskExecutionId, ExpState0);

expect_lane1_task_stopped(<<"paused">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_paused(AtmTaskExecutionId, ExpState0);

expect_lane1_task_stopped(<<"interrupted">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_interrupted(AtmTaskExecutionId, ExpState0);

expect_lane1_task_stopped(<<"failed">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_failed(AtmTaskExecutionId, ExpState0).


%% @private
get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_runner:browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    lists:sort(lists:map(fun(#{<<"value">> := Content}) -> Content end, Items)).
