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
-include("atm/atm_test_store.hrl").

-export([
    resume_atm_workflow_execution_paused_while_scheduled/0,
    resume_atm_workflow_execution_interrupted_while_scheduled/0,

    resume_atm_workflow_execution_paused_while_preparing/0,
    resume_atm_workflow_execution_interrupted_while_preparing/0,

    resume_atm_workflow_execution_paused_while_active/0,
    resume_atm_workflow_execution_interrupted_while_active/0,

    resume_atm_workflow_execution_paused_after_some_tasks_finished/0,
    resume_atm_workflow_execution_interrupted_after_some_tasks_finished/0,

    resume_atm_workflow_execution_paused_after_all_tasks_finished/0,
    resume_atm_workflow_execution_interrupted_after_all_tasks_finished/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).

-define(LAMBDA_DRAFT(__DOCKER_IMAGE, __RELAY_METHOD), #atm_lambda_revision_draft{
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
        relay_method = __RELAY_METHOD
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

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __ITERATED_CONTENT),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, __ITERATED_CONTENT),
                ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
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
            <<"lambda1">> => #{?ECHO_LAMBDA_REVISION_NUM => ?LAMBDA_DRAFT(?ECHO_DOCKER_IMAGE_ID, file_pipe)},
            <<"lambda2">> => #{?ECHO_LAMBDA_REVISION_NUM => ?LAMBDA_DRAFT(
                ?ECHO_WITH_EXCEPTION_ON_EVEN_NUMBERS, return_value
            )}
        }
    }
).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).
-define(TASK2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task2">>}).


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
                    before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{after_step_exp_state_diff = no_diff}
            },
            ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
        ],
        handle_workflow_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_paused]
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
                    before_step_hook = fun atm_workflow_execution_test_utils:delete_offline_session/1,
                    after_step_exp_state_diff = no_diff
                },
                % this is called as part of `handle_workflow_abruptly_stopped`
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [{lane_run, {1, 1}, interrupted}]
                }
            },
            ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_exception, after_step, 1})
        ],
        handle_exception = #atm_step_mock_spec{
            after_step_exp_state_diff = [
                {lane_run, {1, 1}, stopping},
                workflow_stopping
            ]
        },
        handle_workflow_abruptly_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_interrupted]
        }
    },
    resume_atm_workflow_execution_suspended_while_scheduled_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
resume_atm_workflow_execution_suspended_while_scheduled_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [random_odd_number()]
        ),
        incarnations = [
            SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 1}, resuming},
                                workflow_resuming
                            ]
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{defer_after = {prepare_lane, after_step, {1, 1}}}
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_finished
                    ]
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
                    after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                    after_step_exp_state_diff = [
                        {lane_run, {1, 1}, created},
                        {all_tasks, {1, 1}, paused},
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ]
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                }
            },
            ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
        ],
        handle_workflow_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_paused]
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
                    after_step_hook = fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1,
                    after_step_exp_state_diff = [
                        {lane_run, {1, 1}, created},
                        {all_tasks, {1, 1}, interrupted},
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ]
                },
                % this is called as part of `handle_workflow_abruptly_stopped`
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [{lane_run, {1, 1}, interrupted}]
                }
            },
            ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
        ],
        handle_workflow_abruptly_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [workflow_interrupted]
        }
    },
    resume_atm_workflow_execution_suspended_while_preparing_test_base(?FUNCTION_NAME, InterruptedIncarnation).


%% @private
resume_atm_workflow_execution_suspended_while_preparing_test_base(Testcase, SuspendedIncarnation) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [random_odd_number()]
        ),
        incarnations = [
            SuspendedIncarnation#atm_workflow_execution_incarnation_test_spec{
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 1}, resuming},
                                workflow_resuming
                            ],
                            after_step_hook = build_assert_exp_parallel_box_specs_returned_hook([
                                #{?TASK1_SELECTOR({1, 1}) => #{type => async, data_stream_enabled => true}},
                                #{?TASK2_SELECTOR({1, 1}) => #{type => async, data_stream_enabled => false}}
                            ]),
                            after_step_exp_state_diff = [
                                {all_tasks, {1, 1}, pending},
                                {lane_run, {1, 1}, enqueued},
                                workflow_active
                            ]
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{defer_after = {prepare_lane, after_step, {1, 1}}}
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_finished
                    ]
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_while_active() ->
    resume_atm_workflow_execution_suspended_while_active_test_base(?FUNCTION_NAME, paused).


resume_atm_workflow_execution_interrupted_while_active() ->
    resume_atm_workflow_execution_suspended_while_active_test_base(?FUNCTION_NAME, interrupted).


%% @private
resume_atm_workflow_execution_suspended_while_active_test_base(Testcase, SuspendedStatus) ->
    ResumedLaneRunBaseTestSpec = build_lane_run_execution_test_spec_with_even_numbers(
        {1, 1}, {0, 0, 6}, {0, 3, 6}, [2, 4, 6], false
    ),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [1, 2, 3, 4, 5, 6]
        ),
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
                        process_task_result_for_item = 'build mock spec for process_task_result_for_item with even numbers step'(),

                        handle_task_execution_stopped = #atm_step_mock_spec{
                            before_step_hook = fun(AtmMockCallCtx) ->
                                assert_lane1_task_execution_stopped_stats({0, 0, 6}, {0, 1, 3}, AtmMockCallCtx),

                                SuspendHook = get_suspend_hook(SuspendedStatus),
                                SuspendHook(AtmMockCallCtx)
                            end,
                            before_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                                <<"task1">> => [
                                    {all_tasks, {1, 1}, stopping},
                                    {lane_run, {1, 1}, stopping},
                                    workflow_stopping
                                ]
                            }),
                            after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                                [<<"task1">>, <<"task2">>] => build_expectations_for_lane1_task_transitioned_to(
                                    ?TASK_ID_PLACEHOLDER, SuspendedStatus
                                )
                            })
                        },

                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_hook = fun(AtmMockCallCtx) ->
                                assert_exception_store_content([2], {1, 1}, AtmMockCallCtx)
                            end,
                            after_step_exp_state_diff = [{lane_run, {1, 1}, SuspendedStatus}]
                        }
                    },
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_lane_execution_stopped, after_step, {1, 1}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [build_workflow_suspended_expectation(SuspendedStatus)]
                },
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 1}, resuming},
                                workflow_resuming
                            ],
                            after_step_hook = build_assert_exp_parallel_box_specs_returned_hook([
                                #{?TASK1_SELECTOR({1, 1}) => #{type => async, data_stream_enabled => true}},
                                #{?TASK2_SELECTOR({1, 1}) => #{type => async, data_stream_enabled => false}}
                            ]),
                            after_step_exp_state_diff = [
                                {all_tasks, {1, 1}, active},
                                {lane_run, {1, 1}, active},
                                workflow_active
                            ]
                        }
                    },
                    build_lane_run_execution_test_spec_with_even_numbers({1, 2}, {0, 0, 3}, {0, 3, 3}, [2, 4, 6], false),
                    build_lane_run_execution_test_spec_with_even_numbers({1, 3}, {0, 0, 3}, {0, 3, 3}, [2, 4, 6], true),
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 3}, {handle_lane_execution_stopped, after_step, {1, 3}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], rerunable},
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], retriable},
                        workflow_failed
                    ]
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_after_some_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_some_tasks_finished_test_base(?FUNCTION_NAME, paused).


resume_atm_workflow_execution_interrupted_after_some_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_some_tasks_finished_test_base(?FUNCTION_NAME, interrupted).


%% @private
resume_atm_workflow_execution_suspended_after_some_tasks_finished_test_base(Testcase, SuspendedStatus) ->
    ResumedLaneRunBaseTestSpec = build_lane_run_execution_test_spec_with_even_numbers(
        {1, 2}, {0, 0, 0}, {0, 2, 2}, [88, 666], false
    ),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [88, 666, 999]
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    build_lane_run_execution_test_spec_with_even_numbers(
                        {1, 1}, {0, 0, 3}, {0, 2, 3}, [88, 666], false
                    ),
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 2},
                        run_task_for_item = #atm_step_mock_spec{
                            strategy = fun(#atm_mock_call_ctx{
                                workflow_execution_exp_state = ExpState,
                                call_args = [_, _, AtmTaskExecutionId, _, _]
                            }) ->
                                case atm_workflow_execution_exp_state_builder:get_task_schema_id(
                                    AtmTaskExecutionId, ExpState
                                ) of
                                    <<"task1">> -> passthrough;
                                    % Ensure no item is executed by task2 before suspension
                                    <<"task2">> -> {passthrough_with_delay, timer:seconds(1)}
                                end
                            end
                        },
                        process_task_result_for_item = 'build mock spec for process_task_result_for_item with even numbers step'(),

                        handle_task_execution_stopped = #atm_step_mock_spec{
                            before_step_hook = fun(AtmMockCallCtx) ->
                                assert_lane1_task_execution_stopped_stats({0, 0, 2}, {0, 0, 0}, AtmMockCallCtx)
                            end,
                            after_step_hook = get_suspend_hook(SuspendedStatus),
                            after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                                <<"task1">> => lists:flatten([
                                    build_expectations_for_lane1_task_transitioned_to(?TASK1_SELECTOR({1, 2}), finished),
                                    build_expectations_for_lane1_task_transitioned_to(?TASK2_SELECTOR({1, 2}), SuspendedStatus),
                                    {lane_run, {1, 2}, stopping},
                                    workflow_stopping
                                ])
                            })
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_hook = fun(AtmMockCallCtx) ->
                                assert_exception_store_content([], {1, 2}, AtmMockCallCtx)
                            end,
                            after_step_exp_state_diff = [{lane_run, {1, 2}, SuspendedStatus}]
                        }
                    },
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_lane_execution_stopped, after_step, {1, 2}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [build_workflow_suspended_expectation(SuspendedStatus)]
                },
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        prepare_lane = #atm_step_mock_spec{     %% TODO MW why prepare_lane instead of resume_lane ????
                            before_step_exp_state_diff = [
                                {lane_run, {1, 2}, resuming},
                                workflow_resuming
                            ],
                            % task1 already finished so only spec for pb2/task2 should be present
                            after_step_hook = build_assert_exp_parallel_box_specs_returned_hook([
                                #{?TASK2_SELECTOR({1, 2}) => #{type => async, data_stream_enabled => false}}
                            ]),
                            after_step_exp_state_diff = lists:flatten([
                                build_expectations_for_lane1_task_transitioned_to(?TASK2_SELECTOR({1, 2}), pending),
                                {lane_run, {1, 2}, active},
                                workflow_active
                            ])
                        }
                    },
                    build_lane_run_execution_test_spec_with_even_numbers({1, 3}, {0, 0, 2}, {0, 2, 2}, [88, 666], true),
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 3}, {handle_lane_execution_stopped, after_step, {1, 3}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], rerunable},
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], retriable},
                        workflow_failed
                    ]
                }
            }
        ]
    }).


resume_atm_workflow_execution_paused_after_all_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(?FUNCTION_NAME, paused).


resume_atm_workflow_execution_interrupted_after_all_tasks_finished() ->
    resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(?FUNCTION_NAME, interrupted).


%% @private
resume_atm_workflow_execution_suspended_after_all_tasks_finished_test_base(Testcase, SuspendedStatus) ->
    ResumedLaneRunBaseTestSpec = build_lane_run_execution_test_spec_with_even_numbers(
        {1, 2}, {0, 0, 2}, {0, 2, 2}, [22, 44], false
    ),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            Testcase, [11, 22, 44, 55]
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    build_lane_run_execution_test_spec_with_even_numbers(
                        {1, 1}, {0, 0, 4}, {0, 2, 4}, [22, 44], false
                    ),
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            before_step_hook = get_suspend_hook(SuspendedStatus),
                            after_step_hook = fun(AtmMockCallCtx) ->
                                assert_exception_store_content([22, 44], {1, 2}, AtmMockCallCtx)
                            end,
                            after_step_exp_state_diff = [
                                {lane_run, {1, 2}, SuspendedStatus},
                                workflow_stopping
                            ]
                        }
                    },
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_lane_execution_stopped, after_step, {1, 2}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [build_workflow_suspended_expectation(SuspendedStatus)]
                },
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    ResumedLaneRunBaseTestSpec#atm_lane_run_execution_test_spec{
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 2}, resuming},
                                workflow_resuming
                            ],
                            % All tasks already finished so parallel box specs should be empty
                            after_step_hook = build_assert_exp_parallel_box_specs_returned_hook([]),
                            after_step_exp_state_diff = [
                                {lane_run, {1, 2}, active},
                                workflow_active
                            ]
                        }
                    },
                    build_lane_run_execution_test_spec_with_even_numbers({1, 3}, {0, 0, 2}, {0, 2, 2}, [22, 44], true),
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 3}, {handle_lane_execution_stopped, after_step, {1, 3}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], rerunable},
                        {lane_runs, [{1, 1}, {1, 2}, {1, 3}], retriable},
                        workflow_failed
                    ]
                }
            }
        ]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
random_odd_number() ->
    case ?RAND_INT(100) of
        Odd when Odd rem 2 == 1 -> Odd;
        Even -> Even + 1
    end.


%% @private
get_suspend_hook(paused) -> fun atm_workflow_execution_test_utils:pause_workflow_execution/1;
get_suspend_hook(interrupted) -> fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1.


%% @private
build_workflow_suspended_expectation(paused) -> workflow_paused;
build_workflow_suspended_expectation(interrupted) -> workflow_interrupted.


%% @private
build_lane_run_execution_test_spec_with_even_numbers(
    AtmLaneRunSelector, ExpTask1FinalStats, ExpTask2FinalStats, ExpExceptionStoreContent, IsLastExpLaneRun
) ->
    #atm_lane_run_execution_test_spec{
        selector = AtmLaneRunSelector,
        process_task_result_for_item = 'build mock spec for process_task_result_for_item with even numbers step'(),

        handle_task_execution_stopped = #atm_step_mock_spec{
            before_step_hook = fun(AtmMockCallCtx) ->
                assert_lane1_task_execution_stopped_stats(ExpTask1FinalStats, ExpTask2FinalStats, AtmMockCallCtx)
            end,
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                workflow_execution_exp_state = ExpState,
                call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
            }) ->
                StoppedStatus = case atm_workflow_execution_exp_state_builder:get_task_schema_id(
                    AtmTaskExecutionId, ExpState
                ) of
                    <<"task1">> -> finished;
                    <<"task2">> -> failed
                end,
                Expectations = build_expectations_for_lane1_task_transitioned_to(AtmTaskExecutionId, StoppedStatus),
                {true, atm_workflow_execution_exp_state_builder:expect(ExpState, Expectations)}
            end
        },

        handle_lane_execution_stopped = #atm_step_mock_spec{
            after_step_hook = fun(AtmMockCallCtx) ->
                assert_exception_store_content(ExpExceptionStoreContent, AtmLaneRunSelector, AtmMockCallCtx)
            end,
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                Expectations = [{lane_run, AtmLaneRunSelector, failed}, case IsLastExpLaneRun of
                    true -> workflow_stopping;
                    false -> {lane_run, AtmLaneRunSelector, automatic_retry_scheduled}
                end],
                {true, atm_workflow_execution_exp_state_builder:expect(ExpState0, Expectations)}
            end
        }
    }.


%% @private
'build mock spec for process_task_result_for_item with even numbers step'() ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [
                _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
                ItemBatch, _JobBatchResult
            ]
        }) ->
            ItemCount = length(ItemBatch),
            EvenNumberCount = lists:sum(lists:map(fun(Item) -> 1 - (Item rem 2) end, ItemBatch)),

            Expectations = case atm_workflow_execution_exp_state_builder:get_task_schema_id(
                AtmTaskExecutionId, ExpState0
            ) of
                <<"task1">> ->
                    [{task, AtmTaskExecutionId, items_finished, ItemCount}];
                <<"task2">> ->
                    [
                        {task, AtmTaskExecutionId, items_finished, ItemCount - EvenNumberCount},
                        {task, AtmTaskExecutionId, items_failed, EvenNumberCount}
                    ]
            end,
            {true, atm_workflow_execution_exp_state_builder:expect(ExpState0, Expectations)}
        end
    }.


%% @private
build_expectations_for_lane1_task_transitioned_to(AtmTaskExecutionId, Status) ->
    StoppedStatusBin = str_utils:to_binary(Status),

    [
        {task, AtmTaskExecutionId, Status},
        {task, AtmTaskExecutionId, parallel_box_transitioned_to_inferred_status, fun(_, _) -> StoppedStatusBin end}
    ].


%% @private
assert_exception_store_content(ExpContent, AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_utils:browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    ?assertEqual(
        lists:sort(ExpContent),
        lists:sort(lists:map(fun(#{<<"value">> := Content}) -> Content end, Items))
    ).


%% @private
assert_lane1_task_execution_stopped_stats(ExpTask1FinalStats, ExpTask2FinalStats, #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    ?assertEqual(
        case atm_workflow_execution_exp_state_builder:get_task_schema_id(AtmTaskExecutionId, ExpState0) of
            <<"task1">> -> ExpTask1FinalStats;
            _ -> ExpTask2FinalStats
        end,
        atm_workflow_execution_exp_state_builder:get_task_stats(AtmTaskExecutionId, ExpState0)
    ).


%% @private
build_assert_exp_parallel_box_specs_returned_hook(ExpParallelBoxSpecsWithTaskSelectors) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState,
        call_result = {ok, #{parallel_boxes := ReturnedParallelBoxSpecs}}
    }) ->
        ExpParallelBoxSpecs = lists:map(fun(ExpParallelBoxSpec) ->
            maps_utils:map_key_value(fun
                ({_, _, _} = TaskSelector, TaskSpec) ->
                    {atm_workflow_execution_exp_state_builder:get_task_id(TaskSelector, ExpState), TaskSpec};
                (AtmTaskExecutionId, TaskSpec) ->
                    {AtmTaskExecutionId, TaskSpec}
            end, ExpParallelBoxSpec)
        end, ExpParallelBoxSpecsWithTaskSelectors),

        ?assertEqual(ExpParallelBoxSpecs, ReturnedParallelBoxSpecs)
    end.
