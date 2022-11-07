%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning stopping of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_stopping_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    stopping_reason_interrupt_overrides_pause/0,

    stopping_reason_failure_overrides_pause/0,
    stopping_reason_failure_overrides_interrupt/0,

    stopping_reason_cancel_overrides_pause/0,
    stopping_reason_cancel_overrides_interrupt/0,
    stopping_reason_cancel_overrides_failure/0,

    stopping_reason_crash_overrides_pause/0,
    stopping_reason_crash_overrides_interrupt/0,
    stopping_reason_crash_overrides_failure/0,
    stopping_reason_crash_overrides_cancel/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(__ID, __DISPATCH_RULES), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = ?TARGET_STORE_SCHEMA_ID,
        store_content_update_options = #atm_time_series_store_content_update_options{
            dispatch_rules = __DISPATCH_RULES
        }
    }]
}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__RELAY_METHOD, __ITERATED_CONTENT, __DISPATCH_RULES),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(?FUNCTION_NAME),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, __ITERATED_CONTENT),
                ?ATM_TS_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                        ?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task0">>, ?CORRECT_ATM_TS_DISPATCH_RULES)
                    ]}],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                    },
                    max_retries = 0
                },
                #atm_lane_schema_draft{
                    parallel_boxes = [
                        #atm_parallel_box_schema_draft{
                            id = <<"pb1">>,
                            tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task1">>, __DISPATCH_RULES)]
                        },
                        #atm_parallel_box_schema_draft{
                            id = <<"pb2">>,
                            tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task2">>, __DISPATCH_RULES)]
                        }
                    ],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                        max_batch_size = ?RAND_INT(3, 6)
                    },
                    max_retries = 0
                }
            ]
        },
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, __RELAY_METHOD)
            }
        }
    }
).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).
-define(TASK2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task2">>}).

-define(PB1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>}).
-define(PB2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>}).

-define(PB1_TASK1_STATUS_EXPECTATIONS(Status), [
    {task, ?TASK1_SELECTOR({2, 1}), Status},
    {parallel_box, ?PB1_SELECTOR({2, 1}), Status}
]).
-define(PB2_TASK2_STATUS_EXPECTATIONS(Status), [
    {task, ?TASK2_SELECTOR({2, 1}), Status},
    {parallel_box, ?PB2_SELECTOR({2, 1}), Status}
]).

-define(CRASHING_TASK_EXECUTION_STOPPED_STEP_MOCK, #atm_step_mock_spec{
    strategy = {yield, {error, crashed}},
    after_step_exp_state_diff = no_diff
}).

-define(INVALID_DISPATCH_RULE, #atm_time_series_dispatch_rule{
    measurement_ts_name_matcher_type = exact,
    measurement_ts_name_matcher = <<"size">>,
    target_ts_name_generator = <<"missing_generator">>,
    prefix_combiner = overwrite
}).

-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


stopping_reason_interrupt_overrides_pause() ->
    DelayedMeasurementName = <<"delayed">>,
    ContainsDelayedMeasurementPred = fun(Measurements) ->
        lists:any(fun(#{<<"tsName">> := TSName}) -> TSName == DelayedMeasurementName end, Measurements)
    end,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value,
            [gen_time_series_measurement(DelayedMeasurementName) | gen_correct_time_series_measurements()],
            ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
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
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(paused),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ]),
                        % Delay execution of chosen batch to ensure it happens after execution is interrupted
                        strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, ItemBatch, _]}) ->
                            case ContainsDelayedMeasurementPred(ItemBatch) of
                                true -> {passthrough_with_delay, timer:seconds(1)};
                                false -> passthrough
                            end
                        end,
                        after_step_hook = fun atm_workflow_execution_test_utils:delete_offline_session/1
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
                % Ensure interrupt prevails against pause
                after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                % Already paused task2 changes status to interrupted
                after_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted)
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure interrupt prevails against pause
                before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                after_step_exp_state_diff = [workflow_interrupted]
            }
        }]
    }).


stopping_reason_failure_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TS_DISPATCH_RULES]
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},

                    % Failure occurs during streamed data processing
                    process_streamed_task_data = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(paused),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ]),
                        % Already stopped task2 changes status from paused to interrupted due to task1 failure
                        after_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted)
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task1">> => ?PB1_TASK1_STATUS_EXPECTATIONS(failed)
                        })
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {2, 1}, failed}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}, {2, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


stopping_reason_failure_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TS_DISPATCH_RULES]
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},

                    % Failure occurs during streamed data processing
                    process_streamed_task_data = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1,
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against interrupt
                        before_step_hook = fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1,

                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task1">> => ?PB1_TASK1_STATUS_EXPECTATIONS(failed)
                        })
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {2, 1}, failed}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}, {2, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


stopping_reason_cancel_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
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
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(paused),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        % Already stopped task2 changes status from paused to cancelled
                        before_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(cancelled),
                        % Ensure cancel prevails against pause
                        after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task1">> => ?PB1_TASK1_STATUS_EXPECTATIONS(cancelled)
                        })
                    },
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
            }
        }]
    }).


stopping_reason_cancel_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1,
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [
                            {all_tasks, {2, 1}, abruptly, cancelled},
                            {lane_run, {2, 1}, cancelled}
                        ]
                    }
                }
            ],
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure cancel prevails against interrupt
                before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                % Already stopped task2 changes status from interrupted to cancelled
                before_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(cancelled),

                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}, {2, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


stopping_reason_cancel_overrides_failure() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TS_DISPATCH_RULES]
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},

                    % Failure occurs during streamed data processing
                    process_streamed_task_data = #atm_step_mock_spec{
                        after_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        % Already stopped task2 changes status from interrupted to cancelled
                        before_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(cancelled),
                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task1">> => ?PB1_TASK1_STATUS_EXPECTATIONS(cancelled)
                        })
                    },
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
            }
        }]
    }).


stopping_reason_crash_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
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
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(paused),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = ?CRASHING_TASK_EXECUTION_STOPPED_STEP_MOCK,

                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, crashed}
                        ])
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                % Ensure crash prevails against pause
                after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                % Already paused task2 changes status to interrupted
                after_step_exp_state_diff = ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted)
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure crash prevails against pause
                before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                after_step_exp_state_diff = [workflow_crashed]
            }
        }]
    }).


stopping_reason_crash_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1,
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = ?CRASHING_TASK_EXECUTION_STOPPED_STEP_MOCK,

                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, crashed}
                        ])
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                % Ensure crash prevails against interrupt
                before_step_hook = fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure crash prevails against interrupt
                before_step_hook = fun atm_workflow_execution_test_utils:interrupt_workflow_execution/1,
                after_step_exp_state_diff = [workflow_crashed]
            }
        }]
    }).


stopping_reason_crash_overrides_failure() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TS_DISPATCH_RULES]
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},

                    % Failure occurs during streamed data processing
                    process_streamed_task_data = #atm_step_mock_spec{
                        after_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(interrupted),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = ?CRASHING_TASK_EXECUTION_STOPPED_STEP_MOCK,

                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = lists:flatten([
                            % Task transitions to failed status if it was already stopping due to failure when crash occurred
                            ?PB1_TASK1_STATUS_EXPECTATIONS(failed),
                            {lane_run, {2, 1}, crashed}
                        ])
                    }
                }
            ],
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{after_step_exp_state_diff = [workflow_crashed]}
        }]
    }).


stopping_reason_crash_overrides_cancel() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TS_DISPATCH_RULES
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1}
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    process_task_result_for_item = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        before_step_exp_state_diff = lists:flatten([
                            ?PB1_TASK1_STATUS_EXPECTATIONS(stopping),
                            ?PB2_TASK2_STATUS_EXPECTATIONS(cancelled),
                            {lane_run, {2, 1}, stopping},
                            workflow_stopping
                        ])
                    },
                    handle_task_execution_stopped = ?CRASHING_TASK_EXECUTION_STOPPED_STEP_MOCK,

                    % This is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [
                            % Task transitions to cancelled status if it was already stopping due to cancel when crash occurred
                            ?PB1_TASK1_STATUS_EXPECTATIONS(cancelled),
                            {lane_run, {2, 1}, crashed}
                        ]
                    }
                }
            ],
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure crash prevails against cancel
                before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                after_step_exp_state_diff = [workflow_crashed]
            }
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
gen_correct_time_series_measurements() ->
    lists_utils:generate(fun(_) ->
        gen_time_series_measurement(?RAND_ELEMENT([<<"count_erl">>, ?RAND_STR()]))
    end, 10).


%% @private
gen_time_series_measurement(TsName) ->
    #{
        <<"tsName">> => TsName,
        <<"timestamp">> => ?RAND_ELEMENT([?NOW_SEC() - 100, ?NOW_SEC(), ?NOW_SEC() + 3700]),
        <<"value">> => ?RAND_INT(10000000)
    }.
