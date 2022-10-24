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
                #atm_store_schema_draft{
                    id = ?ITERATED_STORE_SCHEMA_ID,
                    type = list,
                    config = #atm_list_store_config{item_data_spec = ?ANY_MEASUREMENT_DATA_SPEC},
                    requires_initial_content = false,
                    default_initial_content = __ITERATED_CONTENT
                },
                ?ATM_TS_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                        ?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task0">>, ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)
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
-define(ATM_WORKFLOW_SCHEMA_DRAFT, ?ATM_WORKFLOW_SCHEMA_DRAFT(
    return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
)).

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

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value,
            [gen_time_series_measurement(DelayedMeasurementName) | gen_correct_time_series_measurements()],
            ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, pause)}
                        end,

                        strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, ItemBatch, _]}) ->
                            ContainsDelayedMeasurement = lists:any(
                                fun(#{<<"tsName">> := TSName}) -> TSName == DelayedMeasurementName end,
                                ItemBatch
                            ),
                            case ContainsDelayedMeasurement of
                                true ->
                                    % Delay execution of batch to ensure it happens after execution is interrupted
                                    % which should result in exception interrupting execution
                                    {passthrough_with_delay, timer:seconds(1)};
                                false ->
                                    passthrough
                            end
                        end,
                        after_step_hook = fun atm_workflow_execution_test_utils:delete_offline_session/1
                    },
                    % this is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped =#atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_abruptly_interrupted(
                                {2, 1}, ExpState0
                            ),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_interrupted({2, 1}, ExpState1)}
                        end
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                % Ensure interrupt prevails against pause
                after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                %% already paused task2 changes status to interrupted
                after_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"interrupted">>)
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure interrupt prevails against pause
                before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_interrupted(ExpState0)}
                end
            }
        }]
    }).


stopping_reason_failure_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES]
        ),
        workflow_schema_revision_num = 1,
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

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, pause)}
                        end,
                        %% already stopped task2 changes status from paused to interrupted due to task1 failure
                        after_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"interrupted">>)
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                        after_step_exp_state_diff = build_lane2_task_execution_stopped_after_step_diff(<<"failed">>)
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({2, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                end
            }
        }]
    }).


stopping_reason_failure_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES]
        ),
        workflow_schema_revision_num = 1,
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
                        before_step_hook = fun interrupt_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against interrupt
                        before_step_hook = fun interrupt_workflow_execution/1,

                        after_step_exp_state_diff = build_lane2_task_execution_stopped_after_step_diff(<<"failed">>)
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({2, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                end
            }
        }]
    }).


stopping_reason_cancel_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, pause)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure cancel prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        %% already stopped task2 changes status from paused to cancelled
                        before_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"cancelled">>),
                        after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        after_step_exp_state_diff = build_lane2_task_execution_stopped_after_step_diff(<<"cancelled">>)
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({2, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            }
        }]
    }).


stopping_reason_cancel_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun atm_workflow_execution_test_utils:report_openfaas_unhealthy/1,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                        end
                    },
                    % this is called as part of `handle_workflow_abruptly_stopped`
                    handle_lane_execution_stopped =#atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_abruptly_cancelled(
                                {2, 1}, ExpState0
                            ),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({2, 1}, ExpState1)}
                        end
                    }
                }
            ],
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                % Ensure cancel prevails against interrupt
                before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                %% already stopped task2 changes status from interrupted to cancelled
                before_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"cancelled">>),

                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            }
        }]
    }).


stopping_reason_cancel_overrides_failure() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES]
        ),
        workflow_schema_revision_num = 1,
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
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                        %% already stopped task2 changes status from interrupted to cancelled
                        before_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"cancelled">>),
                        after_step_exp_state_diff = build_lane2_task_execution_stopped_after_step_diff(<<"cancelled">>)
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({2, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                end
            }
        }]
    }).


stopping_reason_crash_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, pause)}
                        end
                    },
                    handle_task_execution_stopped = build_crash_handle_task_execution_stopped_step_mock_spec(),
                    handle_lane_execution_stopped = build_crash_handle_lane_execution_stopped_step_mock_spec(<<"interrupted">>)
                }
            ],
            handle_exception = #atm_step_mock_spec{
                after_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                %% already paused task2 changes status to interrupted
                after_step_exp_state_diff = build_expect_task2_stopped_exp_state_diff(<<"interrupted">>)
            },
            handle_workflow_abruptly_stopped = build_crash_handle_workflow_abruptly_stopped_step_mock_spec(
                fun atm_workflow_execution_test_utils:pause_workflow_execution/1
            )
        }]
    }).


stopping_reason_crash_overrides_interrupt() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun interrupt_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                        end
                    },
                    handle_task_execution_stopped = build_crash_handle_task_execution_stopped_step_mock_spec(),
                    handle_lane_execution_stopped = build_crash_handle_lane_execution_stopped_step_mock_spec(<<"interrupted">>)
                }
            ],
            handle_exception = #atm_step_mock_spec{
                before_step_hook = fun interrupt_workflow_execution/1
            },
            handle_workflow_abruptly_stopped = build_crash_handle_workflow_abruptly_stopped_step_mock_spec(
                fun interrupt_workflow_execution/1
            )
        }]
    }).


stopping_reason_crash_overrides_failure() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            file_pipe,
            [gen_time_series_measurement(<<"size">>)],
            [?INVALID_DISPATCH_RULE | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES]
        ),
        workflow_schema_revision_num = 1,
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
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                        end
                    },
                    handle_task_execution_stopped = build_crash_handle_task_execution_stopped_step_mock_spec(),
                    % Task transitions to failed status if it was already stopping due to failure when crash occurred
                    handle_lane_execution_stopped = build_crash_handle_lane_execution_stopped_step_mock_spec(<<"failed">>)
                }
            ],
            handle_workflow_abruptly_stopped = build_crash_handle_workflow_abruptly_stopped_step_mock_spec(undefined)
        }]
    }).


stopping_reason_crash_overrides_cancel() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            return_value, gen_correct_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
        ),
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
                        before_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0, cancel)}
                        end
                    },
                    handle_task_execution_stopped = build_crash_handle_task_execution_stopped_step_mock_spec(),
                    % Task transitions to cancelled status if it was already stopping due to cancel when crash occurred
                    handle_lane_execution_stopped = build_crash_handle_lane_execution_stopped_step_mock_spec(<<"cancelled">>)
                }
            ],
            handle_workflow_abruptly_stopped = build_crash_handle_workflow_abruptly_stopped_step_mock_spec(
                fun atm_workflow_execution_test_utils:cancel_workflow_execution/1
            )
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


%% @private
interrupt_workflow_execution(AtmMockCallCtx) ->
    atm_workflow_execution_test_utils:stop_workflow_execution(interrupt, AtmMockCallCtx).


%% @private
expect_execution_stopping_while_processing_lane2(ExpState0, Reason) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping_due_to({2, 1}, Reason, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
build_expect_task2_stopped_exp_state_diff(ExpectTask2FinalStatus) ->
    fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
        AtmTaskExecutionId = atm_workflow_execution_exp_state_builder:get_task_id(
            {{2, 1}, <<"pb2">>, <<"task2">>}, ExpState
        ),
        {true, expect_lane2_pb_stopped(ExpectTask2FinalStatus, AtmTaskExecutionId, ExpState)}
    end.


%% @private
build_crash_handle_task_execution_stopped_step_mock_spec() ->
    #atm_step_mock_spec{
        strategy = {yield, {error, crashed}},
        after_step_exp_state_diff = no_diff
    }.


%% @private
build_crash_handle_lane_execution_stopped_step_mock_spec(ExpTask1FinalStatus) ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
            ExpState1 = expect_pb1_stopped(ExpTask1FinalStatus, ExpState0),
            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({2, 1}, ExpState1)}
        end
    }.


%% @private
build_crash_handle_workflow_abruptly_stopped_step_mock_spec(AfterHook) ->
    #atm_step_mock_spec{
        before_step_hook = AfterHook,
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_crashed(ExpState0)}
        end
    }.


%% @private
build_lane2_task_execution_stopped_after_step_diff(ExpectTask1FinalStatus) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }) ->
        case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
            {_, <<"pb1">>, <<"task1">>} ->
                {true, expect_lane2_pb_stopped(ExpectTask1FinalStatus, AtmTaskExecutionId, ExpState0)};
            {_, <<"pb2">>, <<"task2">>} ->
                % task2 should be stopped before handle_task_execution_stopped callback is called
                false
        end
    end.


%% @private
expect_pb1_stopped(ExpTask1FinalStatus, ExpState) ->
    AtmTask1ExecutionId = atm_workflow_execution_exp_state_builder:get_task_id(
        {{2, 1}, <<"pb1">>, <<"task1">>}, ExpState
    ),
    expect_lane2_pb_stopped(ExpTask1FinalStatus, AtmTask1ExecutionId, ExpState).


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

expect_lane2_task_stopped(<<"skipped">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_skipped(AtmTaskExecutionId, ExpState0);

expect_lane2_task_stopped(<<"failed">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_failed(AtmTaskExecutionId, ExpState0);

expect_lane2_task_stopped(<<"cancelled">>, AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_cancelled(AtmTaskExecutionId, ExpState0).


%% @private
expect_lane_runs_rerunable(AtmLaneRunSelectors, ExpState) ->
    lists:foldl(fun(AtmLaneRunSelector, ExpStateAcc) ->
        atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(AtmLaneRunSelector, ExpStateAcc)
    end, ExpState, AtmLaneRunSelectors).
