%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning stopping of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_stopping_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    stopping_reason_failure_overrides_pause/0,
    stopping_reason_cancel_overrides_pause/0,
    stopping_reason_cancel_overrides_failure/0,

    stopping_finished_atm_workflow_execution/0,
    stopping_cancelled_atm_workflow_execution/0,
    stopping_failed_atm_workflow_execution/0,
    stopping_crashed_atm_workflow_execution/0
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

-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


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
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        after_step_exp_state_diff = build_lane2_task_stopped_after_step_diff(<<"failed">>)
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
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure cancel prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        after_step_exp_state_diff = build_lane2_task_stopped_after_step_diff(<<"cancelled">>)
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
                            {true, expect_execution_stopping_while_processing_lane2(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_exp_state_diff = build_lane2_task_stopped_after_step_diff(<<"cancelled">>)
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


stopping_finished_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{selector = {1, 1}},
                #atm_lane_run_execution_test_spec{selector = {2, 1}}
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_lane_runs_rerunable([{1, 1}, {2, 1}], ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                end
            },
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_stopped/1
        }]
    }).


stopping_cancelled_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
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
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_stopped/1
        }]
    }).


stopping_failed_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        % 'create_run' step execution is mocked entirely so that
                        % no lane run execution component will be created
                        strategy = {yield, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, ExpState)}
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                end
            },
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_stopped/1
        }]
    }).


stopping_crashed_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
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
                        strategy = {yield, {error, crashed}},
                        after_step_exp_state_diff = no_diff
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = expect_execution_stopping_while_processing_lane2(ExpState0),
                    ExpState2 = expect_lane2_pb_stopped(<<"interrupted">>, get_task1_id(ExpState1), ExpState1),
                    ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({2, 1}, ExpState2),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_crashed(ExpState3)}
                end
            },
            after_hook = fun assert_ended_atm_workflow_execution_can_not_be_stopped/1
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
        <<"timestamp">> => ?RAND_ELEMENT([?NOW() - 100, ?NOW(), ?NOW() + 3700]),
        <<"value">> => ?RAND_INT(10000000)
    }.


%% @private
expect_execution_stopping_while_processing_lane2(ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({2, 1}, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
build_lane2_task_stopped_after_step_diff(ExpectTask1FinalStatus) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }) ->
        case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
            {_, <<"pb1">>, <<"task1">>} ->
                {true, expect_lane2_pb_stopped(ExpectTask1FinalStatus, AtmTaskExecutionId, ExpState0)};
            {_, <<"pb2">>, <<"task2">>} ->
                % execution is paused after first items for task1 finished processing
                % and as such no item was ever scheduled for task2
                {true, expect_lane2_pb_stopped(<<"skipped">>, AtmTaskExecutionId, ExpState0)}
        end
    end.


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
get_task1_id(ExpState) ->
    atm_workflow_execution_exp_state_builder:get_task_id({{2, 1}, <<"pb1">>, <<"task1">>}, ExpState).


%% @private
expect_lane_runs_rerunable(AtmLaneRunSelectors, ExpState) ->
    lists:foldl(fun(AtmLaneRunSelector, ExpStateAcc) ->
        atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(AtmLaneRunSelector, ExpStateAcc)
    end, ExpState, AtmLaneRunSelectors).


%% @private
assert_ended_atm_workflow_execution_can_not_be_stopped(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    lists:foreach(fun(StoppingReason) ->
        ?assertEqual(
            ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED,
            atm_workflow_execution_test_runner:stop_workflow_execution(StoppingReason, AtmMockCallCtx)
        )
    end, [crash, cancel, failure, interrupt, pause]),

    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0)).
