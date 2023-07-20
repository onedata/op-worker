%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of repeating (rerunning or retrying) automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_repeat_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    repeat_finished_atm_lane_run_execution/0,
    rerun_failed_iterated_atm_lane_run_execution/0,
    retry_failed_iterated_atm_lane_run_execution/0,
    repeat_failed_while_preparing_atm_lane_run_execution/0,
    repeat_failed_not_iterated_atm_lane_run_execution/0,
    repeat_cancelled_atm_lane_run_execution/0
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

-define(MISSING_TS_NAME_GENERATOR, <<"missing_generator">>).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__ITERATED_CONTENT, __RELAY_METHOD),
    ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, __ITERATED_CONTENT, __RELAY_METHOD)
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __ITERATED_CONTENT, __RELAY_METHOD),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?ATM_LIST_STORE_SCHEMA_DRAFT(
                    ?ITERATED_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, __ITERATED_CONTENT
                ),
                ?ATM_TS_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(
                            <<"task1">>,
                            ?CORRECT_ATM_TS_DISPATCH_RULES
                        )]
                    }],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                    },
                    max_retries = 0
                },
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(
                            <<"task2">>,
                            [
                                #atm_time_series_dispatch_rule{
                                    measurement_ts_name_matcher_type = exact,
                                    measurement_ts_name_matcher = <<"size">>,
                                    target_ts_name_generator = ?MISSING_TS_NAME_GENERATOR,
                                    prefix_combiner = overwrite
                                }
                                | ?CORRECT_ATM_TS_DISPATCH_RULES
                            ]
                        )]
                    }],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                    },
                    max_retries = 1
                }
            ]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, __RELAY_METHOD)
        }}
    }
).

-define(TASK2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task2">>}).

-define(LANE_RUNS_RERUNABLE(__LANE_RUNS), {lane_runs, __LANE_RUNS, rerunable}).
-define(LANE_RUNS_NOT_RERUNABLE(__LANE_RUNS), {lane_runs, __LANE_RUNS, rerunable, false}).

-define(LANE_RUNS_RETRIABLE(__LANE_RUNS), {lane_runs, __LANE_RUNS, retriable}).
-define(LANE_RUNS_NOT_RETRIABLE(__LANE_RUNS), {lane_runs, __LANE_RUNS, retriable, false}).

-define(LANE_RUNS_REPEATABLE(__LANE_RUNS), [
    ?LANE_RUNS_RERUNABLE(__LANE_RUNS),
    ?LANE_RUNS_RETRIABLE(__LANE_RUNS)
]).
-define(LANE_RUNS_NOT_REPEATABLE(__LANE_RUNS), [
    ?LANE_RUNS_NOT_RERUNABLE(__LANE_RUNS),
    ?LANE_RUNS_NOT_RETRIABLE(__LANE_RUNS)
]).

-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


% Retrying finished lane run should fail while rerunning it should succeed
repeat_finished_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{selector = {1, 1}},
                    build_failed_atm_lane_run_execution_test_spec({2, 1}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}, {2, 2}]),
                        ?LANE_RUNS_RETRIABLE([{2, 1}, {2, 2}]),
                        workflow_failed
                    ]
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_utils:repeat_workflow_execution(
                        rerun, {1, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 3},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = lists:flatten([
                                ?LANE_RUNS_NOT_REPEATABLE([{1, 1}, {2, 1}, {2, 2}]),
                                {lane_run, {1, 1}, manual_rerun_scheduled, 3},
                                workflow_scheduled
                            ])
                        }
                    },
                    % Manual repeat disables automatic retries only for directly repeated lane and not for next ones
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 4}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}, {2, 2}, {1, 3}, {2, 3}, {2, 4}]),
                        ?LANE_RUNS_RETRIABLE([{2, 1}, {2, 2}, {2, 3}, {2, 4}]),
                        workflow_failed
                    ]
                }
            }
        ]
    }).


rerun_failed_iterated_atm_lane_run_execution() ->
    repeat_failed_iterated_atm_lane_run_execution_test_base(?FUNCTION_NAME, rerun).


retry_failed_iterated_atm_lane_run_execution() ->
    repeat_failed_iterated_atm_lane_run_execution_test_base(?FUNCTION_NAME, retry).


repeat_failed_iterated_atm_lane_run_execution_test_base(TestCase, RepeatType) ->
    RepeatedLaneRunTestSpec = build_failed_atm_lane_run_execution_test_spec({2, 3}, true),

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            TestCase, gen_time_series_measurements(), return_value
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{selector = {1, 1}},
                    build_failed_atm_lane_run_execution_test_spec({2, 1}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}, {2, 2}]),
                        ?LANE_RUNS_RETRIABLE([{2, 1}, {2, 2}]),
                        workflow_failed
                    ]
                },
                after_hook = fun(AtmMockCallCtx) ->
                    ?assertEqual(ok, atm_workflow_execution_test_utils:repeat_workflow_execution(
                        RepeatType, {2, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    % Manual repeat disables automatic retries for directly repeated lane
                    RepeatedLaneRunTestSpec#atm_lane_run_execution_test_spec{
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = lists:flatten([
                                ?LANE_RUNS_NOT_REPEATABLE([{1, 1}, {2, 1}, {2, 2}]),
                                {lane_run, {2, 1}, manual_repeat_scheduled, RepeatType, 3},
                                workflow_scheduled
                            ])
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}, {2, 2}, {2, 3}]),
                        ?LANE_RUNS_RETRIABLE([{2, 1}, {2, 2}, {2, 3}]),
                        workflow_failed
                    ]
                }
            }
        ]
    }).


% Retrying failed while preparing lane run should fail while rerunning it should succeed
repeat_failed_while_preparing_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        create_run = #atm_step_mock_spec{
                            strategy = {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                        },
                        prepare_lane = #atm_step_mock_spec{
                            after_step_exp_state_diff = no_diff
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {all_tasks, {1, 1}, interrupted},
                                {lane_run, {1, 1}, stopping},
                                workflow_stopping
                            ],
                            after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {prepare_lane, after_step, {1, 1}},
                            before_step_exp_state_diff = no_diff,
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}]),
                        workflow_failed
                    ]
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_utils:repeat_workflow_execution(
                        rerun, {1, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    % Manual repeat disables automatic retries for directly repeated lane
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 2},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = lists:flatten([
                                ?LANE_RUNS_NOT_REPEATABLE([{1, 1}]),
                                {lane_run, {1, 1}, manual_rerun_scheduled, 2},
                                workflow_scheduled
                            ])
                        }
                    },
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {1, 2}, {2, 2}, {2, 3}]),
                        ?LANE_RUNS_RETRIABLE([{2, 2}, {2, 3}]),
                        workflow_failed
                    ]
                }
            }
        ]
    }).


% Retrying failed due to reason other than job failures (e.g. uncorrelated task result processing)
% lane run should fail while rerunning it should succeed
repeat_failed_not_iterated_atm_lane_run_execution() ->
    Lane2Run2TestSpec = 'build failed due to task uncorrelated result processing atm_lane_run_execution_test_spec'(
        {2, 2}
    ),
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), file_pipe
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1}
                    },
                    'build failed due to task uncorrelated result processing atm_lane_run_execution_test_spec'({2, 1})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}]),
                        workflow_failed
                    ]
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_utils:repeat_workflow_execution(
                        rerun, {2, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [Lane2Run2TestSpec#atm_lane_run_execution_test_spec{
                    prepare_lane = #atm_step_mock_spec{
                        before_step_exp_state_diff = lists:flatten([
                            ?LANE_RUNS_NOT_REPEATABLE([{1, 1}, {2, 1}]),
                            {lane_run, {2, 1}, manual_rerun_scheduled, 2},
                            workflow_scheduled
                        ])
                    }
                }],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {2, 1}, {2, 2}]),
                        workflow_failed
                    ]
                }
            }
        ]
    }).


% Retrying cancelled lane run should fail while rerunning it should succeed
repeat_cancelled_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        create_run = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 1}, started_preparing_as_current_lane_run}
                            ],

                            after_step_hook = fun atm_workflow_execution_test_utils:cancel_workflow_execution/1,
                            after_step_exp_state_diff = [
                                {lane_run, {1, 1}, created},
                                {all_tasks, {1, 1}, cancelled},
                                {lane_run, {1, 1}, stopping},
                                workflow_stopping
                            ]
                        },
                        prepare_lane = #atm_step_mock_spec{
                            % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                            % from within lane preparation
                            after_step_exp_state_diff = no_diff
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {prepare_lane, after_step, {1, 1}},
                            before_step_exp_state_diff = no_diff,
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}]),
                        workflow_cancelled
                    ]
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_utils:repeat_workflow_execution(
                        rerun, {1, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    % Manual repeat disables automatic retries for directly repeated lane
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 2},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = lists:flatten([
                                ?LANE_RUNS_NOT_REPEATABLE([{1, 1}]),
                                {lane_run, {1, 1}, manual_rerun_scheduled, 2},
                                workflow_scheduled
                            ])
                        }
                    },
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        ?LANE_RUNS_RERUNABLE([{1, 1}, {1, 2}, {2, 2}, {2, 3}]),
                        ?LANE_RUNS_RETRIABLE([{2, 2}, {2, 3}]),
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
-spec assert_not_retriable(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_retriable(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertThrow(
        ?ERROR_ATM_LANE_EXECUTION_RETRY_FAILED,
        atm_workflow_execution_test_utils:repeat_workflow_execution(retry, AtmLaneRunSelector, AtmMockCallCtx)
    ).


%% @private
-spec gen_time_series_measurements() -> [json_utils:json_map()].
gen_time_series_measurements() ->
    lists_utils:generate(fun(_) ->
        #{
            <<"tsName">> => ?RAND_ELEMENT([<<"count_erl">>, <<"size">>, ?RAND_STR()]),
            <<"timestamp">> => ?RAND_ELEMENT([?NOW_SEC() - 100, ?NOW_SEC(), ?NOW_SEC() + 3700]),
            <<"value">> => ?RAND_INT(10000000)
        }
    end, 40).


%% @private
build_failed_atm_lane_run_execution_test_spec(AtmLaneRunSelector, IsLastExpLaneRun) ->
    #atm_lane_run_execution_test_spec{
        selector = AtmLaneRunSelector,

        process_task_result_for_item = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                workflow_execution_exp_state = ExpState,
                call_args = [_, _, AtmTaskExecutionId, ItemBatch, _]
            }) ->
                ItemCount = length(ItemBatch),
                FailedItemCount = length(filter_size_measurements(ItemBatch)),

                {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                    {task, AtmTaskExecutionId, items_failed, FailedItemCount},
                    {task, AtmTaskExecutionId, items_finished, ItemCount - FailedItemCount}
                ])}
            end
        },

        handle_task_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [{task, ?TASK2_SELECTOR(AtmLaneRunSelector), failed}]
        },

        handle_lane_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [
                {lane_run, AtmLaneRunSelector, failed},
                case IsLastExpLaneRun of
                    true -> workflow_stopping;
                    false -> {lane_run, AtmLaneRunSelector, automatic_retry_scheduled}
                end
            ]
        }
    }.


%% @private
'build failed due to task uncorrelated result processing atm_lane_run_execution_test_spec'(AtmLaneRunSelector) ->
    #atm_lane_run_execution_test_spec{
        selector = AtmLaneRunSelector,
        process_streamed_task_data = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                workflow_execution_exp_state = ExpState,
                call_args = [
                    _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, _AtmTaskExecutionId,
                    {chunk, #{?ECHO_ARG_NAME := UncorrelatedResults}}
                ]
            }) ->
                case does_contain_size_measurement(UncorrelatedResults) of
                    true ->
                        {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                            {all_tasks, AtmLaneRunSelector, stopping_due_to, interrupt},
                            {lane_run, AtmLaneRunSelector, stopping},
                            workflow_stopping
                        ])};
                    false ->
                        false
                end
            end
        },
        handle_task_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [{task, ?TASK2_SELECTOR(AtmLaneRunSelector), failed}]
        },
        handle_lane_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = [{lane_run, AtmLaneRunSelector, failed}]
        }
    }.


%% @private
-spec does_contain_size_measurement([json_utils:json_map()]) -> boolean().
does_contain_size_measurement(Measurements) ->
    lists:any(fun is_size_measurement/1, Measurements).


%% @private
-spec filter_size_measurements([atm_workflow_execution_handler:item()]) ->
    [json_utils:json_map()].
filter_size_measurements(Measurements) ->
    lists:filter(fun is_size_measurement/1, atm_workflow_execution_test_utils:get_values_batch(
        Measurements
    )).


%% @private
-spec is_size_measurement(json_utils:json_map()) -> boolean().
is_size_measurement(#{<<"tsName">> := <<"size">>}) -> true;
is_size_measurement(#{<<"tsName">> := _}) -> false.
