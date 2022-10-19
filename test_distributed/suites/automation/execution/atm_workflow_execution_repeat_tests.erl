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
    repeat_not_ended_atm_workflow_execution/0,
    repeat_crashed_atm_workflow_execution/0,

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
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(
                            <<"lane1_task1">>,
                            ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
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
                            <<"lane2_task1">>,
                            [
                                #atm_time_series_dispatch_rule{
                                    measurement_ts_name_matcher_type = exact,
                                    measurement_ts_name_matcher = <<"size">>,
                                    target_ts_name_generator = ?MISSING_TS_NAME_GENERATOR,
                                    prefix_combiner = overwrite
                                }
                                | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
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
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, __RELAY_METHOD)
            }
        }
    }
).

-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


% Attempting to repeat any lane run while atm workflow execution has not yet ended should fail
repeat_not_ended_atm_workflow_execution() ->
    AssertNotRepeatableFun = fun(AtmMockCallCtx) ->
        lists:foreach(fun(RepeatType) ->
            ?assertThrow(
                ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_ENDED,
                atm_workflow_execution_test_runner:repeat_workflow_execution(
                    RepeatType, {1, 1}, AtmMockCallCtx
                )
            )
        end, [rerun, retry])
    end,
    Lane1Run1StepMockSpecBase = #atm_step_mock_spec{
        before_step_hook = AssertNotRepeatableFun,
        after_step_hook = AssertNotRepeatableFun
    },

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = #atm_workflow_schema_dump_draft{
            name = <<"repeat_not_ended_atm_workflow_execution">>,
            revision_num = 1,
            revision = #atm_workflow_schema_revision_draft{
                stores = [
                    ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st1">>, lists:seq(1, 20))
                ],
                lanes = [#atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        tasks = [#atm_task_schema_draft{
                            id = <<"t1">>,
                            lambda_id = ?ECHO_LAMBDA_ID,
                            lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                            argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                            result_mappings = []
                        }]
                    }],
                    store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st1">>}
                }]
            },
            supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(#atm_data_spec{type = atm_integer_type})
            }}
        },
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = Lane1Run1StepMockSpecBase,
                    create_run = Lane1Run1StepMockSpecBase,
                    run_task_for_item = Lane1Run1StepMockSpecBase,
                    process_task_result_for_item = Lane1Run1StepMockSpecBase,
                    handle_task_execution_stopped = Lane1Run1StepMockSpecBase,
                    handle_lane_execution_stopped = Lane1Run1StepMockSpecBase#atm_step_mock_spec{
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_runner:pause_workflow_execution(AtmMockCallCtx),
                            AssertNotRepeatableFun(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState0)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = Lane1Run1StepMockSpecBase#atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                end
            }
        }]
    }).


% Attempting to repeat any lane run while atm workflow execution has crashed should fail
repeat_crashed_atm_workflow_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
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
                        strategy = {yield, {error, crashed}},
                        after_step_exp_state_diff = no_diff
                    },

                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        % this is called as part of `handle_workflow_abruptly_stopped`
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_abruptly_interrupted(
                                {2, 1}, ExpState0
                            ),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({2, 1}, ExpState1)}
                        end
                    }
                }
            ],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, expect_execution_stopping_while_processing_lane2(ExpState0, interrupt)}
                end
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_crashed(ExpState0)}
                end
            },
            after_hook = fun(AtmMockCallCtx) ->
                assert_not_retriable({1, 1}, AtmMockCallCtx),
                assert_not_rerunable({1, 1}, AtmMockCallCtx),

                assert_not_retriable({2, 1}, AtmMockCallCtx),
                assert_not_rerunable({2, 1}, AtmMockCallCtx)
            end
        }]
    }).


% Retrying finished lane run should fail while rerunning it should succeed
repeat_finished_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{selector = {1, 1}},
                    build_failed_atm_lane_run_execution_test_spec({2, 1}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 1}, {2, 2}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_runner:repeat_workflow_execution(
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
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                                ExpState2 = expect_lane_runs_repeatable([{1, 1}, {2, 1}, {2, 2}], false, false, ExpState1),
                                ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_manual_repeat_scheduled(
                                    rerun, {1, 1}, 3, ExpState2
                                ),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_scheduled(ExpState3)}
                            end
                        }
                    },
                    % Manual repeat disables automatic retries only for directly repeated lane and not for next ones
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 4}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}, {1, 3}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 1}, {2, 2}, {2, 3}, {2, 4}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
                }
            }
        ]
    }).


rerun_failed_iterated_atm_lane_run_execution() ->
    repeat_failed_iterated_atm_lane_run_execution_test_base(?FUNCTION_NAME, rerun).


retry_failed_iterated_atm_lane_run_execution() ->
    repeat_failed_iterated_atm_lane_run_execution_test_base(?FUNCTION_NAME, retry).


repeat_failed_iterated_atm_lane_run_execution_test_base(TestCase, RepeatType) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            TestCase, gen_time_series_measurements(), return_value
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{selector = {1, 1}},
                    build_failed_atm_lane_run_execution_test_spec({2, 1}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 1}, {2, 2}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
                },
                after_hook = fun(AtmMockCallCtx) ->
                    ?assertEqual(ok, atm_workflow_execution_test_runner:repeat_workflow_execution(
                        RepeatType, {2, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    % Manual repeat disables automatic retries for directly repeated lane
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 3},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                                ExpState2 = expect_lane_runs_repeatable([{1, 1}, {2, 1}, {2, 2}], false, false, ExpState1),
                                ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_manual_repeat_scheduled(
                                    RepeatType, {2, 1}, 3, ExpState2
                                ),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_scheduled(ExpState3)}
                            end
                        },
                        process_task_result_for_item = 'build mock spec for process_task_result_for_item step'(),
                        handle_task_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = fun lane2_task1_expect_task_execution_failed/1
                        },
                        handle_lane_execution_stopped = 'build mock for handle_lane_execution_stopped step'(
                            {2, 3}, true
                        )
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 1}, {2, 2}, {2, 3}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
                }
            }
        ]
    }).


% Retrying failed while preparing lane run should fail while rerunning it should succeed
repeat_failed_while_preparing_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        workflow_schema_revision_num = 1,
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
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                                ExpState2 = atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted({1, 1}, ExpState1),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2)}
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
                            before_step_exp_state_diff = no_diff,
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}], true, false, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                    end
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_runner:repeat_workflow_execution(
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
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                                ExpState2 = expect_lane_runs_repeatable([{1, 1}], false, false, ExpState1),
                                ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_manual_repeat_scheduled(
                                    rerun, {1, 1}, 2, ExpState2
                                ),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_scheduled(ExpState3)}
                            end
                        }
                    },
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}, {1, 2}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 2}, {2, 3}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
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
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), file_pipe
        ),
        workflow_schema_revision_num = 1,
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
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}, {2, 1}], true, false, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                    end
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_runner:repeat_workflow_execution(
                        rerun, {2, 1}, AtmMockCallCtx
                    ))
                end
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [Lane2Run2TestSpec#atm_lane_run_execution_test_spec{
                    prepare_lane = #atm_step_mock_spec{
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                            ExpState2 = expect_lane_runs_repeatable([{1, 1}, {2, 1}], false, false, ExpState1),
                            ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_manual_repeat_scheduled(
                                rerun, {2, 1}, 2, ExpState2
                            ),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_scheduled(ExpState3)}
                        end
                    }
                }],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}, {2, 1}, {2, 2}], true, false, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                    end
                }
            }
        ]
    }).


% Retrying cancelled lane run should fail while rerunning it should succeed
repeat_cancelled_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), return_value
        ),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        create_run = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                {true, atm_workflow_execution_exp_state_builder:expect_current_lane_run_started_preparing(
                                    {1, 1}, ExpState0
                                )}
                            end,
                            after_step_hook = fun(AtmMockCallCtx) ->
                                atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_created({1, 1}, ExpState0),
                                ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState1),
                                ExpState3 = atm_workflow_execution_exp_state_builder:expect_all_tasks_cancelled({1, 1}, ExpState2),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState3)}
                            end
                        },
                        prepare_lane = #atm_step_mock_spec{
                            % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                            % from within lane preparation
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
                            defer_after = {prepare_lane, after_step, {1, 1}},
                            before_step_exp_state_diff = no_diff,
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}], true, false, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState1)}
                    end
                },
                after_hook = fun(AtmMockCallCtx) ->
                    assert_not_retriable({1, 1}, AtmMockCallCtx),
                    ?assertEqual(ok, atm_workflow_execution_test_runner:repeat_workflow_execution(
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
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState1}) ->
                                ExpState2 = expect_lane_runs_repeatable([{1, 1}], false, false, ExpState1),
                                ExpState3 = atm_workflow_execution_exp_state_builder:expect_lane_run_manual_repeat_scheduled(
                                    rerun, {1, 1}, 2, ExpState2
                                ),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_scheduled(ExpState3)}
                            end
                        }
                    },
                    build_failed_atm_lane_run_execution_test_spec({2, 2}, false),
                    build_failed_atm_lane_run_execution_test_spec({2, 3}, true)
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = expect_lane_runs_repeatable([{1, 1}, {1, 2}], true, false, ExpState0),
                        ExpState2 = expect_lane_runs_repeatable([{2, 2}, {2, 3}], true, true, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
                    end
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
        atm_workflow_execution_test_runner:repeat_workflow_execution(retry, AtmLaneRunSelector, AtmMockCallCtx)
    ).


%% @private
-spec assert_not_rerunable(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_rerunable(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertThrow(
        ?ERROR_ATM_LANE_EXECUTION_RERUN_FAILED,
        atm_workflow_execution_test_runner:repeat_workflow_execution(rerun, AtmLaneRunSelector, AtmMockCallCtx)
    ).


%% @private
expect_execution_stopping_while_processing_lane2(ExpState0, Reason) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({2, 1}, Reason, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
-spec expect_lane_runs_repeatable(
    [atm_lane_execution:lane_run_selector()],
    boolean(),
    boolean(),
    atm_workflow_execution_exp_state_builder:ctx()
) ->
    ok.
expect_lane_runs_repeatable(AtmLaneRunSelectors, IsRerunable, IsRepeatable, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_lane_run_repeatable(
        AtmLaneRunSelectors, IsRerunable, IsRepeatable, ExpState
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
        process_task_result_for_item = 'build mock spec for process_task_result_for_item step'(),
        handle_task_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = fun lane2_task1_expect_task_execution_failed/1
        },
        handle_lane_execution_stopped = 'build mock for handle_lane_execution_stopped step'(
            AtmLaneRunSelector, IsLastExpLaneRun
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

            FailedItemCount = case filter_size_measurements(ItemBatch) of
                [] ->
                    0;
                FailedItems ->
                    length(FailedItems)
            end,
            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_failed_and_processed(
                AtmTaskExecutionId, FailedItemCount, ExpState0
            ),
            {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                AtmTaskExecutionId, ItemCount - FailedItemCount, ExpState1
            )}
        end
    }.


%% @private
'build mock for handle_lane_execution_stopped step'(AtmLaneRunSelector, IsLastExpLaneRun) ->
    #atm_step_mock_spec{
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
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(
                            atm_workflow_execution_exp_state_builder:expect_lane_run_stopping(
                                AtmLaneRunSelector, atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping(
                                    AtmLaneRunSelector, interrupt, ExpState
                                )
                            )
                        )};
                    false ->
                        false
                end
            end
        },
        handle_task_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = fun lane2_task1_expect_task_execution_failed/1
        },
        handle_lane_execution_stopped = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed(AtmLaneRunSelector, ExpState)}
            end
        }
    }.


%% @private
-spec does_contain_size_measurement([json_utils:json_map()]) -> boolean().
does_contain_size_measurement(Measurements) ->
    lists:any(fun is_size_measurement/1, Measurements).


%% @private
-spec filter_size_measurements([json_utils:json_map()]) -> [json_utils:json_map()].
filter_size_measurements(Measurements) ->
    lists:filter(fun is_size_measurement/1, Measurements).


%% @private
-spec is_size_measurement(json_utils:json_map()) -> boolean().
is_size_measurement(#{<<"tsName">> := <<"size">>}) -> true;
is_size_measurement(#{<<"tsName">> := _}) -> false.


%% @private
-spec lane2_task1_expect_task_execution_failed(
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
lane2_task1_expect_task_execution_failed(#atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_failed(
        AtmTaskExecutionId, ExpState0
    ),
    {true, atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId, fun(_, _) -> <<"failed">> end, ExpState1
    )}.
