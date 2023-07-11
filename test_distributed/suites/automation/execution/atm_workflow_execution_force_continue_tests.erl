%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of force continuing automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_force_continue_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").

-export([
    force_continue_failed_iterated_atm_lane_run_execution/0,
    force_continue_failed_while_preparing_atm_lane_run_execution/0,
    force_continue_failed_not_iterated_atm_lane_run_execution/0
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
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(?FUNCTION_NAME),
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
                    max_retries = 0
                },
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(
                            <<"task2">>,
                            ?CORRECT_ATM_TS_DISPATCH_RULES
                        )]
                    }],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                    },
                    max_retries = 0
                }
            ]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, __RELAY_METHOD)
        }}
    }
).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).

-define(LANE_RUN_2_1_TEST_SPEC, #atm_lane_run_execution_test_spec{
    selector = {2, 1},
    prepare_lane = #atm_step_mock_spec{
        before_step_exp_state_diff = [
            {lane_runs, [{1, 1}], retriable, false},
            {lane_runs, [{1, 1}], rerunable, false},
            {lane_run, {2, 1}, scheduled},
            workflow_resuming
        ]
    }
}).


-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


force_continue_failed_iterated_atm_lane_run_execution() ->
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

                        process_task_result_for_item = #atm_step_mock_spec{
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                                workflow_execution_exp_state = ExpState,
                                call_args = [_, _, AtmTaskExecutionId, ItemBatch, _]
                            }) ->
                                ItemCount = length(ItemBatch),
                                FailedItemCount = case filter_size_measurements(ItemBatch) of
                                    [] -> 0;
                                    FailedItems -> length(FailedItems)
                                end,

                                {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                                    {task, AtmTaskExecutionId, items_failed, FailedItemCount},
                                    {task, AtmTaskExecutionId, items_finished, ItemCount - FailedItemCount}
                                ])}
                            end
                        },

                        handle_task_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{task, ?TASK1_SELECTOR({1, 1}), failed}]
                        },

                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [
                                {lane_run, {1, 1}, failed},
                                workflow_stopping
                            ]
                        }
                    },
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_lane_execution_stopped, after_step, {1, 1}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = lists:flatten([
                        {lane_runs, [{1, 1}], rerunable},
                        {lane_runs, [{1, 1}], retriable},
                        workflow_failed
                    ])
                },
                after_hook = fun atm_workflow_execution_test_utils:force_continue_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [?LANE_RUN_2_1_TEST_SPEC],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}], retriable},
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_finished
                    ]
                }
            }
        ]
    }).


force_continue_failed_while_preparing_atm_lane_run_execution() ->
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
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}], rerunable},
                        workflow_failed
                    ]
                },
                after_hook = fun atm_workflow_execution_test_utils:force_continue_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [?LANE_RUN_2_1_TEST_SPEC],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_finished
                    ]
                }
            }
        ]
    }).


force_continue_failed_not_iterated_atm_lane_run_execution() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(), file_pipe
        ),
        test_gc = false,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
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
                                            {all_tasks, {1, 1}, stopping_due_to, interrupt},
                                            {lane_run, {1, 1}, stopping},
                                            workflow_stopping
                                        ])};
                                    false ->
                                        false
                                end
                            end
                        },
                        handle_task_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{task, ?TASK1_SELECTOR({1, 1}), failed}]
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                        }
                    },
                    ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {handle_lane_execution_stopped, after_step, {1, 1}})
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}], rerunable},
                        workflow_failed
                    ]
                },
                after_hook = fun atm_workflow_execution_test_utils:force_continue_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [?LANE_RUN_2_1_TEST_SPEC],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {lane_runs, [{1, 1}, {2, 1}], rerunable},
                        workflow_finished
                    ]
                }
            }
        ]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
