%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning crash of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_crash_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").

-export([
    crash_atm_workflow_execution_during_prepare_lane_callback/0,
    crash_atm_workflow_execution_during_resume_lane_callback/0,

    crash_atm_workflow_execution_during_run_task_for_item_callback/0,
    crash_atm_workflow_execution_during_process_task_result_for_item_callback/0,
    crash_atm_workflow_execution_during_process_streamed_task_data_callback/0,
    crash_atm_workflow_execution_during_handle_task_results_processed_for_all_items_callback/0,
    crash_atm_workflow_execution_during_handle_task_execution_stopped_callback/0,

    crash_atm_workflow_execution_during_handle_lane_execution_stopped_callback/0,

    crash_atm_workflow_execution_during_handle_workflow_execution_stopped_callback/0,

    crash_atm_workflow_execution_during_handle_exception_callback/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(ATM_TASK_SCHEMA_DRAFT(__ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = ?TARGET_STORE_SCHEMA_ID,
        store_content_update_options = #atm_list_store_content_update_options{
            function = append
        }
    }]
}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE, __RELAY_METHOD),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, lists:seq(1, 10)),
                ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{
                    id = <<"pb0">>,
                    tasks = [?ATM_TASK_SCHEMA_DRAFT(<<"task0">>)]
                }],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                },
                max_retries = 2
            }]
        },
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(
                ?ATM_NUMBER_DATA_SPEC, __RELAY_METHOD
            )}
        }
    }
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT(__RELAY_METHOD),
    ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, __RELAY_METHOD)
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT,
    ?ATM_WORKFLOW_SCHEMA_DRAFT(?RAND_ELEMENT([return_value, file_pipe]))
).

-define(PB0_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb0">>}).
-define(TASK0_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb0">>, <<"task0">>}).


%%%===================================================================
%%% Tests
%%%===================================================================


crash_atm_workflow_execution_during_prepare_lane_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, interrupted, #atm_lane_run_execution_test_spec{
        prepare_lane = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_resume_lane_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(return_value),
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [#atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_utils:pause_workflow_execution/1,
                        before_step_exp_state_diff = [
                            {task, ?TASK0_SELECTOR({1, 1}), stopping},
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        after_step_exp_state_diff = [{task, ?TASK0_SELECTOR({1, 1}), paused}]
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, paused}]
                    }
                }],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [workflow_paused]
                },
                after_hook = fun atm_workflow_execution_test_utils:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = [
                                {lane_run, {1, 1}, resuming},
                                workflow_resuming
                            ],
                            strategy = {yield, {error, crashed}},
                            after_step_exp_state_diff = no_diff
                        },

                        % this is called as part of `handle_workflow_abruptly_stopped`
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = [{lane_run, {1, 1}, crashed}]
                        }
                    }
                ],
                handle_exception = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        % All paused tasks should be immediately transitioned to interrupted status omitting stopping status
                        {all_tasks, {1, 1}, interrupted},
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ]
                },
                handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [workflow_crashed]
                },
                after_hook = fun assert_no_action_possible_on_crashed_atm_workflow_execution/1
            }
        ]
    }).


crash_atm_workflow_execution_during_run_task_for_item_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, interrupted, #atm_lane_run_execution_test_spec{
        run_task_for_item = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_process_task_result_for_item_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, failed, #atm_lane_run_execution_test_spec{
        process_task_result_for_item = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_process_streamed_task_data_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, failed, #atm_lane_run_execution_test_spec{
        process_streamed_task_data = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_task_results_processed_for_all_items_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, failed, #atm_lane_run_execution_test_spec{
        handle_task_results_processed_for_all_items = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_task_execution_stopped_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, failed, #atm_lane_run_execution_test_spec{
        handle_task_execution_stopped = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_lane_execution_stopped_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    strategy = fun(_AtmMockCallCtx) ->
                        Key = {?FUNCTION_NAME, crashed},

                        case get({?FUNCTION_NAME, crashed}) of
                            undefined ->
                                put(Key, true),
                                {yield, {error, crashed}};
                            true ->
                                passthrough
                        end
                    end,
                    after_step_exp_state_diff = no_diff
                }
            }],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {1, 1}, stopping},
                    workflow_stopping
                ]
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {1, 1}, crashed},
                    workflow_crashed
                ]
            },
            after_hook = fun assert_no_action_possible_on_crashed_atm_workflow_execution/1
        }]
    }).


crash_atm_workflow_execution_during_handle_workflow_execution_stopped_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1}
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                strategy = {yield, {error, crashed}},
                after_step_exp_state_diff = no_diff
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_crashed]
            },
            after_hook = fun assert_no_action_possible_on_crashed_atm_workflow_execution/1
        }]
    }).


crash_atm_workflow_execution_during_handle_exception_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                process_task_result_for_item = #atm_step_mock_spec{
                    strategy = {yield, {error, crashed}},
                    after_step_exp_state_diff = no_diff
                }
            }],
            handle_exception = #atm_step_mock_spec{
                strategy = {yield, {error, crashed}},
                after_step_exp_state_diff = no_diff
            },
            % Atm workflow execution should at least transition to crashed status even if
            % not all atm workflow execution components could be properly stopped
            % (they may be left e.g. active)
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_crashed]
            },
            after_hook = fun assert_no_action_possible_on_crashed_atm_workflow_execution/1
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
crash_atm_workflow_execution_test_base(Testcase, ExpTasksFinalStatus, CrashingAtmLaneRunExecutionTestSpec) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(Testcase, file_pipe),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [CrashingAtmLaneRunExecutionTestSpec#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                % this is called as part of `handle_workflow_abruptly_stopped`
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [
                        {all_tasks, {1, 1}, abruptly, ExpTasksFinalStatus},
                        {lane_run, {1, 1}, crashed}
                    ]
                }
            }],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {all_tasks, {1, 1}, stopping_due_to, interrupt},
                    {lane_run, {1, 1}, stopping},
                    workflow_stopping
                ]
            },
            handle_workflow_abruptly_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_crashed]
            },
            after_hook = fun assert_no_action_possible_on_crashed_atm_workflow_execution/1
        }]
    }).


%% @private
assert_no_action_possible_on_crashed_atm_workflow_execution(AtmMockCallCtx) ->
    atm_workflow_execution_test_utils:assert_impossible_actions_are_declined_for_ended_workflow_execution(
        AtmMockCallCtx
    ),

    lists:foreach(fun({RepeatType, ExpError}) ->
        ?assertThrow(ExpError, atm_workflow_execution_test_utils:repeat_workflow_execution(
            RepeatType, {1, 1}, AtmMockCallCtx
        ))
    end, [
        {retry, ?ERROR_ATM_LANE_EXECUTION_RETRY_FAILED},
        {rerun, ?ERROR_ATM_LANE_EXECUTION_RERUN_FAILED}
    ]).
