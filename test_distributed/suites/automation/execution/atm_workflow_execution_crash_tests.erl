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
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

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


-define(INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).

-define(LAST_ITEM, 20).

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
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, lists:seq(1, ?LAST_ITEM)),
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                    ?ATM_TASK_SCHEMA_DRAFT(<<"task0">>)
                ]}],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID
                },
                max_retries = 2
            }]
        },
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?INTEGER_DATA_SPEC, __RELAY_METHOD)
            }
        }
    }
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT(__RELAY_METHOD),
    ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME, __RELAY_METHOD)
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT,
    ?ATM_WORKFLOW_SCHEMA_DRAFT(?RAND_ELEMENT([return_value, file_pipe]))
).


%%%===================================================================
%%% Tests
%%%===================================================================


crash_atm_workflow_execution_during_prepare_lane_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        prepare_lane = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_resume_lane_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(return_value),
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [#atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            {true, expect_execution_stopping_while_processing_lane1(ExpState0, pause)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            {true, expect_lane1_pb_paused(AtmTaskExecutionId, ExpState0)}
                        end
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState)}
                        end
                    }
                }],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                    end
                },
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        resume_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end,
                            strategy = {yield, {error, crashed}},
                            after_step_exp_state_diff = no_diff
                        },

                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            % this is called as part of `handle_workflow_interrupted`
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                {true, atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({1, 1}, ExpState0)}
                            end
                        }
                    }
                ],
                handle_exception = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        % All paused tasks should be immediately transitioned to interrupted status omitting stopping status
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_interrupted({1, 1}, ExpState0),
                        ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2)}
                    end
                },
                handle_workflow_interrupted = atm_workflow_crashed_after_step_mock_spec()
            }
        ]
    }).


crash_atm_workflow_execution_during_run_task_for_item_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        run_task_for_item = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_process_task_result_for_item_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        process_task_result_for_item = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_process_streamed_task_data_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        process_streamed_task_data = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_task_results_processed_for_all_items_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        handle_task_results_processed_for_all_items = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_task_execution_stopped_callback() ->
    crash_atm_workflow_execution_test_base(?FUNCTION_NAME, #atm_lane_run_execution_test_spec{
        handle_task_execution_stopped = #atm_step_mock_spec{
            strategy = {yield, {error, crashed}},
            after_step_exp_state_diff = no_diff
        }
    }).


crash_atm_workflow_execution_during_handle_lane_execution_stopped_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
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
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                end
            },
            handle_workflow_interrupted = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_crashed(ExpState1)}
                end
            }
        }]
    }).


crash_atm_workflow_execution_during_handle_workflow_execution_stopped_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1}
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                strategy = {yield, {error, crashed}},
                after_step_exp_state_diff = no_diff
            },
            handle_workflow_interrupted = atm_workflow_crashed_after_step_mock_spec()
        }]
    }).


crash_atm_workflow_execution_during_handle_exception_callback() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
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
            handle_workflow_interrupted = atm_workflow_crashed_after_step_mock_spec()
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
crash_atm_workflow_execution_test_base(Testcase, CrashingAtmLaneRunExecutionTestSpec) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT(Testcase, file_pipe),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [CrashingAtmLaneRunExecutionTestSpec#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    % this is called as part of `handle_workflow_interrupted`
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_abruptly_interrupted(
                            {1, 1}, ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_crashed({1, 1}, ExpState1)}
                    end
                }
            }],
            handle_exception = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, expect_execution_stopping_while_processing_lane1(ExpState0, interrupt)}
                end
            },
            handle_workflow_interrupted = atm_workflow_crashed_after_step_mock_spec()
        }]
    }).


%% @private
atm_workflow_crashed_after_step_mock_spec() ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_crashed(ExpState0)}
        end
    }.


%% @private
expect_execution_stopping_while_processing_lane1(ExpState0, Reason) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({1, 1}, Reason, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).


%% @private
expect_lane1_pb_paused(AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        % lane2 parallel boxes have only single task and as such their final status
        % will be the same as that of their respective task
        fun(_, _) -> <<"paused">> end,
        atm_workflow_execution_exp_state_builder:expect_task_paused(AtmTaskExecutionId, ExpState0)
    ).
