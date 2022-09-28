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
    stopping_reason_cancel_overrides_failure/0
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
                            {true, expect_execution_stopping(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure failure prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        after_step_exp_state_diff = build_task_stopped_after_step_diff(fun expect_task_failed/2)
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({2, 1}, ExpState1),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState2)}
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
            return_value, gen_time_series_measurements(), ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
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
                            {true, expect_execution_stopping(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure cancel prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        after_step_exp_state_diff = build_task_stopped_after_step_diff(fun expect_task_cancelled/2)
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({2, 1}, ExpState1),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState2)}
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
                            {true, expect_execution_stopping(ExpState0)}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_exp_state_diff = build_task_stopped_after_step_diff(fun expect_task_cancelled/2)
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
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({2, 1}, ExpState1),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState2)}
                end
            }
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
gen_time_series_measurements() ->
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
expect_task_failed(AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        fun(_, _) -> <<"failed">> end,
        atm_workflow_execution_exp_state_builder:expect_task_failed(AtmTaskExecutionId, ExpState0)
    ).


%% @private
expect_task_cancelled(AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        fun(_, _) -> <<"cancelled">> end,
        atm_workflow_execution_exp_state_builder:expect_task_cancelled(AtmTaskExecutionId, ExpState0)
    ).


%% @private
expect_task_skipped(AtmTaskExecutionId, ExpState0) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId,
        fun(_, _) -> <<"skipped">> end,
        atm_workflow_execution_exp_state_builder:expect_task_skipped(AtmTaskExecutionId, ExpState0)
    ).


%% @private
build_task_stopped_after_step_diff(ExpectTask1FinalStatusFun) ->
    fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }) ->
        case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
            {_, <<"pb1">>, <<"task1">>} ->
                {true, ExpectTask1FinalStatusFun(AtmTaskExecutionId, ExpState0)};
            {_, <<"pb2">>, <<"task2">>} ->
                % execution is paused after first items for task1 finished processing
                % and as such no item was ever scheduled for task2
                {true, expect_task_skipped(AtmTaskExecutionId, ExpState0)}
        end
    end.


%% @private
expect_execution_stopping(ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({2, 1}, ExpState0),
    ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
    atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2).
