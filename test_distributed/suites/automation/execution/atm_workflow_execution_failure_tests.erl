%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning failure of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_failure_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error/0,

    fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error/0,
    fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error/0,
    fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error/0,

    fail_atm_workflow_execution_due_to_job_timeout/0,
    fail_atm_workflow_execution_due_to_job_result_store_mapping_error/0,
    fail_atm_workflow_execution_due_to_job_missing_required_results_error/0,
    fail_atm_workflow_execution_due_to_incorrect_result_type_error/0,
    fail_atm_workflow_execution_due_to_lambda_item_exception/0,
    fail_atm_workflow_execution_due_to_lambda_batch_exception/0
]).


-define(SV_STORE_SCHEMA_ID, <<"single_store_id">>).
-define(ITERATED_STORE_SCHEMA_ID, <<"store_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(TARGET_STORE_RESULT_MAPPER(__DISPATCH_RULES), #atm_task_schema_result_mapper{
    result_name = ?ECHO_ARG_NAME,
    store_schema_id = ?TARGET_STORE_SCHEMA_ID,
    store_content_update_options = #atm_time_series_store_content_update_options{
        dispatch_rules = __DISPATCH_RULES
    }
}).

-define(FAILING_LAMBDA_ID, <<"failing_Lambda_id">>).
-define(FAILING_LAMBDA_REVISION_NUM, 1).

-define(FAILING_TASK_SCHEMA_DRAFT(__ARGUMENT_MAPPING, __RESULT_MAPPINGS), #atm_task_schema_draft{
    id = <<"task1">>,
    lambda_id = ?FAILING_LAMBDA_ID,
    lambda_revision_number = ?FAILING_LAMBDA_REVISION_NUM,
    argument_mappings = __ARGUMENT_MAPPING,
    result_mappings = __RESULT_MAPPINGS
}).

-define(ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(__ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TS_DISPATCH_RULES)]
}).

-define(FAILING_WORKFLOW_SCHEMA_DRAFT(
    __TESTCASE,
    __ITERATED_CONTENT,
    __FAILING_TASK_SCHEMA_DRAFT,
    __FAILING_LAMBDA_DRAFT
),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?ATM_SV_STORE_SCHEMA_DRAFT(?SV_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, undefined),
                ?ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ANY_MEASUREMENT_DATA_SPEC, __ITERATED_CONTENT),
                ?ATM_TS_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            __FAILING_TASK_SCHEMA_DRAFT,
                            ?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task2">>)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = <<"pb2">>,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(<<"task3">>)]
                    }
                ],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                    max_batch_size = ?RAND_INT(5, 8)
                },
                max_retries = 2
            }]
        },
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC)
            },
            ?FAILING_LAMBDA_ID => #{
                ?FAILING_LAMBDA_REVISION_NUM => __FAILING_LAMBDA_DRAFT
            }
        }
    }
).

-define(JOB_FAILING_DUE_TO_ARG_MAPPING_WORKFLOW_SCHEMA_DRAFT(__FAILING_ARG_TASK_MAPPER),
    ?FAILING_WORKFLOW_SCHEMA_DRAFT(
        ?FUNCTION_NAME,
        gen_time_series_measurements(),
        ?FAILING_TASK_SCHEMA_DRAFT(
            [__FAILING_ARG_TASK_MAPPER],
            [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TS_DISPATCH_RULES)]
        ),
        ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC)
    )
).

-define(JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(__FAILING_DOCKER_IMAGE_ID),
    ?FAILING_WORKFLOW_SCHEMA_DRAFT(
        ?FUNCTION_NAME,
        gen_time_series_measurements(),
        ?FAILING_TASK_SCHEMA_DRAFT(
            [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
            [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TS_DISPATCH_RULES)]
        ),
        #atm_lambda_revision_draft{
            operation_spec = #atm_openfaas_operation_spec_draft{
                docker_image = __FAILING_DOCKER_IMAGE_ID
            },
            argument_specs = [#atm_parameter_spec{
                name = ?ECHO_ARG_NAME,
                data_spec = ?ANY_MEASUREMENT_DATA_SPEC,
                is_optional = false
            }],
            result_specs = [#atm_lambda_result_spec{
                name = ?ECHO_ARG_NAME,
                data_spec = ?ANY_MEASUREMENT_DATA_SPEC,
                relay_method = return_value
            }]
        }
    )
).

-define(MISSING_TS_NAME_GENERATOR, <<"missing_generator">>).
-define(FAILING_MEASUREMENT_STORE_MAPPING_TASK_SCHEMA_DRAFT, ?FAILING_TASK_SCHEMA_DRAFT(
    [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    [?TARGET_STORE_RESULT_MAPPER([
        #atm_time_series_dispatch_rule{
            measurement_ts_name_matcher_type = exact,
            measurement_ts_name_matcher = <<"size">>,
            target_ts_name_generator = ?MISSING_TS_NAME_GENERATOR,
            prefix_combiner = overwrite
        }
        | ?CORRECT_ATM_TS_DISPATCH_RULES
    ])]
)).

-define(EXP_ERROR_ATM_MEASUREMENT_DISPATCH_FAILED, ?ERROR_ATM_TASK_RESULT_MAPPING_FAILED(
    <<"value">>, ?ERROR_ATM_TASK_RESULT_DISPATCH_FAILED(
        ?TARGET_STORE_SCHEMA_ID,
        ?ERROR_BAD_DATA(<<"dispatchRules">>, str_utils:format_bin(
            "Time series name generator '~s' specified in one of the dispatch rules "
            "does not reference any defined time series schema",
            [?MISSING_TS_NAME_GENERATOR]
        ))
    )
)).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).
-define(TASK3_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>, <<"task3">>}).

-define(PB2_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb2">>}).


-record(fail_atm_workflow_execution_test_spec, {
    testcase_id :: term(),
    job_failure_type :: job_failure_type(),
    atm_workflow_schema_draft :: atm_test_schema_factory:atm_workflow_schema_dump_draft(),
    should_item_processing_fail_fun = fun(_) -> true end :: fun((automation:item()) -> boolean()),
    build_task1_audit_log_exp_content_fun :: fun(([automation:item()]) -> automation:item())
}).
-type fail_atm_workflow_execution_test_spec() :: #fail_atm_workflow_execution_test_spec{}.

-type job_failure_type() :: arg_error | result_error.

-define(NOW_SEC(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            gen_time_series_measurements(),
            ?FAILING_MEASUREMENT_STORE_MAPPING_TASK_SCHEMA_DRAFT,
            ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, file_pipe)
        ),
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    process_streamed_task_data = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_, _, _, {chunk, #{?ECHO_ARG_NAME := UncorrelatedResults}}]
                        }) ->
                            case contains_size_measurements(UncorrelatedResults) of
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
                        after_step_hook = atm_workflow_execution_test_utils:build_task_step_hook(#{
                            <<"task1">> => fun(AtmMockCallCtx = #atm_mock_call_ctx{call_args = [_, _, AtmTaskExecutionId]}) ->
                                ExpTaskLog = #{
                                    <<"description">> => <<"Failed to process uncorrelated task results.">>,
                                    <<"reason">> => errors:to_json(?EXP_ERROR_ATM_MEASUREMENT_DISPATCH_FAILED)
                                },
                                ?assert(lists:member(ExpTaskLog, get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx)))
                            end
                        }),
                        after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                            <<"task1">> => [{task, ?TASK_ID_PLACEHOLDER, failed}],
                            <<"task2">> => [{task, ?TASK_ID_PLACEHOLDER, interrupted}],
                            <<"task3">> => [{task, ?TASK_ID_PLACEHOLDER, interrupted}]
                        })
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error() ->
    IncorrectConst = 10,

    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = arg_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_ARG_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            #atm_task_schema_argument_mapper{
                argument_name = ?ECHO_ARG_NAME,
                value_builder = #atm_task_argument_value_builder{
                    type = const,
                    recipe = IncorrectConst
                }
            }
        ),
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(
                    ?ECHO_ARG_NAME, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(
                        IncorrectConst, atm_time_series_measurement_type
                    )
                ))
            }
        end
    }).


fail_atm_workflow_execution_due_to_incorrect_iterated_item_query_arg_error() ->
    IteratedItemQuery = [<<"NonexistendField">>],

    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = arg_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_ARG_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            #atm_task_schema_argument_mapper{
                argument_name = ?ECHO_ARG_NAME,
                value_builder = #atm_task_argument_value_builder{
                    type = iterated_item,
                    recipe = IteratedItemQuery
                }
            }
        ),
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(
                    ?ECHO_ARG_NAME, ?ERROR_ATM_TASK_ARG_MAPPER_ITERATED_ITEM_QUERY_FAILED(
                        hd(ItemBatch), IteratedItemQuery
                    )
                ))
            }
        end
    }).


fail_atm_workflow_execution_due_to_empty_single_value_store_arg_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = arg_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_ARG_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            #atm_task_schema_argument_mapper{
                argument_name = ?ECHO_ARG_NAME,
                value_builder = #atm_task_argument_value_builder{
                    type = single_value_store_content,
                    recipe = ?SV_STORE_SCHEMA_ID
                }
            }
        ),
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(
                    ?ECHO_ARG_NAME, ?ERROR_ATM_STORE_CONTENT_NOT_SET(
                        ?SV_STORE_SCHEMA_ID
                    )
                ))
            }
        end
    }).


fail_atm_workflow_execution_due_to_job_timeout() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?ECHO_WITH_SLEEP_DOCKER_IMAGE_ID
        ),

        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_TIMEOUT)
            }
        end
    }).


fail_atm_workflow_execution_due_to_job_result_store_mapping_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            gen_time_series_measurements(),
            ?FAILING_MEASUREMENT_STORE_MAPPING_TASK_SCHEMA_DRAFT,
            ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC)
        ),
        should_item_processing_fail_fun = fun is_size_measurement/1,
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Failed to process item.">>,
                    <<"item">> => Item,
                    <<"reason">> => errors:to_json(?EXP_ERROR_ATM_MEASUREMENT_DISPATCH_FAILED)
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_job_missing_required_results_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1
        ),
        should_item_processing_fail_fun = fun is_size_measurement/1,
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Failed to process item.">>,
                    <<"item">> => Item,
                    <<"reason">> => errors:to_json(?ERROR_ATM_TASK_RESULT_MISSING(
                        <<"value">>, [<<"schrodinger_cat">>, <<"schrodinger_dog">>]
                    ))
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_incorrect_result_type_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2
        ),
        should_item_processing_fail_fun = fun is_size_measurement/1,
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Failed to process item.">>,
                    <<"item">> => Item,
                    <<"reason">> => errors:to_json(?ERROR_ATM_TASK_RESULT_MAPPING_FAILED(
                        <<"value">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(
                            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2_RET_VALUE,
                            atm_time_series_measurement_type
                        )
                    ))
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_lambda_item_exception() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3
        ),
        should_item_processing_fail_fun = fun is_size_measurement/1,
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Lambda exception occurred during item processing.">>,
                    <<"item">> => Item,
                    <<"reason">> => ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3_EXCEPTION
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_lambda_batch_exception() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        job_failure_type = result_error,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4
        ),
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4_ERROR_MSG
            }
        end
    }).


%% @private
-spec job_failure_atm_workflow_execution_test_base(fail_atm_workflow_execution_test_spec()) ->
    ok.
job_failure_atm_workflow_execution_test_base(JobFailureTestSpec = #fail_atm_workflow_execution_test_spec{
    atm_workflow_schema_draft = AtmWorkflowSchemaDraft
}) ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        test_gc = false,
        workflow_schema_dump_or_draft = AtmWorkflowSchemaDraft,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                build_job_failure_lane_run_test_spec({1, 1}, false, JobFailureTestSpec),
                build_job_failure_lane_run_test_spec({1, 2}, false, JobFailureTestSpec),
                build_job_failure_lane_run_test_spec({1, 3}, true, JobFailureTestSpec)
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}, {1, 2}, {1, 3}], rerunable},
                    {lane_runs, [{1, 1}, {1, 2}, {1, 3}], retriable},
                    workflow_failed
                ]
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


%% @private
build_job_failure_lane_run_test_spec(AtmLaneRunSelector, IsLastExpLaneRun, #fail_atm_workflow_execution_test_spec{
    testcase_id = TestcaseId,
    job_failure_type = JobFailureType,
    should_item_processing_fail_fun = ShouldItemProcessingFailFun,
    build_task1_audit_log_exp_content_fun = BuildTask1AuditLogExpContentFun
}) ->
    InferFailedItemCountFun = fun(ItemBatch) ->
        record_items_processed(TestcaseId, AtmLaneRunSelector, ItemBatch),

        case lists:filter(ShouldItemProcessingFailFun, ItemBatch) of
            [] ->
                % Batch can be processed further (by following parallel boxes)
                % only if no item from batch fails (limitation of workflow engine)
                inc_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector, length(ItemBatch)),
                0;
            FailedItems ->
                record_failed_item_batch_for_task1(TestcaseId, AtmLaneRunSelector, FailedItems),
                update_exp_exception_store_content(TestcaseId, AtmLaneRunSelector, ItemBatch),
                length(FailedItems)
        end
    end,

    #atm_lane_run_execution_test_spec{
        selector = AtmLaneRunSelector,

        run_task_for_item = #atm_step_mock_spec{after_step_exp_state_diff = case JobFailureType of
            arg_error ->
                fun(#atm_mock_call_ctx{
                    workflow_execution_exp_state = ExpState,
                    call_args = [_, _, AtmTaskExecutionId, _, ItemBatch]
                }) ->
                    ItemCount = length(ItemBatch),
                    Expectations = case atm_workflow_execution_exp_state_builder:get_task_schema_id(
                        AtmTaskExecutionId, ExpState
                    ) of
                        <<"task1">> ->
                            FailedItemCount = InferFailedItemCountFun(ItemBatch),

                            [
                                {task, AtmTaskExecutionId, items_scheduled, ItemCount},
                                {task, AtmTaskExecutionId, items_failed, FailedItemCount},
                                {task, AtmTaskExecutionId, items_finished, ItemCount - FailedItemCount}
                            ];
                        _ ->
                            [{task, AtmTaskExecutionId, items_scheduled, ItemCount}]
                    end,
                    {true, atm_workflow_execution_exp_state_builder:expect(ExpState, Expectations)}
                end;
            result_error ->
                default
        end},

        process_task_result_for_item = #atm_step_mock_spec{
            after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                <<"task1">> => case JobFailureType of
                    arg_error ->
                        no_diff;
                    result_error ->
                        fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState,
                            call_args = [_, _, AtmTaskExecutionId, ItemBatch, _]
                        }) ->
                            ItemCount = length(ItemBatch),
                            FailedItemCount = InferFailedItemCountFun(ItemBatch),

                            {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                                {task, AtmTaskExecutionId, items_failed, FailedItemCount},
                                {task, AtmTaskExecutionId, items_finished, ItemCount - FailedItemCount}
                            ])}
                        end
                end,

                [<<"task2">>, <<"task3">>] => fun(#atm_mock_call_ctx{
                    workflow_execution_exp_state = ExpState,
                    call_args = [_, _, AtmTaskExecutionId, ItemBatch, _]
                }) ->
                    {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                        {task, AtmTaskExecutionId, items_finished, length(ItemBatch)}
                    ])}
                end
            })
        },

        handle_task_execution_stopped = #atm_step_mock_spec{
            after_step_hook = atm_workflow_execution_test_utils:build_task_step_hook(#{
                <<"task1">> => fun(AtmMockCallCtx = #atm_mock_call_ctx{call_args = [_, _, AtmTaskExecutionId]}) ->
                    ExpLogContents = lists:sort(lists:flatten(lists:map(
                        BuildTask1AuditLogExpContentFun,
                        get_all_failed_item_batches_for_task1(TestcaseId, AtmLaneRunSelector)
                    ))),
                    ?assertEqual(ExpLogContents, lists:sort(get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx)))
                end,
                <<"task3">> => fun(#atm_mock_call_ctx{
                    workflow_execution_exp_state = ExpState,
                    call_args = [_, _, AtmTaskExecutionId]
                }) ->
                    ?assertEqual(
                        {0, 0, get_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector)},
                        atm_workflow_execution_exp_state_builder:get_task_stats(AtmTaskExecutionId, ExpState)
                    )
                end
            }),
            after_step_exp_state_diff = atm_workflow_execution_test_utils:build_task_step_exp_state_diff(#{
                <<"task1">> => [{task, ?TASK_ID_PLACEHOLDER, failed}],
                <<"task2">> => [{task, ?TASK_ID_PLACEHOLDER, finished}],
                <<"task3">> => fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                    ExpStatus = case get_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector) of
                        0 -> skipped;
                        _ -> finished
                    end,
                    {true, atm_workflow_execution_exp_state_builder:expect(ExpState, [
                        {task, ?TASK3_SELECTOR(AtmLaneRunSelector), ExpStatus}
                    ])}
                end
            })
        },

        handle_lane_execution_stopped = #atm_step_mock_spec{
            after_step_hook = fun(AtmMockCallCtx) ->
                % Assert all items from prev lane run exception store were processed
                check_iterated_items(TestcaseId, AtmLaneRunSelector, AtmMockCallCtx),
                check_exception_store_content(TestcaseId, AtmLaneRunSelector, AtmMockCallCtx)
            end,
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
-spec get_audit_log_contents(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    [automation:item()].
get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx) ->
    #{<<"logEntries">> := Logs, <<"isLast">> := true} = atm_workflow_execution_test_utils:browse_store(
        ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"content">> := LogContent}) -> LogContent end, Logs).


%% @private
-spec check_iterated_items(
    term(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok | no_return().
check_iterated_items(TestcaseId, {1, 1} = AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_utils:browse_store(
        ?ITERATED_STORE_SCHEMA_ID, undefined, AtmMockCallCtx
    ),
    SrcStoreContent = lists:sort(lists:map(fun(#{<<"value">> := Value}) -> Value end, Items)),

    ?assertEqual(
        SrcStoreContent,
        lists:sort(get_all_processed_items(TestcaseId, AtmLaneRunSelector))
    );

check_iterated_items(TestcaseId, {AtmLaneSelector, RunNum} = AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertEqual(
        lists:sort(get_all_processed_items(TestcaseId, AtmLaneRunSelector)),
        lists:sort(get_exception_store_content({AtmLaneSelector, RunNum - 1}, AtmMockCallCtx))
    ).


%% @private
-spec check_exception_store_content(
    term(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok | no_return().
check_exception_store_content(TestcaseId, AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertEqual(
        lists:sort(get_exp_exception_store_content(TestcaseId, AtmLaneRunSelector)),
        lists:sort(get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx))
    ).


%% @private
-spec get_exception_store_content(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    [automation:item()].
get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_utils:browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"value">> := Content}) -> Content end, Items).


%% @private
-spec record_failed_item_batch_for_task1(
    term(),
    atm_lane_execution:lane_run_selector(),
    [automation:item()]
) ->
    ok.
record_failed_item_batch_for_task1(TestcaseId, AtmLaneRunSelector, FailedItemBatch) ->
    Key = {TestcaseId, ?TASK1_SELECTOR(AtmLaneRunSelector), failed_item_batch},
    node_cache:put(Key, [FailedItemBatch | node_cache:get(Key, [])]).


%% @private
-spec get_all_failed_item_batches_for_task1(term(), atm_lane_execution:lane_run_selector()) ->
    ok.
get_all_failed_item_batches_for_task1(TestcaseId, AtmLaneRunSelector) ->
    Key = {TestcaseId, ?TASK1_SELECTOR(AtmLaneRunSelector), failed_item_batch},
    node_cache:get(Key, []).


%% @private
-spec record_items_processed(term(), atm_lane_execution:lane_run_selector(), [automation:item()]) ->
    ok.
record_items_processed(TestcaseId, AtmLaneRunSelector, ItemsProcessed) ->
    Key = {TestcaseId, AtmLaneRunSelector, items_processed},
    node_cache:put(Key, ItemsProcessed ++ node_cache:get(Key, [])).


%% @private
-spec get_all_processed_items(term(), atm_lane_execution:lane_run_selector()) ->
    [automation:id()].
get_all_processed_items(TestcaseId, AtmLaneRunSelector) ->
    Key = {TestcaseId, AtmLaneRunSelector, items_processed},
    node_cache:get(Key, []).


%% @private
-spec update_exp_exception_store_content(
    term(),
    atm_lane_execution:lane_run_selector(),
    [automation:item()]
) ->
    ok.
update_exp_exception_store_content(TestcaseId, AtmLaneRunSelector, FailedItemBatch) ->
    Key = {TestcaseId, AtmLaneRunSelector, exp_exception_store},
    node_cache:put(Key, FailedItemBatch ++ node_cache:get(Key, [])).


%% @private
-spec get_exp_exception_store_content(term(), atm_lane_execution:lane_run_selector()) ->
    ok.
get_exp_exception_store_content(TestcaseId, AtmLaneRunSelector) ->
    Key = {TestcaseId, AtmLaneRunSelector, exp_exception_store},
    node_cache:get(Key, []).


%% @private
-spec inc_exp_items_processed_by_task3(term(), atm_lane_execution:lane_run_selector(), pos_integer()) ->
    ok.
inc_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector, Count) ->
    Key = {TestcaseId, ?TASK3_SELECTOR(AtmLaneRunSelector), exp_items_processed},
    node_cache:put(Key, Count + node_cache:get(Key, 0)).


%% @private
-spec get_exp_items_processed_by_task3(term(), atm_lane_execution:lane_run_selector()) ->
    pos_integer().
get_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector) ->
    node_cache:get({TestcaseId, ?TASK3_SELECTOR(AtmLaneRunSelector), exp_items_processed}, 0).


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
-spec contains_size_measurements([automation:item()]) -> boolean().
contains_size_measurements(Measurements) ->
    lists:any(fun is_size_measurement/1, Measurements).


%% @private
-spec is_size_measurement(automation:item()) -> boolean().
is_size_measurement(#{<<"tsName">> := <<"size">>}) -> true;
is_size_measurement(#{<<"tsName">> := _}) -> false.
