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


-define(SINGLE_VALUE_STORE_SCHEMA_ID, <<"single_store_id">>).
-define(ITERATED_STORE_SCHEMA_ID, <<"store_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(TARGET_STORE_RESULT_MAPPER(__DISPATCH_RULES), #atm_task_schema_result_mapper{
    result_name = ?ECHO_ARG_NAME,
    store_schema_id = ?TARGET_STORE_SCHEMA_ID,
    store_content_update_options = #atm_time_series_store_content_update_options{
        dispatch_rules = __DISPATCH_RULES
    }
}).

-define(ATM_TASK1_SCHEMA_ID, <<"t1">>).
-define(ATM_TASK2_SCHEMA_ID, <<"t2">>).
-define(ATM_TASK3_SCHEMA_ID, <<"t3">>).

-define(ATM_PARALLEL_BOX1_SCHEMA_ID, <<"pb1">>).
-define(ATM_PARALLEL_BOX2_SCHEMA_ID, <<"pb2">>).

-define(FAILING_LAMBDA_ID, <<"failing_Lambda_id">>).
-define(FAILING_LAMBDA_REVISION_NUM, 1).

-define(FAILING_TASK_SCHEMA_DRAFT(__ARGUMENT_MAPPING, __RESULT_MAPPINGS), #atm_task_schema_draft{
    id = ?ATM_TASK1_SCHEMA_ID,
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
    result_mappings = [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)]
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
                #atm_store_schema_draft{
                    id = ?SINGLE_VALUE_STORE_SCHEMA_ID,
                    type = single_value,
                    config = #atm_single_value_store_config{item_data_spec = ?ANY_MEASUREMENT_DATA_SPEC},
                    requires_initial_content = false
                },
                #atm_store_schema_draft{
                    id = ?ITERATED_STORE_SCHEMA_ID,
                    type = list,
                    config = #atm_list_store_config{item_data_spec = ?ANY_MEASUREMENT_DATA_SPEC},
                    requires_initial_content = false,
                    default_initial_content = __ITERATED_CONTENT
                },
                #atm_store_schema_draft{
                    id = ?TARGET_STORE_SCHEMA_ID,
                    type = time_series,
                    config = #atm_time_series_store_config{
                        time_series_collection_schema = #time_series_collection_schema{
                            time_series_schemas = [
                                ?MAX_FILE_SIZE_TS_SCHEMA,
                                ?COUNT_TS_SCHEMA
                            ]
                        }
                    },
                    requires_initial_content = false,
                    default_initial_content = __ITERATED_CONTENT
                }
            ],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [
                    #atm_parallel_box_schema_draft{
                        id = ?ATM_PARALLEL_BOX1_SCHEMA_ID,
                        tasks = [
                            __FAILING_TASK_SCHEMA_DRAFT,
                            ?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(?ATM_TASK2_SCHEMA_ID)
                        ]
                    },
                    #atm_parallel_box_schema_draft{
                        id = ?ATM_PARALLEL_BOX2_SCHEMA_ID,
                        tasks = [?ECHO_MEASUREMENTS_TASK_SCHEMA_DRAFT(?ATM_TASK3_SCHEMA_ID)]
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
            [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)]
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
            [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)]
        ),
        ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(__FAILING_DOCKER_IMAGE_ID)
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
        | ?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES
    ])]
)).

-define(FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(__FAILING_DOCKER_IMAGE_ID), #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = __FAILING_DOCKER_IMAGE_ID
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = ?ANY_MEASUREMENT_DATA_SPEC,
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = ?ANY_MEASUREMENT_DATA_SPEC,
        relay_method = return_value
    }]
}).

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


-record(fail_atm_workflow_execution_test_spec, {
    testcase_id :: term(),
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
    AnyMeasurementInvalidFun = fun(Measurements) ->
        lists:any(
            fun(Measurement) -> maps:get(<<"tsName">>, Measurement) == <<"size">> end,
            Measurements
        )
    end,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            gen_time_series_measurements(),
            ?FAILING_MEASUREMENT_STORE_MAPPING_TASK_SCHEMA_DRAFT,
            ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC, file_pipe)
        ),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
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
                            case AnyMeasurementInvalidFun(UncorrelatedResults) of
                                true ->
                                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(
                                        atm_workflow_execution_exp_state_builder:expect_lane_run_stopping(
                                            {1, 1}, atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping(
                                                {1, 1}, interrupt, ExpState
                                            )
                                        )
                                    )};
                                false ->
                                    false
                            end
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun uncorrelated_result_failure_expect_task_execution_ended/1
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, ExpState)}
                        end
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                end
            },
            after_hook = fun atm_workflow_execution_test_runner:assert_ended_atm_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


%% @private
-spec uncorrelated_result_failure_expect_task_execution_ended(
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
uncorrelated_result_failure_expect_task_execution_ended(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState) of
        {_, _, ?ATM_TASK1_SCHEMA_ID} ->
            ExpTaskLog = #{
                <<"description">> => <<"Failed to process uncorrelated task results.">>,
                <<"reason">> => errors:to_json(?EXP_ERROR_ATM_MEASUREMENT_DISPATCH_FAILED)
            },
            ?assert(lists:member(ExpTaskLog, get_audit_log_contents(
                AtmTaskExecutionId, AtmMockCallCtx
            ))),

            uncorrelated_result_expect_task1_failed(AtmTaskExecutionId, ExpState);
        {_, _, ?ATM_TASK2_SCHEMA_ID} ->
            uncorrelated_result_expect_task2_interrupted(AtmTaskExecutionId, ExpState);
        {_, _, ?ATM_TASK3_SCHEMA_ID} ->
            uncorrelated_result_expect_task3_interrupted(AtmTaskExecutionId, ExpState)
    end}.


%% @private
-spec uncorrelated_result_expect_task1_failed(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_task1_failed(AtmTask1ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_failed(
        AtmTask1ExecutionId, ExpState0
    ),
    uncorrelated_result_expect_pb1_changed_status(AtmTask1ExecutionId, ExpState1).


%% @private
-spec uncorrelated_result_expect_task2_interrupted(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_task2_interrupted(AtmTask2ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_interrupted(
        AtmTask2ExecutionId, ExpState0
    ),
    uncorrelated_result_expect_pb1_changed_status(AtmTask2ExecutionId, ExpState1).


%% @private
-spec uncorrelated_result_expect_pb1_changed_status(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_pb1_changed_status(AtmTaskExecutionId, ExpState) ->
    InferStatusFun = fun
        (<<"stopping">>, [<<"failed">>, <<"interrupted">>]) -> <<"failed">>;
        (Status, _) -> Status
    end,
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId, InferStatusFun, ExpState
    ).


%% @private
-spec uncorrelated_result_expect_task3_interrupted(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_task3_interrupted(AtmTask3ExecutionId, ExpState) ->
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTask3ExecutionId,
        fun(_, _) -> <<"interrupted">> end,
        atm_workflow_execution_exp_state_builder:expect_task_interrupted(AtmTask3ExecutionId, ExpState)
    ).


fail_atm_workflow_execution_due_to_incorrect_const_arg_type_error() ->
    IncorrectConst = 10,

    job_failure_atm_workflow_execution_test_base(arg_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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

    job_failure_atm_workflow_execution_test_base(arg_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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
    job_failure_atm_workflow_execution_test_base(arg_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_ARG_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            #atm_task_schema_argument_mapper{
                argument_name = ?ECHO_ARG_NAME,
                value_builder = #atm_task_argument_value_builder{
                    type = single_value_store_content,
                    recipe = ?SINGLE_VALUE_STORE_SCHEMA_ID
                }
            }
        ),
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_ATM_TASK_ARG_MAPPING_FAILED(
                    ?ECHO_ARG_NAME, ?ERROR_ATM_STORE_CONTENT_NOT_SET(
                        ?SINGLE_VALUE_STORE_SCHEMA_ID
                    )
                ))
            }
        end
    }).


fail_atm_workflow_execution_due_to_job_timeout() ->
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FUNCTION_NAME,
            gen_time_series_measurements(),
            ?FAILING_TASK_SCHEMA_DRAFT(
                [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)]
            ),
            ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(?ECHO_WITH_SLEEP_DOCKER_IMAGE_ID)
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
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_DUE_TO_RESULT_MAPPING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1
        ),
        should_item_processing_fail_fun = fun is_size_measurement/1,
        build_task1_audit_log_exp_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Failed to process item.">>,
                    <<"item">> => Item,
                    <<"reason">> => errors:to_json(?ERROR_ATM_TASK_RESULT_MISSING(<<"value">>))
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_incorrect_result_type_error() ->
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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
    job_failure_atm_workflow_execution_test_base(result_error, #fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
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
-spec job_failure_atm_workflow_execution_test_base(
    job_failure_type(),
    fail_atm_workflow_execution_test_spec()
) ->
    ok.
job_failure_atm_workflow_execution_test_base(JobFailureType, #fail_atm_workflow_execution_test_spec{
    testcase_id = TestcaseId,
    atm_workflow_schema_draft = AtmWorkflowSchemaDraft,
    should_item_processing_fail_fun = ShouldItemProcessingFailFun,
    build_task1_audit_log_exp_content_fun = BuildTask1AuditLogExpContentFun
}) ->
    BuildAtmLaneRunTestSpecFun = fun(AtmLaneRunSelector, IsLastExpLaneRun) ->
        #atm_lane_run_execution_test_spec{
            selector = AtmLaneRunSelector,
            run_task_for_item = 'build mock spec for run_task_for_item step'(
                JobFailureType, TestcaseId, ShouldItemProcessingFailFun
            ),
            process_task_result_for_item = 'build mock spec for process_task_result_for_item step'(
                JobFailureType, TestcaseId, ShouldItemProcessingFailFun
            ),
            handle_task_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(MockCallCtx) ->
                    job_failure_expect_task_execution_ended(
                        TestcaseId, BuildTask1AuditLogExpContentFun, MockCallCtx
                    )
                end
            },
            handle_lane_execution_stopped = 'build mock for handle_lane_execution_stopped step'(
                TestcaseId, AtmLaneRunSelector, IsLastExpLaneRun
            )
        }
    end,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = AtmWorkflowSchemaDraft,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                BuildAtmLaneRunTestSpecFun({1, 1}, false),
                BuildAtmLaneRunTestSpecFun({1, 2}, false),
                BuildAtmLaneRunTestSpecFun({1, 3}, true)
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = lists:foldl(fun(AtmLaneRunSelector, ExpStateAcc) ->
                        atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable(
                            AtmLaneRunSelector, atm_workflow_execution_exp_state_builder:expect_lane_run_retriable(
                                AtmLaneRunSelector, ExpStateAcc
                            )
                        )
                    end, ExpState0, [{1, 1}, {1, 2}, {1, 3}]),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState1)}
                end
            },
            after_hook = fun atm_workflow_execution_test_runner:assert_ended_atm_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).


%% @private
-spec 'build mock spec for run_task_for_item step'(
    job_failure_type(),
    term(),
    fun((automation:item()) -> boolean())
) ->
    atm_workflow_execution_test_runner:step_mock_spec().
'build mock spec for run_task_for_item step'(arg_error, TestcaseId, ShouldItemProcessingFailFun) ->
    #atm_step_mock_spec{after_step_exp_state_diff = fun(#atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState0,
        call_args = [
            _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
            _AtmJobBatchId, ItemBatch
        ]
    }) ->
        ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_in_processing_increased(
            AtmTaskExecutionId, length(ItemBatch), ExpState0
        ),
        ExpState2 = atm_workflow_execution_exp_state_builder:expect_task_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState1
        ),
        ExpState3 = atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_active_status_if_was_in_pending_status(
            AtmTaskExecutionId, ExpState2
        ),
        ExpState4 = atm_workflow_execution_exp_state_builder:expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
            AtmTaskExecutionId, ExpState3
        ),

        case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState4) of
            {_, _, ?ATM_TASK1_SCHEMA_ID} ->
                task1_job_failure_expect_task_items_processed(
                    TestcaseId, AtmTaskExecutionId, ItemBatch, ShouldItemProcessingFailFun, ExpState4
                );
            _ ->
                {true, ExpState4}
        end
    end};

'build mock spec for run_task_for_item step'(result_error, _TestcaseId, _ShouldItemProcessingFailFun) ->
    #atm_step_mock_spec{}.


%% @private
-spec 'build mock spec for process_task_result_for_item step'(
    job_failure_type(),
    term(),
    fun((automation:item()) -> boolean())
) ->
    atm_workflow_execution_test_runner:step_mock_spec().
'build mock spec for process_task_result_for_item step'(arg_error, _TestcaseId, _ShouldItemProcessingFailFun) ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [
                _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
                ItemBatch, _JobBatchResult
            ]
        }) ->
            case atm_workflow_execution_exp_state_builder:get_task_selector(
                AtmTaskExecutionId, ExpState0
            ) of
                {_, _, ?ATM_TASK1_SCHEMA_ID} ->
                    false;
                _ ->
                    task2_or_task3_job_failure_expect_task_items_processed(AtmTaskExecutionId, ItemBatch, ExpState0)
            end
        end
    };

'build mock spec for process_task_result_for_item step'(result_error, TestcaseId, ShouldItemProcessingFailFun) ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0,
            call_args = [
                _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
                ItemBatch, _JobBatchResult
            ]
        }) ->
            job_failure_expect_task_items_processed(
                TestcaseId, AtmTaskExecutionId, ItemBatch, ShouldItemProcessingFailFun, ExpState0
            )
        end
    }.


%% @private
-spec job_failure_expect_task_items_processed(
    term(),
    atm_task_execution:id(),
    [automation:item()],
    fun((automation:item()) -> boolean()),
    atm_workflow_execution_exp_state_builder:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
job_failure_expect_task_items_processed(
    TestcaseId,
    AtmTaskExecutionId,
    ItemBatch,
    ShouldItemProcessingFailFun,
    ExpState0
) ->
    case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
        {_, _, ?ATM_TASK1_SCHEMA_ID} ->
            task1_job_failure_expect_task_items_processed(
                TestcaseId, AtmTaskExecutionId, ItemBatch, ShouldItemProcessingFailFun, ExpState0
            );
        _ ->
            task2_or_task3_job_failure_expect_task_items_processed(AtmTaskExecutionId, ItemBatch, ExpState0)
    end.


%% @private
-spec task1_job_failure_expect_task_items_processed(
    term(),
    atm_task_execution:id(),
    [automation:item()],
    fun((automation:item()) -> boolean()),
    atm_workflow_execution_exp_state_builder:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
task1_job_failure_expect_task_items_processed(
    TestcaseId,
    AtmTaskExecutionId,
    ItemBatch,
    ShouldItemProcessingFailFun,
    ExpState0
) ->
    ItemCount = length(ItemBatch),

    {AtmLaneRunSelector, _, _} = TaskSelector = atm_workflow_execution_exp_state_builder:get_task_selector(
        AtmTaskExecutionId, ExpState0
    ),
    record_items_processed(TestcaseId, AtmLaneRunSelector, ItemBatch),

    FailedItemCount = case lists:filter(ShouldItemProcessingFailFun, ItemBatch) of
        [] ->
            % Batch can be processed further (by following parallel boxes)
            % only if no item from batch fails (limitation of workflow engine)
            inc_exp_items_processed_by_task3(TestcaseId, element(1, TaskSelector), ItemCount),
            0;
        FailedItems ->
            record_failed_item_batch_for_task1(TestcaseId, TaskSelector, FailedItems),
            update_exp_exception_store_content(TestcaseId, AtmLaneRunSelector, ItemBatch),
            length(FailedItems)
    end,
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_failed_and_processed(
        AtmTaskExecutionId, FailedItemCount, ExpState0
    ),
    {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
        AtmTaskExecutionId, ItemCount - FailedItemCount, ExpState1
    )}.


%% @private
-spec task2_or_task3_job_failure_expect_task_items_processed(
    atm_task_execution:id(),
    [automation:item()],
    atm_workflow_execution_exp_state_builder:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
task2_or_task3_job_failure_expect_task_items_processed(AtmTaskExecutionId, ItemBatch, ExpState0) ->
    {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
        AtmTaskExecutionId, length(ItemBatch), ExpState0
    )}.


%% @private
-spec job_failure_expect_task_execution_ended(
    term(),
    fun(([automation:item()]) -> automation:item()),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
job_failure_expect_task_execution_ended(
    TestcaseId,
    BuildTask1AuditLogExpContentFun,
    AtmMockCallCtx = #atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }
) ->
    {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState) of
        {_, _, ?ATM_TASK1_SCHEMA_ID} = Task1Selector ->
            ExpLogContents = lists:sort(lists:flatten(lists:map(
                BuildTask1AuditLogExpContentFun,
                get_all_failed_item_batches_for_task1(TestcaseId, Task1Selector)
            ))),
            ?assertEqual(
                ExpLogContents,
                lists:sort(get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx))
            ),

            job_failure_expect_task1_failed(AtmTaskExecutionId, ExpState);

        {_, _, ?ATM_TASK2_SCHEMA_ID} ->
            job_failure_expect_task2_finished(AtmTaskExecutionId, ExpState);

        {_, _, ?ATM_TASK3_SCHEMA_ID} ->
            job_failure_expect_task3_ended(TestcaseId, AtmTaskExecutionId, ExpState)
    end}.


%% @private
-spec get_audit_log_contents(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    [automation:item()].
get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx) ->
    #{<<"logEntries">> := Logs, <<"isLast">> := true} = atm_workflow_execution_test_runner:browse_store(
        ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"content">> := LogContent}) -> LogContent end, Logs).


%% @private
-spec job_failure_expect_task1_failed(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_task1_failed(AtmTask1ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_failed(
        AtmTask1ExecutionId, ExpState0
    ),
    job_failure_expect_pb1_changed_status(AtmTask1ExecutionId, ExpState1).


%% @private
-spec job_failure_expect_task2_finished(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_task2_finished(AtmTask2ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_finished(
        AtmTask2ExecutionId, ExpState0
    ),
    job_failure_expect_pb1_changed_status(AtmTask2ExecutionId, ExpState1).


%% @private
-spec job_failure_expect_pb1_changed_status(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_pb1_changed_status(AtmTaskExecutionId, ExpState) ->
    InferStatusFun = fun
        (<<"active">>, [<<"failed">>, <<"finished">>]) -> <<"failed">>;
        (_, _) -> <<"active">>
    end,
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId, InferStatusFun, ExpState
    ).


%% @private
-spec job_failure_expect_task3_ended(
    term(),
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_task3_ended(TestcaseId, AtmTask3ExecutionId, ExpState) ->
    TaskSelector = atm_workflow_execution_exp_state_builder:get_task_selector(
        AtmTask3ExecutionId, ExpState
    ),
    ExpItemsProcessed = get_exp_items_processed_by_task3(TestcaseId, TaskSelector),

    ?assertEqual(
        {0, 0, ExpItemsProcessed},
        atm_workflow_execution_exp_state_builder:get_task_stats(AtmTask3ExecutionId, ExpState)
    ),

    {ExpTaskTransitionFun, ExpPbStatus} = case ExpItemsProcessed of
        0 ->
            {fun atm_workflow_execution_exp_state_builder:expect_task_skipped/2, <<"skipped">>};
        _ ->
            {fun atm_workflow_execution_exp_state_builder:expect_task_finished/2, <<"finished">>}
    end,
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTask3ExecutionId,
        fun(_, _) -> ExpPbStatus end,
        ExpTaskTransitionFun(AtmTask3ExecutionId, ExpState)
    ).


%% @private
-spec 'build mock for handle_lane_execution_stopped step'(
    term(),
    atm_lane_execution:lane_run_selector(),
    boolean()
) ->
    atm_workflow_execution_test_runner:step_mock_spec().
'build mock for handle_lane_execution_stopped step'(TestcaseId, AtmLaneRunSelector, IsLastExpLaneRun) ->
    #atm_step_mock_spec{
        after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
            workflow_execution_exp_state = ExpState0
        }) ->
            % Assert all items from prev lane run exception store were processed
            check_iterated_items(TestcaseId, AtmLaneRunSelector, AtmMockCallCtx),
            check_exception_store_content(TestcaseId, AtmLaneRunSelector, AtmMockCallCtx),

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
-spec check_iterated_items(
    term(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok | no_return().
check_iterated_items(TestcaseId, {1, 1} = AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_runner:browse_store(
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
    #{<<"items">> := Items, <<"isLast">> := true} = atm_workflow_execution_test_runner:browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"value">> := Content}) -> Content end, Items).


%% @private
-spec record_failed_item_batch_for_task1(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector(),
    [automation:item()]
) ->
    ok.
record_failed_item_batch_for_task1(TestcaseId, TaskSelector, FailedItemBatch) ->
    Key = {TestcaseId, TaskSelector, failed_item_batch},
    node_cache:put(Key, [FailedItemBatch | node_cache:get(Key, [])]).


%% @private
-spec get_all_failed_item_batches_for_task1(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector()
) ->
    ok.
get_all_failed_item_batches_for_task1(TestcaseId, TaskSelector) ->
    Key = {TestcaseId, TaskSelector, failed_item_batch},
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
-spec inc_exp_items_processed_by_task3(
    term(),
    atm_lane_execution:lane_run_selector(),
    pos_integer()
) ->
    ok.
inc_exp_items_processed_by_task3(TestcaseId, AtmLaneRunSelector, Count) ->
    Task3Selector = {AtmLaneRunSelector, ?ATM_PARALLEL_BOX2_SCHEMA_ID, ?ATM_TASK3_SCHEMA_ID},
    Key = {TestcaseId, Task3Selector, exp_items_processed},
    node_cache:put(Key, Count + node_cache:get(Key, 0)).


%% @private
-spec get_exp_items_processed_by_task3(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector()
) ->
    pos_integer().
get_exp_items_processed_by_task3(TestcaseId, Task3Selector) ->
    node_cache:get({TestcaseId, Task3Selector, exp_items_processed}, 0).


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
-spec is_size_measurement(automation:item()) -> boolean().
is_size_measurement(#{<<"tsName">> := <<"size">>}) -> true;
is_size_measurement(#{<<"tsName">> := _}) -> false.
