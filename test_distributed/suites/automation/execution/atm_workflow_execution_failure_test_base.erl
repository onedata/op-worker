%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning failure of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_failure_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    fail_atm_workflow_execution_due_to_uncorrelated_result_store_mapping_error/0,
    fail_atm_workflow_execution_due_to_job_result_store_mapping_error/0,
    fail_atm_workflow_execution_due_to_job_missing_required_results_error/0,
    fail_atm_workflow_execution_due_to_incorrect_result_type_error/0,
    fail_atm_workflow_execution_due_to_lambda_exception/0,
    fail_atm_workflow_execution_due_to_lambda_error/0
]).


-define(ANY_MEASUREMENT_DATA_SPEC, #atm_data_spec{
    type = atm_time_series_measurement_type,
    value_constraints = #{specs => [#atm_time_series_measurement_spec{
        name_matcher_type = has_prefix,
        name_matcher = <<>>,
        unit = none
    }]}
}).

-define(CORRECT_ATM_TIME_SERIES_DISPATCH_RULES, [
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = has_prefix,
        measurement_ts_name_matcher = <<"count_">>,
        target_ts_name_generator = ?COUNT_TS_NAME_GENERATOR,
        prefix_combiner = converge
    },
    #atm_time_series_dispatch_rule{
        measurement_ts_name_matcher_type = exact,
        measurement_ts_name_matcher = <<"size">>,
        target_ts_name_generator = ?MAX_FILE_SIZE_TS_NAME,
        prefix_combiner = overwrite
    }
]).

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
    __ITERATED_CONTENT,
    __FAILING_TASK_SCHEMA_DRAFT,
    __FAILING_LAMBDA_DRAFT
),
    #atm_workflow_schema_dump_draft{
        name = <<"doomed">>,
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
                #atm_store_schema_draft{
                    id = ?TARGET_STORE_SCHEMA_ID,
                    type = time_series,
                    config = #atm_time_series_store_config{schemas = [
                        ?MAX_FILE_SIZE_TS_SCHEMA,
                        ?COUNT_TS_SCHEMA
                    ]},
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

-define(JOB_FAILING_WORKFLOW_SCHEMA_DRAFT(__FAILING_LAMBDA_DRAFT), ?FAILING_WORKFLOW_SCHEMA_DRAFT(
    gen_time_series_measurements(),
    ?FAILING_TASK_SCHEMA_DRAFT(
        [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
        [?TARGET_STORE_RESULT_MAPPER(?CORRECT_ATM_TIME_SERIES_DISPATCH_RULES)]
    ),
    __FAILING_LAMBDA_DRAFT
)).


-record(fail_atm_workflow_execution_test_spec, {
    testcase_id :: term(),
    atm_workflow_schema_draft :: atm_test_schema_factory:atm_workflow_schema_dump_draft(),
    filter_out_not_failed_items_in_t1_fun = fun filter_out_not_size_measurements/1
        :: fun(([automation:item()]) -> [automation:item()]),
    build_t1_exp_error_log_content_fun :: fun(([automation:item()]) -> automation:item())
}).
-type fail_atm_workflow_execution_test_spec() :: #fail_atm_workflow_execution_test_spec{}.


-define(NOW(), global_clock:timestamp_seconds()).


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

    % TODO VFS-9452 - check task audit log for failed item entries
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
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
                                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(
                                        atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState)
                                    )};
                                false ->
                                    false
                            end
                        end
                    },
                    handle_task_execution_ended = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun uncorrelated_result_failure_expect_task_execution_ended/1
                    },
                    handle_lane_execution_ended = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, false, ExpState)}
                        end
                    }
                }
            ],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState0)}
                end
            }
        }]
    }).


%% @private
-spec uncorrelated_result_failure_expect_task_execution_ended(
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
uncorrelated_result_failure_expect_task_execution_ended(#atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState,
    call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
}) ->
    {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState) of
        {_, _, ?ATM_TASK1_SCHEMA_ID} ->
            uncorrelated_result_expect_t1_failed(AtmTaskExecutionId, ExpState);
        {_, _, ?ATM_TASK2_SCHEMA_ID} ->
            uncorrelated_result_expect_t2_ended(AtmTaskExecutionId, ExpState);
        {_, _, ?ATM_TASK3_SCHEMA_ID} ->
            uncorrelated_result_expect_t3_ended(AtmTaskExecutionId, ExpState)
    end}.


%% @private
-spec uncorrelated_result_expect_t1_failed(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_t1_failed(AtmTask1ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_failed(
        AtmTask1ExecutionId, ExpState0
    ),
    uncorrelated_result_expect_pb1_changed_status(AtmTask1ExecutionId, ExpState1).


%% @private
-spec uncorrelated_result_expect_t2_ended(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_t2_ended(AtmTask2ExecutionId, ExpState0) ->
    ExpState1 = case atm_workflow_execution_exp_state_builder:get_task_stats(AtmTask2ExecutionId, ExpState0) of
        {0, 0, 0} ->
            atm_workflow_execution_exp_state_builder:expect_task_skipped(AtmTask2ExecutionId, ExpState0);
        {0, 0, _} ->
            atm_workflow_execution_exp_state_builder:expect_task_finished(AtmTask2ExecutionId, ExpState0)
    end,
    uncorrelated_result_expect_pb1_changed_status(AtmTask2ExecutionId, ExpState1).


%% @private
-spec uncorrelated_result_expect_pb1_changed_status(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_pb1_changed_status(AtmTaskExecutionId, ExpState) ->
    InferStatusFun = fun
        (<<"active">>, [<<"failed">>, <<"finished">>]) -> <<"failed">>;
        (<<"active">>, [<<"failed">>, <<"skipped">>]) -> <<"failed">>;
        (_, _) -> <<"active">>
    end,
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTaskExecutionId, InferStatusFun, ExpState
    ).


%% @private
-spec uncorrelated_result_expect_t3_ended(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
uncorrelated_result_expect_t3_ended(AtmTask3ExecutionId, ExpState) ->
    {ExpTaskTransitionFun, ExpPbStatus} = case atm_workflow_execution_exp_state_builder:get_task_stats(
        AtmTask3ExecutionId, ExpState
    ) of
        {0, 0, 0} ->
            {fun atm_workflow_execution_exp_state_builder:expect_task_skipped/2, <<"skipped">>};
        {0, 0, _} ->
            {fun atm_workflow_execution_exp_state_builder:expect_task_finished/2, <<"finished">>}
    end,
    atm_workflow_execution_exp_state_builder:expect_task_parallel_box_transitioned_to_inferred_status(
        AtmTask3ExecutionId,
        fun(_, _) -> ExpPbStatus end,
        ExpTaskTransitionFun(AtmTask3ExecutionId, ExpState)
    ).


fail_atm_workflow_execution_due_to_job_result_store_mapping_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?FAILING_WORKFLOW_SCHEMA_DRAFT(
            gen_time_series_measurements(),
            ?FAILING_MEASUREMENT_STORE_MAPPING_TASK_SCHEMA_DRAFT,
            ?ECHO_LAMBDA_DRAFT(?ANY_MEASUREMENT_DATA_SPEC)
        ),
        build_t1_exp_error_log_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Failed to process item.">>,
                    <<"item">> => Item,
                    <<"reason">> => errors:to_json(?ERROR_ATM_TASK_RESULT_MAPPING_FAILED(
                        <<"value">>, ?ERROR_ATM_TASK_RESULT_DISPATCH_FAILED(
                            ?TARGET_STORE_SCHEMA_ID,
                            ?ERROR_BAD_DATA(<<"dispatchRules">>, str_utils:format_bin(
                                "Time series name generator '~s' specified in one of the dispatch rules "
                                "does not reference any defined time series schema",
                                [?MISSING_TS_NAME_GENERATOR]
                            ))
                        )
                    ))
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_job_missing_required_results_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1)
        ),
        build_t1_exp_error_log_content_fun = fun(ItemBatch) ->
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
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2)
        ),
        build_t1_exp_error_log_content_fun = fun(ItemBatch) ->
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


fail_atm_workflow_execution_due_to_lambda_exception() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3)
        ),
        build_t1_exp_error_log_content_fun = fun(ItemBatch) ->
            lists:map(fun(Item) ->
                #{
                    <<"description">> => <<"Lambda exception occurred during item processing.">>,
                    <<"item">> => Item,
                    <<"reason">> => ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3_EXCEPTION
                }
            end, ItemBatch)
        end
    }).


fail_atm_workflow_execution_due_to_lambda_error() ->
    job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
        testcase_id = ?FUNCTION_NAME,
        atm_workflow_schema_draft = ?JOB_FAILING_WORKFLOW_SCHEMA_DRAFT(
            ?FAILING_ECHO_MEASUREMENTS_LAMBDA_DRAFT(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4)
        ),
        filter_out_not_failed_items_in_t1_fun = fun(Items) -> Items end,
        build_t1_exp_error_log_content_fun = fun(ItemBatch) ->
            #{
                <<"description">> => <<"Failed to process batch of items.">>,
                <<"itemBatch">> => ItemBatch,
                <<"reason">> => errors:to_json(?ERROR_BAD_DATA(
                    <<"lambdaOutput">>,
                    ?ERROR_BAD_MESSAGE(<<"Illegal instruction: kor damp -.-">>)
                ))
            }
        end
    }).


%% @private
-spec job_failure_atm_workflow_execution_test_base(fail_atm_workflow_execution_test_spec()) ->
    ok.
job_failure_atm_workflow_execution_test_base(#fail_atm_workflow_execution_test_spec{
    testcase_id = TestcaseId,
    atm_workflow_schema_draft = AtmWorkflowSchemaDraft,
    filter_out_not_failed_items_in_t1_fun = FilterOutNotFailedItemsFun,
    build_t1_exp_error_log_content_fun = BuildT1ExpErrorLogContentFun
}) ->
    AtmLaneRunTestSpec = #atm_lane_run_execution_test_spec{
        process_task_result_for_item = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(MockCallCtx) ->
                job_failure_expect_task_items_processed(
                    TestcaseId, FilterOutNotFailedItemsFun, MockCallCtx
                )
            end
        },
        handle_task_execution_ended = #atm_step_mock_spec{
            after_step_exp_state_diff = fun(MockCallCtx) ->
                job_failure_expect_task_execution_ended(
                    TestcaseId, BuildT1ExpErrorLogContentFun, MockCallCtx
                )
            end
        }
    },

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = AtmWorkflowSchemaDraft,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                AtmLaneRunTestSpec#atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    handle_lane_execution_ended = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0
                        }) ->
                            check_exception_store_content(TestcaseId, {1, 1}, AtmMockCallCtx),
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, true, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_automatic_retry_scheduled({1, 2}, ExpState1)}
                        end
                    }
                },
                AtmLaneRunTestSpec#atm_lane_run_execution_test_spec{
                    selector = {1, 2},
                    handle_lane_execution_ended = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0
                        }) ->
                            check_exception_store_content(TestcaseId, {1, 2}, AtmMockCallCtx),
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 2}, true, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_automatic_retry_scheduled({1, 3}, ExpState1)}
                        end
                    }
                },
                AtmLaneRunTestSpec#atm_lane_run_execution_test_spec{
                    selector = {1, 3},
                    handle_lane_execution_ended = #atm_step_mock_spec{
                        after_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState
                        }) ->
                            check_exception_store_content(TestcaseId, {1, 3}, AtmMockCallCtx),
                            {true, atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 3}, true, ExpState)}
                        end
                    }
                }
            ],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState0)}
                end
            }
        }]
    }).


%% @private
-spec job_failure_expect_task_items_processed(
    term(),
    fun(([automation:item()]) -> [automation:item()]),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
job_failure_expect_task_items_processed(TestcaseId, FilterOutNotFailedItemsFun, #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0,
    call_args = [
        _AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId,
        ItemBatch, _LambdaOutput
    ]
}) ->
    ItemCount = length(ItemBatch),

    TaskSelector = atm_workflow_execution_exp_state_builder:get_task_selector(
        AtmTaskExecutionId, ExpState0
    ),
    {true, case TaskSelector of
        {AtmLaneRunSelector, _, ?ATM_TASK1_SCHEMA_ID} ->
            FailedItemCount = case FilterOutNotFailedItemsFun(ItemBatch) of
                [] ->
                    % Batch can be processed further (by following parallel boxes)
                    % only if no item from batch fails (limitation of workflow engine)
                    inc_exp_items_processed_by_t3(TestcaseId, element(1, TaskSelector), ItemCount),
                    0;
                FailedItems ->
                    record_failed_item_batch_for_t1(TestcaseId, TaskSelector, FailedItems),
                    update_exp_exception_store_content(TestcaseId, AtmLaneRunSelector, ItemBatch),
                    length(FailedItems)
            end,
            ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_failed_and_processed(
                AtmTaskExecutionId, FailedItemCount, ExpState0
            ),
            atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                AtmTaskExecutionId, ItemCount - FailedItemCount, ExpState1
            );
        _ ->
            atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                AtmTaskExecutionId, ItemCount, ExpState0
            )
    end}.


%% @private
-spec job_failure_expect_task_execution_ended(
    term(),
    fun(([automation:item()]) -> automation:item()),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    atm_workflow_execution_test_runner:exp_state_diff().
job_failure_expect_task_execution_ended(
    TestcaseId,
    BuildT1ExpErrorLogContentFun,
    AtmMockCallCtx = #atm_mock_call_ctx{
        workflow_execution_exp_state = ExpState,
        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
    }
) ->
    {true, case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState) of
        {_, _, ?ATM_TASK1_SCHEMA_ID} = Task1Selector ->
            ExpLogContents = lists:sort(lists:flatten(lists:map(
                BuildT1ExpErrorLogContentFun,
                get_all_failed_item_batches_for_t1(TestcaseId, Task1Selector)
            ))),
            ?assertEqual(
                ExpLogContents,
                lists:sort(get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx))
            ),

            job_failure_expect_t1_failed(AtmTaskExecutionId, ExpState);

        {_, _, ?ATM_TASK2_SCHEMA_ID} ->
            job_failure_expect_t2_finished(AtmTaskExecutionId, ExpState);

        {_, _, ?ATM_TASK3_SCHEMA_ID} ->
            job_failure_expect_t3_ended(TestcaseId, AtmTaskExecutionId, ExpState)
    end}.


%% @private
-spec get_audit_log_contents(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    [automation:item()].
get_audit_log_contents(AtmTaskExecutionId, AtmMockCallCtx) ->
    #{<<"logs">> := Logs, <<"isLast">> := true} = atm_workflow_execution_test_runner:browse_store(
        ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"value">> := #{<<"content">> := LogContent}}) -> LogContent end, Logs).


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
-spec job_failure_expect_t1_failed(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_t1_failed(AtmTask1ExecutionId, ExpState0) ->
    ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_failed(
        AtmTask1ExecutionId, ExpState0
    ),
    job_failure_expect_pb1_changed_status(AtmTask1ExecutionId, ExpState1).


%% @private
-spec job_failure_expect_t2_finished(
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_t2_finished(AtmTask2ExecutionId, ExpState0) ->
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
-spec job_failure_expect_t3_ended(
    term(),
    atm_task_execution:id(),
    atm_workflow_execution_test_runner:exp_state()
) ->
    atm_workflow_execution_test_runner:exp_state().
job_failure_expect_t3_ended(TestcaseId, AtmTask3ExecutionId, ExpState) ->
    TaskSelector = atm_workflow_execution_exp_state_builder:get_task_selector(
        AtmTask3ExecutionId, ExpState
    ),
    ExpItemsProcessed = get_exp_items_processed_by_t3(TestcaseId, TaskSelector),

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
-spec record_failed_item_batch_for_t1(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector(),
    [automation:item()]
) ->
    ok.
record_failed_item_batch_for_t1(TestcaseId, TaskSelector, FailedItemBatch) ->
    Key = {TestcaseId, TaskSelector, failed_item_batch},
    node_cache:put(Key, [FailedItemBatch | node_cache:get(Key, [])]).


%% @private
-spec get_all_failed_item_batches_for_t1(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector()
) ->
    ok.
get_all_failed_item_batches_for_t1(TestcaseId, TaskSelector) ->
    Key = {TestcaseId, TaskSelector, failed_item_batch},
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
-spec inc_exp_items_processed_by_t3(
    term(),
    atm_lane_execution:lane_run_selector(),
    pos_integer()
) ->
    ok.
inc_exp_items_processed_by_t3(TestcaseId, AtmLaneRunSelector, Count) ->
    Task3Selector = {AtmLaneRunSelector, ?ATM_PARALLEL_BOX2_SCHEMA_ID, ?ATM_TASK3_SCHEMA_ID},
    Key = {TestcaseId, Task3Selector, exp_items_processed},
    node_cache:put(Key, Count + node_cache:get(Key, 0)).


%% @private
-spec get_exp_items_processed_by_t3(
    term(),
    atm_workflow_execution_exp_state_builder:task_selector()
) ->
    pos_integer().
get_exp_items_processed_by_t3(TestcaseId, Task3Selector) ->
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
            <<"timestamp">> => ?RAND_ELEMENT([?NOW() - 100, ?NOW(), ?NOW() + 3700]),
            <<"value">> => ?RAND_INT(10000000)
        }
    end, 40).


%% @private
-spec filter_out_not_size_measurements([automation:item()]) -> [automation:item()].
filter_out_not_size_measurements(Measurements) ->
    lists:filter(
        fun(Measurement) -> maps:get(<<"tsName">>, Measurement) == <<"size">> end,
        Measurements
    ).
