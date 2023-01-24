%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning automation workflow execution preparation step.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_preparation_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    first_lane_run_preparation_failure_due_to_lambda_config_acquisition/0,

    first_lane_run_preparation_failure_before_run_was_created/0,
    first_lane_run_preparation_failure_after_run_was_created/0,
    first_lane_run_preparation_interruption_due_to_openfaas_error/0,

    atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created/0,
    atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created/0,
    atm_workflow_execution_cancel_before_lane_run_preparation_failed/0,
    atm_workflow_execution_cancel_in_stopping_status_after_lane_run_preparation_failed/0,

    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1/0,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2/0,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3/0,
    first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4/0,

    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1/0,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2/0,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3/0,
    first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4/0,

    lane_failed_in_advance_is_not_removed_if_first_lane_run_successfully_finished/0
]).


-define(ITERATED_STORE_ID, <<"iterated_store">>).
-define(TARGET_STORE_ID, <<"target_store">>).

-define(ECHO_ATM_LANE_SCHEMA_DRAFT(__LAMBDA_CONFIG), #atm_lane_schema_draft{
    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
        #atm_task_schema_draft{
            lambda_id = ?ECHO_LAMBDA_ID,
            lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
            lambda_config = __LAMBDA_CONFIG,
            argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
            result_mappings = [#atm_task_schema_result_mapper{
                result_name = ?ECHO_ARG_NAME,
                store_schema_id = ?TARGET_STORE_ID,
                store_content_update_options = #atm_list_store_content_update_options{
                    function = append
                }
            }]
        }
    ]}],
    store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = ?ITERATED_STORE_ID},
    max_retries = ?RAND_INT(3, 6)
}).

-define(ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(?FUNCTION_NAME),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_ID, [3, 9, 27]),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_ID)
        ],
        lanes = [?ECHO_ATM_LANE_SCHEMA_DRAFT(#{})]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?NUMBER_ECHO_LAMBDA_DRAFT
    }}
}).

-define(ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(?FUNCTION_NAME),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_ID, [5, 25, 125]),
            ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_ID)
        ],
        lanes = [
            ?ECHO_ATM_LANE_SCHEMA_DRAFT(#{}),
            ?ECHO_ATM_LANE_SCHEMA_DRAFT(#{})
        ]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?NUMBER_ECHO_LAMBDA_DRAFT
    }}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


first_lane_run_preparation_failure_due_to_lambda_config_acquisition() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = #atm_workflow_schema_dump_draft{
            name = str_utils:to_binary(?FUNCTION_NAME),
            revision_num = 1,
            revision = #atm_workflow_schema_revision_draft{
                stores = [
                    ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_ID, [3, 9, 27]),
                    ?INTEGER_ATM_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_ID)
                ],
                lanes = [?ECHO_ATM_LANE_SCHEMA_DRAFT(#{?ECHO_ARG_NAME => 0.1})]
            },
            supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?NUMBER_ECHO_LAMBDA_DRAFT
            }}
        },
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ],
                % Assert not all items were processed by tasks
                before_step_hook = fun(AtmMockCallCtx) ->
                    #{<<"logEntries">> := [#{<<"content">> := #{<<"reason">> := LogContent}}]} = ?assertMatch(
                        #{<<"isLast">> := true, <<"logEntries">> := [#{<<"content">> := #{
                            <<"description">> := <<"Failed to prepare next run of lane number 1.">>
                        }}]},
                        atm_workflow_execution_test_utils:browse_store(
                            ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmMockCallCtx
                        )
                    ),

                    ?assertMatch(
                        ?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(
                            _, ?ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(
                                _, ?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(
                                    _, ?ERROR_ATM_LAMBDA_CONFIG_BAD_VALUE(
                                        ?ECHO_ARG_NAME, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                                            0.1, atm_number_type, #{<<"integersOnly">> := true}
                                        )
                                    )
                                )
                            )
                        ),
                        errors:from_json(LogContent)
                    )
                end
            }
        }]
    }).


first_lane_run_preparation_failure_before_run_was_created() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    % 'create_run' step execution is mocked entirely so that
                    % no lane run execution component will be created
                    strategy = {yield, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                },
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


first_lane_run_preparation_failure_after_run_was_created() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    % 'create_run' step result is replaced by the one specified below but
                    % the step itself is executed normally so that lane run execution
                    % components (e.g. task executions) can be created
                    strategy = {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                },
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
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
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


first_lane_run_preparation_interruption_due_to_openfaas_error() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    before_step_hook = fun(#atm_mock_call_ctx{
                        provider = ProviderSelector,
                        workflow_execution_id = AtmWorkflowExecutionId
                    }) ->
                        atm_openfaas_task_executor_mock:mock_lane_initiation_result(
                            ProviderSelector, AtmWorkflowExecutionId, 1, exception
                        )
                    end,
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    before_step_exp_state_diff = [
                        {all_tasks, {1, 1}, interrupted},
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    after_step_exp_state_diff = [{lane_run, {1, 1}, interrupted}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [workflow_interrupted]
            }
        }]
    }).


atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    % no lane run components should be created
                    after_step_exp_state_diff = no_diff
                },
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, started_preparing_as_current_lane_run}
                    ],
                    after_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                    end,
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
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


atm_workflow_execution_cancel_before_lane_run_preparation_failed() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    before_step_exp_state_diff = [
                        {lane_run, {1, 1}, stopping},
                        workflow_stopping
                    ],
                    strategy = ?RAND_ELEMENT([
                        {yield, {throw, ?ERROR_INTERNAL_SERVER_ERROR}},
                        % Even if lane run creation proceed created components should not be saved but rather
                        % immediately deleted due to cancel
                        {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                    ])
                },
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    % Despite error occurring during lane run preparation cancel was scheduled first and
                    % has higher priority so overall lane run status should be cancelled
                    after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


atm_workflow_execution_cancel_in_stopping_status_after_lane_run_preparation_failed() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_1_LANE_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    strategy = {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                },
                prepare_lane = #atm_step_mock_spec{
                    % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                    % from within lane preparation
                    after_step_exp_state_diff = no_diff
                },
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx) ->
                        % While atm workflow execution as whole has not yet transition to failed status
                        % (last step remaining) the current lane run did. At this point cancel
                        % is no longer possible (execution is treated as failed one)
                        ?assertThrow(
                            ?ERROR_ATM_INVALID_STATUS_TRANSITION(?FAILED_STATUS, ?STOPPING_STATUS),
                            atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                        )
                    end,
                    after_step_exp_state_diff = [
                        {all_tasks, {1, 1}, interrupted},
                        {lane_run, {1, 1}, failed},
                        workflow_stopping
                    ]
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


% Test what happens when first lane run fails (e.g. before lane run was created)
% before the one preparing in advance started preparing
first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_1() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
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
                        before_step_exp_state_diff = [
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
            }
        }]
    }).


% Test what happens when first lane run fails (e.g. after lane run was created)
% while the one preparing in advance started creating execution components
first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_2() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, before_step, {2, 1}},
                        before_step_exp_state_diff =[
                            {lane_run, {2, 1}, started_preparing_in_advance}
                        ],
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        strategy = {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
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
                    create_run = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {1, 1}},
                        before_step_exp_state_diff = no_diff
                    },
                    prepare_lane = #atm_step_mock_spec{
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = [{lane_run, {2, 1}, stopping}],
                        after_step_exp_state_diff = [{lane_run, {2, 1}, interrupted}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


% Test what happens when first lane run fails (e.g. before lane run was created)
% after lane run preparing in advance was enqueued (successfully finished preparation)
first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_3() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {2, 1}},
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
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        % Previously enqueued lane is changed to interrupted
                        after_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, interrupted}
                        ]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


% Test what happens when first lane run fails (e.g. before lane run was created)
% after lane run preparing in advance failed (e.g. before lane run was created)
first_lane_run_preparation_failure_interrupts_lane_preparing_in_advance_4() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {2, 1}},
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
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        after_step_exp_state_diff = [{lane_run, {1, 1}, failed}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
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
                        before_step_exp_state_diff = [
                            {lane_run, {2, 1}, stopping}
                        ],
                        after_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, failed}
                        ]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).


% Test what happens when execution is cancelled (e.g. before first lane run was created)
% before the one preparing in advance started preparing
first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_1() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
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
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        % no lane run components should be created
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                ?UNSCHEDULED_LANE_RUN_TEST_SPEC({2, 1}, {prepare_lane, after_step, {1, 1}})
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, {1, 1}, rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


% Test what happens when execution is cancelled (e.g. after first lane run was created)
% while the one preparing in advance started creating execution components
first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_2() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, before_step, {2, 1}},
                        before_step_exp_state_diff =[
                            {lane_run, {2, 1}, started_preparing_in_advance}
                        ],
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        after_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        after_step_exp_state_diff = [
                            {lane_run, {1, 1}, created},
                            {lane_run, {1, 1}, stopping},
                            {all_tasks, {1, 1}, cancelled},
                            workflow_stopping
                        ]
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {1, 1}},
                        before_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = [{lane_run, {2, 1}, stopping}],
                        after_step_exp_state_diff = [{lane_run, {2, 1}, interrupted}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


% Test what happens when execution is cancelled (e.g. before first lane run was created)
% after lane run preparing in advance was enqueued (successfully finished preparation)
first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_3() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {2, 1}},
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        % no lane run components should be created
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        % Previously enqueued lane is changed to interrupted
                        after_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, interrupted}
                        ]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


% Test what happens when execution is cancelled (e.g. before first lane run was created)
% after lane run preparing in advance failed (e.g. before lane run was created)
first_lane_run_preparation_cancel_interrupts_lane_preparing_in_advance_4() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {prepare_lane, after_step, {2, 1}},
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_utils:cancel_workflow_execution(AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = [
                            {lane_run, {1, 1}, stopping},
                            workflow_stopping
                        ],
                        % no lane run components should be created
                        after_step_exp_state_diff = no_diff
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [{lane_run, {1, 1}, cancelled}]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
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
                        before_step_exp_state_diff = [{lane_run, {2, 1}, stopping}],
                        after_step_exp_state_diff = [{lane_run, {2, 1}, failed}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_run, {2, 1}, removed},
                    {lane_runs, [{1, 1}], rerunable},
                    workflow_cancelled
                ]
            }
        }]
    }).


lane_failed_in_advance_is_not_removed_if_first_lane_run_successfully_finished() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        workflow_schema_dump_or_draft = ?ECHO_2_LANES_ATM_WORKFLOW_SCHEMA_DRAFT,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [
                #atm_lane_run_execution_test_spec{
                    selector = {1, 1},
                    prepare_lane = #atm_step_mock_spec{
                        defer_after = {handle_lane_execution_stopped, after_step, {2, 1}}
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        after_step_exp_state_diff = [
                            {lane_run, {1, 1}, finished},
                            {lane_run, {2, 1}, run_num_set, 1},
                            workflow_stopping
                        ]
                    }
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    prepare_lane = #atm_step_mock_spec{
                        % Due to lane preparation failure 'handle_lane_execution_stopped' was called
                        % from within lane preparation
                        after_step_exp_state_diff = no_diff
                    },
                    create_run = #atm_step_mock_spec{
                        strategy = {passthrough_with_result_override, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                    },
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_exp_state_diff = [
                            {all_tasks, {2, 1}, interrupted},
                            {lane_run, {2, 1}, stopping}
                        ],
                        after_step_exp_state_diff = [{lane_run, {2, 1}, failed}]
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = [
                    {lane_runs, [{1, 1}, {2, 1}], rerunable},
                    workflow_failed
                ]
            }
        }]
    }).
