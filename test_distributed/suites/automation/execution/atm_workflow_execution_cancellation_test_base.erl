%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning cancellation of automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_cancellation_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    cancel_scheduled_atm_workflow_execution_test/0,
    cancel_enqueued_atm_workflow_execution_test/0,
    cancel_active_atm_workflow_execution_test/0,
    cancel_finishing_atm_workflow_execution_test/0,
    cancel_finished_atm_workflow_execution_test/0
]).


-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(__ITEMS_COUNT), #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_src">>, lists:seq(1, __ITEMS_COUNT)),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_dst">>)
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                ?ECHO_TASK_DRAFT(<<"st_dst">>, #atm_list_store_content_update_options{function = append})
            ]}],
            store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_src">>},

            % Check that cancelled executions are not retried automatically
            max_retries = ?RAND_INT(3, 6)
        }]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?INTEGER_ECHO_LAMBDA_DRAFT
    }}
}).
-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(5)).


%%%===================================================================
%%% Tests
%%%===================================================================


cancel_scheduled_atm_workflow_execution_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    before_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                    end,
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                    end
                },
                handle_lane_execution_ended = #atm_step_mock_spec{
                    after_step_exp_state_diff = no_diff
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState0)}
                end
            }
        }]
    }).


cancel_enqueued_atm_workflow_execution_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                prepare_lane = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                    end
                },
                handle_task_execution_ended = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                        workflow_execution_exp_state = ExpState0,
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_task_skipped(
                            AtmTaskExecutionId, ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_task_parallel_box_skipped(
                            AtmTaskExecutionId, ExpState1
                        )}
                    end
                },
                handle_lane_execution_ended = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState0)}
                end
            }
        }]
    }).


cancel_active_atm_workflow_execution_test() ->
    ItemsCount = 100,

    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT(ItemsCount),
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                process_item = #atm_step_mock_spec{
                    before_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                    end
                },
                handle_task_execution_ended = #atm_step_mock_spec{
                    before_step_exp_state_diff = fun(AtmMockCallCtx = #atm_mock_call_ctx{
                        workflow_execution_exp_state = ExpState,
                        call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                    }) ->
                        ExpTaskStats = atm_workflow_execution_exp_state_builder:get_task_stats(
                            AtmTaskExecutionId, ExpState
                        ),
                        % cancel blocks scheduling execution of leftover items
                        % but the ones already scheduled should be finished
                        ?assert(ExpTaskStats < {0, 0, ItemsCount}),

                        % assert all processed items were mapped to st_dst
                        ExpItemsProcessed = element(3, ExpTaskStats),
                        #{<<"items">> := StDstItems} = atm_workflow_execution_test_runner:browse_store(
                            <<"st_dst">>, AtmMockCallCtx
                        ),
                        ?assertEqual(ExpItemsProcessed, length(StDstItems)),

                        {true, ExpState}
                    end
                },
                handle_lane_execution_ended = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState0)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState0)}
                end
            }
        }]
    }).


cancel_finishing_atm_workflow_execution_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                handle_lane_execution_ended = #atm_step_mock_spec{
                    after_step_hook = fun(AtmMockCallCtx) ->
                        % While atm workflow execution as whole has not yet transit to finished status
                        % (last step remaining) the current lane run did. At this point cancel
                        % is no longer possible (execution is treated as successfully ended)
                        ?assertThrow(
                            ?ERROR_ATM_INVALID_STATUS_TRANSITION(?FINISHED_STATUS, ?ABORTING_STATUS),
                            atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                        )
                    end
                }
            }]
        }]
    }).


cancel_finished_atm_workflow_execution_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            incarnation_num = 1,
            lane_runs = [#atm_lane_run_execution_test_spec{selector = {1, 1}}],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_hook = fun(AtmMockCallCtx) ->
                    ?assertThrow(
                        ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED,
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    )
                end
            }
        }]
    }).
