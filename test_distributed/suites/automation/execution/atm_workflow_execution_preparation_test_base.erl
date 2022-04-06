%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning automation workflow execution preparation step.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_preparation_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").

-export([
    first_lane_run_preparation_failure_before_run_was_created_test/0,
    first_lane_run_preparation_failure_after_run_was_created_test/0,

    atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created_test/0,
    atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created_test/0
]).


-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [#atm_store_schema_draft{
            id = <<"st1">>,
            type = list,
            config = #atm_list_store_config{item_data_spec = #atm_data_spec{
                type = atm_string_type
            }},
            requires_initial_content = false,
            default_initial_content = [<<"A">>, <<"B">>, <<"C">>]
        }],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                tasks = [?ECHO_TASK_DRAFT(
                    ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
                    #atm_audit_log_store_content_update_options{function = append}
                )]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = <<"st1">>
            }
        }]
    },
    supplementary_lambdas = #{<<"echo">> => #{1 => ?ECHO_LAMBDA_DRAFT}}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


first_lane_run_preparation_failure_before_run_was_created_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    % 'create_run' step execution is mocked entirely so that
                    % no lane run execution component will be created
                    mock_execution = {true, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState0)}
                end
            }
        }]
    }).


first_lane_run_preparation_failure_after_run_was_created_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    % 'create_run' step result is replaced by the one specified below but
                    % the step itself is executed normally so that lane run execution
                    % components (e.g. task executions) can be created
                    mock_execution = {false, {throw, ?ERROR_INTERNAL_SERVER_ERROR}}
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_task_executions_skipped(
                            {1, 1}, ExpState0
                        ),
                        ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_failed({1, 1}, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState2)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_failed(ExpState)}
                end
            }
        }]
    }).


atm_workflow_execution_cancelled_in_preparing_status_before_run_was_created_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
                selector = {1, 1},
                create_run = #atm_step_mock_spec{
                    before_step_hook = fun(AtmMockCallCtx) ->
                        atm_workflow_execution_test_runner:cancel_workflow_execution(AtmMockCallCtx)
                    end,
                    before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState1)}
                    end,
                    % no lane run components should be created
                    after_step_exp_state_diff = fun(_) -> false end
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState)}
                end
            }
        }]
    }).


atm_workflow_execution_cancelled_in_preparing_status_after_run_was_created_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [#atm_workflow_execution_incarnation_test_spec{
            lane_runs = [#atm_lane_run_execution_test_spec{
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
                        ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_aborting({1, 1}, ExpState1),
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_aborting(ExpState2)}
                    end
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_task_executions_skipped(
                            {1, 1}, ExpState0
                        ),
                        {true, atm_workflow_execution_exp_state_builder:expect_lane_run_cancelled({1, 1}, ExpState1)}
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
