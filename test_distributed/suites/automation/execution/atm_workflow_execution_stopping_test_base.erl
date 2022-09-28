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
-include("modules/automation/atm_execution.hrl").

-export([
    stopping_reason_cancel_overrides_pause/0
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

-define(ATM_WORKFLOW_SCHEMA_DRAFT,
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(?FUNCTION_NAME),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, lists:seq(1, ?LAST_ITEM)),
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                        ?ATM_TASK_SCHEMA_DRAFT(<<"task0">>)
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
                            tasks = [?ATM_TASK_SCHEMA_DRAFT(<<"task1">>)]
                        },
                        #atm_parallel_box_schema_draft{
                            id = <<"pb2">>,
                            tasks = [?ATM_TASK_SCHEMA_DRAFT(<<"task2">>)]
                        }
                    ],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?ITERATED_STORE_SCHEMA_ID,
                        max_batch_size = ?RAND_INT(5, 8)
                    },
                    max_retries = 0
                }
            ]
        },
        supplementary_lambdas = #{
            ?ECHO_LAMBDA_ID => #{
                ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?INTEGER_DATA_SPEC)
            }
        }
    }
).

-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


stopping_reason_cancel_overrides_pause() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
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
                        strategy = fun(#atm_mock_call_ctx{call_args = [_, _, _, ItemBatch, _]}) ->
                            % Delay execution of last batch to ensure it happens
                            % after execution is paused
                            case lists:member(?LAST_ITEM, ItemBatch) of
                                true -> {passthrough_with_delay, timer:seconds(1)};
                                false -> passthrough
                            end
                        end,
                        after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_, _, AtmTaskExecutionId, ItemsBatch, _]
                        }) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_all_tasks_stopping({2, 1}, ExpState0),
                            ExpState2 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState1),
                            ExpState3 = atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState2),
                            {true, atm_workflow_execution_exp_state_builder:expect_task_items_moved_from_processing_to_processed(
                                AtmTaskExecutionId, length(ItemsBatch), ExpState3
                            )}
                        end
                    },
                    handle_task_execution_stopped = #atm_step_mock_spec{
                        % Ensure cancel prevails against pause
                        before_step_hook = fun atm_workflow_execution_test_runner:cancel_workflow_execution/1,
                        after_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,

                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{
                            workflow_execution_exp_state = ExpState0,
                            call_args = [_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId]
                        }) ->
                            case atm_workflow_execution_exp_state_builder:get_task_selector(AtmTaskExecutionId, ExpState0) of
                                {_, <<"pb1">>, <<"task1">>} ->
                                    {true, expect_task_cancelled(AtmTaskExecutionId, ExpState0)};
                                {_, <<"pb2">>, <<"task2">>} ->
                                    % execution is paused after first items for task1 finished processing
                                    % and as such no item was ever scheduled for task2
                                    {true, expect_task_skipped(AtmTaskExecutionId, ExpState0)}
                            end
                        end
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
