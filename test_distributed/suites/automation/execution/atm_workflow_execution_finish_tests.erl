%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning successfully finished automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_finish_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    finish_atm_workflow_execution/0
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

-define(ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(?FUNCTION_NAME),
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
        ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(
            ?INTEGER_DATA_SPEC, ?RAND_ELEMENT([return_value, file_pipe])
        )}
    }
}).


%%%===================================================================
%%% Tests
%%%===================================================================


finish_atm_workflow_execution() ->
    AssertActionsNotPossibleOnNotStoppedExecution = fun(AtmMockCallCtx) ->
        atm_workflow_execution_test_utils:assert_not_stopped_workflow_execution_can_be_neither_repeated_nor_resumed(
            {1, 1}, AtmMockCallCtx
        )
    end,
    Lane1Run1StepMockSpecBase = #atm_step_mock_spec{
        before_step_hook = AssertActionsNotPossibleOnNotStoppedExecution,
        after_step_hook = AssertActionsNotPossibleOnNotStoppedExecution
    },

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
                prepare_lane = Lane1Run1StepMockSpecBase,
                create_run = Lane1Run1StepMockSpecBase,
                run_task_for_item = Lane1Run1StepMockSpecBase,
                process_task_result_for_item = Lane1Run1StepMockSpecBase,
                handle_task_execution_stopped = Lane1Run1StepMockSpecBase,
                handle_lane_execution_stopped = #atm_step_mock_spec{
                    before_step_hook = AssertActionsNotPossibleOnNotStoppedExecution,
                    after_step_hook = fun(AtmMockCallCtx) ->
                        % While atm workflow execution as whole has not yet transition to finished status
                        % (last step remaining) the current lane run did. At this point stopping it
                        % is no longer possible (execution is treated as successfully ended)
                        lists:foreach(fun(StoppingReason) ->
                            ?assertEqual(
                                ?ERROR_ATM_INVALID_STATUS_TRANSITION(?FINISHED_STATUS, ?STOPPING_STATUS),
                                atm_workflow_execution_test_utils:stop_workflow_execution(
                                    StoppingReason, AtmMockCallCtx
                                )
                            )
                        end, ?STOPPING_REASONS),

                        AssertActionsNotPossibleOnNotStoppedExecution(AtmMockCallCtx)
                    end
                }
            }],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_rerunable({1, 1}, ExpState0),
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_finished(ExpState1)}
                end
            },
            after_hook = fun atm_workflow_execution_test_utils:assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1
        }]
    }).
