%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests of repeating (rerunning or retrying) atm workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_repeat_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    repeat_not_ended_atm_workflow_execution/0
]).


-define(SRC_STORE_ID, <<"st_1">>).

-define(ATM_TASK_SCHEMA_DRAFT(__ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = []
}).

-define(ATM_LANE_SCHEMA_DRAFT(__TASK_ID, __MAX_RETRIES), #atm_lane_schema_draft{
    parallel_boxes = [#atm_parallel_box_schema_draft{
        tasks = [?ATM_TASK_SCHEMA_DRAFT(__TASK_ID)]
    }],
    store_iterator_spec = #atm_store_iterator_spec_draft{
        store_schema_id = ?SRC_STORE_ID,
        max_batch_size = ?RAND_INT(5, 8)
    },
    max_retries = __MAX_RETRIES
}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"repeat">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?SRC_STORE_ID, lists:seq(1, 20))
        ],
        lanes = [
            ?ATM_LANE_SCHEMA_DRAFT(<<"t1">>, 0),
            ?ATM_LANE_SCHEMA_DRAFT(<<"t2">>, 1)
        ]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(#atm_data_spec{type = atm_integer_type})
    }}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


repeat_not_ended_atm_workflow_execution() ->
    Lane1Run1StepMockSpecBase = #atm_step_mock_spec{
        before_step_hook = fun(AtmMockCallCtx) -> assert_not_repeatable({1, 1}, AtmMockCallCtx) end,
        after_step_hook = fun(AtmMockCallCtx) -> assert_not_repeatable({1, 1}, AtmMockCallCtx) end
    },
    Lane2Run1StepHookBase = fun(AtmMockCallCtx) ->
        assert_not_repeatable({1, 1}, AtmMockCallCtx),
        assert_not_repeatable({2, 1}, AtmMockCallCtx)
    end,
    Lane2Run1StepMockSpecBase = #atm_step_mock_spec{
        before_step_hook = Lane2Run1StepHookBase,
        after_step_hook = Lane2Run1StepHookBase
    },

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
                    selector = {1, 1},
                    prepare_lane = Lane1Run1StepMockSpecBase,
                    create_run = Lane1Run1StepMockSpecBase,
                    run_task_for_item = Lane1Run1StepMockSpecBase,
                    process_task_result_for_item = Lane1Run1StepMockSpecBase,
                    handle_task_execution_stopped = Lane1Run1StepMockSpecBase,
                    handle_lane_execution_stopped = Lane1Run1StepMockSpecBase
                },
                #atm_lane_run_execution_test_spec{
                    selector = {2, 1},
                    prepare_lane = Lane2Run1StepMockSpecBase,
                    create_run = Lane2Run1StepMockSpecBase,
                    run_task_for_item = Lane2Run1StepMockSpecBase,
                    process_task_result_for_item = Lane2Run1StepMockSpecBase,
                    handle_task_execution_stopped = Lane2Run1StepMockSpecBase,
                    handle_lane_execution_stopped = #atm_step_mock_spec{
                        before_step_hook = fun(AtmMockCallCtx) ->
                            atm_workflow_execution_test_runner:pause_workflow_execution(AtmMockCallCtx),
                            assert_not_repeatable({1, 1}, AtmMockCallCtx),
                            assert_not_repeatable({2, 1}, AtmMockCallCtx)
                        end,
                        before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({2, 1}, ExpState0),
                            {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                        end,
                        after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                            ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_paused({2, 1}, ExpState0),
                            {true, ExpState1}
                        end,
                        after_step_hook = Lane2Run1StepHookBase
                    }
                }
            ],
            handle_workflow_execution_stopped = #atm_step_mock_spec{
                before_step_hook = Lane2Run1StepHookBase,
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                end,
                after_step_hook = Lane2Run1StepHookBase
            }
        }]
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_not_repeatable(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_repeatable(AtmLaneRunSelector, AtmMockCallCtx) ->
    assert_not_repeatable(rerun, AtmLaneRunSelector, AtmMockCallCtx),
    assert_not_repeatable(retry, AtmLaneRunSelector, AtmMockCallCtx).


%% @private
-spec assert_not_repeatable(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_repeatable(RepeatType, AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertThrow(
        ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_ENDED,
        atm_workflow_execution_test_runner:repeat_workflow_execution(
            RepeatType, AtmLaneRunSelector, AtmMockCallCtx
        )
    ).
