%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests of resuming atm workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_resume_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    resume_atm_workflow_execution_paused_while_scheduled/0
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).
-define(TARGET_STORE_SCHEMA_ID, <<"target_store_id">>).

-define(INTEGER_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).

-define(ECHO_ATM_TASK_SCHEMA_DRAFT(__ID), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = ?ECHO_ARG_NAME,
        store_schema_id = ?TARGET_STORE_SCHEMA_ID,
        store_content_update_options = #atm_list_store_content_update_options{function = append}
    }]
}).

-define(ITERATOR_SPEC_DRAFT, #atm_store_iterator_spec_draft{store_schema_id = ?ITERATED_STORE_SCHEMA_ID}).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__TESTCASE),
    #atm_workflow_schema_dump_draft{
        name = str_utils:to_binary(__TESTCASE),
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, [?RAND_INT(100)]),
                ?INTEGER_LIST_STORE_SCHEMA_DRAFT(?TARGET_STORE_SCHEMA_ID)
            ],
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        id = <<"pb1">>,
                        tasks = [
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task1">>),
                            ?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task2">>)
                        ]
                    }],
                    store_iterator_spec = ?ITERATOR_SPEC_DRAFT,
                    max_retries = 0
                },
                #atm_lane_schema_draft{
                    parallel_boxes = [#atm_parallel_box_schema_draft{
                        tasks = [?ECHO_ATM_TASK_SCHEMA_DRAFT(<<"task3">>)]
                    }],
                    store_iterator_spec = ?ITERATOR_SPEC_DRAFT
                }
            ]
        },
        supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
            ?ECHO_LAMBDA_REVISION_NUM => ?ECHO_LAMBDA_DRAFT(?INTEGER_DATA_SPEC, ?RAND_ELEMENT([
                return_value, file_pipe
            ]))
        }}
    }
).
-define(ATM_WORKFLOW_SCHEMA_DRAFT, ?ATM_WORKFLOW_SCHEMA_DRAFT(?FUNCTION_NAME)).


-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% Tests
%%%===================================================================


resume_atm_workflow_execution_paused_while_scheduled() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ATM_WORKFLOW_SCHEMA_DRAFT,
        workflow_schema_revision_num = 1,
        incarnations = [
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 1,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_hook = fun atm_workflow_execution_test_runner:pause_workflow_execution/1,
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_stopping({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_stopping(ExpState1)}
                            end,
                            after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState}) ->
                                {true, atm_workflow_execution_exp_state_builder:expect_lane_run_paused({1, 1}, ExpState)}
                            end
                        },
                        handle_lane_execution_stopped = #atm_step_mock_spec{
                            after_step_exp_state_diff = no_diff
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {prepare_lane, after_step, {1, 1}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ],
                handle_workflow_execution_stopped = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_paused(ExpState0)}
                    end
                },
                after_hook = fun atm_workflow_execution_test_runner:resume_workflow_execution/1
            },
            #atm_workflow_execution_incarnation_test_spec{
                incarnation_num = 2,
                lane_runs = [
                    #atm_lane_run_execution_test_spec{
                        selector = {1, 1},
                        prepare_lane = #atm_step_mock_spec{
                            before_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                                ExpState1 = atm_workflow_execution_exp_state_builder:expect_lane_run_resuming({1, 1}, ExpState0),
                                {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_resuming(ExpState1)}
                            end
                        }
                    },
                    #atm_lane_run_execution_test_spec{
                        selector = {2, 1},
                        prepare_lane = #atm_step_mock_spec{
                            defer_after = {prepare_lane, after_step, {1, 1}},
                            after_step_exp_state_diff = no_diff
                        }
                    }
                ]
            }
        ]
    }).
