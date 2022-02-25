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

-export([
    create_first_lane_run_failure_test/0
]).


-define(ECHO_LAMBDA_DRAFT, #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = <<"test/echo">>
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type}
    }]
}).

-define(ECHO_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [#atm_store_schema_draft{
            id = <<"st1">>,
            type = list,
            config = #atm_list_store_config{item_data_spec = #atm_data_spec{
                type = atm_integer_type
            }},
            requires_initial_content = false,
            default_initial_content = [1, 2, 3]
        }],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                tasks = [#atm_task_schema_draft{
                    lambda_id = <<"echo">>,
                    lambda_revision_number = 1,
                    argument_mappings = [#atm_task_schema_argument_mapper{
                        argument_name = <<"val">>,
                        value_builder = #atm_task_argument_value_builder{
                            type = iterated_item,
                            recipe = undefined
                        }
                    }],
                    result_mappings = []
                }]
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


create_first_lane_run_failure_test() ->
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
                        {true, atm_workflow_execution_exp_state_builder:report_lane_run_started_preparing(
                            {1, 1}, ExpState0
                        )}
                    end,
                    mock_result = {true, ?ERROR_INTERNAL_SERVER_ERROR}
                },
                prepare_lane = #atm_step_mock_spec{
                    after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                        ExpState1 = atm_workflow_execution_exp_state_builder:report_lane_run_failed({1, 1}, ExpState0),
                        {true, atm_workflow_execution_exp_state_builder:report_workflow_execution_aborting(ExpState1)}
                    end
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:report_workflow_execution_failed(ExpState0)}
                end
            }
        }]
    }).
