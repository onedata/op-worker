%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning cancellation of ongoing automation workflow
%%% execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_ongoing_workflow_execution_cancellation_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    cancel_finished_atm_workflow_execution_after_test/0
]).


-define(ECHO_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"echo">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            #atm_store_schema_draft{
                id = <<"st1">>,
                name = <<"st1">>,
                type = list,
                config = #atm_list_store_config{item_data_spec = #atm_data_spec{
                    type = atm_integer_type
                }},
                requires_initial_content = false,
                default_initial_content = [1, 2, 3]
            },
            #atm_store_schema_draft{
                id = <<"st2">>,
                name = <<"st2">>,
                type = list,
                config = #atm_list_store_config{item_data_spec = #atm_data_spec{
                    type = atm_integer_type
                }},
                requires_initial_content = false
            }
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                tasks = [?ECHO_TASK_DRAFT(
                    <<"st2">>,
                    #atm_list_store_content_update_options{function = append}
                )]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = <<"st1">>
            },
            max_retries = 0
        }]
    },
    supplementary_lambdas = #{<<"echo">> => #{1 => ?ECHO_LAMBDA_DRAFT}}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


cancel_finished_atm_workflow_execution_after_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?ECHO_SCHEMA_DRAFT,
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
