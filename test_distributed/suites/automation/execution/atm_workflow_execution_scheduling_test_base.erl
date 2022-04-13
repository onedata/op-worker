%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests of scheduling non executable automation workflow schemas.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_scheduling_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").

-export([
    atm_workflow_with_no_lanes_scheduling_should_fail_test/0,
    atm_workflow_with_empty_lane_scheduling_should_fail_test/0,
    atm_workflow_with_empty_parallel_box_scheduling_should_fail_test/0,
    atm_workflow_scheduling_with_openfaas_not_configured_should_fail_test/0,
    atm_workflow_with_invalid_initial_store_content_scheduling_should_fail_test/0,
    atm_workflow_execution_cancelled_in_scheduled_status_test/0
]).


-define(EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID, <<"st1">>).

-define(EXAMPLE_INTEGER_LIST_STORE_SCHEMA_DRAFT, #atm_store_schema_draft{
    id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID,
    type = list,
    config = #atm_list_store_config{item_data_spec = #atm_data_spec{
        type = atm_integer_type
    }},
    requires_initial_content = false,
    default_initial_content = [1, 2, 3]
}).

-define(EXAMPLE_EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"example_executable_atm_workflow">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_DRAFT],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                tasks = [
                    ?ECHO_TASK_DRAFT(
                        ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
                        #atm_audit_log_store_content_update_options{function = append}
                    )
                ]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID
            }
        }]
    },
    supplementary_lambdas = #{<<"echo">> => #{1 => ?ECHO_LAMBDA_DRAFT}}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


atm_workflow_with_no_lanes_scheduling_should_fail_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_no_lanes">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_DRAFT],
            lanes = []
        }
    }),

    ?assertThrow(
        ?ERROR_ATM_WORKFLOW_EMPTY,
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


atm_workflow_with_empty_lane_scheduling_should_fail_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_empty_lane">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_DRAFT],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID
                }
            }]
        }
    }),
    EmptyAtmLaneSchemaId = atm_workflow_schema_query:run(
        atm_test_inventory:get_workflow_schema_revision(1, AtmWorkflowSchemaId),
        [lanes, 1, id]
    ),

    ?assertThrow(
        ?ERROR_ATM_LANE_EMPTY(EmptyAtmLaneSchemaId),
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


atm_workflow_with_empty_parallel_box_scheduling_should_fail_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_empty_parallel_box">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_DRAFT],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{
                    tasks = []
                }],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID
                }
            }]
        }
    }),
    EmptyAtmParallelBoxSchemaId = atm_workflow_schema_query:run(
        atm_test_inventory:get_workflow_schema_revision(1, AtmWorkflowSchemaId),
        [lanes, 1, parallel_boxes, 1, id]
    ),

    ?assertThrow(
        ?ERROR_ATM_PARALLEL_BOX_EMPTY(EmptyAtmParallelBoxSchemaId),
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


atm_workflow_scheduling_with_openfaas_not_configured_should_fail_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?EXAMPLE_EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT
    ),

    ?assertThrow(
        ?ERROR_ATM_OPENFAAS_NOT_CONFIGURED,
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


% NOTE: Only single example of store content type and initial content mismatch is checked
% to assert overall behaviour (failure to schedule execution). More such combinations are
% checked in respective store test suites.
atm_workflow_with_invalid_initial_store_content_scheduling_should_fail_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?EXAMPLE_EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT
    ),
    InvalidInitialItem = <<"STR">>,

    ExpError = ?ERROR_ATM_STORE_CREATION_FAILED(
        ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID,
        ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED([InvalidInitialItem], atm_array_type, #{
            <<"$[0]">> => errors:to_json(?ERROR_ATM_DATA_TYPE_UNVERIFIED(
                InvalidInitialItem, atm_integer_type
            ))
        })
    ),
    ?assertThrow(ExpError, try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1, #{
        ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID => [InvalidInitialItem]
    })).


atm_workflow_execution_cancelled_in_scheduled_status_test() ->
    atm_workflow_execution_test_runner:run(#atm_workflow_execution_test_spec{
        provider = ?PROVIDER_SELECTOR,
        user = ?USER_SELECTOR,
        space = ?SPACE_SELECTOR,
        workflow_schema_dump_or_draft = ?EXAMPLE_EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT,
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
                }
            }],
            handle_workflow_execution_ended = #atm_step_mock_spec{
                after_step_exp_state_diff = fun(#atm_mock_call_ctx{workflow_execution_exp_state = ExpState0}) ->
                    {true, atm_workflow_execution_exp_state_builder:expect_workflow_execution_cancelled(ExpState0)}
                end
            }
        }]
    }).


%===================================================================
% Internal functions
%===================================================================


%% @private
-spec try_to_schedule_workflow_execution(
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
try_to_schedule_workflow_execution(AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum) ->
    try_to_schedule_workflow_execution(AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum, #{}).


%% @private
-spec try_to_schedule_workflow_execution(
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    atm_workflow_execution_api:store_initial_content_overlay()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
try_to_schedule_workflow_execution(
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    StoreInitialContents
) ->
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?PROVIDER_SELECTOR),
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    ?erpc(?PROVIDER_SELECTOR, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        StoreInitialContents, undefined
    )).
