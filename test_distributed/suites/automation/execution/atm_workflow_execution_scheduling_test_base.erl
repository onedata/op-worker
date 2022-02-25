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

-export([
    atm_workflow_with_no_lanes_scheduling_should_fail_test/0,
    atm_workflow_with_empty_lane_scheduling_should_fail_test/0,
    atm_workflow_with_empty_parallel_box_scheduling_should_fail_test/0
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
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID
                    }
                }
            ]
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
            lanes = [
                #atm_lane_schema_draft{
                    parallel_boxes = [
                        #atm_parallel_box_schema_draft{
                            tasks = []
                        }
                    ],
                    store_iterator_spec = #atm_store_iterator_spec_draft{
                        store_schema_id = ?EXAMPLE_INTEGER_LIST_STORE_SCHEMA_ID
                    }
                }
            ]
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
    atm_workflow_execution_api:store_initial_contents()
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
