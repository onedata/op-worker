%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bases for tests concerning scheduling automation workflow schemas.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_scheduling_test_base).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("atm/atm_test_schema_drafts.hrl").

-export([
    schedule_atm_workflow_with_no_lanes_test/0,
    schedule_atm_workflow_with_empty_lane_test/0,
    schedule_atm_workflow_with_empty_parallel_box_test/0,
    schedule_atm_workflow_with_openfaas_not_configured_test/0,
    schedule_atm_workflow_with_invalid_initial_store_content_test/0
]).


-define(EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT, #atm_workflow_schema_dump_draft{
    name = <<"example_executable_atm_workflow">>,
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_src">>, [1, 9, 64]),
            ?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_dst">>)
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{tasks = [
                ?ECHO_TASK_DRAFT(<<"st_dst">>, #atm_list_store_content_update_options{function = append})
            ]}],
            store_iterator_spec = #atm_store_iterator_spec_draft{store_schema_id = <<"st_src">>}
        }]
    },
    supplementary_lambdas = #{?ECHO_LAMBDA_ID => #{
        ?ECHO_LAMBDA_REVISION_NUM => ?INTEGER_ECHO_LAMBDA_DRAFT
    }}
}).


%%%===================================================================
%%% Tests
%%%===================================================================


schedule_atm_workflow_with_no_lanes_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_no_lanes">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_src">>)],
            lanes = []
        }
    }),

    ?assertThrow(
        ?ERROR_ATM_WORKFLOW_EMPTY,
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


schedule_atm_workflow_with_empty_lane_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_empty_lane">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_src">>)],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = <<"st_src">>
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


schedule_atm_workflow_with_empty_parallel_box_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(#atm_workflow_schema_dump_draft{
        name = <<"atm_workflow_with_empty_parallel_box">>,
        revision_num = 1,
        revision = #atm_workflow_schema_revision_draft{
            stores = [?INTEGER_LIST_STORE_SCHEMA_DRAFT(<<"st_src">>)],
            lanes = [#atm_lane_schema_draft{
                parallel_boxes = [#atm_parallel_box_schema_draft{
                    tasks = []
                }],
                store_iterator_spec = #atm_store_iterator_spec_draft{
                    store_schema_id = <<"st_src">>
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


schedule_atm_workflow_with_openfaas_not_configured_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT
    ),

    ?assertThrow(
        ?ERROR_ATM_OPENFAAS_NOT_CONFIGURED,
        try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1)
    ).


% NOTE: Only single example of store content type and initial content mismatch is checked
% to assert overall behaviour (failure to schedule execution). More such combinations are
% checked in respective store test suites.
schedule_atm_workflow_with_invalid_initial_store_content_test() ->
    AtmWorkflowSchemaId = atm_test_inventory:add_workflow_schema(
        ?EXECUTABLE_ATM_WORKFLOW_SCHEMA_DRAFT
    ),
    InvalidInitialItem = <<"STR">>,

    ExpError = ?ERROR_ATM_STORE_CREATION_FAILED(
        <<"st_src">>,
        ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED([InvalidInitialItem], atm_array_type, #{
            <<"$[0]">> => errors:to_json(?ERROR_ATM_DATA_TYPE_UNVERIFIED(
                InvalidInitialItem, atm_integer_type
            ))
        })
    ),
    ?assertThrow(ExpError, try_to_schedule_workflow_execution(AtmWorkflowSchemaId, 1, #{
        <<"st_src">> => [InvalidInitialItem]
    })).


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
