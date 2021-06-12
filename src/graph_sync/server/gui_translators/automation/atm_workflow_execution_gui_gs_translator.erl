%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation workflow execution entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, #atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId,

    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

    store_registry = AtmStoreRegistry,
    lanes = AtmLaneExecutions,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #{
        <<"space">> => gri:serialize(#gri{
            type = op_space, id = SpaceId,
            aspect = instance, scope = private
        }),
        <<"atmInventory">> => gri:serialize(#gri{
            type = op_atm_inventory, id = AtmInventoryId,
            aspect = instance, scope = private
        }),
        <<"atmWorkflowSchemaSnapshot">> => gri:serialize(#gri{
            type = op_atm_workflow_schema_snapshot, id = AtmWorkflowSchemaSnapshotId,
            aspect = instance, scope = private
        }),
        <<"lambdaSnapshotRegistry">> => AtmLambdaSnapshotRegistry,

        <<"storeRegistry">> => AtmStoreRegistry,
        <<"lanes">> => lists:map(fun atm_lane_execution:to_json/1, AtmLaneExecutions),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime
    }.
