%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_workflow_execution_model).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([build/3]).


%%%===================================================================
%%% API
%%%===================================================================


build(SpaceId, ApproxScheduleTime, AtmLaneSchemas) ->
    FirstAtmLaneSchema = hd(AtmLaneSchemas),

    FirstLane = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_initial_regular_lane_run(1)]
    },
    LeftoverLanes = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, tl(AtmLaneSchemas)),

    #{
        <<"spaceId">> => SpaceId,
%%        <<"atmInventoryId">> => AtmInventoryId,
%%        <<"name">> => Name,
%%        <<"atmWorkflowSchemaSnapshotId">> => AtmWorkflowSchemaSnapshotId,
%%        <<"lambdaSnapshotRegistry">> => AtmLambdaSnapshotRegistry,
%%
%%        <<"storeRegistry">> => AtmStoreRegistry,
%%        <<"systemAuditLogId">> => AtmWorkflowAuditLogId,

        <<"lanes">> => [FirstLane | LeftoverLanes],

        <<"status">> => atom_to_binary(?SCHEDULED_STATUS, utf8),

        <<"approxScheduleTime">> => ApproxScheduleTime,
        <<"approxStartTime">> => 0,
        <<"approxFinishTime">> => 0
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


build_initial_regular_lane_run(RunNum) ->
    #{
        <<"runNumber">> => RunNum,
        <<"originRunNumber">> => null,
        <<"status">> => atom_to_binary(?SCHEDULED_STATUS, utf8),
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => <<"regular">>,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.

%%
%% pbox:
%%
%%    #{
%%        <<"schemaId">> => AtmParallelBoxSchemaId,
%%        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
%%        <<"taskRegistry">> => AtmTaskExecutionRegistry
%%    }.


-spec atm_workflow_execution_to_json(atm_workflow_execution:record()) ->
    json_utils:json_map().
atm_workflow_execution_to_json(#atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId,

    name = Name,
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

    store_registry = AtmStoreRegistry,
    system_audit_log_id = AtmWorkflowAuditLogId,

    lanes = AtmLaneExecutions,
    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #{
        <<"spaceId">> => SpaceId,
%%        <<"atmInventoryId">> => AtmInventoryId,
%%        <<"name">> => Name,
%%        <<"atmWorkflowSchemaSnapshotId">> => AtmWorkflowSchemaSnapshotId,
%%        <<"lambdaSnapshotRegistry">> => AtmLambdaSnapshotRegistry,
%%
%%        <<"storeRegistry">> => AtmStoreRegistry,
%%        <<"systemAuditLogId">> => AtmWorkflowAuditLogId,

        <<"lanes">> => lists:map(
            fun(LaneIndex) -> atm_lane_execution:to_json(maps:get(LaneIndex, AtmLaneExecutions)) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime
    }.
