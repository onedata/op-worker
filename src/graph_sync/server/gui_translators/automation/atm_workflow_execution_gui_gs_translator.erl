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
-include("modules/automation/atm_execution.hrl").

%% API
-export([translate_value/2, translate_resource/2]).

%% Util functions
-export([
    translate_atm_workflow_execution/1,
    translate_atm_workflow_execution_summary/1
]).


%%%===================================================================
%%% API
%%%===================================================================



-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = batch}, DiscardResults) ->
    TranslateDiscardResultFun = fun
        (_, ok) ->
            #{<<"success">> => true};
        (_, {error, _} = Error) ->
            #{<<"success">> => false, <<"error">> => errors:to_json(Error)}
    end,
    maps:map(TranslateDiscardResultFun, DiscardResults).


-spec translate_resource(gri:gri(), Data :: term()) -> gs_protocol:data().
translate_resource(#gri{aspect = instance, scope = private}, AtmWorkflowExecution) ->
    translate_atm_workflow_execution(AtmWorkflowExecution);

translate_resource(#gri{aspect = summary, scope = private}, AtmWorkflowExecution) ->
    translate_atm_workflow_execution_summary(AtmWorkflowExecution).


%%%===================================================================
%%% Util functions
%%%===================================================================


-spec translate_atm_workflow_execution(atm_workflow_execution:record()) ->
    json_utils:json_map().
translate_atm_workflow_execution(AtmWorkflowExecution = #atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId,

    name = Name,
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

    store_registry = AtmStoreRegistry,
    system_audit_log_store_id = AtmWorkflowAuditLogStoreId,

    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime,
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

        <<"name">> => Name,
        <<"atmWorkflowSchemaSnapshot">> => gri:serialize(#gri{
            type = op_atm_workflow_schema_snapshot, id = AtmWorkflowSchemaSnapshotId,
            aspect = instance, scope = private
        }),
        <<"lambdaSnapshotRegistry">> => AtmLambdaSnapshotRegistry,

        <<"storeRegistry">> => AtmStoreRegistry,
        <<"systemAuditLogId">> => utils:undefined_to_null(AtmWorkflowAuditLogStoreId),

        <<"lanes">> => lists:map(
            fun(AtmLaneIndex) -> atm_lane_execution:to_json(AtmLaneIndex, AtmWorkflowExecution) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"suspendTime">> => SuspendTime,
        <<"finishTime">> => FinishTime
    }.


-spec translate_atm_workflow_execution_summary(atm_workflow_execution:summary()) ->
    json_utils:json_map().
translate_atm_workflow_execution_summary(#atm_workflow_execution_summary{
    atm_workflow_execution_id = AtmWorkflowExecutionId,

    name = Name,
    atm_workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    atm_inventory_id = AtmInventoryId,

    status = AtmWorkflowExecutionStatus,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime,
    finish_time = FinishTime
}) ->
    #{
        <<"gri">> => gri:serialize(#gri{
            type = op_atm_workflow_execution, id = AtmWorkflowExecutionId,
            aspect = summary, scope = private
        }),

        <<"atmWorkflowExecution">> => gri:serialize(#gri{
            type = op_atm_workflow_execution, id = AtmWorkflowExecutionId,
            aspect = instance, scope = private
        }),

        <<"name">> => Name,
        <<"atmWorkflowSchemaRevisionNumber">> => AtmWorkflowSchemaRevisionNum,
        <<"atmInventory">> => gri:serialize(#gri{
            type = op_atm_inventory, id = AtmInventoryId,
            aspect = instance, scope = private
        }),

        <<"status">> => atom_to_binary(AtmWorkflowExecutionStatus, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"suspendTime">> => SuspendTime,
        <<"finishTime">> => FinishTime
    }.
