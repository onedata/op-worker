%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% atm_workflow_execution entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_rest_translator).
-author("Lukasz Opiola").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = instance}, _, resource, {#gri{id = AtmWorkflowExecutionId}, _}) ->
    PathTokens = [<<"automation">>, <<"execution">>, <<"workflows">>, AtmWorkflowExecutionId],
    ?CREATED_REPLY(PathTokens, #{<<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{id = AtmWorkflowExecutionId}, #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    name = Name,
    atm_inventory_id = AtmInventoryId,
    space_id = SpaceId,
    user_id = UserId,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime,

    lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
    store_registry = AtmStoreRegistry,
    system_audit_log_id = AtmWorkflowAuditLogId,

    lanes = AtmLaneExecutions,
    lanes_num = AtmLanesNum
}) ->
    ?OK_REPLY(#{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"atmWorkflowSchemaSnapshotId">> => AtmWorkflowSchemaSnapshotId,
        <<"name">> => Name,
        <<"atmInventoryId">> => AtmInventoryId,
        <<"spaceId">> => SpaceId,
        <<"userId">> => UserId,

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime,

        <<"lambdaSnapshotRegistry">> => AtmLambdaSnapshotRegistry,
        <<"storeRegistry">> => AtmStoreRegistry,
        <<"systemAuditLogId">> => utils:undefined_to_null(AtmWorkflowAuditLogId),

        <<"lanes">> => lists:map(
            fun(LaneIndex) -> atm_lane_execution:to_json(maps:get(LaneIndex, AtmLaneExecutions)) end,
            lists:seq(1, AtmLanesNum)
        )
    }).
