%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing automation (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_atm).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    schedule_workflow_execution/6,
    cancel_workflow_execution/2,
    repeat_workflow_execution/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_workflow_execution(
    session:id(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    atm_workflow_execution_api:store_initial_content_overlay(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
schedule_workflow_execution(
    SessionId,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    AtmStoreInitialContentOverlay,
    CallbackUrl
) ->
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #atm_workflow_execution_schedule_request{
        atm_workflow_schema_id = AtmWorkflowSchemaId,
        atm_workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
        store_initial_content_overlay = AtmStoreInitialContentOverlay,
        callback_url = CallbackUrl
    }).


-spec cancel_workflow_execution(session:id(), atm_workflow_execution:id()) ->
    ok | no_return().
cancel_workflow_execution(SessionId, AtmWorkflowExecutionId) ->
    SpaceGuid = atm_workflow_execution_id_to_space_guid(AtmWorkflowExecutionId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #atm_workflow_execution_cancel_request{
        atm_workflow_execution_id = AtmWorkflowExecutionId
    }).


-spec repeat_workflow_execution(
    session:id(),
    atm_workflow_execution:repeat_type(),
    atm_workflow_execution:id(),
    atm_lane_execution:lane_run_selector()
) ->
    ok | errors:error().
repeat_workflow_execution(SessionId, RepeatType, AtmWorkflowExecutionId, AtmLaneRunSelector) ->
    SpaceGuid = atm_workflow_execution_id_to_space_guid(AtmWorkflowExecutionId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #atm_workflow_execution_repeat_request{
        type = RepeatType,
        atm_workflow_execution_id = AtmWorkflowExecutionId,
        atm_lane_run_selector = AtmLaneRunSelector
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec atm_workflow_execution_id_to_space_guid(atm_workflow_execution:id()) ->
    file_id:file_guid() | no_return().
atm_workflow_execution_id_to_space_guid(AtmWorkflowExecutionId) ->
    #atm_workflow_execution{space_id = SpaceId} = ?check(atm_workflow_execution_api:get(
        AtmWorkflowExecutionId
    )),
    fslogic_file_id:spaceid_to_space_dir_guid(SpaceId).
