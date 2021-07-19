%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs automation-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_atm).
-author("Bartosz Walkowicz").

-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([schedule_workflow_execution/5, cancel_workflow_execution/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_workflow_execution(
    session:id(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_execution_api:store_initial_values(),
    undefined | http_client:url()
) ->
    {ok, atm_workflow_execution:id(), atm_workflow_execution:record()} | lfm:error_reply().
schedule_workflow_execution(SessId, SpaceId, AtmWorkflowSchemaId, AtmStoreInitialValues, CallbackUrl) ->
    remote_utils:call_fslogic(
        SessId,
        provider_request,
        fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
        #schedule_atm_workflow_execution{
            atm_workflow_schema_id = AtmWorkflowSchemaId,
            store_initial_values = AtmStoreInitialValues,
            callback_url = CallbackUrl
        },
        fun(#atm_workflow_execution_scheduled{id = AtmWorkflowExecutionId, record = AtmWorkflowExecution}) ->
            {ok, AtmWorkflowExecutionId, AtmWorkflowExecution}
        end
    ).


-spec cancel_workflow_execution(session:id(), atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:id(), atm_workflow_execution:record()} | lfm:error_reply().
cancel_workflow_execution(SessId, AtmWorkflowExecutionId) ->
    % TODO VFS-7800 rm if sending space dir ctx is no longer necessary after introducing new worker
    {ok, #atm_workflow_execution{space_id = SpaceId}} = atm_workflow_execution_api:get(
        AtmWorkflowExecutionId
    ),

    remote_utils:call_fslogic(
        SessId,
        provider_request,
        fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
        #cancel_atm_workflow_execution{atm_workflow_execution_id = AtmWorkflowExecutionId},
        fun(_) -> ok end
    ).
