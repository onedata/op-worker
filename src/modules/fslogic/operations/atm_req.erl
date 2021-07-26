%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests concerning an automation.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_req).
-author("Bartosz Walkowicz").

-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    schedule_workflow_execution/5,
    cancel_workflow_execution/1
]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec schedule_workflow_execution(
    user_ctx:ctx(),
    file_ctx:ctx(),
    od_atm_workflow_schema:id(),
    atm_workflow_execution_api:store_initial_values(),
    undefined | http_client:url()
) ->
    fslogic_worker:provider_response().
schedule_workflow_execution(UserCtx, SpaceDirCtx, AtmWorkflowSchemaId, AtmStoreInitialValues, CallbackUrl) ->
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    try
        {AtmWorkflowExecutionId, AtmWorkflowExecution} = atm_workflow_execution_api:create(
            UserCtx, SpaceId, AtmWorkflowSchemaId, AtmStoreInitialValues, CallbackUrl
        ),
        ?PROVIDER_OK_RESP(#atm_workflow_execution_scheduled{
            id = AtmWorkflowExecutionId,
            record = AtmWorkflowExecution
        })
    catch _:_ ->
        %% TODO VFS-7208 do not catch errors after introducing API errors to fslogic
        #provider_response{status = #status{code = ?EINVAL}}
    end.


-spec cancel_workflow_execution(atm_workflow_execution:id())->
    fslogic_worker:provider_response().
cancel_workflow_execution(AtmWorkflowExecutionId) ->
    try
        ok = atm_workflow_execution_api:cancel(AtmWorkflowExecutionId),
        ?PROVIDER_OK_RESP
    catch _:_ ->
        %% TODO VFS-7208 do not catch errors after introducing API errors to fslogic
        #provider_response{status = #status{code = ?EINVAL}}
    end.
