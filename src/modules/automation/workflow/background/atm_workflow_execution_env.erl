%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution environment which consists of conditions in which
%%% specific workflow is being executed (e.g. mapping of store schema id to
%%% actual store id).
%%% Main uses of automation workflow environment are:
%%% 1) mapping of store schema id to actual store id.
%%% 2) acquisition of automation workflow context.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_env).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    build/5,
    get_workflow_store_id/2,
    acquire_workflow_execution_auth/1
]).


-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_store_registry :: atm_workflow_execution:store_registry(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record(),
    task_store_registry :: undefined | atm_task_execution_factory:task_store_registry()
}).
-type record() :: #atm_workflow_execution_env{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    od_space:id(),
    atm_workflow_execution:id(),
    atm_workflow_execution:store_registry(),
    undefined | atm_store_container:record(),
    undefined | atm_task_execution_factory:task_store_registry()
) ->
    record().
build(
    SpaceId,
    AtmWorkflowExecutionId,
    AtmWorkflowStoreRegistry,
    AtmWorkflowAuditLogStoreContainer,
    AtmTaskStoreRegistry
) ->
    #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_store_registry = AtmWorkflowStoreRegistry,
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer,
        task_store_registry = AtmTaskStoreRegistry
    }.


-spec get_workflow_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_workflow_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    workflow_store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.


-spec acquire_workflow_execution_auth(record()) -> atm_workflow_execution_auth:record().
acquire_workflow_execution_auth(#atm_workflow_execution_env{
    space_id = SpaceId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    CreatorUserCtx = atm_workflow_execution_session:acquire(AtmWorkflowExecutionId),
    atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, CreatorUserCtx).
