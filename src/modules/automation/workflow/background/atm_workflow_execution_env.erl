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
    build/3,
    get_store_id/2,
    acquire_workflow_execution_ctx/1
]).


-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    store_registry :: atm_workflow_execution:store_registry()
}).
-type record() :: #atm_workflow_execution_env{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    od_space:id(),
    atm_workflow_execution:id(),
    atm_workflow_execution:store_registry()
) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, AtmStoreRegistry) ->
    #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        store_registry = AtmStoreRegistry
    }.


-spec get_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.


-spec acquire_workflow_execution_ctx(record()) -> atm_workflow_execution_ctx:record().
acquire_workflow_execution_ctx(#atm_workflow_execution_env{
    space_id = SpaceId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    CreatorUserCtx = atm_workflow_execution_session:acquire(AtmWorkflowExecutionId),
    atm_workflow_execution_ctx:build(SpaceId, AtmWorkflowExecutionId, CreatorUserCtx).
