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
    build/2, build/3,
    add_workflow_store/3,
    set_workflow_audit_log_store_container/2,
    set_lane_run_exception_store_container/2,
    set_task_audit_log_store_container/3,

    acquire_auth/1,
    acquire_logger/3,

    get_space_id/1,
    get_workflow_execution_id/1,

    list_workflow_stores/1,
    get_workflow_store_id/2
]).

-type task_audit_logs_registry() :: #{atm_task_execution:id() => atm_store_container:record()}.

-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_store_registry :: atm_workflow_execution:store_registry(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record(),
    lane_exception_store_container :: undefined | atm_store_container:record(),
    task_audit_logs_registry :: task_audit_logs_registry()
}).
-type record() :: #atm_workflow_execution_env{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), atm_workflow_execution:id()) -> record().
build(SpaceId, AtmWorkflowExecutionId) ->
    build(SpaceId, AtmWorkflowExecutionId, #{}).


-spec build(od_space:id(), atm_workflow_execution:id(), atm_workflow_execution:store_registry()) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, AtmWorkflowStoreRegistry) ->
    #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_store_registry = AtmWorkflowStoreRegistry,
        workflow_audit_log_store_container = undefined,
        lane_exception_store_container = undefined,
        task_audit_logs_registry = #{}
    }.


-spec add_workflow_store(automation:id(), atm_store:id(), record()) -> record().
add_workflow_store(AtmStoreSchemaId, AtmStoreId, Record = #atm_workflow_execution_env{
    workflow_store_registry = AtmWorkflowStoreRegistry
}) ->
    Record#atm_workflow_execution_env{workflow_store_registry = AtmWorkflowStoreRegistry#{
        AtmStoreSchemaId => AtmStoreId
    }}.


-spec set_workflow_audit_log_store_container(undefined | atm_store_container:record(), record()) ->
    record().
set_workflow_audit_log_store_container(AtmWorkflowAuditLogStoreContainer, Record) ->
    Record#atm_workflow_execution_env{
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
    }.


-spec set_lane_run_exception_store_container(undefined | atm_store_container:record(), record()) ->
    record().
set_lane_run_exception_store_container(AtmLaneRunExceptionStoreContainer, Record) ->
    Record#atm_workflow_execution_env{
        lane_exception_store_container = AtmLaneRunExceptionStoreContainer
    }.


-spec set_task_audit_log_store_container(
    atm_task_execution:id(),
    undefined | atm_store_container:record(),
    record()
) ->
    record().
set_task_audit_log_store_container(
    AtmTaskExecutionId,
    AtmTaskAuditLogStoreContainer,
    #atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry} = Record
) ->
    Record#atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry#{
        AtmTaskExecutionId => AtmTaskAuditLogStoreContainer
    }}.


-spec acquire_auth(record()) -> atm_workflow_execution_auth:record() | no_return().
acquire_auth(#atm_workflow_execution_env{
    space_id = SpaceId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    CreatorUserCtx = atm_workflow_execution_session:acquire(AtmWorkflowExecutionId),
    atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, CreatorUserCtx).


-spec acquire_logger(
    undefined | atm_task_execution:id(),
    atm_workflow_execution_auth:record(),
    record()
) ->
    atm_workflow_execution_logger:record() | no_return().
acquire_logger(AtmTaskExecutionId, AtmWorkflowExecutionAuth, #atm_workflow_execution_env{
    workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
} = Record) ->
    atm_workflow_execution_logger:build(
        AtmWorkflowExecutionAuth,
        get_task_audit_log_store_container(AtmTaskExecutionId, Record),
        AtmWorkflowAuditLogStoreContainer
    ).


-spec get_space_id(record()) -> od_space:id().
get_space_id(#atm_workflow_execution_env{space_id = SpaceId}) ->
    SpaceId.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_env{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    AtmWorkflowExecutionId.


-spec list_workflow_stores(record()) -> [atm_store:id()].
list_workflow_stores(#atm_workflow_execution_env{workflow_store_registry = AtmStoreRegistry}) ->
    maps:values(AtmStoreRegistry).


-spec get_workflow_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_workflow_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    workflow_store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_STORE_NOT_FOUND(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_task_audit_log_store_container(undefined | atm_task_execution:id(), record()) ->
    undefined | atm_store_container:record().
get_task_audit_log_store_container(AtmTaskExecutionId, #atm_workflow_execution_env{
    task_audit_logs_registry = AtmTaskAuditLogsRegistry
}) ->
    maps:get(AtmTaskExecutionId, AtmTaskAuditLogsRegistry, undefined).
