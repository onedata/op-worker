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
%%% 1) quick access to basic information about workflow (e.g. space id)
%%% 2) quick access to all global stores (store defined in schema and accessible
%%%    on all levels of execution) and other stores in current scope
%%%    (e.g. currently executed lane run exception store).
%%% 3) acquisition of automation workflow auth.
%%% 4) acquisition of automation workflow logger.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_env).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    build/2, build/3,
    add_global_store_mapping/3,
    set_workflow_incarnation/2,
    set_workflow_audit_log_store_container/2,
    set_lane_run_exception_store_container/2,
    add_task_audit_log_store_container/3
]).
-export([
    get_space_id/1,
    get_workflow_execution_id/1,

    list_global_stores/1,
    get_global_store_id/2,

    get_lane_run_exception_store_container/1,

    acquire_auth/1,
    acquire_logger/3
]).


% See module doc for more information.
-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    global_store_registry :: atm_workflow_execution:store_registry(),
    workflow_execution_incarnation :: undefined | atm_workflow_execution:incarnation(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record(),
    lane_exception_store_container :: undefined | atm_store_container:record(),
    task_audit_logs_registry :: #{atm_task_execution:id() => atm_store_container:record()}
}).
-type record() :: #atm_workflow_execution_env{}.

-type diff() :: fun((record()) -> record()).

-export_type([record/0, diff/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), atm_workflow_execution:id()) -> record().
build(SpaceId, AtmWorkflowExecutionId) ->
    build(SpaceId, AtmWorkflowExecutionId, #{}).


-spec build(od_space:id(), atm_workflow_execution:id(), atm_workflow_execution:store_registry()) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, AtmGlobalStoreRegistry) ->
    #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        global_store_registry = AtmGlobalStoreRegistry,
        workflow_execution_incarnation = undefined,
        workflow_audit_log_store_container = undefined,
        lane_exception_store_container = undefined,
        task_audit_logs_registry = #{}
    }.


-spec add_global_store_mapping(automation:id(), atm_store:id(), record()) -> record().
add_global_store_mapping(AtmStoreSchemaId, AtmStoreId, Record = #atm_workflow_execution_env{
    global_store_registry = AtmGlobalStoreRegistry
}) ->
    Record#atm_workflow_execution_env{global_store_registry = AtmGlobalStoreRegistry#{
        AtmStoreSchemaId => AtmStoreId
    }}.


-spec set_workflow_incarnation(atm_workflow_execution:incarnation(), record()) -> record().
set_workflow_incarnation(AtmWorkflowExecutionIncarnation, Record) ->
    Record#atm_workflow_execution_env{
        workflow_execution_incarnation = AtmWorkflowExecutionIncarnation
    }.


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


-spec add_task_audit_log_store_container(
    atm_task_execution:id(),
    undefined | atm_store_container:record(),
    record()
) ->
    record().
add_task_audit_log_store_container(
    AtmTaskExecutionId,
    AtmTaskAuditLogStoreContainer,
    #atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry} = Record
) ->
    Record#atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry#{
        AtmTaskExecutionId => AtmTaskAuditLogStoreContainer
    }}.


-spec get_space_id(record()) -> od_space:id().
get_space_id(#atm_workflow_execution_env{space_id = SpaceId}) ->
    SpaceId.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_env{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    AtmWorkflowExecutionId.


-spec list_global_stores(record()) -> [atm_store:id()].
list_global_stores(#atm_workflow_execution_env{global_store_registry = AtmStoreRegistry}) ->
    maps:values(AtmStoreRegistry).


-spec get_global_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_global_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    global_store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_STORE_NOT_FOUND(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.


-spec get_lane_run_exception_store_container(record()) ->
    undefined | atm_store_container:record().
get_lane_run_exception_store_container(#atm_workflow_execution_env{
    lane_exception_store_container = AtmLaneExceptionStoreContainer
}) ->
    AtmLaneExceptionStoreContainer.


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
