%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution ctx which consists of information necessary to
%%% validate data, access stores or perform logging.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    acquire/1, acquire/2, acquire/3,
    set_env/2,
    configure_processed_task_id/2
]).
-export([
    get_workflow_execution_id/1,
    get_workflow_execution_incarnation/1,

    get_env/1,
    get_auth/1,

    get_log_level_int/1,
    get_logger/1,

    is_global_store/2,
    get_global_store_id/2,

    get_lane_run_instant_failure_exception_threshold/1,

    get_task_execution_id/1,
    get_task_time_series_store_id/1
]).


-record(atm_workflow_execution_ctx, {
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    workflow_execution_env :: atm_workflow_execution_env:record(),
    processed_task_id :: undefined | atm_task_execution:id()
}).
-type record() :: #atm_workflow_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec acquire(atm_workflow_execution_env:record()) -> record() | no_return().
acquire(AtmWorkflowExecutionEnv) ->
    acquire(undefined, AtmWorkflowExecutionEnv).


-spec acquire(undefined | atm_task_execution:id(), atm_workflow_execution_env:record()) ->
    record() | no_return().
acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_env:acquire_auth(AtmWorkflowExecutionEnv),
    acquire(AtmTaskExecutionId, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv).


-spec acquire(
    undefined | atm_task_execution:id(),
    atm_workflow_execution_auth:record(),
    atm_workflow_execution_env:record()
) ->
    record() | no_return().
acquire(AtmTaskExecutionId, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv) ->
    #atm_workflow_execution_ctx{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_execution_env = AtmWorkflowExecutionEnv,
        processed_task_id = AtmTaskExecutionId
    }.


-spec set_env(atm_workflow_execution_env:record(), record()) -> record().
set_env(AtmWorkflowExecutionEnv, Record) ->
    Record#atm_workflow_execution_ctx{workflow_execution_env = AtmWorkflowExecutionEnv}.


-spec configure_processed_task_id(undefined | atm_task_execution:id(), record()) -> record().
configure_processed_task_id(AtmTaskExecutionId, Record = #atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    Record#atm_workflow_execution_ctx{
        workflow_execution_env = atm_workflow_execution_env:ensure_task_registered(
            AtmTaskExecutionId, AtmWorkflowExecutionEnv
        ),
        processed_task_id = AtmTaskExecutionId
    }.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_workflow_execution_id(AtmWorkflowExecutionEnv).


-spec get_workflow_execution_incarnation(record()) -> atm_workflow_execution:incarnation().
get_workflow_execution_incarnation(#atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_workflow_execution_incarnation(AtmWorkflowExecutionEnv).


-spec get_env(record()) -> atm_workflow_execution_env:record().
get_env(#atm_workflow_execution_ctx{workflow_execution_env = AtmWorkflowExecutionEnv}) ->
    AtmWorkflowExecutionEnv.


-spec get_auth(record()) -> atm_workflow_execution_auth:record().
get_auth(#atm_workflow_execution_ctx{workflow_execution_auth = AtmWorkflowExecutionAuth}) ->
    AtmWorkflowExecutionAuth.


-spec get_log_level_int(record()) -> audit_log:entry_severity_int().
get_log_level_int(#atm_workflow_execution_ctx{workflow_execution_env = AtmWorkflowExecutionEnv}) ->
    atm_workflow_execution_env:get_log_level_int(AtmWorkflowExecutionEnv).


-spec get_logger(record()) -> atm_workflow_execution_logger:record().
get_logger(#atm_workflow_execution_ctx{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    workflow_execution_env = AtmWorkflowExecutionEnv,
    processed_task_id = AtmTaskExecutionId
}) ->
    atm_workflow_execution_env:build_logger(
        AtmTaskExecutionId, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv
    ).


-spec is_global_store(atm_store:id(), record()) -> boolean().
is_global_store(AtmStoreId, #atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    lists:member(AtmStoreId, atm_workflow_execution_env:list_global_stores(
        AtmWorkflowExecutionEnv
    )).


-spec get_global_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_global_store_id(AtmStoreSchemaId, #atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_global_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv).


-spec get_lane_run_instant_failure_exception_threshold(record()) -> float().
get_lane_run_instant_failure_exception_threshold(#atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_lane_run_instant_failure_exception_threshold(
        AtmWorkflowExecutionEnv
    ).


-spec get_task_execution_id(record()) -> undefined | atm_task_execution:id().
get_task_execution_id(#atm_workflow_execution_ctx{processed_task_id = AtmTaskExecutionId}) ->
    AtmTaskExecutionId.


-spec get_task_time_series_store_id(record()) -> undefined | atm_store:id().
get_task_time_series_store_id(#atm_workflow_execution_ctx{processed_task_id = undefined}) ->
    undefined;

get_task_time_series_store_id(#atm_workflow_execution_ctx{
    processed_task_id = AtmTaskExecutionId,
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_task_time_series_store_id(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ).
