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
-export([acquire/2]).
-export([
    get_env/1,
    get_auth/1,
    get_logger/1,
    get_workflow_store_id/2
]).


-record(atm_workflow_execution_ctx, {
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    workflow_execution_logger :: atm_workflow_execution_logger:record(),
    workflow_execution_env :: atm_workflow_execution_env:record()
}).
-type record() :: #atm_workflow_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec acquire(undefined | atm_task_execution:id(), atm_workflow_execution_env:record()) ->
    record() | no_return().
acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_env:acquire_auth(AtmWorkflowExecutionEnv),

    #atm_workflow_execution_ctx{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_execution_logger = atm_workflow_execution_env:acquire_logger(
            AtmTaskExecutionId, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv
        ),
        workflow_execution_env = AtmWorkflowExecutionEnv
    }.


-spec get_env(record()) -> atm_workflow_execution_env:record().
get_env(#atm_workflow_execution_ctx{workflow_execution_env = AtmWorkflowExecutionEnv}) ->
    AtmWorkflowExecutionEnv.


-spec get_auth(record()) -> atm_workflow_execution_auth:record().
get_auth(#atm_workflow_execution_ctx{workflow_execution_auth = AtmWorkflowExecutionAuth}) ->
    AtmWorkflowExecutionAuth.


-spec get_logger(record()) -> atm_workflow_execution_logger:record().
get_logger(#atm_workflow_execution_ctx{workflow_execution_logger = AtmWorkflowExecutionLogger}) ->
    AtmWorkflowExecutionLogger.


-spec get_workflow_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_workflow_store_id(AtmStoreSchemaId, #atm_workflow_execution_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv
}) ->
    atm_workflow_execution_env:get_workflow_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv).
