%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation job
%%% context which consists of information necessary to run or schedule task
%%% execution for specific batch of jobs (automation job is automation task
%%% execution for specific item).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_run_job_batch_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% API
-export([build/2]).
-export([
    get_workflow_execution_ctx/1,
    get_workflow_execution_auth/1,
    get_workflow_execution_id/1,
    get_access_token/1
]).


-record(atm_run_job_batch_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    is_in_readonly_mode :: boolean()
}).
-type record() :: #atm_run_job_batch_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_workflow_execution_ctx:record(), atm_task_execution:record()) ->
    record().
build(AtmWorkflowExecutionCtx, #atm_task_execution{
    executor = AtmTaskExecutor
}) ->
    #atm_run_job_batch_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        is_in_readonly_mode = atm_task_executor:is_in_readonly_mode(AtmTaskExecutor)
    }.


-spec get_workflow_execution_ctx(record()) -> atm_workflow_execution_ctx:record().
get_workflow_execution_ctx(#atm_run_job_batch_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    AtmWorkflowExecutionCtx.


-spec get_workflow_execution_auth(record()) -> atm_workflow_execution_auth:record().
get_workflow_execution_auth(#atm_run_job_batch_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx).


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_run_job_batch_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx).


-spec get_access_token(record()) -> auth_manager:access_token().
get_access_token(#atm_run_job_batch_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    is_in_readonly_mode = IsInReadonlyMode
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    AccessToken = atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth),

    case IsInReadonlyMode of
        true -> tokens:confine(AccessToken, #cv_data_readonly{});
        false -> AccessToken
    end.
