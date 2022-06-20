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
-export([build/4]).
-export([
    get_workflow_execution_ctx/1,
    get_workflow_execution_auth/1,
    get_access_token/1,
    get_forward_output_url/1,
    get_heartbeat_url/1
]).


-record(atm_run_job_batch_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    is_in_readonly_mode :: boolean(),
    forward_output_url :: binary(),
    heartbeat_url :: binary()
}).
-type record() :: #atm_run_job_batch_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_workflow_execution_ctx:record(), binary(), binary(), atm_task_execution:record()) ->
    record().
build(AtmWorkflowExecutionCtx, ForwardOutputUrl, HeartbeatUrl, #atm_task_execution{
    executor = AtmTaskExecutor
}) ->
    #atm_run_job_batch_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        is_in_readonly_mode = atm_task_executor:is_in_readonly_mode(AtmTaskExecutor),
        forward_output_url = ForwardOutputUrl,
        heartbeat_url = HeartbeatUrl
    }.


-spec get_workflow_execution_ctx(record()) -> atm_workflow_execution_ctx:record().
get_workflow_execution_ctx(#atm_run_job_batch_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    AtmWorkflowExecutionCtx.


-spec get_workflow_execution_auth(record()) -> atm_workflow_execution_auth:record().
get_workflow_execution_auth(#atm_run_job_batch_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx).


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


-spec get_forward_output_url(record()) -> binary().
get_forward_output_url(#atm_run_job_batch_ctx{forward_output_url = ForwardOutputUrl}) ->
    ForwardOutputUrl.


-spec get_heartbeat_url(record()) -> binary().
get_heartbeat_url(#atm_run_job_batch_ctx{heartbeat_url = HeartbeatUrl}) ->
    HeartbeatUrl.
