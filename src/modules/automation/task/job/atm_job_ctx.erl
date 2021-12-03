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
%%% execution for specific batch of items (automation job is automation task
%%% execution for specific batch of items).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_job_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% API
-export([build/3]).
-export([
    get_workflow_execution_ctx/1,
    get_workflow_execution_auth/1,
    get_access_token/1,
    get_report_result_url/1
]).


-record(atm_job_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    in_readonly_mode :: boolean(),
    report_result_url :: undefined | binary()
}).
-type record() :: #atm_job_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_workflow_execution_ctx:record(), boolean(), undefined | binary()) ->
    record().
build(AtmWorkflowExecutionCtx, InReadonlyMode, ReportResultUrl) ->
    #atm_job_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        in_readonly_mode = InReadonlyMode,
        report_result_url = ReportResultUrl
    }.


-spec get_workflow_execution_ctx(record()) -> atm_workflow_execution_ctx:record().
get_workflow_execution_ctx(#atm_job_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    AtmWorkflowExecutionCtx.


-spec get_workflow_execution_auth(record()) -> atm_workflow_execution_auth:record().
get_workflow_execution_auth(#atm_job_ctx{workflow_execution_ctx = AtmWorkflowExecutionCtx}) ->
    atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx).


-spec get_access_token(record()) -> auth_manager:access_token().
get_access_token(#atm_job_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    in_readonly_mode = InReadonlyMode
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    AccessToken = atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth),

    case InReadonlyMode of
        true -> tokens:confine(AccessToken, #cv_data_readonly{});
        false -> AccessToken
    end.


-spec get_report_result_url(record()) -> undefined | binary().
get_report_result_url(#atm_job_ctx{report_result_url = ReportResultUrl}) ->
    ReportResultUrl.
