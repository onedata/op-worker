%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% job execution context which consists of information necessary to run
%%% or schedule task execution for specific iterated item (automation job is
%%% automation task execution for specific item).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_job_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/4]).
-export([
    get_workflow_execution_ctx/1,
    get_item/1,
    get_report_result_url/1
]).


-record(atm_job_execution_ctx, {
    workflow_execution_env :: atm_workflow_execution_env:record(),
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    item :: json_utils:json_term(),
    report_result_url :: undefined | binary(),
    heartbeat_url :: undefined | binary()
}).
-type record() :: #atm_job_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_env:record(),
    json_utils:json_term(),
    undefined | binary(),
    undefined | binary()
) ->
    record().
build(AtmWorkflowExecutionEnv, Item, ReportResultUrl, HeartbeatUrl) ->
    #atm_job_execution_ctx{
        workflow_execution_env = AtmWorkflowExecutionEnv,
        workflow_execution_ctx = atm_workflow_execution_env:acquire_workflow_execution_ctx(
            AtmWorkflowExecutionEnv
        ),
        item = Item,
        report_result_url = ReportResultUrl,
        heartbeat_url = HeartbeatUrl
    }.


-spec get_workflow_execution_ctx(record()) -> atm_workflow_execution_ctx:record().
get_workflow_execution_ctx(#atm_job_execution_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx
}) ->
    AtmWorkflowExecutionCtx.


-spec get_item(record()) -> json_utils:json_term().
get_item(#atm_job_execution_ctx{item = Item}) ->
    Item.


-spec get_report_result_url(record()) -> undefined | binary().
get_report_result_url(#atm_job_execution_ctx{report_result_url = ReportResultUrl}) ->
    ReportResultUrl.
