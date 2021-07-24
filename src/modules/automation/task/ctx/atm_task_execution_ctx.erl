%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% task execution context which consists of information necessary to run
%%% or schedule task execution for specific item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_ctx).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% API
-export([build/5]).
-export([
    get_workflow_execution_env/1,
    get_workflow_execution_auth/1,
    get_access_token/1,
    get_item/1,
    get_report_result_url/1,
    get_heartbeat_url/1
]).


-record(atm_task_execution_ctx, {
    workflow_execution_env :: atm_workflow_execution_env:record(),
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    in_readonly_mode :: boolean(),
    item :: json_utils:json_term(),
    report_result_url :: undefined | binary(),
    heartbeat_url :: undefined | binary()
}).
-type record() :: #atm_task_execution_ctx{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_env:record(),
    boolean(),
    json_utils:json_term(),
    undefined | binary(),
    undefined | binary()
) ->
    record().
build(AtmWorkflowExecutionEnv, InReadonlyMode, Item, ReportResultUrl, HeartbeatUrl) ->
    #atm_task_execution_ctx{
        workflow_execution_env = AtmWorkflowExecutionEnv,
        workflow_execution_auth = atm_workflow_execution_env:acquire_workflow_execution_auth(
            AtmWorkflowExecutionEnv
        ),
        in_readonly_mode = InReadonlyMode,
        item = Item,
        report_result_url = ReportResultUrl,
        heartbeat_url = HeartbeatUrl
    }.


-spec get_workflow_execution_env(record()) -> atm_workflow_execution_env:record().
get_workflow_execution_env(#atm_task_execution_ctx{workflow_execution_env = AtmWorkflowExecutionEnv}) ->
    AtmWorkflowExecutionEnv.


-spec get_workflow_execution_auth(record()) -> atm_workflow_execution_auth:record().
get_workflow_execution_auth(#atm_task_execution_ctx{workflow_execution_auth = AtmWorkflowExecutionAuth}) ->
    AtmWorkflowExecutionAuth.


-spec get_access_token(record()) -> auth_manager:access_token().
get_access_token(#atm_task_execution_ctx{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    in_readonly_mode = InReadonlyMode
}) ->
    AccessToken = atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth),

    case InReadonlyMode of
        true -> tokens:confine(AccessToken, #cv_data_readonly{});
        false -> AccessToken
    end.


-spec get_item(record()) -> json_utils:json_term().
get_item(#atm_task_execution_ctx{item = Item}) ->
    Item.


-spec get_report_result_url(record()) -> undefined | binary().
get_report_result_url(#atm_task_execution_ctx{report_result_url = ReportResultUrl}) ->
    ReportResultUrl.


-spec get_heartbeat_url(record()) -> undefined | binary().
get_heartbeat_url(#atm_task_execution_ctx{heartbeat_url = HeartbeatUrl}) ->
    HeartbeatUrl.
