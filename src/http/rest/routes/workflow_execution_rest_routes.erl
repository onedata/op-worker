%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of workflow_execution REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(workflow_execution_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of workflow_execution REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% List workflow executions
    {<<"/spaces/:sid/automation/execution/workflows">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = atm_workflow_executions, 
            scope = private
        }
    }},
    %% Schedule workflow execution
    {<<"/automation/execution/workflows">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = undefined, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Delete workflow execution
    {<<"/automation/execution/workflows/:wid">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get workflow execution details
    {<<"/automation/execution/workflows/:wid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Cancel workflow execution
    {<<"/automation/execution/workflows/:wid/cancel">>, rest_handler, #rest_req{
        method = 'POST',
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = cancel, 
            scope = private
        }
    }},
    %% Pause workflow execution
    {<<"/automation/execution/workflows/:wid/pause">>, rest_handler, #rest_req{
        method = 'POST',
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = pause, 
            scope = private
        }
    }},
    %% Resume workflow execution
    {<<"/automation/execution/workflows/:wid/resume">>, rest_handler, #rest_req{
        method = 'POST',
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = resume, 
            scope = private
        }
    }},
    %% Force continue workflow execution
    {<<"/automation/execution/workflows/:wid/force_continue">>, rest_handler, #rest_req{
        method = 'POST',
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = force_continue, 
            scope = private
        }
    }},
    %% Rerun workflow execution
    {<<"/automation/execution/workflows/:wid/rerun">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = rerun, 
            scope = private
        }
    }},
    %% Retry workflow execution
    {<<"/automation/execution/workflows/:wid/retry">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_atm_workflow_execution, 
            id = ?BINDING(wid), 
            aspect = retry, 
            scope = private
        }
    }}
].
