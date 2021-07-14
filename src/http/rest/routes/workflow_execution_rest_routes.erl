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
    %% Create workflow execution
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
    }}
].
