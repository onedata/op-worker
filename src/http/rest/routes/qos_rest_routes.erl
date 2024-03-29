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
%%% This module contains definitions of qos REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of qos REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Add QoS requirement
    {<<"/qos_requirements">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_qos, 
            id = undefined, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Remove QoS requirement
    {<<"/qos_requirements/:qid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_qos, 
            id = ?BINDING(qid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get QoS requirement
    {<<"/qos_requirements/:qid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_qos, 
            id = ?BINDING(qid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get QoS audit log
    {<<"/qos_requirements/:qid/audit_log">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_qos, 
            id = ?BINDING(qid), 
            aspect = audit_log, 
            scope = private
        }
    }},
    %% Get QoS summary for file or directory
    {<<"/data/:id/qos/summary">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = qos_summary, 
            scope = private
        }
    }},
    %% Evaluate QoS expression
    {<<"/spaces/:sid/evaluate_qos_expression">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = evaluate_qos_expression, 
            scope = private
        }
    }}
].
