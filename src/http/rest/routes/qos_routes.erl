%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of qos REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_routes).

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
    %% Add QoS entry (by path)
    {<<"/qos/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Get QoS summary (by path)
    {<<"/qos/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?PATH_BINDING, aspect = effective_qos}
    }},
    %% Add QoS entry (by id)
    {<<"/qos-id/:id">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?OBJECTID_BINDING(id), aspect = instance}
    }},
    %% Get QoS summary (by id)
    {<<"/qos-id/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?OBJECTID_BINDING(id), aspect = effective_qos}
    }},
    %% Get QoS entry
    {<<"/qos-entry/:qid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?BINDING(qid), aspect = instance}
    }},
    %% Remove QoS entry
    {<<"/qos-entry/:qid">>, rest_handler, #rest_req{
        method = 'DELETE',
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_qos, id = ?BINDING(qid), aspect = instance}
    }}
].
