%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of replication REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(replication_routes).

-include("http/rest/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of replication REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Evict existing replica by file path
    {<<"/replicas/:path">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?PATH_BINDING, aspect = eviction}
    }},
    %% Get replicas by path
    {<<"/replicas/:path">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?PATH_BINDING, aspect = replicas}
    }},
    %% Replicate file or folder by path
    {<<"/replicas/:path">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?PATH_BINDING, aspect = replication}
    }},
    %% Evict existing replica by file Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?OBJECTID_BINDING(fid), aspect = eviction}
    }},
    %% Get replicas by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?OBJECTID_BINDING(fid), aspect = replicas}
    }},
    %% Replicate file or folder by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?OBJECTID_BINDING(fid), aspect = replication}
    }},
    %% Evict existing replicas by index
    {<<"/replicas-index/:index_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?BINDING(index_name), aspect = eviction_by_index}
    }},
    %% Replicate files by index
    {<<"/replicas-index/:index_name">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replication, id = ?BINDING(index_name), aspect = replication_by_index}
    }}
].
