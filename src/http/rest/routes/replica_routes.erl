%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of replica REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_routes).

-include("http/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of replica REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Evict existing replica by file path
    {<<"/replicas/[...]">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Get replicas by path
    {<<"/replicas/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?PATH_BINDING, aspect = distribution}
    }},
    %% Replicate file or folder by path
    {<<"/replicas/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Evict existing replica by file Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?OBJECTID_BINDING(fid), aspect = instance}
    }},
    %% Get replicas by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?OBJECTID_BINDING(fid), aspect = distribution}
    }},
    %% Replicate file or folder by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?OBJECTID_BINDING(fid), aspect = instance}
    }},
    %% Evict existing replicas by index
    {<<"/replicas-index/:index_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?BINDING(index_name), aspect = evict_by_index}
    }},
    %% Replicate files by index
    {<<"/replicas-index/:index_name">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_replica, id = ?BINDING(index_name), aspect = replicate_by_index}
    }}
].
