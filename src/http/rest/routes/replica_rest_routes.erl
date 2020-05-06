%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of replica REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_rest_routes).

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
        b_gri = #b_gri{
            type = op_replica, 
            id = ?PATH_BINDING, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get replicas by path
    {<<"/replicas/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?PATH_BINDING, 
            aspect = distribution, 
            scope = private
        }
    }},
    %% Replicate file or directory by path
    {<<"/replicas/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?PATH_BINDING, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Evict existing replica by file Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?OBJECTID_BINDING(fid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get replicas by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?OBJECTID_BINDING(fid), 
            aspect = distribution, 
            scope = private
        }
    }},
    %% Replicate file or directory by Id
    {<<"/replicas-id/:fid">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?OBJECTID_BINDING(fid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Evict existing replicas by view
    {<<"/replicas-view/:view_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?BINDING(view_name), 
            aspect = evict_by_view, 
            scope = private
        }
    }},
    %% Replicate files by view
    {<<"/replicas-view/:view_name">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_replica, 
            id = ?BINDING(view_name), 
            aspect = replicate_by_view, 
            scope = private
        }
    }}
].
