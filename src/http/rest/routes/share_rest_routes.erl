%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of share REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(share_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of share REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Unshare a folder by path
    {<<"/shares/[...]">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_share, 
            id = ?PATH_BINDING, 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Get share info by folder path
    {<<"/shares/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?PATH_BINDING, 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Rename share by folder path
    {<<"/shares/[...]">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?PATH_BINDING, 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Share a folder by path
    {<<"/shares/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?PATH_BINDING, 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Unshare a folder by Id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_share, 
            id = ?OBJECTID_BINDING(id), 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Get share info by folder id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?OBJECTID_BINDING(id), 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Rename share by folder id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?OBJECTID_BINDING(id), 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Share a folder by Id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?OBJECTID_BINDING(id), 
            aspect = shared_dir, 
            scope = private
        }
    }},
    %% Unshare a folder by public share Id
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_share, 
            id = ?BINDING(shid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get share info by public share Id
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?BINDING(shid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Rename share by public share Id
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?BINDING(shid), 
            aspect = instance, 
            scope = private
        }
    }}
].
