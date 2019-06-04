%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of share REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(share_routes).

-include("http/rest/rest.hrl").

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
    %% Share a folder by path
    {<<"/shares/:path">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Get share info by folder path
    {<<"/shares/:path">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Unshare a folder by path
    {<<"/shares/:path">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_share, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Rename share by folder path
    {<<"/shares/:path">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?PATH_BINDING, aspect = instance}
    }},
    %% Share a folder by ID
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?OBJECTID_BINDING(id), aspect = instance}
    }},
    %% Get share info by folder id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?OBJECTID_BINDING(id), aspect = instance}
    }},
    %% Unshare a folder by ID
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_share, id = ?OBJECTID_BINDING(id), aspect = instance}
    }},
    %% Rename share by folder id
    {<<"/shares-id/:id">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?OBJECTID_BINDING(id), aspect = instance}
    }},
    %% Get share info by public share ID
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?BINDING(shid), aspect = instance}
    }},
    %% Unshare a folder by public share ID
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_share, id = ?BINDING(shid), aspect = instance}
    }},
    %% Rename share by public share ID
    {<<"/shares-public-id/:shid">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_share, id = ?BINDING(shid), aspect = instance}
    }}
].
