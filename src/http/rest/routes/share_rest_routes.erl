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
    %% Create share
    {<<"/shares">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = undefined, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Remove a specific share
    {<<"/shares/:shid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_share, 
            id = ?BINDING(shid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get share info
    {<<"/shares/:shid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_share, 
            id = ?BINDING(shid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Rename share
    {<<"/shares/:shid">>, rest_handler, #rest_req{
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
