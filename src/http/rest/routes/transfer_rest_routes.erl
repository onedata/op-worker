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
%%% This module contains definitions of transfer REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of transfer REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get all space transfers
    {<<"/spaces/:sid/transfers">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = transfers, 
            scope = private
        }
    }},
    %% Create transfer
    {<<"/transfers">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_transfer, 
            id = undefined, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Cancel specific transfer
    {<<"/transfers/:tid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_transfer, 
            id = ?BINDING(tid), 
            aspect = cancel, 
            scope = private
        }
    }},
    %% Get transfer status
    {<<"/transfers/:tid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_transfer, 
            id = ?BINDING(tid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Rerun ended transfer
    {<<"/transfers/:tid/rerun">>, rest_handler, #rest_req{
        method = 'POST',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_transfer, 
            id = ?BINDING(tid), 
            aspect = rerun, 
            scope = private
        }
    }}
].
