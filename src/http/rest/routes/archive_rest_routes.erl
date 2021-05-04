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
%%% This module contains definitions of archive REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(archive_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of archive REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Archive dataset
    {<<"/archives">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_archive, 
            id = undefined, 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get archive information
    {<<"/archives/:aid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_archive, 
            id = ?BINDING(aid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Update archive
    {<<"/archives/:aid">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_archive, 
            id = ?BINDING(aid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Purge archive
    {<<"/archives/:aid/init_purge">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_archive,
            id = ?BINDING(aid),
            aspect = purge,
            scope = private
        }
    }},
    %% List archives of the dataset
    {<<"/datasets/:did/archives">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset, 
            id = ?BINDING(did), 
            aspect = archives, 
            scope = private
        }
    }}
].
