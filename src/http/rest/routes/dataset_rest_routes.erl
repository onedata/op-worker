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
%%% This module contains definitions of dataset REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(dataset_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of dataset REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% List space top datasets
    {<<"/spaces/:sid/datasets">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space,
            id = ?BINDING(sid),
            aspect = datasets,
            scope = private
        }
    }},
    %% List child datasets of a dataset
    {<<"/datasets/:did/children">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset,
            id = ?BINDING(did),
            aspect = children,
            scope = private
        }
    }},
    %% Establish dataset
    {<<"/datasets">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset,
            id = undefined,
            aspect = instance,
            scope = private
        }
    }},
    %% Remove dataset
    {<<"/datasets/:did">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_dataset,
            id = ?BINDING(did),
            aspect = instance,
            scope = private
        }
    }},
    %% Get dataset information
    {<<"/datasets/:did">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset,
            id = ?BINDING(did),
            aspect = instance,
            scope = private
        }
    }},
    %% Update dataset
    {<<"/datasets/:did">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset,
            id = ?BINDING(did),
            aspect = instance,
            scope = private
        }
    }},
    %% Get dataset summary for file or directory
    {<<"/data/:id/dataset/summary">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file,
            id = ?OBJECTID_BINDING(id),
            aspect = dataset_summary,
            scope = private
        }
    }}
].
