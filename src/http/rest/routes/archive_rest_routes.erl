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
    %% List archives of a dataset
    {<<"/datasets/:did/archives">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_dataset, 
            id = ?BINDING(did), 
            aspect = archives, 
            scope = private
        }
    }},
    %% Create archive from a dataset
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
    {<<"/archives/:aid/purge">>, rest_handler, #rest_req{
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
    %% Recall archive
    {<<"/archives/:aid/recall">>, rest_handler, #rest_req{
        method = 'POST',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_archive, 
            id = ?BINDING(aid), 
            aspect = recall, 
            scope = private
        }
    }},
    %% Get details of an archive recall
    {<<"/data/:id/recall/details">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = archive_recall_details, 
            scope = private
        }
    }},
    %% Get progress of an archive recall
    {<<"/data/:id/recall/progress">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = archive_recall_progress, 
            scope = private
        }
    }}
].
