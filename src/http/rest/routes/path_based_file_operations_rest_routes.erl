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
%%% This module contains definitions of path-based_file_operations REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(path-based_file_operations_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of path-based_file_operations REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Lookup file id
    {<<"/lookup-file-id/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = object_id, 
            scope = private
        }
    }},
    %% Remove file at path
    {<<"/data/:id/path/[...]">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = file_at_path, 
            scope = private
        }
    }},
    %% Download file content by path
    {<<"/data/:id/path/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = file_at_path, 
            scope = private
        }
    }},
    %% Create file at path
    {<<"/data/:id/path/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        consumes = [<<"application/octet-stream">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = file_at_path, 
            scope = private
        }
    }}
].
