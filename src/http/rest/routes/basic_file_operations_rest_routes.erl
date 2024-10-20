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
%%% This module contains definitions of basic_file_operations REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(basic_file_operations_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of basic_file_operations REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% List directory files and subdirectories
    {<<"/data/:id/children">>, rest_handler, #rest_req{
        method = 'GET',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = children, 
            scope = private
        }
    }},
    %% Create file in directory
    {<<"/data/:id/children">>, rest_handler, #rest_req{
        method = 'POST',
        consumes = [<<"application/octet-stream">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = child, 
            scope = private
        }
    }},
    %% List files recursively
    {<<"/data/:id/files">>, rest_handler, #rest_req{
        method = 'GET',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = files, 
            scope = private
        }
    }},
    %% Download file content
    {<<"/data/:id/content">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = content, 
            scope = private
        }
    }},
    %% Update file content
    {<<"/data/:id/content">>, rest_handler, #rest_req{
        method = 'PUT',
        consumes = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = content, 
            scope = private
        }
    }},
    %% Remove file
    {<<"/data/:id">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get file attributes
    {<<"/data/:id">>, rest_handler, #rest_req{
        method = 'GET',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Set file attribute
    {<<"/data/:id">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = attrs, 
            scope = private
        }
    }},
    %% Get file hard links
    {<<"/data/:id/hardlinks">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = hardlinks, 
            scope = private
        }
    }},
    %% Test for hard link between files
    {<<"/data/:id/hardlinks/:hid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = {hardlinks, ?OBJECTID_BINDING(hid)}, 
            scope = private
        }
    }},
    %% Get symbolic link value
    {<<"/data/:id/symlink_value">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = symlink_value, 
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
