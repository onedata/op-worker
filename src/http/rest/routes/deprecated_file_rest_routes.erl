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
%%% This module contains definitions of deprecated_file REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(deprecated_file_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of deprecated_file REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% List files and directories (deprecated)
    {<<"/files/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = list, 
            scope = private
        }
    }},
    %% List files and directories by Id (deprecated)
    {<<"/files-id/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = list, 
            scope = private
        }
    }},
    %% List file shares by path (deprecated)
    {<<"/file-shares/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = shares, 
            scope = private
        }
    }},
    %% List file shares by Id (deprecated)
    {<<"/file-id-shares/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = shares, 
            scope = private
        }
    }},
    %% Get file attributes (deprecated)
    {<<"/metadata/attrs/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = attrs, 
            scope = private
        }
    }},
    %% Set file attribute (deprecated)
    {<<"/metadata/attrs/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = attrs, 
            scope = private
        }
    }},
    %% Get file extended attributes (deprecated)
    {<<"/metadata/xattrs/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = xattrs, 
            scope = private
        }
    }},
    %% Set file extended attribute (deprecated)
    {<<"/metadata/xattrs/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = xattrs, 
            scope = private
        }
    }},
    %% Get file json metadata (deprecated)
    {<<"/metadata/json/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = json_metadata, 
            scope = private
        }
    }},
    %% Set file json metadata (deprecated)
    {<<"/metadata/json/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = json_metadata, 
            scope = private
        }
    }},
    %% Get file rdf metadata (deprecated)
    {<<"/metadata/rdf/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = rdf_metadata, 
            scope = private
        }
    }},
    %% Set file rdf metadata (deprecated)
    {<<"/metadata/rdf/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = rdf_metadata, 
            scope = private
        }
    }},
    %% Get file attributes by Id (deprecated)
    {<<"/metadata-id/attrs/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = attrs, 
            scope = private
        }
    }},
    %% Set file attribute by Id (deprecated)
    {<<"/metadata-id/attrs/:id">>, rest_handler, #rest_req{
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
    %% Get file extended attributes by Id (deprecated)
    {<<"/metadata-id/xattrs/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = xattrs, 
            scope = private
        }
    }},
    %% Set file extended attribute by Id (deprecated)
    {<<"/metadata-id/xattrs/:id">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = xattrs, 
            scope = private
        }
    }},
    %% Get file json metadata by Id (deprecated)
    {<<"/metadata-id/json/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = json_metadata, 
            scope = private
        }
    }},
    %% Set file json metadata by Id (deprecated)
    {<<"/metadata-id/json/:id">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = json_metadata, 
            scope = private
        }
    }},
    %% Get file rdf metadata by Id (deprecated)
    {<<"/metadata-id/rdf/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = rdf_metadata, 
            scope = private
        }
    }},
    %% Set file rdf metadata by Id (deprecated)
    {<<"/metadata-id/rdf/:id">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = rdf_metadata, 
            scope = private
        }
    }}
].
