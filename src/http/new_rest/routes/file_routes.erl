%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of file REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(file_routes).

-include("http/rest/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of file REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% List files and folders
    {<<"/files/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = list}
    }},
    %% Get file attributes
    {<<"/attributes/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attributes}
    }},
    %% Set file attribute
    {<<"/attributes/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attributes}
    }},
    %% Get file attributes
    {<<"/metadata/attrs/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attrs}
    }},
    %% Set file attribute
    {<<"/metadata/attrs/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attrs}
    }},
    %% Get file extended attributes
    {<<"/metadata/xattrs/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = xattrs}
    }},
    %% Set file extended attribute
    {<<"/metadata/xattrs/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = xattrs}
    }},
    %% Get file json metadata
    {<<"/metadata/json/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = json_metadata}
    }},
    %% Set file json metadata
    {<<"/metadata/json/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = json_metadata}
    }},
    %% Get file rdf metadata
    {<<"/metadata/rdf/[...]">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = rdf_metadata}
    }},
    %% Set file rdf metadata
    {<<"/metadata/rdf/[...]">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = rdf_metadata}
    }}
].
