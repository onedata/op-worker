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
    {<<"/files/:path">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = list}
    }},
    %% Get file attributes
    {<<"/attributes/:path">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attributes}
    }},
    %% Set file attribute
    {<<"/attributes/:path">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_json_params,
        consumes = [<<"application/json">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = attributes}
    }},
    %% Get file metadata
    {<<"/metadata/:path">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>, <<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = metadata}
    }},
    %% Set file metadata
    {<<"/metadata/:path">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>, <<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?PATH_BINDING, aspect = metadata}
    }},
    %% Get file metadata by ID
    {<<"/metadata-id/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>, <<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?OBJECTID_BINDING(id), aspect = metadata}
    }},
    %% Set file metadata by ID
    {<<"/metadata-id/:id">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/json">>, <<"application/rdf+xml">>],
        b_gri = #b_gri{type = op_file, id = ?OBJECTID_BINDING(id), aspect = metadata}
    }}
].
