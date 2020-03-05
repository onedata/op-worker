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
%%% This module contains definitions of data REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(data_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of data REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get file attributes
    {<<"/data/:id">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = attrs, 
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
    %% List directory files and subdirectories
    {<<"/data/:id/children">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = children, 
            scope = private
        }
    }},
    %% Get file json metadata
    {<<"/data/:id/metadata/json">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = json_metadata, 
            scope = private
        }
    }},
    %% Set file json metadata
    {<<"/data/:id/metadata/json">>, rest_handler, #rest_req{
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
    %% Get file rdf metadata
    {<<"/data/:id/metadata/rdf">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = rdf_metadata, 
            scope = private
        }
    }},
    %% Set file rdf metadata
    {<<"/data/:id/metadata/rdf">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/rdf+xml">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = rdf_metadata, 
            scope = private
        }
    }},
    %% Get file extended attributes
    {<<"/data/:id/metadata/xattrs">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = xattrs, 
            scope = private
        }
    }},
    %% Set file extended attribute
    {<<"/data/:id/metadata/xattrs">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"metadata">>},
        consumes = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?OBJECTID_BINDING(id), 
            aspect = xattrs, 
            scope = private
        }
    }}
].
