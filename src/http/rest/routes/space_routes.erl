%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of space REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(space_routes).

-include("http/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of space REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get all spaces
    {<<"/spaces">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = undefined, aspect = list}
    }},
    %% Get basic space information
    {<<"/spaces/:sid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = instance}
    }},
    %% Get all space indexes
    {<<"/spaces/:sid/indexes">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = views}
    }},
    %% Create index
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(index_name)}}
    }},
    %% Get index
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(index_name)}}
    }},
    %% Remove index
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(index_name)}}
    }},
    %% Update index
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(index_name)}}
    }},
    %% Remove index reduce function
    {<<"/spaces/:sid/indexes/:index_name/reduce">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view_reduce_function, ?BINDING(index_name)}}
    }},
    %% Update index reduce function
    {<<"/spaces/:sid/indexes/:index_name/reduce">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view_reduce_function, ?BINDING(index_name)}}
    }},
    %% Query index
    {<<"/spaces/:sid/indexes/:index_name/query">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {query_view, ?BINDING(index_name)}}
    }},
    %% Get all space views
    {<<"/spaces/:sid/views">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = views}
    }},
    %% Create view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(view_name)}}
    }},
    %% Get view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(view_name)}}
    }},
    %% Remove view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(view_name)}}
    }},
    %% Update view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view, ?BINDING(view_name)}}
    }},
    %% Remove view reduce function
    {<<"/spaces/:sid/views/:view_name/reduce">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view_reduce_function, ?BINDING(view_name)}}
    }},
    %% Update view reduce function
    {<<"/spaces/:sid/views/:view_name/reduce">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {view_reduce_function, ?BINDING(view_name)}}
    }},
    %% Query view
    {<<"/spaces/:sid/views/:view_name/query">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = {query_view, ?BINDING(view_name)}}
    }},
    %% Get all transfers
    {<<"/spaces/:sid/transfers">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_space, id = ?BINDING(sid), aspect = transfers}
    }}
].