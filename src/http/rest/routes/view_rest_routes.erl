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
%%% This module contains definitions of view REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(view_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of view REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get all space views
    {<<"/spaces/:sid/views">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = views, 
            scope = private
        }
    }},
    %% Remove view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Get view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Update view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = {as_is, <<"mapFunction">>},
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Create view
    {<<"/spaces/:sid/views/:view_name">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"mapFunction">>},
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Remove view reduce function
    {<<"/spaces/:sid/views/:view_name/reduce">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view_reduce_function, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Update view reduce function
    {<<"/spaces/:sid/views/:view_name/reduce">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = {as_is, <<"reduceFunction">>},
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view_reduce_function, ?BINDING(view_name)}, 
            scope = private
        }
    }},
    %% Query view
    {<<"/spaces/:sid/views/:view_name/query">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {query_view, ?BINDING(view_name)}, 
            scope = private
        }
    }}
].
