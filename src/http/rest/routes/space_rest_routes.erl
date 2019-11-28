%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of space REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(space_rest_routes).

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
        b_gri = #b_gri{
            type = op_space, 
            id = undefined, 
            aspect = list, 
            scope = private
        }
    }},
    %% Get basic space information
    {<<"/spaces/:sid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = instance, 
            scope = private
        }
    }},
    %% Get all space indexes (deprecated)
    {<<"/spaces/:sid/indexes">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = views, 
            scope = private
        }
    }},
    %% Remove index (deprecated)
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Get index (deprecated)
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Update index (deprecated)
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'PATCH',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Create index (deprecated)
    {<<"/spaces/:sid/indexes/:index_name">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Remove index reduce function (deprecated)
    {<<"/spaces/:sid/indexes/:index_name/reduce">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view_reduce_function, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Update index reduce function (deprecated)
    {<<"/spaces/:sid/indexes/:index_name/reduce">>, rest_handler, #rest_req{
        method = 'PUT',
        parse_body = as_is,
        consumes = [<<"application/javascript">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {view_reduce_function, ?BINDING(index_name)}, 
            scope = private
        }
    }},
    %% Query index (deprecated)
    {<<"/spaces/:sid/indexes/:index_name/query">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = {query_view, ?BINDING(index_name)}, 
            scope = private
        }
    }},
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
        parse_body = as_is,
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
        parse_body = as_is,
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
        parse_body = as_is,
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
    }},
    %% Get all transfers
    {<<"/spaces/:sid/transfers">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_space, 
            id = ?BINDING(sid), 
            aspect = transfers, 
            scope = private
        }
    }}
].
