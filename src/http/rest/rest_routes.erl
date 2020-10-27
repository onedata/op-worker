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
%%% This module contains definitions of REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_routes).

-include("http/rest.hrl").
-include("global_definitions.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of file REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{Path :: binary(), Handler :: module(), RoutesForPath :: map()}].
routes() ->
    AllRoutes = lists:flatten([
        basic_file_operations_rest_routes:routes(),
        custom_file_metadata_rest_routes:routes(),
        deprecated_file_api_rest_routes:routes(),
        file_distribution_rest_routes:routes(),
        file_path_resolution_rest_routes:routes(),
        file_registration_rest_routes:routes(),
        monitoring_rest_routes:routes(),
        oneprovider_rest_routes:routes(),
        qos_rest_routes:routes(),
        replica_rest_routes:routes(),
        share_rest_routes:routes(),
        space_rest_routes:routes(),
        transfer_rest_routes:routes(),
        view_rest_routes:routes()
    ]),

    SortedRoutes = sort_routes(AllRoutes),

    % Aggregate routes that share the same path
    AggregatedRoutes = lists:foldr(fun
        ({Path, Handler, #rest_req{method = Method} = RestReq}, [{Path, _, RoutesForPath} | Acc]) ->
            [{Path, Handler, RoutesForPath#{Method => RestReq}} | Acc];
        ({Path, Handler, #rest_req{method = Method} = RestReq}, Acc) ->
            [{Path, Handler, #{Method => RestReq}} | Acc]
    end, [], SortedRoutes),

    % Convert all routes to cowboy-compliant routes
    % - prepend REST prefix to every route
    % - rest handler module must be added as second element to the tuples
    % - RoutesForPath will serve as Opts to rest handler init.
    {ok, PrefixStr} = application:get_env(?APP_NAME, op_rest_api_prefix),
    Prefix = str_utils:to_binary(PrefixStr),
    lists:map(fun({Path, Handler, RoutesForPath}) ->
        {<<Prefix/binary, Path/binary>>, Handler, RoutesForPath}
    end, AggregatedRoutes).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sorts rest routes alphanumerically with accounting for fact that any concrete
%% path must precede path with match (e.g. `data/register` must precede `data/:id`).
%% Otherwise it would be impossible to make requests for such routes.
%% @end
%%--------------------------------------------------------------------
-spec sort_routes([{Path :: binary(), Handler :: module(), #rest_req{}}]) ->
    [{Path :: binary(), Handler :: module(), #rest_req{}}].
sort_routes(AllRoutes) ->
    % Replace ':' (ASCII 58) with `}` (ASCII 125) as this makes routes properly sortable
    SortableYetInvalidRoutes = lists:map(fun({Path, Handler, RestReq}) ->
        {binary:replace(Path, <<":">>, <<"}">>, [global]), Handler, RestReq}
    end, AllRoutes),

    SortedInvalidRoutes = lists:sort(SortableYetInvalidRoutes),

    lists:map(fun({Path, Handler, RestReq}) ->
        {binary:replace(Path, <<"}">>, <<":">>, [global]), Handler, RestReq}
    end, SortedInvalidRoutes).
