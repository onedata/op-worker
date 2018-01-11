%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains common macros for modules related to Graph Sync.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PROVIDER_GRAPH_SYNC_HRL).
-define(PROVIDER_GRAPH_SYNC_HRL, 1).

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

% Global worker identifier
-define(GS_CLIENT_WORKER_GLOBAL_NAME, graph_sync_client_worker).

% Not configurable as it depends on current implementation in OP.
-define(GS_PROTOCOL_VERSION, 1).

% Graph Sync config
-define(GS_CHANNEL_PORT, application:get_env(?APP_NAME,
    graph_sync_port, 443)).

-define(GS_CHANNEL_PATH, application:get_env(?APP_NAME,
    graph_sync_path, "/graph_sync/provider")).

-define(GS_REQUEST_TIMEOUT, application:get_env(?APP_NAME,
    graph_sync_request_timeout, timer:seconds(6))).

-define(GS_HEALTHCHECK_INTERVAL, application:get_env(?APP_NAME,
    graph_sync_healthcheck_interval, timer:seconds(4))).
-define(GS_RECONNECT_BACKOFF_RATE, application:get_env(?APP_NAME,
    graph_sync_reconnect_backoff_rate, 1.5)).
-define(GS_RECONNECT_MAX_BACKOFF, application:get_env(?APP_NAME,
    graph_sync_reconnect_max_backoff, timer:minutes(5))).

% Macros to strip results from create into simpler form.
-define(CREATE_RETURN_ID(__Expr),
    case __Expr of
        {error, _} = __Err ->
            __Err;
        ok ->
            throw(create_did_not_return_id);
        {ok, {#gri{id = __Id}, _}} ->
            {ok, __Id};
        {ok, __Data} ->
            throw(create_did_not_return_id)
    end
).

-define(CREATE_RETURN_DATA(__Expr),
    case __Expr of
        {error, _} = __Err ->
            __Err;
        ok ->
            throw(create_did_not_return_data);
        {ok, {_, __Data}} ->
            {ok, __Data};
        {ok, __Data} ->
            {ok, __Data}
    end
).

-define(CREATE_RETURN_OK(__Expr),
    case __Expr of
        {error, _} = __Err ->
            __Err;
        ok ->
            ok;
        {ok, _} ->
            ok
    end
).

% Used to run given code if given request result is positive. Always returns
% the result (function return value is ignored).
-define(ON_SUCCESS(__Result, Fun),
    case __Result of
        __Error = {error, _} ->
            __Error;
        __Success ->
            Fun(__Success),
            __Success
    end
).

-endif.