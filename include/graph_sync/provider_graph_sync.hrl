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

% Not configurable as it depends on current implementation in OP.
-define(GS_PROTOCOL_VERSION, 4).

% Graph Sync config
-define(GS_CHANNEL_SERVICE_NAME, <<"GS-channel-service">>).

-define(GS_CHANNEL_PORT, op_worker:get_env(
    graph_sync_port, 443)).

-define(GS_CHANNEL_PATH, op_worker:get_env(
    graph_sync_path, "/graph_sync/provider")).

-define(GS_REQUEST_TIMEOUT, op_worker:get_env(
    graph_sync_request_timeout, timer:seconds(30))).

-define(GS_RECONNECT_BASE_INTERVAL, op_worker:get_env(
    graph_sync_reconnect_base_interval, timer:seconds(3))).
-define(GS_RECONNECT_BACKOFF_RATE, op_worker:get_env(
    graph_sync_reconnect_backoff_rate, 1.35)).
-define(GS_RECONNECT_MAX_BACKOFF, op_worker:get_env(
    graph_sync_reconnect_max_backoff, timer:seconds(20))).

% how often logs appear when waiting for Onezone connection
-define(OZ_CONNECTION_AWAIT_LOG_INTERVAL, 300). % 5 minutes

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