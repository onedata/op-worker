%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for starting and monitoring a global
%%% gs_client_worker instance that maintains Graph Sync channel with OneZone.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0]).

-define(GS_WORKER_SUP, gs_worker_sup).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    erlang:send_after(?GS_HEALTHCHECK_INTERVAL, self(),
        {sync_timer, {connection_healthcheck, 0}}
    ),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck | {connection_healthcheck, integer()}) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle({connection_healthcheck, FailedRetries}) ->
    NewFailedRetries = case connection_healthcheck() of
        ok -> 0;
        error -> FailedRetries + 1
    end,
    Interval = case NewFailedRetries of
        0 ->
            ?GS_HEALTHCHECK_INTERVAL;
        _ ->
            BackoffFactor = math:pow(?GS_RECONNECT_BACKOFF_RATE, NewFailedRetries),
            Res = min(
                ?GS_RECONNECT_MAX_BACKOFF,
                round(?GS_HEALTHCHECK_INTERVAL * BackoffFactor)
            ),
            ?info("Next OneZone connection attempt in ~B seconds.", [
                Res div 1000
            ]),
            Res
    end,
    erlang:send_after(Interval, self(),
        {sync_timer, {connection_healthcheck, NewFailedRetries}}
    ),
    ok;
handle(Request) ->
    ?log_bad_request(Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok | {error, timeout | term()}.
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns supervisor flags for gs_worker.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 0, period => 1}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if gs_client_worker instance is up and running on one of cluster nodes
%% and in case it's not, starts the worker on one of the nodes (this code is
%% run on all cluster nodes).
%% @end
%%--------------------------------------------------------------------
-spec connection_healthcheck() -> ok | error.
connection_healthcheck() ->
    try
        Pid = global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME),
        Node = consistent_hasing:get_node(?GS_CLIENT_WORKER_GLOBAL_NAME),
        case {Pid, Node =:= node()} of
            {undefined, true} ->
                start_gs_client_worker();
            _ ->
                ok
        end
    catch
        _:Reason ->
            ?error_stacktrace(
                "Failed to start connection to OneZone due to: ~p",
                [Reason]
            ),
            error
    end.


-spec start_gs_client_worker() -> ok | error.
start_gs_client_worker() ->
    case supervisor:start_child(?GS_WORKER_SUP, gs_client_worker_spec()) of
        {ok, _} -> ok;
        {error, _} -> error
    end.


-spec gs_client_worker_spec() -> supervisor:child_spec().
gs_client_worker_spec() -> #{
    id => ?GS_CLIENT_WORKER_GLOBAL_NAME,
    start => {gs_client_worker, start_link, []},
    restart => temporary,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [gs_client_worker]
}.
