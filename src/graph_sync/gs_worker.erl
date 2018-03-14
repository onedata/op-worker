%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for starting and monitoring a global
%%% gs_client_worker instance that maintains Graph Sync channel with Onezone.
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
-export([ensure_connected/0]).
-export([restart_connection/0]).
-export([supervisor_flags/0]).

-define(GS_WORKER_SUP, gs_worker_sup).

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

%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if the provider is actively connected to Onezone via
%% GraphSync channel. If not, tries to reconnect before returning false.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected() -> boolean().
ensure_connected() ->
    worker_proxy:call(?MODULE, ensure_connected).

%%--------------------------------------------------------------------
%% @doc
%% Kills existing Onezone connection (if any) and starts a new one if run on
%% dedicated graph_sync node.
%% If run on all cluster nodes, the connection will be restarted immediately.
%% @end
%%--------------------------------------------------------------------
-spec restart_connection() -> ok.
restart_connection() ->
    case global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME) of
        Pid when is_pid(Pid) -> exit(Pid, kill);
        _ -> ok
    end,
    % Force a reconnection attempt:
    %   * if this is the dedicated node, the connection will be started immediately
    %   * if not, next periodic healthcheck will start the connection
    ensure_connected(),
    ok.

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
handle(ensure_connected) ->
    case connection_healthcheck() of
        alive -> true;
        _ -> false
    end;
handle({connection_healthcheck, FailedRetries}) ->
    {Interval, NewFailedRetries} = case connection_healthcheck() of
        unregistered ->
            ?debug("Skipping connection to Onezone as the provider is not registered."),
            {?GS_HEALTHCHECK_INTERVAL, 0};
        skipped ->
            ?debug("Skipping connection to Onezone as this is not the dedicated node."),
            {?GS_HEALTHCHECK_INTERVAL, 0};
        alive ->
            {?GS_HEALTHCHECK_INTERVAL, 0};
        connection_error ->
            FailedRetries2 = FailedRetries + 1,
            BackoffFactor = math:pow(?GS_RECONNECT_BACKOFF_RATE, FailedRetries2),
            Interval2 = min(
                ?GS_RECONNECT_MAX_BACKOFF,
                round(?GS_HEALTHCHECK_INTERVAL * BackoffFactor)
            ),
            ?info("Next Onezone connection attempt in ~B seconds.", [
                Interval2 div 1000
            ]),
            {Interval2, FailedRetries2}
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
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if gs_client_worker instance is up and running on one of cluster nodes
%% and in case it's not, starts the worker if this is the dedicated node. Must
%% be run on all nodes to ensure that the connection is restarted.
%% @end
%%--------------------------------------------------------------------
-spec connection_healthcheck() -> unregistered | skipped | alive | connection_error.
connection_healthcheck() ->
    try
        IsRegistered = provider_auth:is_registered(),
        Pid = global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME),
        LocalNode = node() == consistent_hasing:get_node(?GS_CLIENT_WORKER_GLOBAL_NAME),
        case {IsRegistered, Pid, LocalNode} of
            {false, _, _} -> unregistered;
            {true, undefined, false} -> skipped;
            {true, undefined, true} -> start_gs_client_worker();
            {true, Pid, _} when is_pid(Pid) -> alive
        end
    catch
        _:Reason ->
            ?error_stacktrace(
                "Failed to start connection to Onezone due to: ~p",
                [Reason]
            ),
            error
    end.


-spec start_gs_client_worker() -> alive | connection_error.
start_gs_client_worker() ->
    case supervisor:start_child(?GS_WORKER_SUP, gs_client_worker_spec()) of
        {ok, _} -> alive;
        {error, _} -> connection_error
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
