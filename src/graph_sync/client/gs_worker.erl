%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
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
-export([is_connected/0, force_connection_start/0]).
-export([terminate_connection/0, restart_connection/0]).
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
%% GraphSync channel.
%% @end
%%--------------------------------------------------------------------
is_connected() ->
    worker_proxy:call(?MODULE, is_connected).


%%--------------------------------------------------------------------
%% @doc
%% Immediately triggers onezone connection attempt.
%% Returns boolean indicating success.
%% @end
%%--------------------------------------------------------------------
-spec force_connection_start() -> boolean() | no_return().
force_connection_start() ->
    ?info("Attempting to start connection to Onezone (forced)"),
    Node = get_gs_client_node(),
    case rpc:call(Node, worker_proxy, call, [?MODULE, ensure_connected]) of
        % throw on rpc error
        Result when is_boolean(Result) -> Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% Terminates existing Onezone connection (if any).
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection() -> ok.
terminate_connection() ->
    ?info("Terminating connection to Onezone (forced)"),
    gs_client_worker:force_terminate().


%%--------------------------------------------------------------------
%% @doc
%% Kills existing Onezone connection (if any) and starts a new one.
%% @end
%%--------------------------------------------------------------------
-spec restart_connection() -> ok.
restart_connection() ->
    terminate_connection(),
    force_connection_start(),
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
handle(is_connected) ->
    case get_connection_status() of
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
        error ->
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
-spec connection_healthcheck() -> unregistered | skipped | alive | error.
connection_healthcheck() ->
    try
        case get_connection_status() of
            not_started -> start_gs_client_worker();
            Status -> Status
        end
    catch
        Type:Reason ->
            ?error_stacktrace(
                "Failed to start connection to Onezone due to ~p:~p",
                [Type, Reason]
            ),
            error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks connection status on the current node.
%% Status skipped indicates that another node is responsible for
%% the graph sync connection.
%% @end
%%--------------------------------------------------------------------
-spec get_connection_status() -> unregistered | skipped | not_started | alive.
get_connection_status() ->
    IsRegistered = provider_auth:is_registered(),
    Pid = global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME),
    LocalNode = node() == get_gs_client_node(),
    case {IsRegistered, Pid, LocalNode} of
        {false, _, _} -> unregistered;
        {true, undefined, false} -> skipped;
        {true, undefined, true} -> not_started;
        {true, Pid, _} when is_pid(Pid) -> alive
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node on which gs_client_worker should be running.
%% @end
%%--------------------------------------------------------------------
get_gs_client_node() ->
    consistent_hashing:get_node(?GS_CLIENT_WORKER_GLOBAL_NAME).


-spec start_gs_client_worker() -> alive | error.
start_gs_client_worker() ->
    case supervisor:start_child(?GS_WORKER_SUP, gs_client_worker_spec()) of
        {ok, _} ->
            try
                ?info("Running on-connect procedures"),
                oneprovider:on_connect_to_oz(),
                alive
            catch Type:Reason ->
                ?error_stacktrace(
                    "Failed to execute on-connect procedures, disconnecting - ~p:~p",
                    [Type, Reason]
                ),
                % Kill the connection to Onezone, which will cause a
                % connection retry during the next healthcheck
                gs_client_worker:force_terminate(),
                error
            end;
        {error, _} ->
            error
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
