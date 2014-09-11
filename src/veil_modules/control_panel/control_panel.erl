%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks.
%% It is responsible for spawning processes which then process HTTP requests.
%% @end
%% ===================================================================

-module(control_panel).
-behaviour(worker_plugin_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Sets up cowboy handlers for GUI and REST.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init(_Args) ->
    % Schedule the clearing of expired sessions - a periodical job
    % This job will run on every instance of control_panel, but since it is rare and lightweight
    % it won't cause performance problems.
    Pid = self(),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, 1, {clear_expired_sessions, Pid}}}),
    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version,
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {spawn_handler, SocketPid}) ->
    Pid = spawn(
        fun() ->
            erlang:monitor(process, SocketPid),
            veil_cowboy_bridge:set_socket_pid(SocketPid),
            veil_cowboy_bridge:request_processing_loop()
        end),
    Pid;

handle(ProtocolVersion, {clear_expired_sessions, Pid}) ->
    NumSessionsCleared = gui_session_handler:clear_expired_sessions(),
    ?info("Expired GUI sessions cleared (~p tokens removed)", [NumSessionsCleared]),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, ProtocolVersion, {clear_expired_sessions, Pid}}}),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ok.


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Stops cowboy listener and terminates
%% @end
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    ok.