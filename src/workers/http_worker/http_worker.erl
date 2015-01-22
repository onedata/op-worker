%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%% It is responsible for spawning processes which then process HTTP requests.
%%% @end
%%%--------------------------------------------------------------------
-module(http_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
init(_Args) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1. <br/>
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
    Result :: ok | {ok, Response} | {error, Error} | pong,
    Response :: term(),
    Error :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle({spawn_handler, SocketPid}) ->
    Pid = spawn(
        fun() ->
            erlang:monitor(process, SocketPid),
            opn_cowboy_bridge:set_socket_pid(SocketPid),
            opn_cowboy_bridge:request_processing_loop()
        end),
    {ok, Pid};

handle(_Msg) ->
    ?warning("http server unknown message: ~p", [_Msg]).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.