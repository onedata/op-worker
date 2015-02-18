%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a ranch handler for TCP server mocks.
%%% @end
%%%-------------------------------------------------------------------
-module(tcp_mock_handler).
-behaviour(ranch_protocol).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% Ranch API
-export([start_link/4]).
-export([init/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Ranch callback, called to spawn a new handler process for a connection.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Ref :: ranch:ref(), Socket :: term(), Transport :: module(), ProtoOpts :: term()) ->
    {ok, ConnectionPid :: pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.


%%--------------------------------------------------------------------
%% @doc
%% Init function will be called by the newly spawned process.
%% @end
%%--------------------------------------------------------------------
-spec init(Ref :: ranch:ref(), Socket :: term(), Transport :: module(), ProtoOpts :: term()) -> term().
init(Ref, Socket, Transport, [Port]) ->
    ok = ranch:accept_ack(Ref),
    {ok, Timeout } = application:get_env(?APP_NAME, tcp_connection_timeout),
    loop(Socket, Transport, Port, Timeout).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loop that handles a TCP connection.
%% @end
%%--------------------------------------------------------------------
loop(Socket, Transport, Port, Timeout) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            tcp_mock_server:register_packet(Port, Data),
            Transport:send(Socket, Data),
            loop(Socket, Transport, Port, Timeout);
        _ ->
            ok = Transport:close(Socket)
    end.
