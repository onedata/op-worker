%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is responsible for handling tcp aspects of dns protocol.
%% @end
%% ===================================================================

-module(dns_tcp_handler).
-behaviour(ranch_protocol).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/4, loop/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/4
%% ====================================================================
%% @doc Starts handler.
%% @end
%% ====================================================================
-spec start_link(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: term()) -> Result when
    Result :: {ok, Pid},
    Pid :: pid().
%% ====================================================================
start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(fun() -> init(Ref, Socket, Transport, Opts) end),
    {ok, Pid}.


%% init/4
%% ====================================================================
%% @doc Initializes handler loop.
%% @end
%% ====================================================================
-spec init(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: list()) -> Result when
    Result :: ok.
%% ====================================================================
init(Ref, Socket, Transport, Opts) ->
    TransportOpts = ranch:filter_options(Opts, [packet, keepalive], []),

    TCPIdleTimeInSecs = proplists:get_value(dns_tcp_timeout, Opts, 30),
    TCPIdleTime = timer:seconds(TCPIdleTimeInSecs),

    Transport:setopts(Socket, TransportOpts),
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, TCPIdleTime).


%% loop/3
%% ====================================================================
%% @doc Main handler loop.
%% @end
%% ====================================================================
-spec loop(Socket, Transport, TCPIdleTime) -> ok when
    Socket :: inet:socket(),
    Transport :: term(),
    TCPIdleTime :: non_neg_integer().
%% ====================================================================
loop(Socket, Transport, TCPIdleTime) ->
    case Transport:recv(Socket, 0, TCPIdleTime) of
        {ok, Packet} ->
            handle_request(Socket, Transport, Packet),
            ?MODULE:loop(Socket, Transport, TCPIdleTime);
        {error, closed} ->
            ok;
        {error, Reason} ->
            ?warning("Error receiving packet, reason ~p", [Reason]),
            Transport:close(Socket)
    end.


%% handle_request/3
%% ====================================================================
%% @doc Handles dns request.
%% @end
%% ====================================================================
-spec handle_request(Socket, Transport, Packet) -> term() when
    Socket :: inet:socket(),
    Transport :: term(),
    Packet :: binary().
%% ====================================================================
handle_request(Socket, Transport, Packet) ->
    try
        case  dns_server:handle_query(Packet, tcp) of
            {ok, Response} ->
                Transport:send(Socket, Response);
            _ ->
                Transport:close(Socket)
        end
    catch T:M ->
        ?error_stacktrace("Error processing TCP DNS request ~p:~p", [T, M]),
        Transport:close(Socket)
    end.