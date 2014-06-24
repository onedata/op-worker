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

-module(dns_ranch_tcp_handler).
-include("registered_names.hrl").
-behaviour(ranch_protocol).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/4, loop/5]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/4
%% ====================================================================
%% @doc Starts handler.
%% @end
%% ====================================================================
-spec start_link(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: term()) -> Result when
	Result ::  {ok, Pid},
	Pid :: pid().
%% ====================================================================
start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(fun () -> init(Ref, Socket, Transport, Opts) end),
	{ok, Pid}.


%% init/4
%% ====================================================================
%% @doc Initializes handler loop.
%% @end
%% ====================================================================
-spec init(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: list()) -> Result when
	Result ::  ok.
%% ====================================================================
init(Ref, Socket, Transport, Opts) ->
	TransportOpts = ranch:filter_options(Opts, [packet, keepalive], []),

	ResponseTTL = proplists:get_value(dns_response_ttl, Opts, 60),
	TCPIdleTimeInSecs = proplists:get_value(dns_tcp_timeout, Opts, 30),
	DispatcherTimeout = proplists:get_value(dispatcher_timeout, Opts, 30),
	TCPIdleTime = timer:seconds(TCPIdleTimeInSecs),

	Transport:setopts(Socket, TransportOpts),
	ok = ranch:accept_ack(Ref),
	loop(Socket, Transport, ResponseTTL, TCPIdleTime, DispatcherTimeout).


%% loop/5
%% ====================================================================
%% @doc Main handler loop.
%% @end
%% ====================================================================
-spec loop(Socket, Transport, ResponseTTL, TCPIdleTime, DispatcherTimeout) -> ok when
	Socket :: inet:socket(),
	Transport :: term(),
	ResponseTTL :: non_neg_integer(),
	TCPIdleTime :: non_neg_integer(),
	DispatcherTimeout :: non_neg_integer().
%% ====================================================================
loop(Socket, Transport, ResponseTTL, TCPIdleTime, DispatcherTimeout) ->
	case Transport:recv(Socket, 0, TCPIdleTime) of
		{ok, Packet} -> handle_request(Socket, Transport, Packet, ResponseTTL, DispatcherTimeout),
						?MODULE:loop(Socket, Transport, ResponseTTL, TCPIdleTime, DispatcherTimeout);
		{error, closed} -> ok;
		{error, Reason} -> ?warning("Error receiving packet, reason ~p", [Reason]),
						   Transport:close(Socket)
	end.


%% handle_request/5
%% ====================================================================
%% @doc Handles dns request.
%% @end
%% ====================================================================
-spec handle_request(Socket, Transport, Packet, ResponseTTL, DispatcherTimeout) -> term() when
	Socket :: inet:socket(),
	Transport :: term(),
	Packet :: binary(),
	ResponseTTL :: non_neg_integer(),
	DispatcherTimeout :: non_neg_integer().

%% ====================================================================
handle_request(Socket, Transport, Packet, ResponseTTL, DispatcherTimeout) ->
	case dns_utils:generate_answer(Packet, ?Dispatcher_Name, DispatcherTimeout, ResponseTTL, tcp) of
		{ok, Response} -> Transport:send(Socket, Response);
		{error, Reason} -> ?error("Error processing dns request ~p", [Reason]),
						   Transport:close(Socket)
	end.
