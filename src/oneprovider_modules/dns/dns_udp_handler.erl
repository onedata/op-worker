%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is responsible for handling udp aspects of dns protocol.
%% @end
%% ===================================================================

-module(dns_udp_handler).
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/3, loop/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/3
%% ====================================================================
%% @doc Creates process to handle udp socket.
%% @end
%% ====================================================================
-spec start_link(Port, ResponseTTLInSecs, DispatcherTimeout) -> {ok, pid()} when
	Port :: non_neg_integer(),
	ResponseTTLInSecs :: non_neg_integer(),
	DispatcherTimeout :: non_neg_integer().
%% ====================================================================
start_link(Port, ResponseTTLInSecs, DispatcherTimeout) ->
	Pid = spawn_link(fun () -> open_socket_and_loop(Port, ResponseTTLInSecs, DispatcherTimeout) end),
	{ok, Pid}.

%% open_socket_and_loop/3
%% ====================================================================
%% @doc Creates udp socket and starts looping.
%% @end
%% ====================================================================
-spec open_socket_and_loop(Port, ResponseTTL, DispatcherTimeout) -> no_return() when
	Port :: non_neg_integer(),
	ResponseTTL :: non_neg_integer(),
	DispatcherTimeout :: non_neg_integer().
%% ====================================================================
open_socket_and_loop(Port, ResponseTTL, DispatcherTimeout) ->
	Opts = [binary, {active, false}, {reuseaddr, true}, {recbuf, 32768}],
	{ok, Socket} = gen_udp:open(Port, Opts),
	loop(Socket, ResponseTTL, DispatcherTimeout).


%% loop/3
%% ====================================================================
%% @doc Loop maintaining state.
%% @end
%% ====================================================================
-spec loop(Socket, ResponseTTL, DispatcherTimeout) -> no_return() when
	Socket :: inet:socket(),
	ResponseTTL :: non_neg_integer(),
	DispatcherTimeout :: non_neg_integer().
%% ====================================================================
loop(Socket, ResponseTTL, DispatcherTimeout) ->
	{ok, {Address, Port, Packet}} = gen_udp:recv(Socket, 0, infinity),
	spawn(fun () -> handle_request(Socket, Address, Port, Packet, DispatcherTimeout, ResponseTTL) end),
	?MODULE:loop(Socket, ResponseTTL, DispatcherTimeout).


%% handle_request/6
%% ====================================================================
%% @doc Handles dns request.
%% @end
%% ====================================================================
-spec handle_request(Socket, Address, Port, Packet, DispatcherTimeout, ResponseTTL) -> ok when
	Socket :: inet:socket(),
	Address :: inet:ip_address(),
	Port :: inet:port_number(),
	Packet :: binary(),
	DispatcherTimeout :: non_neg_integer(),
	ResponseTTL :: non_neg_integer().
%% ====================================================================
handle_request(Socket, Address, Port, Packet, DispatcherTimeout, ResponseTTL) ->
	case dns_utils:generate_answer(Packet, ?Dispatcher_Name, DispatcherTimeout, ResponseTTL, udp) of
		{ok, Response} -> gen_udp:send(Socket, Address, Port, Response);
		{error, Reason} -> ?error("Error processing dns request ~p", [Reason]), ok
	end.
