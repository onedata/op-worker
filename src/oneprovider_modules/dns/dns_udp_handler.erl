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
-export([start_link/1, loop/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/1
%% ====================================================================
%% @doc Creates process to handle udp socket.
%% @end
%% ====================================================================
-spec start_link(Port) -> {ok, pid()} when
    Port :: non_neg_integer().
%% ====================================================================
start_link(Port) ->
    Pid = spawn_link(fun() -> open_socket_and_loop(Port) end),
    {ok, Pid}.

%% open_socket_and_loop/1
%% ====================================================================
%% @doc Creates udp socket and starts looping.
%% @end
%% ====================================================================
-spec open_socket_and_loop(Port) -> no_return() when
    Port :: non_neg_integer().
%% ====================================================================
open_socket_and_loop(Port) ->
    Opts = [binary, {active, false}, {reuseaddr, true}, {recbuf, 32768}],
    {ok, Socket} = gen_udp:open(Port, Opts),
    loop(Socket).


%% loop/1
%% ====================================================================
%% @doc Loop maintaining state.
%% @end
%% ====================================================================
-spec loop(Socket) -> no_return() when
    Socket :: inet:socket().
%% ====================================================================
loop(Socket) ->
    {ok, {Address, Port, Packet}} = gen_udp:recv(Socket, 0, infinity),
    spawn(fun() -> handle_request(Socket, Address, Port, Packet) end),
    ?MODULE:loop(Socket).


%% handle_request/4
%% ====================================================================
%% @doc Handles dns request.
%% @end
%% ====================================================================
-spec handle_request(Socket, Address, Port, Packet) -> ok when
    Socket :: inet:socket(),
    Address :: inet:ip_address(),
    Port :: inet:port_number(),
    Packet :: binary().
%% ====================================================================
handle_request(Socket, Address, Port, Packet) ->
    try
        case dns_server:handle_query(Packet, udp) of
            {ok, Response} ->
                gen_udp:send(Socket, Address, Port, Response);
            _ ->
                ok
        end
    catch T:M ->
        ?error_stacktrace("Error processing UDP DNS request ~p:~p", [T, M])
    end.
