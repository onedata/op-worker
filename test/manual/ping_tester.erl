%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains functions that allow manually test
%% if cluster is avaliable.
%% @end
%% ===================================================================
-module(ping_tester).
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([ping/2, ping/4]).

%% ====================================================================
%% API functions
%% ====================================================================

ping(Host, Module) ->
  ping(Host, Module, "veilfs.pem", 5555).

ping(Host, Module, Cert, Port) ->
  ssl:start(),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = atom_to_list(Module), message_type = "atom", answer_type = "atom",
  synch = true, protocol_version = 1, input = PingBytes},
  Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, raw}, {certfile, Cert}]),
  ssl:send(Socket, Msg),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  MsgAtom = communication_protocol_pb:decode_atom(Bytes),
  records_translator:translate(MsgAtom).