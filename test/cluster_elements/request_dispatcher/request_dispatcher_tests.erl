%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of request_dispatcher.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(request_dispatcher_tests).
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("modules_and_args.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher uses protocol buffer correctly.
protocol_buffers_test() ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
  synch = true, protocol_version = 1, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, ProtocolVersion, Msg, Answer_type} = ranch_handler:decode_protocol_buffer(MessageBytes),
  ?assert(Synch),
  ?assert(Msg =:= ping),
  ?assert(Task =:= module),
  ?assert(ProtocolVersion == 1),
  ?assert(Answer_type =:= "atom"),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),

  Message2 = #answer{answer_status = "ok", worker_answer = PongBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message2)),

  EncodedPong = ranch_handler:encode_answer(ok, "atom", pong),
  ?assert(EncodedPong =:= MessageBytes2).

-endif.
