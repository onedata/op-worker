%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(fslogic_tests).
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher can decode messages to fslogic
protocol_buffers_test() ->
  FileLocationMessage = #getfilelocation{file_logic_name = "some_file"},
  FileLocationMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FileLocationMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getfilelocation", input = FileLocationMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "module", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, Answer_type} = ranch_handler:decode_protocol_buffer(MessageBytes),
  ?assert(Synch),
  ?assert(Task =:= module),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(Answer_type =:= "atom"),

  ?assert(is_record(Msg, fusemessage)),
  ?assert(Msg#fusemessage.id =:= "1"),
  ?assert(Msg#fusemessage.message_type =:= getfilelocation),

  InternalMsg = Msg#fusemessage.input,
  ?assert(is_record(InternalMsg, getfilelocation)),
  ?assert(InternalMsg#getfilelocation.file_logic_name =:= "some_file").

-endif.
