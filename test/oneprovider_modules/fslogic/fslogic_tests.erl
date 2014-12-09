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

%% TODO testy obecnie są w test/manual/fslogic_tester (testują również poprawność zachowania bazy danych) oraz istniejet test ct
%% dopisać tutaj takie same testy wykorzystujące mocki
%% (obecnie wiemy, że wysztsko działa dobrze, takie testy z mockami przydadzą się jeśli coś nie będzie działać
%% - łatwiej zdiagnozujemy gdzie jest problem)

-module(fslogic_tests).
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("files_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

-define(LOCATION_VALIDITY, 60*15).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher can decode messages to fslogic
protocol_buffers_test() ->
  FileLocationMessage = #getfilelocation{file_logic_name = "some_file"},
  FileLocationMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FileLocationMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FileLocationMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, {_, _}} = ws_handler:decode_clustermsg_pb(MessageBytes),
  ?assert(Task =:= fslogic),
  ?assert(Answer_decoder_name =:= "fuse_messages"),
  ?assert(ProtocolVersion == 1),
  ?assert(MsgId == 0),
  ?assert(Answer_type =:= "filelocation"),

  ?assert(is_record(Msg, fusemessage)),
  ?assert(Msg#fusemessage.message_type =:= getfilelocation),

  InternalMsg = Msg#fusemessage.input,
  ?assert(is_record(InternalMsg, getfilelocation)),
  ?assert(InternalMsg#getfilelocation.file_logic_name =:= "some_file").

-endif.
