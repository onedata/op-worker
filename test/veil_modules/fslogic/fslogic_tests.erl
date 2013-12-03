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
-include("veil_modules/dao/dao_types.hrl").
-include_lib("veil_modules/dao/dao.hrl").
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
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type} = ws_handler:decode_protocol_buffer(MessageBytes, standard_user),
  ?assert(Synch),
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

%% getfilelocation_test() ->
%%   PVersion = 1,
%%   File = "fslogic_test_file",
%%   FuseID = "1",
%%   SHelper = "1",
%%   FileId = File ++ "ID",
%%
%%   {ok, Mock} = gen_server_mock:new(),
%%   gen_server_mock:expect_call(Mock, fun({dao, ProtocolVersion, Pid, 30, {vfs, get_file, [File]}}, _From, _State) ->
%%     FileLoc = #file_location{storage_helper_id = SHelper, file_id = FileId},
%%     FileRec = #file{location = FileLoc},
%%     DaoAns = #veil_document{uuid = "1", record = FileRec},
%%     Pid ! DaoAns,
%%     ok
%%   end),
%%
%%   gen_server_mock:expect_call(Mock, fun({dao, ProtocolVersion, Pid, 20, {vfs, list_descriptors, [{by_file_n_owner, {File, FuseID}}, 10, 0]}}, _From, _State) ->
%%     Pid ! {ok, []},
%%     ok
%%   end),
%%
%%   gen_server_mock:expect_call(Mock, fun({dao, ProtocolVersion, Pid, 100, {vfs, save_descriptor, [Descriptor]}}, _From, _State) ->
%%     Pid ! {ok, ok},
%%     ok
%%   end),
%%
%%   Record = #getfilelocation{file_logic_name = File},
%%   Ans = fslogic:handle_fuse_message(PVersion, Record, FuseID),
%%   gen_server_mock:assert_expectations(Mock),
%%   ?assertEqual(SHelper, Ans#filelocation.storage_helper),
%%   ?assertEqual(FileId, Ans#filelocation.file_id),
%%   ?assertEqual(?LOCATION_VALIDITY, Ans#filelocation.validity),
%%   ok.

-endif.
