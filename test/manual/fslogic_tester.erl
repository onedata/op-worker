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
-module(fslogic_tester).
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([test/0, test/1, test/3]).

%% ====================================================================
%% API functions
%% ====================================================================

test() ->
  test("localhost").

test(Host) ->
  test(Host, "veilfs.pem", 5555).

test(Host, Cert, Port) ->
  ssl:start(),

  {Status, Helper, Id, Validity} = create_file(Host, Cert, Port, "test_file"),
  io:format("Test file creation: aswer status: ~s, helper: ~s, id: ~s, validity: ~b~n", [Status, Helper, Id, Validity]),

  {Status2, Helper2, Id2, Validity2} = get_file_location(Host, Cert, Port, "test_file"),
  io:format("Test file location check: aswer status: ~s, helper: ~s, id: ~s, validity: ~b~n", [Status2, Helper2, Id2, Validity2]).

create_file(Host, Cert, Port, FileName) ->
  FileLocationMessage = #getnewfilelocation{file_logic_name = FileName},
  FileLocationMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FileLocationMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getnewfilelocation", input = FileLocationMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper, Location2#filelocation.file_id, Location2#filelocation.validity}.

get_file_location(Host, Cert, Port, FileName) ->
  FileLocationMessage = #getfilelocation{file_logic_name = FileName},
  FileLocationMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FileLocationMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getfilelocation", input = FileLocationMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper, Location2#filelocation.file_id, Location2#filelocation.validity}.