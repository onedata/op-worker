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
  TestFile = "test_file3",
  DirName = "test_dir3",

  {Status, Helper, Id, Validity} = create_file(Host, Cert, Port, TestFile),
  io:format("Test file creation: aswer status: ~s, helper: ~s, id: ~s, validity: ~b~n", [Status, Helper, Id, Validity]),

  {Status2, Helper2, Id2, Validity2} = get_file_location(Host, Cert, Port, TestFile),
  io:format("Test file location check: aswer status: ~s, helper: ~s, id: ~s, validity: ~b~n", [Status2, Helper2, Id2, Validity2]),

  {Status3, Answer3, Validity3} = renew_file_location(Host, Cert, Port, TestFile),
  io:format("Test renewing location: aswer status: ~s, answer: ~s, validity: ~b~n", [Status3, Answer3, Validity3]),

  {Status4, Answer4} = file_not_used(Host, Cert, Port, TestFile),
  io:format("Test file not used message: aswer status: ~s, answer: ~p~n", [Status4, Answer4]),

  {Status5, Answer5} = mkdir(Host, Cert, Port, DirName),
  io:format("Test directory creation: aswer status: ~s, answer: ~p~n", [Status5, Answer5]),

  FilesInDir = [DirName ++ "/file_in_dir1", DirName ++ "/file_in_dir2", DirName ++ "/file_in_dir3"],
  CreateFile = fun(File) ->
    {Status6, Helper6, Id6, Validity6} = create_file(Host, Cert, Port, File),
    io:format("Test file ~s creation: aswer status: ~s, helper: ~s, id: ~s, validity: ~b~n", [File, Status6, Helper6, Id6, Validity6])
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status7, Files7} = ls(Host, Cert, Port, DirName),
  io:format("ls test: aswer status: ~s~n", [Status7]),
  PrintFiles = fun({Helper7, Id7, Validity7}) ->
    io:format("ls test output file: helper: ~s, id: ~s, validity: ~b~n", [Helper7, Id7, Validity7])
  end,
  lists:foreach(PrintFiles, Files7).

create_file(Host, Cert, Port, FileName) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
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
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getfilelocation", input = FslogicMessageMessageBytes},
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

renew_file_location(Host, Cert, Port, FileName) ->
  FslogicMessage = #renewfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "renewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocationvalidity",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Validity = fuse_messages_pb:decode_filelocationvalidity(Bytes),
  Validity2 = records_translator:translate(Validity, "fuse_messages"),
  {Status, Validity2#filelocationvalidity.answer, Validity2#filelocationvalidity.validity}.

file_not_used(Host, Cert, Port, FileName) ->
  FslogicMessage = #filenotused{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_filenotused(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "filenotused", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

mkdir(Host, Cert, Port, DirName) ->
  FslogicMessage = #createdir{dir_logic_name = DirName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createdir(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "createdir", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

ls(Host, Cert, Port, Dir) ->
  FslogicMessage = #getfilechildren{dir_logic_name = Dir},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilechildren(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getfilechildren", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filechildren",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Files = fuse_messages_pb:decode_filechildren(Bytes),
  Files2 = records_translator:translate(Files, "fuse_messages"),
  {Status, translate_file_list(Files2#filechildren.child_location)}.

translate_file_list(Files) ->
  translate_file_list(Files, []).

translate_file_list([], Ans) ->
  Ans;

translate_file_list([File | Rest], Ans) ->
  AnsPart = {File#filelocation.storage_helper, File#filelocation.file_id, File#filelocation.validity},
  translate_file_list(Rest, [AnsPart | Ans]).
