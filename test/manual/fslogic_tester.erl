%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains functions that allow manually test
%% fslogic.
%% @end
%% ===================================================================
-module(fslogic_tester).
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("registered_names.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([test/1, test/2, test/4]).

%% ====================================================================
%% API functions
%% ====================================================================

%% Three functions below (test functions with different number of parameters)
%% do full test of fslogic. They simulate different requests from FSE.
test(FSLogicNode) ->
  test("localhost", FSLogicNode).

test(Host, FSLogicNode) ->
  test(Host, "cacerts/onedata.pem", 5555, FSLogicNode).

test(Host, Cert, Port, FSLogicNode) ->
  ssl:start(),
  TestFile = "fslogic_test_file",
  DirName = "fslogic_test_dir",
  FilesInDir = [DirName ++ "/file_in_dir1", DirName ++ "/file_in_dir2", DirName ++ "/file_in_dir3", DirName ++ "/file_in_dir4", DirName ++ "/file_in_dir5"],
  NewNameOfFIle = "new_name_of_file",

  {ok, _} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", "/tmp/onedata"]),

  {Status, Helper, Id, Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile),
  io:format("Test file creation: aswer status: ~s, helper: ~s, id: ~s, validity: ~b, answer: ~p ~n", [Status, Helper, Id, Validity, AnswerOpt0]),
  {Status1, Helper1, Id1, Validity1, AnswerOpt1} = create_file(Host, Cert, Port, TestFile),
  io:format("Test file created second time: aswer status: ~s, helper: ~s, id: ~s, validity: ~b answer: ~p ~n", [Status1, Helper1, Id1, Validity1, AnswerOpt1]),

  {Status2, Helper2, Id2, Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile),
  io:format("Test file location check: aswer status: ~s, helper: ~s, id: ~s, validity: ~b answer: ~p ~n", [Status2, Helper2, Id2, Validity2, AnswerOpt2]),

  {Status3, Answer3, Validity3, AnswerOpt3} = renew_file_location(Host, Cert, Port, TestFile),
  io:format("Test renewing location: aswer status: ~s, answer: ~s, validity: ~b answer: ~p ~n", [Status3, Answer3, Validity3, AnswerOpt3]),

  {Status4, Answer4} = file_not_used(Host, Cert, Port, TestFile),
  io:format("Test file not used message: aswer status: ~s, answer: ~p~n", [Status4, Answer4]),
  {Status4_1, Answer4_1} = file_not_used(Host, Cert, Port, TestFile),
  io:format("Test file not used message (second time): aswer status: ~s, answer: ~p~n", [Status4_1, Answer4_1]),



  io:format("Test automatic descriptors cleaning~n"),
  {Status4_2, Helper4_2, Id4_2, Validity4_2, AnswerOpt4} = get_file_location(Host, Cert, Port, TestFile),
  io:format("Test file location check: aswer status: ~s, helper: ~s, id: ~s, validity: ~b answer: ~p ~n", [Status4_2, Helper4_2, Id4_2, Validity4_2, AnswerOpt4]),

  clear_old_descriptors(FSLogicNode),

  {Status4_4, Answer4_4, Validity4_4, AnswerOpt5} = renew_file_location(Host, Cert, Port, TestFile),
  io:format("Test renewing location: aswer status: ~s, answer: ~s, validity: ~b answer: ~p ~n", [Status4_4, Answer4_4, Validity4_4, AnswerOpt5]),



  {Status5, Answer5} = mkdir(Host, Cert, Port, DirName),
  io:format("Test directory creation: aswer status: ~s, answer: ~p~n", [Status5, Answer5]),
  {Status5_1, Answer5_1} = mkdir(Host, Cert, Port, DirName),
  io:format("Test directory created second time: aswer status: ~s, answer: ~p~n", [Status5_1, Answer5_1]),

  CreateFile = fun(File) ->
    {Status6, Helper6, Id6, Validity6, AnswerOpt6} = create_file(Host, Cert, Port, File),
    io:format("Test file ~s creation: aswer status: ~s, helper: ~s, id: ~s, validity: ~b answer: ~p ~n", [File, Status6, Helper6, Id6, Validity6, AnswerOpt6])
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status7, Files7, AnswerOpt7} = ls(Host, Cert, Port, DirName, 10, 0),
  io:format("ls test (num 10, offset 0): aswer status: ~s, answer: ~p ~n", [Status7, AnswerOpt7]),
  PrintFiles = fun(Name7) ->
    io:format("ls test output file name: ~s~n", [Name7])
  end,
  lists:foreach(PrintFiles, Files7),

  {Status7_1, Files7_1, AnswerOpt8} = ls(Host, Cert, Port, DirName, 3, non),
  io:format("ls test (num 3, offset not specified): aswer status: ~s, answer: ~p ~n", [Status7_1, AnswerOpt8]),
  lists:foreach(PrintFiles, Files7_1),

  {Status7_2, Files7_2, AnswerOpt9} = ls(Host, Cert, Port, DirName, 5, 3),
  io:format("ls test (num 5, offset 3): aswer status: ~s, answer: ~p ~n", [Status7_2, AnswerOpt9]),
  lists:foreach(PrintFiles, Files7_2),

  [FirstFileInDir | FilesInDirTail] = FilesInDir,
  {Status8, Answer8} = delete_file(Host, Cert, Port, FirstFileInDir),
  io:format("Test file ~s delete: aswer status: ~s, answer: ~p~n", [FirstFileInDir, Status8, Answer8]),
  {Status8_1, Answer8_1} = delete_file(Host, Cert, Port, FirstFileInDir),
  io:format("Test file deleted second time: aswer status: ~s, answer: ~p~n", [Status8_1, Answer8_1]),

  {Status9, Files9, AnswerOpt10} = ls(Host, Cert, Port, DirName, 10, non),
  io:format("ls test: aswer status: ~s, answer: ~p ~n", [Status9, AnswerOpt10]),
  lists:foreach(PrintFiles, Files9),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  {Status10, Answer10} = rename_file(Host, Cert, Port, SecondFileInDir, NewNameOfFIle),
  io:format("Test file rename: aswer status: ~s, answer: ~p~n", [Status10, Answer10]),

  {Status16, Answer16} = change_file_perms(Host, Cert, Port, NewNameOfFIle, 8#400),
  io:format("Test file chmod: aswer status: ~s, answer: ~p~n", [Status16, Answer16]),

  {Status11, Files11, AnswerOpt11} = ls(Host, Cert, Port, DirName, 10, non),
  io:format("ls test: aswer status: ~s, answer: ~p ~n", [Status11, AnswerOpt11]),
  lists:foreach(PrintFiles, Files11),

  {Status12, Answer12} = delete_file(Host, Cert, Port, DirName),
  io:format("Directory delete: aswer status: ~s, answer: ~p~n", [Status12, Answer12]),

  Delete = fun(File) ->
    {Status13, Answer13} = delete_file(Host, Cert, Port, File),
    io:format("Test file ~s delete: aswer status: ~s, answer: ~p~n", [File, Status13, Answer13])
  end,
  lists:foreach(Delete, FilesInDirTail2),

  {Status14, Answer14} = delete_file(Host, Cert, Port, DirName),
  io:format("Directory delete: aswer status: ~s, answer: ~p~n", [Status14, Answer14]),
  {Status14_1, Answer14_1} = delete_file(Host, Cert, Port, DirName),
  io:format("Directory delete second time: aswer status: ~s, answer: ~p~n", [Status14_1, Answer14_1]),

  {Status15, Answer15} = delete_file(Host, Cert, Port, TestFile),
  io:format("Test file ~s delete: aswer status: ~s, answer: ~p~n", [TestFile, Status15, Answer15]),

  {Status17, Answer17} = delete_file(Host, Cert, Port, NewNameOfFIle),
  io:format("Test file ~s delete: aswer status: ~s, answer: ~p~n", [NewNameOfFIle, Status17, Answer17]).

%% Each of following functions simulate one request from FUSE.
create_file(Host, Cert, Port, FileName) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

get_file_location(Host, Cert, Port, FileName) ->
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

renew_file_location(Host, Cert, Port, FileName) ->
  FslogicMessage = #renewfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocationvalidity",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Validity = fuse_messages_pb:decode_filelocationvalidity(Bytes),
  Validity2 = records_translator:translate(Validity, "fuse_messages"),
  {Status, Validity2#filelocationvalidity.answer, Validity2#filelocationvalidity.validity, Validity2#filelocationvalidity.answer}.

file_not_used(Host, Cert, Port, FileName) ->
  FslogicMessage = #filenotused{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_filenotused(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "filenotused", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

mkdir(Host, Cert, Port, DirName) ->
  FslogicMessage = #createdir{dir_logic_name = DirName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createdir(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createdir", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

ls(Host, Cert, Port, Dir, Num, Offset) ->
  FslogicMessage = case Offset of
    non -> #getfilechildren{dir_logic_name = Dir, children_num = Num};
    _Other -> #getfilechildren{dir_logic_name = Dir, children_num = Num, offset = Offset}
  end,
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilechildren(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilechildren", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filechildren",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Files = fuse_messages_pb:decode_filechildren(Bytes),
  Files2 = records_translator:translate(Files, "fuse_messages"),
  FileList1 = lists:map(fun(#filechildren_direntry{name = Name}) -> Name end, Files2#filechildren.entry),
  {Status, FileList1, Files2#filechildren.answer}.

delete_file(Host, Cert, Port, FileName) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

rename_file(Host, Cert, Port, FileName, NewName) ->
  FslogicMessage = #renamefile{from_file_logic_name = FileName, to_file_logic_name = NewName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renamefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renamefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

change_file_perms(Host, Cert, Port, FileName, Perms) ->
    FslogicMessage = #changefileperms{file_logic_name = FileName, perms = Perms},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileperms(FslogicMessage)),

    FuseMessage = #fusemessage{message_type = "changefileperms", input = FslogicMessageMessageBytes},
    FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

    Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
    MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
    ssl:send(Socket, MessageBytes),
    {ok, Ans} = ssl:recv(Socket, 0, 5000),

    #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
    Answer = communication_protocol_pb:decode_atom(Bytes),
    Answer2 = records_translator:translate(Answer, "communication_protocol"),
    {Status, Answer2}.


clear_old_descriptors(Node) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds + 60*15 + 1,
  gen_server:call({?Dispatcher_Name, Node}, {fslogic, 1, {delete_old_descriptors_test, Time}}),
  timer:sleep(500).
