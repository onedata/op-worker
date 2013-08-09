%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic.
%% It contains tests that base on ct.
%% @end
%% ===================================================================

-module(fslogic_test_SUITE).
-include_lib("common_test/include/ct.hrl").

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-export([all/0]).
-export([integration_test/1]).

all() -> [integration_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Checks fslogic integration with dao and db
integration_test(_Config) ->
  ?INIT_DIST_TEST,

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = 6666,

  nodes_manager:start_deps_for_tester_node(),
  NodesUp = nodes_manager:start_test_on_nodes(1),
  false = lists:member(error, NodesUp),

  [FSLogicNode | _] = NodesUp,

  DB_Node = nodes_manager:get_db_node(),
  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}]]),
  false = lists:member(error, StartLog),

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  TestFile = "fslogic_test_file",
  DirName = "fslogic_test_dir",
  FilesInDirNames = ["file_in_dir1", "file_in_dir2", "file_in_dir3", "file_in_dir4",  "file_in_dir5"],
  FilesInDir = lists:map(fun(N) ->
    DirName ++ "/" ++ N
  end, FilesInDirNames),
  NewNameOfFIle = "new_name_of_file",

  {ok, _} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ["/tmp/root"]]),

  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile),
  Status = "ok",
  AnswerOpt0 = ?VOK,
  {Status1, _Helper1, _Id1, _Validity1, AnswerOpt1} = create_file(Host, Cert, Port, TestFile),
  Status1 = "ok",
  AnswerOpt1 = ?VEEXIST,



  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile),
  Status2 = "ok",
  AnswerOpt2 = ?VOK,
  Helper2 = Helper,
  Id2 = Id,

  {Status3, _Validity3, AnswerOpt3} = renew_file_location(Host, Cert, Port, TestFile),
  Status3 = "ok",
  AnswerOpt3 = ?VOK,

  {Status4, Answer4} = file_not_used(Host, Cert, Port, TestFile),
  Status4 = "ok",
  Answer4 = list_to_atom(?VOK),
  {Status4_1, Answer4_1} = file_not_used(Host, Cert, Port, TestFile),
  Status4_1 = "ok",
  Answer4_1 = list_to_atom(?VOK),



  %% Test automatic descriptors cleaning
  {Status4_2, Helper4_2, Id4_2, _Validity4_2, AnswerOpt4_2} = get_file_location(Host, Cert, Port, TestFile),
  Status4_2 = "ok",
  AnswerOpt4_2 = ?VOK,
  Helper4_2 = Helper,
  Id4_2 = Id,

  clear_old_descriptors(FSLogicNode),

  {Status4_4, _Validity4_4, AnswerOpt4_4} = renew_file_location(Host, Cert, Port, TestFile),
  Status4_4 = "ok",
  AnswerOpt4_4 = ?VENOENT,



  {Status5, Answer5} = mkdir(Host, Cert, Port, DirName),
  Status5 = "ok",
  Answer5 = list_to_atom(?VOK),
  {Status5_1, Answer5_1} = mkdir(Host, Cert, Port, DirName),
  Status5_1 = "ok",
  Answer5_1 = list_to_atom(?VEEXIST),

  CreateFile = fun(File) ->
    {Status6, _Helper6, _Id6, _Validity6, AnswerOpt6} = create_file(Host, Cert, Port, File),
    Status6 = "ok",
    AnswerOpt6 = ?VOK
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status7, Files7, AnswerOpt7} = ls(Host, Cert, Port, DirName, 10, 0),
  Status7 = "ok",
  AnswerOpt7 = ?VOK,
  Check7 = (length(FilesInDir) == length(Files7)),
  Check7 = true,
  lists:foreach(fun(Name7) ->
    CheckElem7 = lists:member(Name7, Files7),
    CheckElem7 = true
  end, FilesInDirNames),


  {Status7_1, Files7_1, AnswerOpt7_1} = ls(Host, Cert, Port, DirName, 3, non),
  Status7_1 = "ok",
  AnswerOpt7_1 = ?VOK,
  Check7_1 = (length(Files7_1) == 3),
  Check7_1 = true,

  {Status7_2, Files7_2, AnswerOpt7_2} = ls(Host, Cert, Port, DirName, 5, 3),
  Status7_2 = "ok",
  AnswerOpt7_2 = ?VOK,
  Check7_2 = (length(Files7_2) == 2),
  Check7_2 = true,
  lists:foreach(fun(Name7_2) ->
    CheckElem7_sum = (lists:member(Name7_2, Files7_2) or lists:member(Name7_2, Files7_1)),
    CheckElem7_sum = true
  end, FilesInDirNames),



  [FirstFileInDir | FilesInDirTail] = FilesInDir,
  [_ | FilesInDirNamesTail] = FilesInDirNames,
  {Status8, Answer8} = delete_file(Host, Cert, Port, FirstFileInDir),
  Status8 = "ok",
  Answer8 = list_to_atom(?VOK),

  {Status8_1, Answer8_1} = delete_file(Host, Cert, Port, FirstFileInDir),
  Status8_1 = "ok",
  Answer8_1 = list_to_atom(?VEIO),

  {Status9, Files9, AnswerOpt9} = ls(Host, Cert, Port, DirName, 10, non),
  Status9 = "ok",
  AnswerOpt9 = ?VOK,
  Check9 = (length(FilesInDirTail) == length(Files9)),
  Check9 = true,
  lists:foreach(fun(Name9) ->
    CheckElem9 = lists:member(Name9, Files9),
    CheckElem9 = true
  end, FilesInDirNamesTail),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  [_ | FilesInDirNamesTail2] = FilesInDirNamesTail,
  {Status10, Answer10} = rename_file(Host, Cert, Port, SecondFileInDir, NewNameOfFIle),
  Status10 = "ok",
  Answer10 = list_to_atom(?VOK),

  {Status10_2, Answer10_2} = change_file_perms(Host, Cert, Port, NewNameOfFIle, 8#400),
  Status10_2 = "ok",
  Answer10_2 = list_to_atom(?VOK),

  {Status11, Files11, AnswerOpt11} = ls(Host, Cert, Port, DirName, 10, non),
  Status11 = "ok",
  AnswerOpt11 = ?VOK,
  Check11 = (length(FilesInDirNamesTail2) == length(Files11)),
  Check11 = true,
  lists:foreach(fun(Name11) ->
    CheckElem11 = lists:member(Name11, Files11),
    CheckElem11 = true
  end, FilesInDirNamesTail2),



  {Status12, Answer12} = delete_file(Host, Cert, Port, DirName),
  Status12 = "ok",
  Answer12 = list_to_atom(?VENOTEMPTY),

  Delete = fun(File) ->
    {Status13, Answer13} = delete_file(Host, Cert, Port, File),
    Status13 = "ok",
    Answer13 = list_to_atom(?VOK)
  end,
  lists:foreach(Delete, FilesInDirTail2),

  {Status14, Answer14} = delete_file(Host, Cert, Port, DirName),
  Status14 = "ok",
  Answer14 = list_to_atom(?VOK),
  {Status14_1, Answer14_1} = delete_file(Host, Cert, Port, DirName),
  Status14_1 = "ok",
  Answer14_1 = list_to_atom(?VEIO),

  {Status15, Answer15} = delete_file(Host, Cert, Port, TestFile),
  Status15 = "ok",
  Answer15 = list_to_atom(?VOK),

  {Status16, Answer16} = delete_file(Host, Cert, Port, NewNameOfFIle),
  Status16 = "ok",
  Answer16 = list_to_atom(?VOK),

  StopLog = nodes_manager:stop_app_on_nodes(NodesUp),
  false = lists:member(error, StopLog),
  ok = nodes_manager:stop_nodes(NodesUp),
  nodes_manager:stop_deps_for_tester_node().

%% ====================================================================
%% Helper functions
%% ====================================================================

%% Each of following functions simulate one request from FUSE.
create_file(Host, Cert, Port, FileName) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
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

  FuseMessage = #fusemessage{id = "1", message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
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

  FuseMessage = #fusemessage{id = "1", message_type = "renewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocationvalidity",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Validity = fuse_messages_pb:decode_filelocationvalidity(Bytes),
  Validity2 = records_translator:translate(Validity, "fuse_messages"),
  {Status, Validity2#filelocationvalidity.validity, Validity2#filelocationvalidity.answer}.

file_not_used(Host, Cert, Port, FileName) ->
  FslogicMessage = #filenotused{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_filenotused(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "filenotused", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
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

  FuseMessage = #fusemessage{id = "1", message_type = "createdir", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
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

  FuseMessage = #fusemessage{id = "1", message_type = "getfilechildren", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filechildren",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Files = fuse_messages_pb:decode_filechildren(Bytes),
  Files2 = records_translator:translate(Files, "fuse_messages"),
  {Status, Files2#filechildren.child_logic_name, Files2#filechildren.answer}.

delete_file(Host, Cert, Port, FileName) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
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

  FuseMessage = #fusemessage{id = "1", message_type = "renamefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ok, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ssl:send(Socket, MessageBytes),
  {ok, Ans} = ssl:recv(Socket, 0, 5000),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

change_file_perms(Host, Cert, Port, FileName, Perms) ->
  FslogicMessage = #changefileperms{logic_file_name = FileName, perms = Perms},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileperms(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "changefileperms", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
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