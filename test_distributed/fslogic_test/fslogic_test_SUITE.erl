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

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([integration_test/1]).

all() -> [integration_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Checks fslogic integration with dao and db
integration_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

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

  {InsertStorageAns, _} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ["/tmp/root"]]),
  ?assertEqual(ok, InsertStorageAns),

  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),
  {Status1, _Helper1, _Id1, _Validity1, AnswerOpt1} = create_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status1),
  ?assertEqual(?VEEXIST, AnswerOpt1),



  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Validity3, AnswerOpt3} = renew_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),

  {Status4, Answer4} = file_not_used(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),
  {Status4_1, Answer4_1} = file_not_used(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status4_1),
  ?assertEqual(list_to_atom(?VOK), Answer4_1),



  %% Test automatic descriptors cleaning
  {Status4_2, Helper4_2, Id4_2, _Validity4_2, AnswerOpt4_2} = get_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status4_2),
  ?assertEqual(?VOK, AnswerOpt4_2),
  ?assertEqual(Helper, Helper4_2),
  ?assertEqual(Id, Id4_2),

  clear_old_descriptors(FSLogicNode),

  {Status4_4, _Validity4_4, AnswerOpt4_4} = renew_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status4_4),
  ?assertEqual(?VENOENT, AnswerOpt4_4),



  {Status5, Answer5} = mkdir(Host, Cert, Port, DirName),
  ?assertEqual("ok", Status5),
  ?assertEqual(list_to_atom(?VOK), Answer5),
  {Status5_1, Answer5_1} = mkdir(Host, Cert, Port, DirName),
  ?assertEqual("ok", Status5_1),
  ?assertEqual(list_to_atom(?VEEXIST), Answer5_1),

  CreateFile = fun(File) ->
    {Status6, _Helper6, _Id6, _Validity6, AnswerOpt6} = create_file(Host, Cert, Port, File),
    ?assertEqual("ok", Status6),
    ?assertEqual(?VOK, AnswerOpt6)
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status7, Files7, AnswerOpt7} = ls(Host, Cert, Port, DirName, 10, 0),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VOK, AnswerOpt7),
  Check7 = (length(FilesInDir) == length(Files7)),
  Check7 = true,
  lists:foreach(fun(Name7) ->
    ?assert(lists:member(Name7, Files7))
  end, FilesInDirNames),


  {Status7_1, Files7_1, AnswerOpt7_1} = ls(Host, Cert, Port, DirName, 3, non),
  ?assertEqual("ok", Status7_1),
  ?assertEqual(?VOK, AnswerOpt7_1),
  ?assertEqual(3, length(Files7_1)),

  {Status7_2, Files7_2, AnswerOpt7_2} = ls(Host, Cert, Port, DirName, 5, 3),
  ?assertEqual("ok", Status7_2),
  ?assertEqual(?VOK, AnswerOpt7_2),
  ?assertEqual(2, length(Files7_2)),
  lists:foreach(fun(Name7_2) ->
    ?assert(lists:member(Name7_2, Files7_2) or lists:member(Name7_2, Files7_1))
  end, FilesInDirNames),



  [FirstFileInDir | FilesInDirTail] = FilesInDir,
  [_ | FilesInDirNamesTail] = FilesInDirNames,
  {Status8, Answer8} = delete_file(Host, Cert, Port, FirstFileInDir),
  ?assertEqual("ok", Status8),
  ?assertEqual(list_to_atom(?VOK), Answer8),

  {Status8_1, Answer8_1} = delete_file(Host, Cert, Port, FirstFileInDir),
  ?assertEqual("ok", Status8_1),
  ?assertEqual(list_to_atom(?VEIO), Answer8_1),

  {Status9, Files9, AnswerOpt9} = ls(Host, Cert, Port, DirName, 10, non),
  ?assertEqual("ok", Status9),
  ?assertEqual(?VOK, AnswerOpt9),
  ?assertEqual(length(FilesInDirTail), length(Files9)),
  lists:foreach(fun(Name9) ->
    ?assert(lists:member(Name9, Files9))
  end, FilesInDirNamesTail),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  [_ | FilesInDirNamesTail2] = FilesInDirNamesTail,
  {Status10, Answer10} = rename_file(Host, Cert, Port, SecondFileInDir, NewNameOfFIle),
  ?assertEqual("ok", Status10),
  ?assertEqual(list_to_atom(?VOK), Answer10),

  {Status10_2, Answer10_2} = change_file_perms(Host, Cert, Port, NewNameOfFIle, 8#400),
  ?assertEqual("ok", Status10_2),
  ?assertEqual(list_to_atom(?VOK), Answer10_2),

  {Status11, Files11, AnswerOpt11} = ls(Host, Cert, Port, DirName, 10, non),
  ?assertEqual("ok", Status11),
  ?assertEqual(?VOK, AnswerOpt11),
  ?assertEqual(length(FilesInDirNamesTail2), length(Files11)),
  lists:foreach(fun(Name11) ->
    ?assert(lists:member(Name11, Files11))
  end, FilesInDirNamesTail2),



  {Status12, Answer12} = delete_file(Host, Cert, Port, DirName),
  ?assertEqual("ok", Status12),
  ?assertEqual(list_to_atom(?VENOTEMPTY), Answer12),

  Delete = fun(File) ->
    {Status13, Answer13} = delete_file(Host, Cert, Port, File),
    ?assertEqual("ok", Status13),
    ?assertEqual(list_to_atom(?VOK), Answer13)
  end,
  lists:foreach(Delete, FilesInDirTail2),

  {Status14, Answer14} = delete_file(Host, Cert, Port, DirName),
  ?assertEqual("ok", Status14),
  ?assertEqual(list_to_atom(?VOK), Answer14),
  {Status14_1, Answer14_1} = delete_file(Host, Cert, Port, DirName),
  ?assertEqual("ok", Status14_1),
  ?assertEqual(list_to_atom(?VEIO), Answer14_1),

  {Status15, Answer15} = delete_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status15),
  ?assertEqual(list_to_atom(?VOK), Answer15),

  {Status16, Answer16} = delete_file(Host, Cert, Port, NewNameOfFIle),
  ?assertEqual("ok", Status16),
  ?assertEqual(list_to_atom(?VOK), Answer16),

  %% Link tests

  % Create link
  {Status17, Answer17} = create_link(Host, Cert, Port, "link_name", "/target/path"),
  ?assertEqual("ok", Status17),
  ?assertEqual(list_to_atom(?VOK), Answer17),

  % Create same link second time
  {Status18, Answer18} = create_link(Host, Cert, Port, "link_name", "/target/path1"),
  ?assertEqual("ok", Status18),
  ?assertEqual(list_to_atom(?VEEXIST), Answer18),

  % Check if created link has valid data
  {Status19, Answer19, LinkPath} = get_link(Host, Cert, Port, "link_name"),
  ?assertEqual("ok", Status19),
  ?assertEqual("ok", Answer19),
  ?assertEqual("/target/path", LinkPath),

  % Try to fetch invalid link data
  {Status20, Answer20, _} = get_link(Host, Cert, Port, "link_name1"),
  ?assertEqual("ok", Status20),
  ?assertEqual(?VENOENT, Answer20),
  ok.

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [FSLogicNode | _] = NodesUp,

  DB_Node = nodes_manager:get_db_node(),
  Port = 6666,
  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{port, Port}, {nodes, NodesUp}, {assertions, Assertions}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

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

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

create_link(Host, Cert, Port, From, To) ->
  FslogicMessage = #createlink{from_file_logic_name = From, to_file_logic_name = To},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createlink(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "createlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

get_link(Host, Cert, Port, FileName) ->
  FslogicMessage = #getlink{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getlink(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "linkinfo",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_linkinfo(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp#linkinfo.answer, Resp1#linkinfo.file_logic_name}.


clear_old_descriptors(Node) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds + 60*15 + 1,
  gen_server:call({?Dispatcher_Name, Node}, {fslogic, 1, {delete_old_descriptors_test, Time}}),
  timer:sleep(500).