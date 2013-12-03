%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of remote_files_manager.
%% It contains tests that base on ct.
%% @end
%% ===================================================================

-module(remote_files_manager_test_SUITE).

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("veil_modules/dao/dao_vfs.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("remote_file_management_pb.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([storage_helpers_management_test/1, helper_requests_test/1]).

all() -> [storage_helpers_management_test, helper_requests_test].

-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem
-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% Checks if appropriate storage helpers are used for different users
storage_helpers_management_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  ST_Helper1 = "ClusterProxy",
  ST_Helper2 = "DirectIO",
  TestFile = "storage_helpers_management_test_file",

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  FuseId1 = wss:handshakeInit(Socket, "hostname", []), %% Get first fuseId
  FuseId2 = wss:handshakeInit(Socket, "hostname", []), %% Get second fuseId
  wss:close(Socket),

  Fuse_groups = [#fuse_group_info{name = FuseId2, storage_helper = #storage_helper_info{name = ST_Helper2, init_args = ?TEST_ROOT}}],
  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, [ST_Helper1, [], Fuse_groups]),
  ?assertEqual(ok, InsertStorageAns),


  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile, FuseId1),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),
  ?assertEqual(ST_Helper1, Helper),

  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile, FuseId1),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(ST_Helper1, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, Helper3, _Id3, _Validity3, AnswerOpt3} = get_file_location(Host, Cert, Port, TestFile, FuseId2),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),
  ?assertEqual(ST_Helper2, Helper3),

  {Status4, Answer4} = delete_file(Host, Cert, Port, TestFile, FuseId2),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks if requests from helper "Cluster Proxy" are handled correctly
helper_requests_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  ST_Helper = "ClusterProxy",
  TestFile = "helper_requests_test_file",

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  Fuse_groups = [#fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ?TEST_ROOT}}],
  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, [ST_Helper, [], Fuse_groups]),
  ?assertEqual(ok, InsertStorageAns),

  {ReadFileAns, PemBin} = file:read_file(Cert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  %% Get FuseId
  {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  FuseId = wss:handshakeInit(Socket, "hostname", []),

  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile, FuseId),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),
  ?assertEqual(ST_Helper, Helper),

  {Status2, Answer2} = create_file_on_storage(Host, Cert, Port, Id),
  ?assertEqual("ok", Status2),
  ?assertEqual(list_to_atom(?VOK), Answer2),

  {WriteStatus, WriteAnswer, BytesWritten} = write(Host, Cert, Port, Id, 0, list_to_binary("abcdefgh")),
  ?assertEqual("ok", WriteStatus),
  ?assertEqual(?VOK, WriteAnswer),
  ?assertEqual(length("abcdefgh"), BytesWritten),

  {ReadStatus, ReadAnswer, ReadData} = read(Host, Cert, Port, Id, 2, 2),
  ?assertEqual("ok", ReadStatus),
  ?assertEqual(?VOK, ReadAnswer),
  ?assertEqual("cd", binary_to_list(ReadData)),

  {ReadStatus2, ReadAnswer2, ReadData2} = read(Host, Cert, Port, Id, 7, 2),
  ?assertEqual("ok", ReadStatus2),
  ?assertEqual(?VOK, ReadAnswer2),
  ?assertEqual("h", binary_to_list(ReadData2)),

  {WriteStatus2, WriteAnswer2, BytesWritten2} = write(Host, Cert, Port, Id, 3, list_to_binary("123")),
  ?assertEqual("ok", WriteStatus2),
  ?assertEqual(?VOK, WriteAnswer2),
  ?assertEqual(length("123"), BytesWritten2),

  {ReadStatus3, ReadAnswer3, ReadData3} = read(Host, Cert, Port, Id, 2, 5),
  ?assertEqual("ok", ReadStatus3),
  ?assertEqual(?VOK, ReadAnswer3),
  ?assertEqual("c123g", binary_to_list(ReadData3)),

  {ReadStatus4, ReadAnswer4, ReadData4} = read(Host, Cert, Port, Id, 0, 100),
  ?assertEqual("ok", ReadStatus4),
  ?assertEqual(?VOK, ReadAnswer4),
  ?assertEqual("abc123gh", binary_to_list(ReadData4)),

  {TruncateStatus, TruncateAnswer} = truncate_file_on_storage(Host, Cert, Port, Id, 5),
  ?assertEqual("ok", TruncateStatus),
  ?assertEqual(list_to_atom(?VOK), TruncateAnswer),

  {ReadStatus5, ReadAnswer5, ReadData5} = read(Host, Cert, Port, Id, 0, 100),
  ?assertEqual("ok", ReadStatus5),
  ?assertEqual(?VOK, ReadAnswer5),
  ?assertEqual("abc12", binary_to_list(ReadData5)),

  {Status3, Answer3} = delete_file_on_storage(Host, Cert, Port, Id),
  ?assertEqual("ok", Status3),
  ?assertEqual(list_to_atom(?VOK), Answer3),

  {Status4, Answer4} = delete_file(Host, Cert, Port, TestFile, FuseId),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

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

%% Simulates request from FUSE
create_file(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

%% Simulates request from FUSE
get_file_location(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

%% Simulates request from FUSE
delete_file(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

%% Each of following functions simulate one request from Cluster Proxy.
create_file_on_storage(Host, Cert, Port, FileID) ->
  OperationMessage = #createfile{file_id  = FileID},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_createfile(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "createfile", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

delete_file_on_storage(Host, Cert, Port, FileID) ->
  OperationMessage = #deletefileatstorage{file_id  = FileID},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_deletefileatstorage(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "deletefileatstorage", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

truncate_file_on_storage(Host, Cert, Port, FileID, Length) ->
  OperationMessage = #truncatefile{file_id  = FileID, length = Length},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_truncatefile(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "truncatefile", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

read(Host, Cert, Port, FileID, Offset, Size) ->
  OperationMessage = #readfile{file_id  = FileID, offset = Offset, size = Size},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_readfile(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "readfile", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "filedata",
  answer_decoder_name = "remote_file_management", synch = true, protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Data = remote_file_management_pb:decode_filedata(Bytes),
  Data2 = records_translator:translate(Data, "remote_file_management"),
  {Status, Data2#filedata.answer_status, Data2#filedata.data}.

write(Host, Cert, Port, FileID, Offset, WriteData) ->
  OperationMessage = #writefile{file_id  = FileID, offset = Offset, data = WriteData},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_writefile(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "writefile", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "writeinfo",
  answer_decoder_name = "remote_file_management", synch = true, protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  WriteInfo = remote_file_management_pb:decode_writeinfo(Bytes),
  WriteInfo2 = records_translator:translate(WriteInfo, "remote_file_management"),
  {Status, WriteInfo2#writeinfo.answer_status, WriteInfo2#writeinfo.bytes_written}.
