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

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("remote_file_management_pb.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([storage_helpers_management_test/1, helper_requests_test/1, permissions_test/1]).

all() -> [storage_helpers_management_test, helper_requests_test, permissions_test].

-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% Tests if not permitted operations can not be executed
permissions_test(Config) ->
  NodesUp = ?config(nodes, Config),

  ST_Helper = "ClusterProxy",
  Team1 = ?TEST_GROUP,
  TestFile = "permissions_test_file",
  TestFile2 = filename:join([?SPACES_BASE_DIR_NAME, Team1, "permissions_test_file"]),

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  ?ENABLE_PROVIDER(Config),

  Fuse_groups = [#fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ?ARG_TEST_ROOT}}],
  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, [ST_Helper, [], Fuse_groups]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, Team1]),
  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, Team1]),

  Login = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc1]),
  Login2 = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc2]),

  [DN | _] = UserDoc1#db_document.record#user.dn_list,
  [DN2 | _] = UserDoc2#db_document.record#user.dn_list,

  %% Get FuseId
  {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  _FuseId = wss:handshakeInit(Socket, "hostname", []),

  %% Get FuseId
  {ok, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}]),
  FuseId2 = wss:handshakeInit(Socket2, "hostname", []),
  wss:handshakeAck(Socket2,FuseId2),

  {Status0, _, _, Id0, _, AnswerOpt0} = create_file(Host, Cert2, Port, TestFile, FuseId2),
  ?assertEqual("ok", Status0),
  ?assertEqual(?VOK, AnswerOpt0),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Host, Cert2, Port, TestFile, FuseId2),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),


  {Status1, AnswerOpt1} = create_file_on_storage(Host, Cert2, Port, Id0),
  ?assertEqual("ok", Status1),
  ?assertEqual(list_to_atom(?VOK), AnswerOpt1),

  UsrBeg = string:str(Id0, Login2),
  WrongId1 = string:substr(Id0, 1, UsrBeg - 1) ++ Login ++ string:substr(Id0, UsrBeg + length(Login2)),
  {Status2, AnswerOpt2} = create_file_on_storage(Host, Cert2, Port, WrongId1),
  ?assertEqual("ok", Status2),
  ?assertEqual(list_to_atom(?VEACCES), AnswerOpt2),

  WrongId2 = string:substr(Id0, 1, UsrBeg - 2) ++ string:substr(Id0, UsrBeg + length(Login2)),

  {Status3, AnswerOpt3} = create_file_on_storage(Host, Cert2, Port, WrongId2),
  ?assertEqual("ok", Status3),
  ?assertEqual(list_to_atom(?VEACCES), AnswerOpt3),

  {WriteStatus0, WriteAnswer0, BytesWritten0} = write(Host, Cert2, Port, Id0, 0, list_to_binary("abcdefgh")),
  ?assertEqual("ok", WriteStatus0),
  ?assertEqual(?VOK, WriteAnswer0),
  ?assertEqual(length("abcdefgh"), BytesWritten0),


  {Status4, _, _, Id1, _, AnswerOpt4} = create_file(Host, Cert2, Port, TestFile2, FuseId2),
  ?assertEqual("ok", Status4),
  ?assertEqual(?VOK, AnswerOpt4),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Host, Cert2, Port, TestFile2, FuseId2),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {Status5, AnswerOpt5} = create_file_on_storage(Host, Cert2, Port, Id1),
  ?assertEqual("ok", Status5),
  ?assertEqual(list_to_atom(?VOK), AnswerOpt5),

  {WriteStatus01, WriteAnswer01, BytesWritten01} = write(Host, Cert2, Port, Id1, 0, list_to_binary("abcdefgh")),
  ?assertEqual("ok", WriteStatus01),
  ?assertEqual(?VOK, WriteAnswer01),
  ?assertEqual(length("abcdefgh"), BytesWritten01),

  UsrBeg2 = string:str(Id1, Team1),
  WrongId3 = string:substr(Id1, 1, UsrBeg2 - 1) ++ "other_team" ++ string:substr(Id1, UsrBeg2 + length(Team1)),
  {Status6, AnswerOpt6} = create_file_on_storage(Host, Cert2, Port, WrongId3),
  ?assertEqual("ok", Status6),
  ?assertEqual(list_to_atom(?VENOENT), AnswerOpt6),




  {DeleteStatus, DeleteAnswer} = delete_file_on_storage(Host, Cert, Port, Id0),
  ?assertEqual("ok", DeleteStatus),
  ?assertEqual(list_to_atom(?VEACCES), DeleteAnswer),

  {WriteStatus, WriteAnswer, _} = write(Host, Cert, Port, Id0, 0, list_to_binary("xyz")),
  ?assertEqual("ok", WriteStatus),
  ?assertEqual(?VEACCES, WriteAnswer),

  {TruncateStatus, TruncateAnswer} = truncate_file_on_storage(Host, Cert, Port, Id0, 5),
  ?assertEqual("ok", TruncateStatus),
  ?assertEqual(list_to_atom(?VEACCES), TruncateAnswer),

  {ReadStatus, ReadAnswer, _} = read(Host, Cert, Port, Id0, 2, 2),
  ?assertEqual("ok", ReadStatus),
  ?assertEqual(?VEACCES, ReadAnswer),


  {PermStatus, PermAnswer} = change_perm_on_storage(Host, Cert, Port, Id0, 8#521),
  ?assertEqual("ok", PermStatus),
  ?assertEqual(list_to_atom(?VEPERM), PermAnswer),




  {DeleteStatus2, DeleteAnswer2} = delete_file_on_storage(Host, Cert, Port, WrongId2),
  ?assertEqual("ok", DeleteStatus2),
  ?assertEqual(list_to_atom(?VENOENT), DeleteAnswer2),

  {WriteStatus2, WriteAnswer2, _} = write(Host, Cert, Port, WrongId2, 0, list_to_binary("xyz")),
  ?assertEqual("ok", WriteStatus2),
  ?assertEqual(?VENOENT, WriteAnswer2),

  {TruncateStatus2, TruncateAnswer2} = truncate_file_on_storage(Host, Cert, Port, WrongId2, 5),
  ?assertEqual("ok", TruncateStatus2),
  ?assertEqual(list_to_atom(?VENOENT), TruncateAnswer2),

  {ReadStatus2, ReadAnswer2, _} = read(Host, Cert, Port, WrongId2, 2, 2),
  ?assertEqual("ok", ReadStatus2),
  ?assertEqual(?VENOENT, ReadAnswer2),


  {PermStatus2, PermAnswer2} = change_perm_on_storage(Host, Cert, Port, WrongId2, 8#521),
  ?assertEqual("ok", PermStatus2),
  ?assertEqual(list_to_atom(?VENOENT), PermAnswer2),



    %% Normally its possible to delete file when user has permissions to write both file and its parent directory
%%   {DeleteStatus3, DeleteAnswer3} = delete_file_on_storage(Host, Cert, Port, Id1),
%%   ?assertEqual("ok", DeleteStatus3),
%%   ?assertEqual(list_to_atom(?VEACCES), DeleteAnswer3),

%% Share file with other user (with 'Cert')
  {PermStatus3, PermAnswer3} = change_perm_on_storage(Host, Cert2, Port, Id1, 8#660),
  ?assertEqual("ok", PermStatus3),
  ?assertEqual(list_to_atom(?VOK), PermAnswer3),

  {WriteStatus3, WriteAnswer3, BytesWritten3} = write(Host, Cert, Port, Id1, 0, list_to_binary("xyz")),
  ?assertEqual("ok", WriteStatus3),
  ?assertEqual(?VOK, WriteAnswer3),
  ?assertEqual(length("xyz"), BytesWritten3),

  {TruncateStatus3, TruncateAnswer3} = truncate_file_on_storage(Host, Cert, Port, Id1, 5),
  ?assertEqual("ok", TruncateStatus3),
  ?assertEqual(list_to_atom(?VOK), TruncateAnswer3),

  {ReadStatus3, ReadAnswer3, ReadData3} = read(Host, Cert, Port, Id1, 2, 2),
  ?assertEqual("ok", ReadStatus3),
  ?assertEqual(?VOK, ReadAnswer3),
  ?assertEqual("zd", binary_to_list(ReadData3)),

  {PermStatus4, PermAnswer4} = change_perm_on_storage(Host, Cert, Port, Id1, 8#521),
  ?assertEqual("ok", PermStatus4),
  ?assertEqual(list_to_atom(?VEPERM), PermAnswer4),





  {PermStatus4_2, PermAnswer4_2} = change_perm_on_storage(Host, Cert2, Port, Id1, 8#640),
  ?assertEqual("ok", PermStatus4_2),
  ?assertEqual(list_to_atom(?VOK), PermAnswer4_2),




  {DeleteStatus4, DeleteAnswer4} = delete_file_on_storage(Host, Cert, Port, Id1),
  ?assertEqual("ok", DeleteStatus4),
  ?assertEqual(list_to_atom(?VEPERM), DeleteAnswer4),

  {WriteStatus4, WriteAnswer4, _} = write(Host, Cert, Port, Id1, 0, list_to_binary("qpr")),
  ?assertEqual("ok", WriteStatus4),
  ?assertEqual(?VEACCES, WriteAnswer4),

  {TruncateStatus4, TruncateAnswer4} = truncate_file_on_storage(Host, Cert, Port, Id1, 3),
  ?assertEqual("ok", TruncateStatus4),
  ?assertEqual(list_to_atom(?VEACCES), TruncateAnswer4),

  {ReadStatus4, ReadAnswer4, ReadData4} = read(Host, Cert, Port, Id1, 2, 2),
  ?assertEqual("ok", ReadStatus4),
  ?assertEqual(?VOK, ReadAnswer4),
  ?assertEqual("zd", binary_to_list(ReadData4)),

  {PermStatus5, PermAnswer5} = change_perm_on_storage(Host, Cert, Port, Id1, 8#521),
  ?assertEqual("ok", PermStatus5),
  ?assertEqual(list_to_atom(?VEPERM), PermAnswer5),




  {DeleteStatus5, DeleteAnswer5} = delete_file_on_storage(Host, Cert2, Port, Id1),
  ?assertEqual("ok", DeleteStatus5),
  ?assertEqual(list_to_atom(?VOK), DeleteAnswer5),



  {FSLogicDelStatus, FSLogicDelAnswer} = delete_file(Host, Cert2, Port, TestFile, FuseId2),
  ?assertEqual("ok", FSLogicDelStatus),
  ?assertEqual(list_to_atom(?VOK), FSLogicDelAnswer),

  {FSLogicDelStatus2, FSLogicDelAnswer2} = delete_file(Host, Cert2, Port, TestFile2, FuseId2),
  ?assertEqual("ok", FSLogicDelStatus2),
  ?assertEqual(list_to_atom(?VOK), FSLogicDelAnswer2),


  wss:close(Socket),
  wss:close(Socket2),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team1], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER2], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),

  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% Checks if appropriate storage helpers are used for different users
storage_helpers_management_test(Config) ->
  NodesUp = ?config(nodes, Config),

  ST_Helper = "DirectIO",
  TestFile = "storage_helpers_management_test_file",

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  ?ENABLE_PROVIDER(Config),

  UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_GROUP]),
  [DN | _] = UserDoc#db_document.record#user.dn_list,

  {ok, Socket1} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  {ok, Socket2} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  FuseId1 = wss:handshakeInit(Socket1, "hostname", [{testvar1, "testvalue1"}, {group_id, "group1"}]), %% Get first fuseId
  FuseId2 = wss:handshakeInit(Socket2, "hostname", [{testvar1, "testvalue1"}, {group_id, "group2"}]), %% Get second fuseId
  wss:handshakeAck(Socket1,FuseId1),
  wss:handshakeAck(Socket2,FuseId2),
  Fuse_groups = [#fuse_group_info{name = "group2", storage_helper = #storage_helper_info{name = ST_Helper, init_args = ?ARG_TEST_ROOT2}}],
  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, [ST_Helper, ?ARG_TEST_ROOT, Fuse_groups]),
  ?assertEqual(ok, InsertStorageAns),


  {Status, _, Helper, Id, _Validity, AnswerOpt0} = create_file(Host, Cert, Port, TestFile, FuseId1),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),
  ?assertEqual(?ARG_TEST_ROOT, Helper),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Host, Cert, Port, TestFile, FuseId1),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {Status2, _, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile, FuseId1),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(?ARG_TEST_ROOT, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _, Helper3, _Id3, _Validity3, AnswerOpt3} = get_file_location(Host, Cert, Port, TestFile, FuseId2),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),
  ?assertEqual(?ARG_TEST_ROOT2, Helper3),

  {Status4, Answer4} = delete_file(Host, Cert, Port, TestFile, FuseId2),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),

  wss:close(Socket1),
  wss:close(Socket2),
  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_GROUP], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks if requests from helper "Cluster Proxy" are handled correctly
helper_requests_test(Config) ->
  NodesUp = ?config(nodes, Config),

  ST_Helper = "ClusterProxy",
  TestFile = "helper_requests_test_file",
  TestFile2 = "../helper_requests_test_file2",

  Cert = ?COMMON_FILE("peer.pem"),
  _Cert2 = ?COMMON_FILE("peer2.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  ?ENABLE_PROVIDER(Config),

  Fuse_groups = [#fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ?ARG_TEST_ROOT}}],
  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, [ST_Helper, [], Fuse_groups]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),

  [DN | _] = user_logic:get_dn_list(UserDoc),
  Login = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc]),

  %% Get FuseId
  {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  FuseId = wss:handshakeInit(Socket, "hostname", []),
  wss:handshakeAck(Socket,FuseId),

  {Status, Helper, _, Id, _Validity, AnswerOpt} = create_file(Host, Cert, Port, TestFile, FuseId),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt),
  ?assertEqual(ST_Helper, Helper),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Host, Cert, Port, TestFile, FuseId),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {Status2, _, _, _, _, AnswerOpt2} = create_file(Host, Cert, Port, TestFile2, FuseId),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VEREMOTEIO, AnswerOpt2),

  Tokens = string:tokens(Id, "/"),
  ?assertEqual(4, length(Tokens)),
  [StorageNum | Path] = Tokens,
  [MainDir | Path2] = Path,
  [Dir | NameEnding] = Path2,
  ?assert(is_integer(list_to_integer(StorageNum))),
  ?assertEqual("spaces", MainDir),
  ?assertEqual(Login, Dir),

  {Status2, Answer2} = create_file_on_storage(Host, Cert, Port, Id),
  ?assertEqual("ok", Status2),
  ?assertEqual(list_to_atom(?VOK), Answer2),

  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Dir ++ "/" ++ NameEnding)),
  {OwnStatus, User, _Group} = files_tester:get_owner(?TEST_ROOT ++ "/spaces/" ++ Dir ++ "/" ++ NameEnding),
  ?assertEqual(ok, OwnStatus),
  ?assert(User /= 0),

  %% Group is not set for files in user-only-context
  %% ?assert(Group /= 0),

  {WriteStatus, WriteAnswer, BytesWritten} = write(Host, Cert, Port, Id, 0, list_to_binary("abcdefgh")),
  ?assertEqual("ok", WriteStatus),
  ?assertEqual(?VOK, WriteAnswer),
  ?assertEqual(length("abcdefgh"), BytesWritten),
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

  wss:close(Socket),
  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_GROUP], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [FSLogicNode | _] = NodesUp,

  DB_Node = ?DB_NODE,
  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}, {heart_beat, 1}]]),

  discover_default_file_mode(FSLogicNode),

  lists:append([{port, Port}, {nodes, NodesUp}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().

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
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  wss:close(Socket),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.storage_helper_args, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

send_creation_ack(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #createfileack{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createfileack(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createfileack", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  wss:close(Socket),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

%% Simulates request from FUSE
get_file_location(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "filelocation",
  answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  wss:close(Socket),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.storage_helper_args, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

%% Simulates request from FUSE
delete_file(Host, Cert, Port, FileName, FuseID) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseID), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  wss:close(Socket),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

%% Each of following functions simulate one request from Cluster Proxy.
create_file_on_storage(Host, Cert, Port, FileID) ->
  OperationMessage = #createfile{file_id  = FileID,mode = get(new_file_storage_mode)},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_createfile(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "createfile", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
	wss:close(Socket),
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
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
	wss:close(Socket),
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
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
	wss:close(Socket),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

change_perm_on_storage(Host, Cert, Port, FileID, NewPerm) ->
  OperationMessage = #changepermsatstorage{file_id  = FileID, perms = NewPerm},
  OperationMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_changepermsatstorage(OperationMessage)),

  RemoteMangementMessage = #remotefilemangement{message_type = "changepermsatstorage", input = OperationMessageBytes},
  RemoteMangementMessageBytes = erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RemoteMangementMessage)),

  Message = #clustermsg{module_name = "remote_files_manager", message_type = "remotefilemangement",
  message_decoder_name = "remote_file_management", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),
  wss:close(Socket),

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
  answer_decoder_name = "remote_file_management", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
	wss:close(Socket),
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
  answer_decoder_name = "remote_file_management", protocol_version = 1, input = RemoteMangementMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
	wss:close(Socket),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  WriteInfo = remote_file_management_pb:decode_writeinfo(Bytes),
  WriteInfo2 = records_translator:translate(WriteInfo, "remote_file_management"),
  {Status, WriteInfo2#writeinfo.answer_status, WriteInfo2#writeinfo.bytes_written}.

discover_default_file_mode(Node) ->
    Ans = rpc:call(Node,application,get_env,[?APP_Name, new_file_storage_mode]),
    ?assertMatch({ok,_},Ans),
    {ok,DefaultMode} = Ans,
    put(new_file_storage_mode,DefaultMode).
