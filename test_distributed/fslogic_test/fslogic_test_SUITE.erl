%% ===================================================================
%% @author Michal Wrzeszcz
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

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/fslogic/fslogic_acl.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_share.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([spaces_permissions_test/1, files_manager_standard_files_test/1, files_manager_tmp_files_test/1, storage_management_test/1]).
-export([permissions_management_test/1, user_creation_test/1, get_file_links_test/1, fuse_requests_test/1, users_separation_test/1]).
-export([file_sharing_test/1, dir_mv_test/1, user_file_counting_test/1, user_file_size_test/1, dirs_creating_test/1, spaces_test/1]).
-export([get_by_uuid_test/1, concurrent_file_creation_test/1, create_standard_share/2, create_share/3, get_share/2, get_acl/2, make_dir/2, xattrs_test/1, acl_test/1]).
-export([get_file_local_location_test/1, block_creation_test/1, block_registration_test/1, available_blocks_cache_test/1]).
-export([get_file_uuid/2]).

all() ->
 [spaces_test, files_manager_tmp_files_test, files_manager_standard_files_test, storage_management_test, permissions_management_test, user_creation_test,
   fuse_requests_test, spaces_permissions_test, users_separation_test, file_sharing_test, dir_mv_test, user_file_counting_test, dirs_creating_test, get_by_uuid_test,
   concurrent_file_creation_test, get_file_links_test, user_file_size_test, xattrs_test, acl_test, get_file_local_location_test, block_creation_test,
   block_registration_test, available_blocks_cache_test
 ].

-define(SH, "DirectIO").
-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% Tests if not permitted operations can not be executed by fslogic
spaces_permissions_test(Config) ->
  NodesUp = ?config(nodes, Config),

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

  Team = ?TEST_GROUP,
  Team2 = ?TEST_GROUP2,
  Team3 = ?TEST_GROUP3,
  TestFile = "spaces/" ++ Team ++ "/spaces_permissions_test_file",
  TestFileNewName = TestFile ++ "2",
  TestFile2 = "spaces/" ++ Team2 ++ "/spaces_permissions_test_file",
  TestFile3 = "spaces/" ++ Team3 ++ "/spaces_permissions_test_file",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, Team]),
  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, Team, Team2, Team3]),

  %% UID2 = list_to_integer(UserDoc2#db_document.uuid),

  [DN1 | _] = user_logic:get_dn_list(UserDoc1),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),

  %% Connect to cluster
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),
  {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns2),

  {Status, _, _, _, AnswerOpt} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {Status2, _, _, _, AnswerOpt2} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),

  {Status3, _, _, _, AnswerOpt3} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),

  {Status4, Answer4} = rename_file(Socket2, TestFile, TestFileNewName),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VEACCES), Answer4),

  {Status5, Answer5} = rename_file(Socket, TestFile, TestFileNewName),
  ?assertEqual("ok", Status5),
  ?assertEqual(list_to_atom(?VOK), Answer5),

  {Status6, Answer6} = change_file_perms(Socket2, TestFileNewName, 8#400),
  ?assertEqual("ok", Status6),
  ?assertEqual(list_to_atom(?VEACCES), Answer6),

  {Status7, Answer7} = change_file_perms(Socket, TestFileNewName, 8#400),
  ?assertEqual("ok", Status7),
  ?assertEqual(list_to_atom(?VOK), Answer7),

  {Status8, Answer8} = chown(Socket2, TestFileNewName, 20000),
  ?assertEqual("ok", Status8),
  ?assertEqual(list_to_atom(?VEACCES), Answer8),

  {Status9, Answer9} = chown(Socket, TestFileNewName, 20000),
  ?assertEqual("ok", Status9),
  ?assertEqual(list_to_atom(?VEACCES), Answer9),

  {GetFileAns, FileDoc} = rpc:call(FSLogicNode, dao_vfs, get_file, [TestFileNewName]),
  ?assertEqual(ok, GetFileAns),
  {GetFileMetaAns, FileMetaDoc} = rpc:call(FSLogicNode, dao_vfs, get_file_meta, [FileDoc#db_document.record#file.meta_doc]),
  ?assertEqual(ok, GetFileMetaAns),
  FileMetaUID = FileMetaDoc#db_document.record#file_meta.uid,
  ?assertEqual(UserDoc1#db_document.uuid, FileMetaUID),

  %% @todo: remove this assertion since chown is no longer supported
  %% ?assertEqual(ok, rpc:call(FSLogicNode, logical_files_manager, chown, [TestFileNewName, UID2])),

  {GetFileAns1, FileDoc1} = rpc:call(FSLogicNode, dao_vfs, get_file, [TestFileNewName]),
  ?assertEqual(ok, GetFileAns1),
  {GetFileMetaAns1, FileMetaDoc1} = rpc:call(FSLogicNode, dao_vfs, get_file_meta, [FileDoc1#db_document.record#file.meta_doc]),
  ?assertEqual(ok, GetFileMetaAns1),
  FileMetaUID1 = FileMetaDoc1#db_document.record#file_meta.uid,
  ?assertEqual(UserDoc1#db_document.uuid, FileMetaUID1),

  ?assertNotEqual(FileMetaUID, FileMetaUID1),

  {Status10, Answer10} = delete_file(Socket, TestFileNewName),
  ?assertEqual("ok", Status10),
  ?assertEqual(list_to_atom(?VEACCES), Answer10),

  {Status11, Answer11} = delete_file(Socket2, TestFileNewName),
  ?assertEqual("ok", Status11),
  ?assertEqual(list_to_atom(?VOK), Answer11),

  {Status12, _, _, _, AnswerOpt12} = create_file(Socket, TestFile2),
  ?assertEqual("ok", Status12),
  ?assertEqual(?VEACCES, AnswerOpt12),

  {Status13, _, _, _, AnswerOpt13} = create_file(Socket, TestFile3),
  ?assertEqual("ok", Status13),
  ?assertEqual(?VEACCES, AnswerOpt13),

  TestFile4 = "spaces/" ++ Team ++ "/spaces_permissions_test_file4",

  TestDir = "spaces/" ++ Team ++ "/spaces_permissions_test_dir",
  TestDirFile = TestDir ++ "/file1",
  TestDirDir = TestDir ++ "/dir1",

  LinkName = TestDir ++ "/link1",

  {Status14, Answer14} = mkdir(Socket2, TestDir),
  ?assertEqual("ok", Status14),
  ?assertEqual(list_to_atom(?VOK), Answer14),

  {Status15, Answer15} = change_file_perms(Socket2, TestDir, 8#640),
  ?assertEqual("ok", Status15),
  ?assertEqual(list_to_atom(?VOK), Answer15),

  {Status16, _, _, _, AnswerOpt16} = create_file(Socket, TestDirFile),
  ?assertEqual("ok", Status16),
  ?assertEqual(?VEACCES, AnswerOpt16),

  {Status17, _, _, _, AnswerOpt17} = create_file(Socket2, TestDirFile),
  ?assertEqual("ok", Status17),
  ?assertEqual(?VOK, AnswerOpt17),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket2, TestDirFile),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {Status18, Answer18} = mkdir(Socket, TestDirDir),
  ?assertEqual("ok", Status18),
  ?assertEqual(list_to_atom(?VEACCES), Answer18),

  {Status19, Answer19} = mkdir(Socket2, TestDirDir),
  ?assertEqual("ok", Status19),
  ?assertEqual(list_to_atom(?VOK), Answer19),

  {Status20, Answer20} = create_link(Socket, LinkName, TestDirFile),
  ?assertEqual("ok", Status20),
  ?assertEqual(list_to_atom(?VEACCES), Answer20),

  {Status21, Answer21} = create_link(Socket2, LinkName, TestDirFile),
  ?assertEqual("ok", Status21),
  ?assertEqual(list_to_atom(?VOK), Answer21),

  {Status22, Answer22} = delete_file(Socket2, TestDirDir),
  ?assertEqual("ok", Status22),
  ?assertEqual(list_to_atom(?VOK), Answer22),

  {Status23, Answer23} = delete_file(Socket2, LinkName),
  ?assertEqual("ok", Status23),
  ?assertEqual(list_to_atom(?VOK), Answer23),

  {Status24, Answer24} = rename_file(Socket, TestDirFile, TestFile4),
  ?assertEqual("ok", Status24),
  ?assertEqual(list_to_atom(?VEACCES), Answer24),

  {Status25, Answer25} = rename_file(Socket2, TestDirFile, TestFile4),
  ?assertEqual("ok", Status25),
  ?assertEqual(list_to_atom(?VOK), Answer25),

  {Status26, Answer26} = rename_file(Socket, TestFile4, TestDirFile),
  ?assertEqual("ok", Status26),
  ?assertEqual(list_to_atom(?VEACCES), Answer26),

  {Status27, Answer27} = rename_file(Socket2, TestFile4, TestDirFile),
  ?assertEqual("ok", Status27),
  ?assertEqual(list_to_atom(?VOK), Answer27),

  {Status28, Answer28} = delete_file(Socket2, TestDirFile),
  ?assertEqual("ok", Status28),
  ?assertEqual(list_to_atom(?VOK), Answer28),

  {Status29, Answer29} = change_file_perms(Socket2, TestDir, 8#660),
  ?assertEqual("ok", Status29),
  ?assertEqual(list_to_atom(?VOK), Answer29),

  {Status30, _, _, _, AnswerOpt30} = create_file(Socket, TestDirFile),
  ?assertEqual("ok", Status30),
  ?assertEqual(?VOK, AnswerOpt30),

  {CreationAckStatus3, CreationAckAnswerOpt3} = send_creation_ack(Socket, TestDirFile),
  ?assertEqual("ok", CreationAckStatus3),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt3),

  {Status31, Answer31} = mkdir(Socket, TestDirDir),
  ?assertEqual("ok", Status31),
  ?assertEqual(list_to_atom(?VOK), Answer31),

  {Status32, Answer32} = create_link(Socket, LinkName, TestDirFile),
  ?assertEqual("ok", Status32),
  ?assertEqual(list_to_atom(?VOK), Answer32),

  {Status33, Answer33} = delete_file(Socket, TestDirDir),
  ?assertEqual("ok", Status33),
  ?assertEqual(list_to_atom(?VOK), Answer33),

  {Status34, Answer34} = delete_file(Socket, LinkName),
  ?assertEqual("ok", Status34),
  ?assertEqual(list_to_atom(?VOK), Answer34),

  {Status35, Answer35} = rename_file(Socket, TestDirFile, TestFile4),
  ?assertEqual("ok", Status35),
  ?assertEqual(list_to_atom(?VOK), Answer35),

  {Status36, Answer36} = rename_file(Socket, TestFile4, TestDirFile),
  ?assertEqual("ok", Status36),
  ?assertEqual(list_to_atom(?VOK), Answer36),

  {Status37, Answer37} = delete_file(Socket, TestDirFile),
  ?assertEqual("ok", Status37),
  ?assertEqual(list_to_atom(?VOK), Answer37),

  {Status38, Answer38} = delete_file(Socket2, TestDir),
  ?assertEqual("ok", Status38),
  ?assertEqual(list_to_atom(?VOK), Answer38),

  wss:close(Socket),
  wss:close(Socket2),

  rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team], ?ProtocolVersion]),
  rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team2], ?ProtocolVersion]),
  rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER], ?ProtocolVersion]),
  rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER2], ?ProtocolVersion]),
  rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion]),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN1}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% This test checks if creation of file works well when many concurrent creation requests are sent
concurrent_file_creation_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  TestFile = "concurrent_file_creation_test_file",

  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),

  MainProc = self(),
  TestFun = fun(File) ->
    spawn_link(Node1, fun() ->
      CreateAns = logical_files_manager:create(File),
      MainProc ! {create_ans, CreateAns}
    end)
  end,

  TestFun(TestFile),
  TestFun(TestFile),

  Ans1 = receive
           {create_ans, Msg} -> Msg
         after 2000 ->
           timeout
         end,
  Ans2 = receive
           {create_ans, Msg2} -> Msg2
         after 2000 ->
           timeout
         end,

  CreateAns = [Ans1, Ans2],
  ?assert(lists:member(ok, CreateAns)),
  ?assert(lists:member({error, file_exists}, CreateAns)),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),

  AnsDel = rpc:call(Node1, logical_files_manager, delete, [TestFile]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

%% Test file and data getting by uuid (fslogic normally uses path instead of uuid)
get_by_uuid_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  TestFile = "get_by_uuid_test_file",

  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),
  AnsCreate = rpc:call(Node1, logical_files_manager, create, [TestFile]),
  ?assertEqual(ok, AnsCreate),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),

  {FileLocationAns, FileLocation} = rpc:call(Node1, files_tester, get_file_location, [TestFile]),
  ?assertEqual(ok, FileLocationAns),

  {DocFindStatus, FileDoc} = rpc:call(Node1, fslogic_objects, get_file, [1, TestFile, ?CLUSTER_FUSE_ID]),
  ?assertEqual(ok, DocFindStatus),
  FileLocation2 = rpc:call(Node1, fslogic, handle, [1, {getfilelocation_uuid, FileDoc#db_document.uuid}]),
  ?assertEqual(?VOK, FileLocation2#filelocation.answer),
  Root = ?TEST_ROOT,
  ?assertEqual(FileLocation, Root ++ "/" ++ FileLocation2#filelocation.file_id),

  {FileLocationAns3, {SHI3, FileLocation3}} = rpc:call(Node1, logical_files_manager, getfilelocation, [TestFile]),
  ?assertEqual(ok, FileLocationAns3),
  ?assertEqual("DirectIO", SHI3#storage_helper_info.name),
  ?assertEqual(?ARG_TEST_ROOT, SHI3#storage_helper_info.init_args),
  ?assertEqual(FileLocation, Root ++ "/" ++ FileLocation3),

  {FileLocationAns4, {SHI4, FileLocation4}} = rpc:call(Node1, logical_files_manager, getfilelocation, [{uuid, FileDoc#db_document.uuid}]),
  ?assertEqual(ok, FileLocationAns4),
  ?assertEqual(SHI3, SHI4),
  ?assertEqual(FileLocation3, FileLocation4),

  {FMStatys, FM_Attrs} = rpc:call(Node1, logical_files_manager, getfileattr, [TestFile]),
  ?assertEqual(ok, FMStatys),

  {FMStatys2, FM_Attrs2} = rpc:call(Node1, logical_files_manager, getfileattr, [{uuid, FileDoc#db_document.uuid}]),
  ?assertEqual(ok, FMStatys2),
  ?assertEqual(FM_Attrs, FM_Attrs2),

  {FNameAns, FName} = rpc:call(Node1, logical_files_manager, get_file_name_by_uuid, [FileDoc#db_document.uuid]),
  ?assertEqual(ok, FNameAns),
  ?assertEqual(TestFile, FName),

  AnsWrite1 = rpc:call(Node1, logical_files_manager, write, [TestFile, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),
  ?assertEqual({ok, "abcdefgh"}, rpc:call(Node1, files_tester, read_file, [TestFile, 100])),

  {StatusRead1, AnsRead1} = rpc:call(Node1, logical_files_manager, read, [{uuid, FileDoc#db_document.uuid}, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, logical_files_manager, read, [{uuid, FileDoc#db_document.uuid}, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsDel = rpc:call(Node1, logical_files_manager, delete, [TestFile]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(FileLocation)),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

%% This test checks if spaces are working as intended.
%% I.e all users see files moved/created in theirs group directory and users see only their spaces
spaces_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  Cert1 = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),

  Host = "localhost",
  Port = ?config(port, Config),

  %% Cluster init
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  %% files_manager call with given user's DN
  FM = fun(M, A, DN) ->
    Me = self(),
    Pid = spawn_link(Node, fun() -> fslogic_context:set_user_dn(DN),
      Me ! {self(), apply(logical_files_manager, M, A)} end),
    receive
      {Pid, Resp} -> Resp
    end
  end,

  %% Gets uid by name
  UID = fun(Name) ->
    list_to_integer(os:cmd("id -u " ++ Name) -- "\n")
  end,

  %% Gets gid by name
  GID = fun(Name) ->
    list_to_integer(os:cmd("getent group " ++ Name ++ " | cut -d: -f3") -- "\n")
  end,

  %% Init storage
  {InsertStorageAns, _} = rpc:call(Node, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert1, [?TEST_USER, ?TEST_GROUP]),
  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, ?TEST_GROUP, ?TEST_GROUP2]),

  [DN1 | _] = user_logic:get_dn_list(UserDoc1),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),

  %% Init connections
  {ConAns1, Socket1} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}, auto_handshake]),
  ?assertEqual(ok, ConAns1),
  {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns2),
  %% END init connections

  %% Test not allowed operations
  {"ok", A2} = mkdir(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, "test"])),
  ?assertEqual(eacces, A2),

  {"ok", A3} = delete_file(Socket1, ?SPACES_BASE_DIR_NAME),
  ?assertEqual(eacces, A3),

  {"ok", A10} = delete_file(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP])),
  ?assertEqual(eacces, A10),

  {"ok", A11} = delete_file(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP2])),
  ?assertEqual(eacces, A11),

  {"ok", A4} = rename_file(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP2]), "/test"),
  ?assertEqual(eacces, A4),

  {"ok", A5} = rename_file(Socket1, ?SPACES_BASE_DIR_NAME, "/test"),
  ?assertEqual(eacces, A5),

  {"ok", ok} = mkdir(Socket1, "/test"), %% Test dir
  {"ok", _, _, _, "ok"} = create_file(Socket1, "/file"), %% Test file
  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket1, "/file"),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {"ok", A6} = rename_file(Socket1, "/test", fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, "test"])),
  ?assertEqual(eacces, A6),

  {"ok", A14} = rename_file(Socket1, "/file", fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, "test"])),
  ?assertEqual(eacces, A14),

  {"ok", A7} = change_file_perms(Socket1, ?SPACES_BASE_DIR_NAME, 8#555),
  ?assertEqual(eacces, A7),

  {"ok", A8} = change_file_perms(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP]), 8#555),
  ?assertEqual(eacces, A8),

  {"ok", A9} = change_file_perms(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP2]), 8#555),
  ?assertEqual(eacces, A9),

  {"ok", A12} = chown(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP2]), 500),
  ?assertEqual(eacces, A12),

  {"ok", A13} = chown(Socket1, ?SPACES_BASE_DIR_NAME, 500),
  ?assertEqual(eacces, A13),

  {"ok", _, _, _, A15} = create_file(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, "file"])),
  ?assertEqual(eacces, list_to_atom(A15)),

  {"ok", A16} = create_link(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, "file"]), "link"),
  ?assertEqual(eacces, A16),


  %% Test spaces visibility
  {"ok", C17, A17} = ls(Socket1, ?SPACES_BASE_DIR_NAME, 10, 0),
  ?assertEqual(ok, list_to_atom(A17)),
  ?assert(lists:member(?TEST_USER, C17)),
  ?assert(lists:member(?TEST_GROUP, C17)),

  {"ok", C18, A18} = ls(Socket2, ?SPACES_BASE_DIR_NAME, 10, 0),
  ?assertEqual(ok, list_to_atom(A18)),
  ?assert(lists:member(?TEST_USER2, C18)),
  ?assert(lists:member(?TEST_GROUP, C18)),
  ?assert(lists:member(?TEST_GROUP2, C18)),

  %% Try to use group dir that is not visible for the user
  {"ok", A19} = mkdir(Socket1, fslogic_path:absolute_join([?SPACES_BASE_DIR_NAME, ?TEST_GROUP2, "testdir"])),
  ?assertNotEqual(ok, A19),


  %% Files visibility test
  {"ok", A20} = mkdir(Socket1, "/spaces/" ++ ?TEST_GROUP ++ "/dir"),
  ?assertEqual(ok, A20),

  {"ok", _, _, _, A21} = create_file(Socket2, "/spaces/" ++ ?TEST_GROUP ++ "/file"),
  ?assertEqual(ok, list_to_atom(A21)),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket2, "/spaces/" ++ ?TEST_GROUP ++ "/file"),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {"ok", A22} = mkdir(Socket1, "/spaces/" ++ ?TEST_GROUP ++ "/dir"), %% Dir should already exist
  ?assertEqual(eexist, A22),

  {"ok", A23} = mkdir(Socket2, "/spaces/" ++ ?TEST_GROUP ++ "/file"), %% File should already exist
  ?assertEqual(eexist, A23),

  A24 = FM(create, ["/spaces/" ++ ?TEST_GROUP ++ "/file2"], DN1),
  ?assertEqual(ok, A24),

  ?assert(rpc:call(Node, files_tester, file_exists, ["/spaces/" ++ ?TEST_GROUP ++ "/file2"])),
  {ok, L1} = rpc:call(Node, files_tester, get_file_location, ["/spaces/" ++ ?TEST_GROUP ++ "/file2"]),
  [_, _, BaseDir1, SecDir1 | _] = string:tokens(L1, "/"),
  ?assertEqual("spaces", BaseDir1),
  ?assertEqual(?TEST_GROUP, SecDir1),

  %% Check if owners are set correctly
  {"ok", #fileattr{gname = GName0}} = get_file_attr(Socket1, "/spaces/" ++ ?TEST_GROUP ++ "/dir"),
  ?assertEqual(?TEST_GROUP, GName0),

  {"ok", #fileattr{gname = GName0}} = get_file_attr(Socket1, "/spaces/" ++ ?TEST_GROUP ++ "/file"),
  ?assertEqual(?TEST_GROUP, GName0),

  %% Onwer on storage
  {ok, User1, Grp1} = rpc:call(Node, files_tester, get_owner, [L1]),
  ?assertEqual(UID(?TEST_USER), User1),
  ?assertEqual(GID(?TEST_GROUP), Grp1),


  %% Check if file move changes group owner & storage file location
  A25 = FM(create, ["/f1"], DN1),
  ?assertEqual(ok, A25),

  A26 = FM(create, ["/spaces/" ++ ?TEST_GROUP2 ++ "/f2"], DN2),
  ?assertEqual(ok, A26),

  {ok, L2} = rpc:call(Node, files_tester, get_file_location, ["/spaces/" ++ ?TEST_GROUP2 ++ "/f2"]),
  {ok, User2, Grp2} = rpc:call(Node, files_tester, get_owner, [L2]),
  ?assertEqual(UID(?TEST_USER2), User2),
  ?assertEqual(GID(?TEST_GROUP2), Grp2),
  [_, _, BaseDir2, SecDir2 | _] = string:tokens(L2, "/"),
  ?assertEqual("spaces", BaseDir2),
  ?assertEqual(?TEST_GROUP2, SecDir2),

  {ok, L3} = rpc:call(Node, files_tester, get_file_location, ["/spaces/" ++ ?TEST_USER ++ "/f1"]),
  [_, _, BaseDir3, SecDir3 | _] = string:tokens(L3, "/"),
  ?assertEqual("spaces", BaseDir3),
  ?assertEqual(?TEST_USER, SecDir3),

  %% Now move those files
  A27 = FM(mv, ["/f1", "/spaces/" ++ ?TEST_GROUP ++ "/f1"], DN1),
  ?assertEqual(ok, A27),

  A28 = FM(mv, ["/spaces/" ++ ?TEST_GROUP2 ++ "/f2", "/spaces/" ++ ?TEST_GROUP ++ "/f2"], DN2),
  ?assertEqual(ok, A28),

  %% Check its location
  {ok, L4} = rpc:call(Node, files_tester, get_file_location, ["/spaces/" ++ ?TEST_GROUP ++ "/f1"]),
  {ok, L5} = rpc:call(Node, files_tester, get_file_location, ["/spaces/" ++ ?TEST_GROUP ++ "/f2"]),
  [_, _, BaseDir4, SecDir4 | _] = string:tokens(L4, "/"),
  [_, _, BaseDir5, SecDir5 | _] = string:tokens(L5, "/"),
  ?assertEqual("spaces", BaseDir4),
  ?assertEqual(?TEST_GROUP, SecDir4),
  ?assertEqual("spaces", BaseDir5),
  ?assertEqual(?TEST_GROUP, SecDir5),

  ?assert(rpc:call(Node, files_tester, file_exists, ["/spaces/" ++ ?TEST_GROUP ++ "/f1"])),
  ?assert(rpc:call(Node, files_tester, file_exists, ["/spaces/" ++ ?TEST_GROUP ++ "/f2"])),

  %% ... and owners
  {ok, User3, Grp3} = rpc:call(Node, files_tester, get_owner, [L4]),
  {ok, User4, Grp4} = rpc:call(Node, files_tester, get_owner, [L5]),
  ?assertEqual(UID(?TEST_USER), User3),
  ?assertEqual(GID(?TEST_GROUP), Grp3),

  ?assertEqual(UID(?TEST_USER2), User4),
  ?assertEqual(GID(?TEST_GROUP), Grp4),


  %% Cleanup
  wss:close(Socket1),
  wss:close(Socket2).

%% Checks creating of directories at storage for users' files.
%% The test creates path for a new file that contains 2 directories
%% (fslogic uses it when the user has a lot of files).
dirs_creating_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},

  AnsCreate = rpc:call(Node1, fslogic_storage, create_dirs, [50, 5, SHInfo, "/"]),
  ?assertEqual(2, length(string:tokens(AnsCreate, "/"))),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ AnsCreate)).

%% Checks user counting view.
%% The test creates some files for two users, and then checks if the view counts them properly.
user_file_counting_test(Config) ->
  NodesUp = ?config(nodes, Config),

  FileBeg = "user_dirs_at_storage_test_file",
  User1FilesEnding = ["1", "2", "3", "4"],
  User2FilesEnding = ["x", "y", "z"],

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),   %% Cert of second test user (the test uses 2 users to check if files of one user are not counted as files of other user)
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, ?TEST_GROUP2]),

  [DN1 | _] = user_logic:get_dn_list(UserDoc1),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),

  UserID1 = UserDoc1#db_document.uuid,
  UserID2 = UserDoc2#db_document.uuid,

  rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, "not_existing_id", 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus0, Count0} = rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, "not_existing_id", 1]),
  ?assertEqual(ok, CountStatus0),
  ?assertEqual(0, Count0),

  rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID1, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus00, Count00} = rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID1, 1]),
  ?assertEqual(ok, CountStatus00),
  ?assertEqual(0, Count00),

  %% Connect to cluster
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  %% Connect to cluster, user2
  {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns2),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, _, _, _, AnswerOpt} = create_file(Socket, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(?VOK, AnswerOpt),

    {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket, FileName),
    ?assertEqual("ok", CreationAckStatus),
    ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt)
  end, User1FilesEnding),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, _, _, _, AnswerOpt} = create_file(Socket2, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(?VOK, AnswerOpt),

    {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket2, FileName),
    ?assertEqual("ok", CreationAckStatus2),
    ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2)
  end, User2FilesEnding),

  rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID1, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus, Count} = rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID1, 1]),
  ?assertEqual(ok, CountStatus),
  ?assertEqual(length(User1FilesEnding), Count),

  rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID2, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus2, Count2} = rpc:call(FSLogicNode, fslogic_utils, get_files_number, [user, UserID2, 1]),
  ?assertEqual(ok, CountStatus2),
  ?assertEqual(length(User2FilesEnding), Count2),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, Answer} = delete_file(Socket, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(list_to_atom(?VOK), Answer)
  end, User1FilesEnding),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    {Status, Answer} = delete_file(Socket2, FileName),
    ?assertEqual("ok", Status),
    ?assertEqual(list_to_atom(?VOK), Answer)
  end, User2FilesEnding),

  wss:close(Socket),
  wss:close(Socket2),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN1}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% Checks user files size view.
%% The test creates some files for two users, and then checks if the view counts their size properly.
user_file_size_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  Cert1 = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),

  Host = "localhost",
  Port = ?config(port, Config),

  %% Cluster init
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  %% files_manager call with given user's DN
  FM = fun(M, A, DN) ->
    Me = self(),
    Pid = spawn_link(Node, fun() -> fslogic_context:set_user_dn(DN),
      Me ! {self(), apply(logical_files_manager, M, A)} end),
    receive
      {Pid, Resp} -> Resp
    end
  end,

  %% Init storage
  {InsertStorageAns, _} = rpc:call(Node, fslogic_storage, insert_storage, [?SH, ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert1, [?TEST_USER, ?TEST_GROUP]),
  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, ?TEST_GROUP]),

  [DN1 | _] = user_logic:get_dn_list(UserDoc1),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),

  UserID1 = UserDoc1#db_document.uuid,
  UserID2 = UserDoc2#db_document.uuid,

  %% Init connections
  {ConAns1, Socket1} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}, auto_handshake]),
  ?assertEqual(ok, ConAns1),
  {ConAns2, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns2),
  %% END init connections

  FileBeg = "user_dirs_at_storage_test_file_size",
  User1FilesEnding = ["1", "2", "3", "4"],
  User2FilesEnding = ["x", "y", "z"],
  User1TestUpdateFile = FileBeg ++ "_update",
  FileSize = 100,

  rpc:call(Node, fslogic_utils, get_files_size, ["not_existing_id", 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus0, Count0} = rpc:call(Node, user_logic, get_files_size, ["not_existing_id", 1]),
  ?assertEqual(ok, CountStatus0),
  ?assertEqual(0, Count0),

  rpc:call(Node, fslogic_utils, get_files_size, [UserID1, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus00, Count00} = rpc:call(Node, user_logic, get_files_size, [UserID1, 1]),
  ?assertEqual(ok, CountStatus00),
  ?assertEqual(0, Count00),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    AnsCreate = FM(create, [FileName], DN1),
    ?assertEqual(ok, AnsCreate),
    AnsTruncate = FM(truncate, [FileName, FileSize], DN1),
    ?assertEqual(ok, AnsTruncate),
    {AnsGetFileAttr, _} = FM(getfileattr, [FileName], DN1),
    ?assertEqual(ok, AnsGetFileAttr)
  end, User1FilesEnding),

  lists:foreach(fun(FileEnding) ->
    FileName = FileBeg ++ FileEnding,
    AnsCreate = FM(create, [FileName], DN2),
    ?assertEqual(ok, AnsCreate),
    AnsTruncate = FM(truncate, [FileName, FileSize], DN2),
    ?assertEqual(ok, AnsTruncate),
    {AnsGetFileAttr, _} = FM(getfileattr, [FileName], DN2),
    ?assertEqual(ok, AnsGetFileAttr)
  end, User2FilesEnding),

  rpc:call(Node, user_logic, get_files_size, [UserID1, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus, Count} = rpc:call(Node, user_logic, get_files_size, [UserID1, 1]),
  ?assertEqual(ok, CountStatus),
  ?assertEqual(length(User1FilesEnding) * FileSize, Count),

  rpc:call(Node, user_logic, get_files_size, [UserID2, 1]),
  test_utils:wait_for_db_reaction(),
  {CountStatus2, Count2} = rpc:call(Node, user_logic, get_files_size, [UserID2, 1]),
  ?assertEqual(ok, CountStatus2),
  ?assertEqual(length(User2FilesEnding) * FileSize, Count2),

  % Test periodical update of user files size
  AnsCreate = FM(create, [User1TestUpdateFile], DN1),
  ?assertEqual(ok, AnsCreate),
  AnsTruncate = FM(truncate, [User1TestUpdateFile, FileSize], DN1),
  ?assertEqual(ok, AnsTruncate),
  {AnsGetFileAttr, _} = FM(getfileattr, [User1TestUpdateFile], DN1),
  ?assertEqual(ok, AnsGetFileAttr),
  test_utils:wait_for_db_reaction(),
  {ok, Interval} = rpc:call(Node, application, get_env, [?APP_Name, user_files_size_view_update_period]),
  timer:sleep(Interval * 1000),

  {CountStatus3, Count3} = rpc:call(Node, user_logic, get_files_size, [UserID1, 1]),
  ?assertEqual(ok, CountStatus3),
  ?assertEqual(length(User1FilesEnding) * FileSize + FileSize, Count3),

  wss:close(Socket1),
  wss:close(Socket2).

%% Checks permissions management functions
%% The tests checks some files and then changes their permissions. Erlang functions are used to test if permissions were change properly.
permissions_management_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},
  File = "permissions_management_test_file",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {PermStatus, Perms} = files_tester:get_permissions(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, PermStatus),
  ?assertEqual(8#600, Perms rem 8#01000),

  {OwnStatus, User, Group} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus),
  ?assert(is_integer(User)),
  ?assert(is_integer(Group)),

  NewPerms = 8#521,
  AnsChmod = rpc:call(Node1, storage_files_manager, chmod, [SHInfo, File, NewPerms]),
  ?assertEqual(ok, AnsChmod),

  {PermStatus2, Perms2} = files_tester:get_permissions(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, PermStatus2),
  ?assertEqual(NewPerms, Perms2 rem 8#01000),

  TestUser = ?TEST_USER,
  TestGroup = ?TEST_GROUP,
  AnsChown = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, TestUser, ""]),
  ?assertEqual(ok, AnsChown),

  {OwnStatus2, User2, Group2} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus2),
  ?assert(is_integer(User2)),
  ?assertEqual(false, User =:= User2),
  ?assertEqual(Group, Group2),

  AnsChown2 = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, "", TestGroup]),
  ?assertEqual(ok, AnsChown2),

  {OwnStatus3, User3, Group3} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus3),
  ?assertEqual(User2, User3),
  ?assertEqual(false, Group =:= Group3),

  AnsChown3 = rpc:call(Node1, storage_files_manager, chown, [SHInfo, File, "root", "root"]),
  ?assertEqual(ok, AnsChown3),

  {OwnStatus4, User4, Group4} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus4),
  ?assertEqual(User, User4),
  ?assertEqual(Group, Group4),

  % Test to assert permission setting and checking in user context.
  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  Cert1 = ?COMMON_FILE("peer.pem"),
  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert1, [?TEST_GROUP]),
  [DN1 | _] = user_logic:get_dn_list(UserDoc1),

  FilePath = "/file.txt",

  LFM = fun(Fun, Args) ->
    Me = self(),
    Pid = spawn_link(Node1, fun() -> fslogic_context:set_user_dn(DN1),
      Me ! {self(), apply(logical_files_manager, Fun, Args)} end),
    receive
      {Pid, Resp} -> Resp
    end
  end,

  ?assertEqual(ok, LFM(create, [FilePath])),
  ?assertEqual(ok, LFM(change_file_perm, [FilePath, 8#000, true])),

  ?assertEqual(true, LFM(check_file_perm, [FilePath, ''])),
  ?assertEqual(false, LFM(check_file_perm, [FilePath, root])),
  ?assertEqual(true, LFM(check_file_perm, [FilePath, owner])),
  ?assertEqual(true, LFM(check_file_perm, [FilePath, delete])),

  ExpectedPerms = [
    {8#000, [
      {read, false},
      {write, false},
      {execute, false},
      {rdwr, false}
    ]},
    {8#100, [
      {read, false},
      {write, false},
      {execute, true},
      {rdwr, false}
    ]},
    {8#200, [
      {read, false},
      {write, true},
      {execute, false},
      {rdwr, false}
    ]},
    {8#300, [
      {read, false},
      {write, true},
      {execute, true},
      {rdwr, false}
    ]},
    {8#400, [
      {read, true},
      {write, false},
      {execute, false},
      {rdwr, false}
    ]},
    {8#500, [
      {read, true},
      {write, false},
      {execute, true},
      {rdwr, false}
    ]},
    {8#600, [
      {read, true},
      {write, true},
      {execute, false},
      {rdwr, true}
    ]},
    {8#700, [
      {read, true},
      {write, true},
      {execute, true},
      {rdwr, true}
    ]}
  ],

  lists:foreach(
    fun({PermsInt, Assertions}) ->
      ?assertEqual(ok, LFM(change_file_perm, [FilePath, PermsInt, true])),
      lists:foreach(
        fun({Type, Assertion}) ->
          ?assertEqual(Assertion, LFM(check_file_perm, [FilePath, Type]))
        end, Assertions)
    end, ExpectedPerms),

  RemoveUserAns = rpc:call(Node1, user_logic, remove_user, [{dn, DN1}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

%% Checks user creation (root and dirs at storage creation).
%% The test checks if directories for user and group files are created when the user is added to the system,
%% and when new storage is created (after adding users)
user_creation_test(Config) ->
  NodesUp = ?config(nodes, Config),

  Cert1 = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  [FSLogicNode | _] = NodesUp,
  SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},

  Team1 = ?TEST_GROUP,
  Team2 = ?TEST_GROUP2,

  Login1 = ?TEST_USER,
  Teams1 = [Team1, Team2],

  Login2 = ?TEST_USER2,
  Teams2 = Teams1,

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces")),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces")),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team1)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team2)),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team1)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team2)),

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT3 ++ "/spaces/" ++ Team1)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT3 ++ "/spaces/" ++ Team2)),

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  {InsertStorageAns2, StorageUUID2} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT2]),
  ?assertEqual(ok, InsertStorageAns2),

  UserDoc1 = test_utils:add_user(Config, Login1, Cert1, Teams1),
  [DN1 | _] = user_logic:get_dn_list(UserDoc1),

  {PermStatusGroupsDir, PermsUserGroupsDir} = files_tester:get_permissions(?TEST_ROOT ++ "/spaces"),
  ?assertEqual(ok, PermStatusGroupsDir),
  ?assertEqual(8#711, PermsUserGroupsDir rem 8#01000),

  {PermStatusGroupsDir2, PermsUserGroupsDir2} = files_tester:get_permissions(?TEST_ROOT2 ++ "/spaces"),
  ?assertEqual(ok, PermStatusGroupsDir2),
  ?assertEqual(8#711, PermsUserGroupsDir2 rem 8#01000),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team2)),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team2)),

  {PermStatus2, Perms2} = files_tester:get_permissions(?TEST_ROOT ++ "/spaces/" ++ Team1),
  ?assertEqual(ok, PermStatus2),
  ?assertEqual(8#731, Perms2 rem 8#01000),
  {PermStatus3, Perms3} = files_tester:get_permissions(?TEST_ROOT ++ "/spaces/" ++ Team2),
  ?assertEqual(ok, PermStatus3),
  ?assertEqual(8#731, Perms3 rem 8#01000),

  {PermStatus5, Perms5} = files_tester:get_permissions(?TEST_ROOT2 ++ "/spaces/" ++ Team1),
  ?assertEqual(ok, PermStatus5),
  ?assertEqual(8#731, Perms5 rem 8#01000),
  {PermStatus6, Perms6} = files_tester:get_permissions(?TEST_ROOT2 ++ "/spaces/" ++ Team2),
  ?assertEqual(ok, PermStatus6),
  ?assertEqual(8#731, Perms6 rem 8#01000),

  File = "user_creation_test_file",
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(FSLogicNode, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {OwnStatus0, User0, Group0} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus0),
  ?assert(is_integer(User0)),
  ?assert(is_integer(Group0)),

  AnsChown = rpc:call(FSLogicNode, storage_files_manager, chown, [SHInfo, File, Login1, Login1]),
  ?assertEqual(ok, AnsChown),

  {OwnStatus, User, Group} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus),
  ?assert(is_integer(User)),
  ?assert(is_integer(Group)),

  AnsChown2 = rpc:call(FSLogicNode, storage_files_manager, chown, [SHInfo, File, "", Team1]),
  ?assertEqual(ok, AnsChown2),

  {OwnStatus2, User2, Group2} = files_tester:get_owner(?TEST_ROOT ++ "/" ++ File),
  ?assertEqual(ok, OwnStatus2),
  ?assertEqual(User, User2),
  ?assertEqual(false, Group =:= Group2),

  %% Groups are not changed currently in this context
  %% ?assertEqual(Group, Group3),

  {OwnStatus4, User4, Group4} = files_tester:get_owner(?TEST_ROOT ++ "/spaces/" ++ Team1),
  ?assertEqual(ok, OwnStatus4),
  ?assertEqual(User0, User4),
  ?assertEqual(Group2, Group4),

  %% Groups are not changed currently in this context
  %% ?assertEqual(Group, Group5),

  {OwnStatus6, User6, Group6} = files_tester:get_owner(?TEST_ROOT2 ++ "/spaces/" ++ Team1),
  ?assertEqual(ok, OwnStatus6),
  ?assertEqual(User0, User6),
  ?assertEqual(Group2, Group6),

  UserDoc2 = test_utils:add_user(Config, Login2, Cert2, Teams2),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/spaces/" ++ Team2)),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT2 ++ "/spaces/" ++ Team2)),

  % check dirs creation in new storage for existing users
  {InsertStorageAns3, StorageUUID3} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT3]),
  ?assertEqual(ok, InsertStorageAns3),

  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT3 ++ "/spaces/" ++ Team1)),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT3 ++ "/spaces/" ++ Team2)),

  {PermStatus11, Perms11} = files_tester:get_permissions(?TEST_ROOT3 ++ "/spaces/" ++ Team1),
  ?assertEqual(ok, PermStatus11),
  ?assertEqual(8#731, Perms11 rem 8#01000),
  {PermStatus12, Perms12} = files_tester:get_permissions(?TEST_ROOT3 ++ "/spaces/" ++ Team2),
  ?assertEqual(ok, PermStatus12),
  ?assertEqual(8#731, Perms12 rem 8#01000),

  %cleanup
  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveStorageAns2 = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID2}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns2),

  RemoveStorageAns3 = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID3}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns3),

  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team1], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team2], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN1}]),
  ?assertEqual(ok, RemoveUserAns),

  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% Checks storage management functions
%% The tests checks if functions used to manage user's files at storage (e.g. mv, mkdir) works well.
storage_management_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},
  File = "storage_management_test_file",
  Dir = "storage_management_test_dir",
  NewDirName = "storage_management_test_dir_new_name",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {CreateStatus2, AnsCreate2} = rpc:call(Node1, storage_files_manager, mkdir, [SHInfo, File]),
  ?assertEqual(error, CreateStatus2),
  ?assertEqual(dir_or_file_exists, AnsCreate2),

  AnsCreate3 = rpc:call(Node1, storage_files_manager, mkdir, [SHInfo, Dir]),
  ?assertEqual(ok, AnsCreate3),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ Dir)),

  AnsMV = rpc:call(Node1, storage_files_manager, mv, [SHInfo, Dir, NewDirName]),
  ?assertEqual(ok, AnsMV),
  ?assertEqual(dir, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NewDirName)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ Dir)),

  {MVStatus2, AnsMV2} = rpc:call(Node1, storage_files_manager, mv, [SHInfo, Dir, NewDirName]),
  ?assertEqual(wrong_rename_return_code, MVStatus2),
  ?assert(is_integer(AnsMV2)),

  AnsDel = rpc:call(Node1, storage_files_manager, delete_dir, [SHInfo, NewDirName]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NewDirName)),

  {DelStatus2, AnsDel2} = rpc:call(Node1, storage_files_manager, delete_dir, [SHInfo, NewDirName]),
  ?assertEqual(wrong_getatt_return_code, DelStatus2),
  ?assert(is_integer(AnsDel2)),

  AnsDel3 = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(ok, AnsDel3),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)).

%% Checks directory moving.
%% The test checks if fslogic blocks dir moving to its child.
dir_mv_test(Config) ->
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  DirName = "dir_mv_test_dir",
  DirName2 = "dir_mv_test_dir2",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
  [DN | _] = user_logic:get_dn_list(UserDoc),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertMatch({ok, _}, {ConAns, Socket}),

  {StatusMkdir, AnswerMkdir} = mkdir(Socket, DirName),
  ?assertEqual("ok", StatusMkdir),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir),

  {StatusMkdir2, AnswerMkdir2} = mkdir(Socket, DirName2),
  ?assertEqual("ok", StatusMkdir2),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir2),

  {Status_MV, AnswerMV} = rename_file(Socket, DirName2, DirName ++ "/" ++ DirName2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(list_to_atom(?VOK), AnswerMV),

  {Status_MV2, AnswerMV2} = rename_file(Socket, DirName, DirName ++ "/" ++ DirName2 ++ "/new_dir_name"),
  ?assertEqual("ok", Status_MV2),
  ?assertEqual(list_to_atom(?VEREMOTEIO), AnswerMV2),

  {Status_MV3, AnswerMV3} = rename_file(Socket, DirName, DirName ++ "/new_dir_name"),
  ?assertEqual("ok", Status_MV3),
  ?assertEqual(list_to_atom(?VEREMOTEIO), AnswerMV3),

  {StatusDelete, AnswerDelete} = delete_file(Socket, DirName ++ "/" ++ DirName2),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  {StatusDelete2, AnswerDelete2} = delete_file(Socket, DirName),
  ?assertEqual("ok", StatusDelete2),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete2),

  wss:close(Socket),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks file sharing functions
file_sharing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  TestFile = "file_sharing_test_file",
  TestFile2 = "file_sharing_test_file2",
  DirName = "file_sharing_test_dir",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
  [DN | _] = user_logic:get_dn_list(UserDoc),
  Login = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc]),

  fslogic_context:set_user_dn(DN),

  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {StatusCreate1, AnsCreate1} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(error, StatusCreate1),
  ?assertEqual(file_not_found, AnsCreate1),

  {StatusCreateFile, _Helper, _Id, _Validity, AnswerCreateFile} = create_file(Socket, TestFile),
  ?assertEqual("ok", StatusCreateFile),
  ?assertEqual(?VOK, AnswerCreateFile),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {StatusCreate2, AnsCreate2} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(ok, StatusCreate2),

  {StatusCreate3, AnsCreate3} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile, DN]),
  ?assertEqual(exists, StatusCreate3),
  ?assertEqual(AnsCreate2, AnsCreate3#db_document.uuid),

  {StatusGet, AnsGet} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate2}, DN]),
  ?assertEqual(ok, StatusGet),
  ?assertEqual(AnsCreate2, AnsGet#db_document.uuid),

  {StatusGet2, AnsGet2} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile}, DN]),
  ?assertEqual(ok, StatusGet2),
  ?assertEqual(AnsCreate2, AnsGet2#db_document.uuid),

  {StatusGet3, AnsGet3} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{user, UserDoc#db_document.uuid}, DN]),
  ?assertEqual(ok, StatusGet3),
  ?assertEqual(AnsCreate2, AnsGet3#db_document.uuid),
  ShareDoc = AnsGet3#db_document.record,

  {StatusCreateFile2, _Helper2, _Id2, _Validity2, AnswerCreateFile2} = create_file(Socket, TestFile2),
  ?assertEqual("ok", StatusCreateFile2),
  ?assertEqual(?VOK, AnswerCreateFile2),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket, TestFile2),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {StatusCreate4, AnsCreate4} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_share, [TestFile, some_share, DN]),
  ?assertEqual(ok, StatusCreate4),

  {StatusCreate5, AnsCreate5} = rpc:call(FSLogicNode, fslogic_test_SUITE, create_standard_share, [TestFile2, DN]),
  ?assertEqual(ok, StatusCreate5),

  {StatusGet4, AnsGet4} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate4}, DN]),
  ?assertEqual(ok, StatusGet4),
  ?assertEqual(AnsCreate4, AnsGet4#db_document.uuid),

  {StatusGet5, AnsGet5} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{uuid, AnsCreate5}, DN]),
  ?assertEqual(ok, StatusGet5),
  ?assertEqual(AnsCreate5, AnsGet5#db_document.uuid),
  ShareDoc2 = AnsGet5#db_document.record,

  {StatusGet6, AnsGet6} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile}, DN]),
  ?assertEqual(ok, StatusGet6),
  ?assertEqual(2, length(AnsGet6)),
  ?assert(lists:member(AnsGet, AnsGet6)),
  ?assert(lists:member(AnsGet4, AnsGet6)),

  {StatusGet7, AnsGet7} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{user, UserDoc#db_document.uuid}, DN]),
  ?assertEqual(ok, StatusGet7),
  ?assertEqual(3, length(AnsGet7)),
  ?assert(lists:member(AnsGet, AnsGet7)),
  ?assert(lists:member(AnsGet4, AnsGet7)),
  ?assert(lists:member(AnsGet5, AnsGet7)),

  {StatusGet8, AnsGet8} = rpc:call(FSLogicNode, logical_files_manager, get_file_by_uuid, [ShareDoc#share_desc.file]),
  ?assertEqual(ok, StatusGet8),
  ?assertEqual(ShareDoc#share_desc.file, AnsGet8#db_document.uuid),
  FileRecord = AnsGet8#db_document.record,
  ?assertEqual(TestFile, FileRecord#file.name),

  {StatusGet9, AnsGet9} = rpc:call(FSLogicNode, logical_files_manager, get_file_full_name_by_uuid, [ShareDoc#share_desc.file]),
  ?assertEqual(ok, StatusGet9),
  ?assertEqual("spaces/" ++ Login ++ "/" ++ TestFile, AnsGet9),


  {StatusMkdir, AnswerMkdir} = mkdir(Socket, DirName),
  ?assertEqual("ok", StatusMkdir),
  ?assertEqual(list_to_atom(?VOK), AnswerMkdir),

  {Status_MV, AnswerMV} = rename_file(Socket, TestFile2, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(list_to_atom(?VOK), AnswerMV),

  {StatusGet10, AnsGet10} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, DirName ++ "/" ++ TestFile2}, DN]),
  ?assertEqual(ok, StatusGet10),
  ?assertEqual(AnsCreate5, AnsGet10#db_document.uuid),

  {StatusGet11, AnsGet11} = rpc:call(FSLogicNode, fslogic_test_SUITE, get_share, [{file, TestFile2}, DN]),
  ?assertEqual(error, StatusGet11),
  ?assertEqual(file_not_found, AnsGet11),

  {StatusGet12, AnsGet12} = rpc:call(FSLogicNode, logical_files_manager, get_file_full_name_by_uuid, [ShareDoc2#share_desc.file]),
  ?assertEqual(ok, StatusGet12),
  ?assertEqual("spaces/" ++ Login ++ "/" ++ DirName ++ "/" ++ TestFile2, AnsGet12),


  AnsRemove = rpc:call(FSLogicNode, logical_files_manager, remove_share, [{uuid, AnsCreate2}]),
  ?assertEqual(ok, AnsRemove),


  {StatusDelete, AnswerDelete} = delete_file(Socket, TestFile),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  {StatusDelete2, AnswerDelete2} = delete_file(Socket, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", StatusDelete2),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete2),

  {StatusDelete, AnswerDelete} = delete_file(Socket, DirName),
  ?assertEqual("ok", StatusDelete),
  ?assertEqual(list_to_atom(?VOK), AnswerDelete),

  wss:close(Socket),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks fslogic integration with dao and db
fuse_requests_test(Config) ->
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  TestFile = "fslogic_test_file",
  TestFile2 = "fslogic_test_file2",
  DirName = "fslogic_test_dir",
  FilesInDirNames = ["file_in_dir1", "file_in_dir2", "file_in_dir3", "file_in_dir4", "file_in_dir5"],
  FilesInDir = lists:map(fun(N) ->
    DirName ++ "/" ++ N
  end, FilesInDirNames),
  NewNameOfFIle = "new_name_of_file",

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
  [DN | _] = user_logic:get_dn_list(UserDoc),

  %% Connect to cluster
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {GetStatus, _, _, _, GetAnswerOpt} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", GetStatus),
  ?assertEqual(?VENOENT, GetAnswerOpt),

  {CreationAckStatus0, CreationAckAnswerOpt0} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus0),
  ?assertEqual(list_to_atom(?VENOENT), CreationAckAnswerOpt0),

  {Status, Helper, Id, _Validity, AnswerOpt0} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt0),

  {GetStatus2, _, _, _, GetAnswerOpt2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", GetStatus2),
  ?assertEqual(?VENOENT, GetAnswerOpt2),

  {Status01, Helper01, Id01, _Validity01, AnswerOpt01} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status01),
  ?assertEqual(?VOK, AnswerOpt01),
  ?assertEqual(Helper, Helper01),
  ?assertEqual(Id, Id01),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {Status1, _Helper1, _Id1, _Validity1, AnswerOpt1} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status1),
  ?assertEqual(?VEEXIST, AnswerOpt1),


  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Validity3, AnswerOpt3} = renew_file_location(Socket, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VOK, AnswerOpt3),

  {Status4, Answer4} = file_not_used(Socket, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(list_to_atom(?VOK), Answer4),
  {Status4_1, Answer4_1} = file_not_used(Socket, TestFile),
  ?assertEqual("ok", Status4_1),
  ?assertEqual(list_to_atom(?VOK), Answer4_1),


  %% Test automatic descriptors cleaning
  {Status4_2, Helper4_2, Id4_2, _Validity4_2, AnswerOpt4_2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status4_2),
  ?assertEqual(?VOK, AnswerOpt4_2),
  ?assertEqual(Helper, Helper4_2),
  ?assertEqual(Id, Id4_2),

  clear_old_descriptors(FSLogicNode),

  {Status4_4, _Validity4_4, AnswerOpt4_4} = renew_file_location(Socket, TestFile),
  ?assertEqual("ok", Status4_4),
  ?assertEqual(?VENOENT, AnswerOpt4_4),


  {Status5, Answer5} = mkdir(Socket, DirName),
  ?assertEqual("ok", Status5),
  ?assertEqual(list_to_atom(?VOK), Answer5),
  {Status5_1, Answer5_1} = mkdir(Socket, DirName),
  ?assertEqual("ok", Status5_1),
  ?assertEqual(list_to_atom(?VEEXIST), Answer5_1),

  CreateFile = fun(File) ->
    {Status6, _Helper6, _Id6, _Validity6, AnswerOpt6} = create_file(Socket, File),
    ?assertEqual("ok", Status6),
    ?assertEqual(?VOK, AnswerOpt6)
  end,
  lists:foreach(CreateFile, FilesInDir),

  {Status6_2, Files6_2, AnswerOpt6_2} = ls(Socket, DirName, 10, 0),
  ?assertEqual("ok", Status6_2),
  ?assertEqual(?VOK, AnswerOpt6_2),
  ?assertEqual(0, length(Files6_2)),

  AckCreateFile = fun(File) ->
    {CreationAckStatus3, CreationAckAnswerOpt3} = send_creation_ack(Socket, File),
    ?assertEqual("ok", CreationAckStatus3),
    ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt3)
  end,
  lists:foreach(AckCreateFile, FilesInDir),

  {Status7, Files7, AnswerOpt7} = ls(Socket, DirName, 10, 0),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VOK, AnswerOpt7),
  ?assertEqual(length(FilesInDir), length(Files7)),
  lists:foreach(fun(Name7) ->
    ?assert(lists:member(Name7, Files7))
  end, FilesInDirNames),


  {Status7_1, Files7_1, AnswerOpt7_1} = ls(Socket, DirName, 3, non),
  ?assertEqual("ok", Status7_1),
  ?assertEqual(?VOK, AnswerOpt7_1),
  ?assertEqual(3, length(Files7_1)),

  {Status7_2, Files7_2, AnswerOpt7_2} = ls(Socket, DirName, 5, 3),
  ?assertEqual("ok", Status7_2),
  ?assertEqual(?VOK, AnswerOpt7_2),
  ?assertEqual(2, length(Files7_2)),
  lists:foreach(fun(Name7_2) ->
    ?assert(lists:member(Name7_2, Files7_2) or lists:member(Name7_2, Files7_1))
  end, FilesInDirNames),


  [FirstFileInDir | FilesInDirTail] = FilesInDir,
  [_ | FilesInDirNamesTail] = FilesInDirNames,
  {Status8, Answer8} = delete_file(Socket, FirstFileInDir),
  ?assertEqual("ok", Status8),
  ?assertEqual(list_to_atom(?VOK), Answer8),

  {Status8_1, Answer8_1} = delete_file(Socket, FirstFileInDir),
  ?assertEqual("ok", Status8_1),
  ?assertEqual(list_to_atom(?VENOENT), Answer8_1),

  {Status9, Files9, AnswerOpt9} = ls(Socket, DirName, 10, non),
  ?assertEqual("ok", Status9),
  ?assertEqual(?VOK, AnswerOpt9),
  ?assertEqual(length(FilesInDirTail), length(Files9)),
  lists:foreach(fun(Name9) ->
    ?assert(lists:member(Name9, Files9))
  end, FilesInDirNamesTail),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  [_ | FilesInDirNamesTail2] = FilesInDirNamesTail,

  {Status19, Attr3} = get_file_attr(Socket, SecondFileInDir),
  ?assertEqual("ok", Status19),


  %% updatetimes message test
  CurrentTime = utils:time(),
  {Status20, Answer20} = update_times(Socket, SecondFileInDir, CurrentTime + 1234, CurrentTime + 4321),
  ?assertEqual("ok", Status20),
  ?assertEqual(list_to_atom(?VOK), Answer20),

  %% times update is async so we need to wait for it
  test_utils:wait_for_db_reaction(),
  {Status21, Attr4} = get_file_attr(Socket, SecondFileInDir),
  ?assertEqual("ok", Status21),

  ?assertEqual(CurrentTime + 1234, Attr4#fileattr.atime),
  ?assertEqual(CurrentTime + 4321, Attr4#fileattr.mtime),
  %% updatetimes message test end


  test_utils:wait_for_db_reaction(),
  {Status10, Answer10} = rename_file(Socket, SecondFileInDir, NewNameOfFIle),
  ?assertEqual("ok", Status10),
  ?assertEqual(list_to_atom(?VOK), Answer10),

  %% ctime update is async so we need to wait for it
  test_utils:wait_for_db_reaction(),

  {Status17, Attr1} = get_file_attr(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status17),

  %% Check if ctime was updated after rename
  ?assert(Attr1#fileattr.ctime > Attr3#fileattr.ctime),


  test_utils:wait_for_db_reaction(),
  {Status10_2, Answer10_2} = change_file_perms(Socket, NewNameOfFIle, 8#400),
  ?assertEqual("ok", Status10_2),
  ?assertEqual(list_to_atom(?VOK), Answer10_2),

  %% ctime update is async so we need to wait for it
  test_utils:wait_for_db_reaction(),

  {Status18, Attr2} = get_file_attr(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status18),

  %% Check if ctime was updated after chmod
  ?assert(Attr2#fileattr.ctime > Attr1#fileattr.ctime),

  %% Check if perms are set
  ?assertEqual(8#400, Attr2#fileattr.mode),

  {Status11, Files11, AnswerOpt11} = ls(Socket, DirName, 10, non),
  ?assertEqual("ok", Status11),
  ?assertEqual(?VOK, AnswerOpt11),
  ?assertEqual(length(FilesInDirNamesTail2), length(Files11)),
  lists:foreach(fun(Name11) ->
    ?assert(lists:member(Name11, Files11))
  end, FilesInDirNamesTail2),


  %% create file and move to dir
  {Status_MV, _, _, _, AnswerMV} = create_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV),
  ?assertEqual(?VOK, AnswerMV),

  {CreationAckStatus4, CreationAckAnswerOpt4} = send_creation_ack(Socket, TestFile2),
  ?assertEqual("ok", CreationAckStatus4),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt4),

  {Status_MV2, AnswerMV2} = rename_file(Socket, TestFile2, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", Status_MV2),
  ?assertEqual(list_to_atom(?VOK), AnswerMV2),

  {Status_MV3, _, _, _, AnswerMV3} = create_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV3),
  ?assertEqual(?VOK, AnswerMV3),

  {CreationAckStatus5, CreationAckAnswerOpt5} = send_creation_ack(Socket, TestFile2),
  ?assertEqual("ok", CreationAckStatus5),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt5),

  {Status_MV4, AnswerMV4} = delete_file(Socket, TestFile2),
  ?assertEqual("ok", Status_MV4),
  ?assertEqual(list_to_atom(?VOK), AnswerMV4),

  {Status_MV5, AnswerMV5} = delete_file(Socket, DirName ++ "/" ++ TestFile2),
  ?assertEqual("ok", Status_MV5),
  ?assertEqual(list_to_atom(?VOK), AnswerMV5),


  {Status12, Answer12} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status12),
  ?assertEqual(list_to_atom(?VENOTEMPTY), Answer12),

  Delete = fun(File) ->
    {Status13, Answer13} = delete_file(Socket, File),
    ?assertEqual("ok", Status13),
    ?assertEqual(list_to_atom(?VOK), Answer13)
  end,
  lists:foreach(Delete, FilesInDirTail2),

  {Status14, Answer14} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status14),
  ?assertEqual(list_to_atom(?VOK), Answer14),
  {Status14_1, Answer14_1} = delete_file(Socket, DirName),
  ?assertEqual("ok", Status14_1),
  ?assertEqual(list_to_atom(?VENOENT), Answer14_1),

  {Status15, Answer15} = delete_file(Socket, TestFile),
  ?assertEqual("ok", Status15),
  ?assertEqual(list_to_atom(?VOK), Answer15),

  {Status16, Answer16} = delete_file(Socket, NewNameOfFIle),
  ?assertEqual("ok", Status16),
  ?assertEqual(list_to_atom(?VOK), Answer16),

  wss:close(Socket),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks fslogic integration with dao and db
%% This test also checks chown & chgrp behaviour
users_separation_test(Config) ->
  NodesUp = ?config(nodes, Config),

  Cert1 = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  TestFile = "users_separation_test_file",

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert1, [?TEST_USER, ?TEST_GROUP]),
  [DN1 | _] = user_logic:get_dn_list(UserDoc1),
  Login1 = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc1]),
  UserID1 = UserDoc1#db_document.uuid,

  UserDoc2 = test_utils:add_user(Config, ?TEST_USER2, Cert2, [?TEST_USER2, ?TEST_GROUP2]),
  [DN2 | _] = user_logic:get_dn_list(UserDoc2),
  Login2 = rpc:call(FSLogicNode, user_logic, get_login, [UserDoc2]),
  UserID2 = UserDoc2#db_document.uuid,

  %% Open connections
  {ConAns, Socket} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}, auto_handshake]),
  ?assertEqual(ok, ConAns),

  {ConAns1, Socket2} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}, auto_handshake]),
  ?assertEqual(ok, ConAns1),

  %% Current time
  Time = utils:time(),
  test_utils:wait_for_db_reaction(),

  %% Users have different IDs
  ?assert(UserID1 =/= UserID2),

  {Status, Helper, Id, _Validity, AnswerOpt} = create_file(Socket, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt),

  {CreationAckStatus, CreationAckAnswerOpt} = send_creation_ack(Socket, TestFile),
  ?assertEqual("ok", CreationAckStatus),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt),

  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Helper3, _Id3, _Validity3, AnswerOpt3} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VENOENT, AnswerOpt3),

  {Status4, Helper4, Id4, _Validity4, AnswerOpt4} = create_file(Socket2, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(?VOK, AnswerOpt4),

  {CreationAckStatus2, CreationAckAnswerOpt2} = send_creation_ack(Socket2, TestFile),
  ?assertEqual("ok", CreationAckStatus2),
  ?assertEqual(list_to_atom(?VOK), CreationAckAnswerOpt2),

  {Status5, Helper5, Id5, _Validity5, AnswerOpt5} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status5),
  ?assertEqual(?VOK, AnswerOpt5),
  ?assertEqual(Helper4, Helper5),
  ?assertEqual(Id4, Id5),

  %% Check if owners are set correctly

  {Status21, Attr1} = get_file_attr(Socket, TestFile),
  ?assertEqual("ok", Status21),
  {Status22, Attr2} = get_file_attr(Socket2, TestFile),
  ?assertEqual("ok", Status22),

  %% Check logins
  ?assertEqual(Login1, Attr1#fileattr.uname),
  ?assertEqual(Login2, Attr2#fileattr.uname),


  test_utils:wait_for_db_reaction(),

  %% chown test
    %% @todo: remove chown?
%%   {Status23, Answer23} = chown(Socket, TestFile, 77777),
%%   ?assertEqual("ok", Status23),
%%   ?assertEqual(list_to_atom(?VEACCES), Answer23),
%%
%%   ?assertEqual(ok, rpc:call(FSLogicNode, logical_files_manager, chown, ["/spaces/" ++ Login1 ++ "/" ++ TestFile, UID2])),
%%   ?assertEqual(ok, rpc:call(FSLogicNode, logical_files_manager, chown, ["/spaces/" ++ Login2 ++ "/" ++ TestFile, UID1])),
%%
%%   %% Check if owners are set properly
%%   {Status26, Attr3} = get_file_attr(Socket, TestFile),
%%   ?assertEqual("ok", Status26),
%%   {Status27, Attr4} = get_file_attr(Socket2, TestFile),
%%   ?assertEqual("ok", Status27),
%%
%%   %% Check logins
%%   ?assertEqual(Login2, Attr3#fileattr.uname),
%%   ?assertEqual(Login1, Attr4#fileattr.uname),
%%
%%   %% Check UIDs
%%   ?assertEqual(UID2, Attr3#fileattr.uid),
%%   ?assertEqual(UID1, Attr4#fileattr.uid),

  %% Check if change time was updated and if times was setup correctly on file creation
  ?assert(Attr1#fileattr.atime > Time),
  ?assert(Attr1#fileattr.mtime > Time),
  ?assert(Attr1#fileattr.ctime > Time),
  ?assert(Attr2#fileattr.atime > Time),
  ?assert(Attr2#fileattr.mtime > Time),
  ?assert(Attr2#fileattr.ctime > Time),

%%   ?assert(Attr3#fileattr.ctime > Attr1#fileattr.ctime),
%%   ?assert(Attr4#fileattr.ctime > Attr2#fileattr.ctime),

  %% Check attrs in logical_files_manager
  {FMStatys, FM_Attrs} = rpc:call(FSLogicNode, logical_files_manager, getfileattr, ["/spaces/" ++ Login2 ++ "/" ++ TestFile]),
  ?assertEqual(ok, FMStatys),
%%   ?assertEqual(Attr4#fileattr.mode, FM_Attrs#fileattributes.mode),
%%   ?assertEqual(Attr4#fileattr.uid, FM_Attrs#fileattributes.uid),
%%   ?assertEqual(Attr4#fileattr.gid, FM_Attrs#fileattributes.gid),
%%   ?assertEqual(Attr4#fileattr.type, FM_Attrs#fileattributes.type),
%%   ?assertEqual(Attr4#fileattr.size, FM_Attrs#fileattributes.size),
%%   ?assertEqual(Attr4#fileattr.uname, utils:ensure_list(FM_Attrs#fileattributes.uname)),
%%   ?assertEqual(Attr4#fileattr.gname, FM_Attrs#fileattributes.gname),
%%   ?assertEqual(Attr4#fileattr.ctime, FM_Attrs#fileattributes.ctime),
%%   ?assertEqual(Attr4#fileattr.mtime, FM_Attrs#fileattributes.mtime),
%%   ?assertEqual(Attr4#fileattr.atime, FM_Attrs#fileattributes.atime),

%%   ?assertEqual(ok, rpc:call(FSLogicNode, logical_files_manager, chown, ["/spaces/" ++ Login1 ++ "/" ++ TestFile, UID1])),
  {Status6, Answer6} = delete_file(Socket, TestFile),
  ?assertEqual("ok", Status6),
  ?assertEqual(list_to_atom(?VOK), Answer6),

  {Status7, _Helper7, _Id7, _Validity7, AnswerOpt7} = get_file_location(Socket, TestFile),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VENOENT, AnswerOpt7),

  {Status8, Helper8, Id8, _Validity8, AnswerOpt8} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status8),
  ?assertEqual(?VOK, AnswerOpt8),
  ?assertEqual(Helper4, Helper8),
  ?assertEqual(Id4, Id8),

%%   ?assertEqual(ok, rpc:call(FSLogicNode, logical_files_manager, chown, ["/spaces/" ++ Login2 ++ "/" ++ TestFile, UID2])),
  {Status9, Answer9} = delete_file(Socket2, TestFile),
  ?assertEqual("ok", Status9),
  ?assertEqual(list_to_atom(?VOK), Answer9),

  {Status10, _Helper10, _Id10, _Validity10, AnswerOpt10} = get_file_location(Socket2, TestFile),
  ?assertEqual("ok", Status10),
  ?assertEqual(?VENOENT, AnswerOpt10),

  %% Link tests

  % Create link
  {Status17, Answer17} = create_link(Socket, "link_name", "/target/path"),
  ?assertEqual("ok", Status17),
  ?assertEqual(list_to_atom(?VOK), Answer17),

  % Create same link second time
  {Status18, Answer18} = create_link(Socket, "link_name", "/target/path1"),
  ?assertEqual("ok", Status18),
  ?assertEqual(list_to_atom(?VEEXIST), Answer18),

  % Check if created link has valid data
  {Status19, Answer19, LinkPath} = get_link(Socket, "link_name"),
  ?assertEqual("ok", Status19),
  ?assertEqual("ok", Answer19),
  ?assertEqual("/target/path", LinkPath),

  {Status19_2, Answer19_2} = delete_file(Socket, "link_name"),
  ?assertEqual("ok", Status19_2),
  ?assertEqual(list_to_atom(?VOK), Answer19_2),

  % Try to fetch invalid link data
  {Status20, Answer20, _} = get_link(Socket, "link_name1"),
  ?assertEqual("ok", Status20),
  ?assertEqual(?VENOENT, Answer20),

  wss:close(Socket),
  wss:close(Socket2),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN1}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% Checks files manager (manipulation on tmp files copies)
files_manager_tmp_files_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},
  File = "files_manager_test_file1",
  NotExistingFile = "files_manager_test_not_existing_file",

  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ NotExistingFile)),

  AnsCreate = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  AnsCreate2 = rpc:call(Node1, storage_files_manager, create, [SHInfo, File]),
  ?assertEqual({error, file_exists}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, 0, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),
  ?assertEqual({ok, "abcdefgh"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead1, AnsRead1} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),
  ?assertEqual({ok, "abc123gh"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead3, AnsRead3} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, storage_files_manager, write, [SHInfo, File, 8, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),
  ?assertEqual({ok, "abc123ghXYZ"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead4, AnsRead4} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, storage_files_manager, truncate, [SHInfo, File, 5]),
  ?assertEqual(ok, AnsTruncate),
  ?assertEqual({ok, "abc12"}, files_tester:read_file_storage(?TEST_ROOT ++ "/" ++ File, 100)),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, storage_files_manager, read, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, storage_files_manager, read, [SHInfo, NotExistingFile, 0, 100]),
  ?assertEqual(wrong_getatt_return_code, StatusRead6),
  ?assert(is_integer(AnsRead6)),

  AnsDel = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(?TEST_ROOT ++ "/" ++ File)),

  {StatusDel2, AnsDel2} = rpc:call(Node1, storage_files_manager, delete, [SHInfo, File]),
  ?assertEqual(wrong_getatt_return_code, StatusDel2),
  ?assert(is_integer(AnsDel2)).

%% Checks files manager (manipulation on users' files)
files_manager_standard_files_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  TestFile = "files_manager_standard_test_file",
  DirName = "fslogic_test_dir2",
  FileInDir = "files_manager_test_file2",
  FileInDir2 = "files_manager_test_file3",
  FileInDir3 = "files_manager_test_file4",
  FileInDir2NewName = "files_manager_test_file3_new_name",
  File = DirName ++ "/" ++ FileInDir,
  File2 = DirName ++ "/" ++ FileInDir2,
  File2NewName = DirName ++ "/" ++ FileInDir2NewName,
  File3 = DirName ++ "/../" ++ FileInDir3,
  File4 = DirName ++ "/./" ++ FileInDir3,
  File4NoDot = DirName ++ "/" ++ FileInDir3,

  NotExistingFile = "files_manager_test_not_existing_file",

  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2NewName])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File3])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File4])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File4NoDot])),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [NotExistingFile])),

  MkDirAns = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual(ok, MkDirAns),

  MkDirAns2 = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual({error, dir_exists}, MkDirAns2),

  ?assertEqual(false, rpc:call(Node1, logical_files_manager, exists, [File])),

  AnsCreate = rpc:call(Node1, logical_files_manager, create, [File]),
  ?assertEqual(ok, AnsCreate),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File])),

  ?assert(rpc:call(Node1, logical_files_manager, exists, [File])),

  AnsCreate2 = rpc:call(Node1, logical_files_manager, create, [File]),
  ?assertEqual({error, file_exists}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, logical_files_manager, write, [File, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),
  ?assertEqual({ok, "abcdefgh"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead1, AnsRead1} = rpc:call(Node1, logical_files_manager, read, [File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, logical_files_manager, read, [File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, logical_files_manager, write, [File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),
  ?assertEqual({ok, "abc123gh"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead3, AnsRead3} = rpc:call(Node1, logical_files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, logical_files_manager, write, [File, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),
  ?assertEqual({ok, "abc123ghXYZ"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead4, AnsRead4} = rpc:call(Node1, logical_files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, logical_files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, logical_files_manager, truncate, [File, 5]),
  ?assertEqual(ok, AnsTruncate),
  ?assertEqual({ok, "abc12"}, rpc:call(Node1, files_tester, read_file, [File, 100])),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, logical_files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, logical_files_manager, read, [NotExistingFile, 0, 100]),
  ?assertEqual(logical_file_system_error, StatusRead6),
  ?assertEqual(?VENOENT, AnsRead6),

  AnsCreate3 = rpc:call(Node1, logical_files_manager, create, [File2]),
  ?assertEqual(ok, AnsCreate3),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File2])),

  {StatusLs, AnsLs} = rpc:call(Node1, logical_files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs),
  ?assertEqual(2, length(AnsLs)),
  FileList1 = lists:map(fun(#dir_entry{name = Name}) -> Name end, AnsLs),
  ?assert(lists:member(FileInDir, FileList1)),
  ?assert(lists:member(FileInDir2, FileList1)),

  {File2LocationAns, File2Location} = rpc:call(Node1, files_tester, get_file_location, [File2]),
  ?assertEqual(ok, File2LocationAns),
  AnsMv = rpc:call(Node1, logical_files_manager, mv, [File2, File2NewName]),
  ?assertEqual(ok, AnsMv),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [File2])),
  ?assert(rpc:call(Node1, files_tester, file_exists, [File2NewName])),
  {File2LocationAns2, File2Location2} = rpc:call(Node1, files_tester, get_file_location, [File2NewName]),
  ?assertEqual(ok, File2LocationAns2),
  ?assertEqual(File2Location, File2Location2),

  AnsChPerm = rpc:call(Node1, logical_files_manager, change_file_perm, [File, 8#777, true]),
  ?assertEqual(ok, AnsChPerm),

  {StatusLs2, AnsLs2} = rpc:call(Node1, logical_files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs2),
  ?assertEqual(2, length(AnsLs2)),
  FileList2 = lists:map(fun(#dir_entry{name = Name}) -> Name end, AnsLs2),
  ?assert(lists:member(FileInDir, FileList2)),
  ?assert(lists:member(FileInDir2NewName, FileList2)),


  %% create file and move to dir
  AnsCreate4 = rpc:call(Node1, logical_files_manager, create, [TestFile]),
  ?assertEqual(ok, AnsCreate4),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),

  {TestFileLocationAns, TestFileLocation} = rpc:call(Node1, files_tester, get_file_location, [TestFile]),
  ?assertEqual(ok, TestFileLocationAns),
  AnsMv2 = rpc:call(Node1, logical_files_manager, mv, [TestFile, DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, AnsMv2),
  ?assertEqual(file_not_exists_in_db, rpc:call(Node1, files_tester, file_exists, [TestFile])),
  ?assert(rpc:call(Node1, files_tester, file_exists, [DirName ++ "/" ++ TestFile])),
  {TestFileLocationAns2, TestFileLocation2} = rpc:call(Node1, files_tester, get_file_location, [DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, TestFileLocationAns2),
  ?assertEqual(TestFileLocation, TestFileLocation2),

  AnsCreate5 = rpc:call(Node1, logical_files_manager, create, [TestFile]),
  ?assertEqual(ok, AnsCreate5),
  ?assert(rpc:call(Node1, files_tester, file_exists, [TestFile])),
  {TestFileLocationAns3, TestFileLocation3} = rpc:call(Node1, files_tester, get_file_location, [TestFile]),
  ?assertEqual(ok, TestFileLocationAns3),
  ?assertEqual(false, TestFileLocation =:= TestFileLocation3),

  AnsMvDel = rpc:call(Node1, logical_files_manager, delete, [TestFile]),
  ?assertEqual(ok, AnsMvDel),
  ?assertEqual(false, files_tester:file_exists_storage(TestFileLocation3)),
  ?assert(files_tester:file_exists_storage(TestFileLocation)),

  AnsMvDel2 = rpc:call(Node1, logical_files_manager, delete, [DirName ++ "/" ++ TestFile]),
  ?assertEqual(ok, AnsMvDel2),
  ?assertEqual(false, files_tester:file_exists_storage(TestFileLocation)),

  AnsCreate6 = rpc:call(Node1, logical_files_manager, create, [File3]),
  ?assertEqual({logical_file_system_error, ?VEREMOTEIO}, AnsCreate6),

  AnsCreate7 = rpc:call(Node1, logical_files_manager, create, [File4]),
  ?assertEqual(ok, AnsCreate7),
  ?assert(rpc:call(Node1, logical_files_manager, exists, [File4])),
  {File4LocationAns, _} = rpc:call(Node1, files_tester, get_file_location, [File4NoDot]),
  ?assertEqual(ok, File4LocationAns),

  {FileLocationAns, FileLocation} = rpc:call(Node1, files_tester, get_file_location, [File]),
  ?assertEqual(ok, FileLocationAns),
  AnsDel = rpc:call(Node1, logical_files_manager, delete, [File]),
  ?assertEqual(ok, AnsDel),
  ?assertEqual(false, files_tester:file_exists_storage(FileLocation)),

  AnsDel2 = rpc:call(Node1, logical_files_manager, delete, [File2NewName]),
  ?assertEqual(ok, AnsDel2),
  ?assertEqual(false, files_tester:file_exists_storage(File2Location2)),

  AnsDel3 = rpc:call(Node1, logical_files_manager, delete, [File2NewName]),
  ?assertEqual({logical_file_system_error, ?VENOENT}, AnsDel3),

  AnsDel4 = rpc:call(Node1, logical_files_manager, delete, [File4]),
  ?assertEqual(ok, AnsDel4),

  AnsDirDelete = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual(ok, AnsDirDelete),

  AnsDirDelete2 = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual({logical_file_system_error, ?VENOENT}, AnsDirDelete2),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

get_file_links_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  DirName = "base_dir",

  AnsDirCreate1 = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual(ok, AnsDirCreate1),

  %% Check number of links for empty directory
  {AttrAns1, Attrs1} = rpc:call(Node1, logical_files_manager, getfileattr, [DirName]),
  ?assertEqual(ok, AttrAns1),
  ?assertEqual(2, Attrs1#fileattributes.links),

  AnsFileCreate1 = rpc:call(Node1, logical_files_manager, create, [DirName ++ "/file"]),
  ?assertEqual(ok, AnsFileCreate1),

  %% Check number of links for directory with one regular file
  {AttrAns2, Attrs2} = rpc:call(Node1, logical_files_manager, getfileattr, [DirName]),
  ?assertEqual(ok, AttrAns2),
  ?assertEqual(2, Attrs2#fileattributes.links),

  %% Check number of links for regular file
  {AttrAns3, Attrs3} = rpc:call(Node1, logical_files_manager, getfileattr, [DirName ++ "/file"]),
  ?assertEqual(ok, AttrAns3),
  ?assertEqual(1, Attrs3#fileattributes.links),

  AnsDirCreate2 = rpc:call(Node1, logical_files_manager, mkdir, [DirName ++ "/dir1"]),
  ?assertEqual(ok, AnsDirCreate2),

  AnsDirCreate3 = rpc:call(Node1, logical_files_manager, mkdir, [DirName ++ "/dir2"]),
  ?assertEqual(ok, AnsDirCreate3),

  AnsDirCreate4 = rpc:call(Node1, logical_files_manager, mkdir, [DirName ++ "/dir1/dir11"]),
  ?assertEqual(ok, AnsDirCreate4),

  AnsFileCreate2 = rpc:call(Node1, logical_files_manager, create, [DirName ++ "/dir2/file"]),
  ?assertEqual(ok, AnsFileCreate2),

  %% Check number of links for directory with more complicated structure
  {AttrAns4, Attrs4} = rpc:call(Node1, logical_files_manager, getfileattr, [DirName]),
  ?assertEqual(ok, AttrAns4),
  ?assertEqual(4, Attrs4#fileattributes.links),

  %% Remove created files
  AnsDel1 = rpc:call(Node1, logical_files_manager, delete, [DirName ++ "/file"]),
  ?assertEqual(ok, AnsDel1),

  AnsDel2 = rpc:call(Node1, logical_files_manager, delete, [DirName ++ "/dir2/file"]),
  ?assertEqual(ok, AnsDel2),

  AnsDel3 = rpc:call(Node1, logical_files_manager, rmdir, [DirName ++ "/dir1/dir11"]),
  ?assertEqual(ok, AnsDel3),

  AnsDel4 = rpc:call(Node1, logical_files_manager, rmdir, [DirName ++ "/dir1"]),
  ?assertEqual(ok, AnsDel4),

  AnsDel5 = rpc:call(Node1, logical_files_manager, rmdir, [DirName ++ "/dir2"]),
  ?assertEqual(ok, AnsDel5),

  AnsDel6 = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual(ok, AnsDel6),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

xattrs_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  DirName = "test_xattr_dir",

  % make test file
  AnsDirCreate1 = rpc:call(Node1, logical_files_manager, mkdir, [DirName]),
  ?assertEqual(ok, AnsDirCreate1),

  % test setting and getting xattrs
  Ans1 = rpc:call(Node1, logical_files_manager, set_xattr, [DirName, "name1", "value1"]),
  ?assertEqual(ok, Ans1),
  Ans2 = rpc:call(Node1, logical_files_manager, get_xattr, [DirName, "name1"]),
  ?assertEqual({ok, "value1"}, Ans2),

  % test replacing xattr
  Ans3 = rpc:call(Node1, logical_files_manager, set_xattr, [DirName, "name1", "other_value"]),
  ?assertEqual(ok, Ans3),
  Ans4 = rpc:call(Node1, logical_files_manager, get_xattr, [DirName, "name1"]),
  ?assertEqual({ok, "other_value"}, Ans4),

  % test listing xattr
  Ans5 = rpc:call(Node1, logical_files_manager, set_xattr, [DirName, "name2", "value2"]),
  ?assertEqual(ok, Ans5),
  Ans6 = rpc:call(Node1, logical_files_manager, list_xattr, [DirName]),
  ?assertEqual({ok, [{"name2", "value2"}, {"name1", "other_value"}]}, Ans6),

  % test removing xattr
  Ans7 = rpc:call(Node1, logical_files_manager, remove_xattr, [DirName, "name2"]),
  ?assertEqual(ok, Ans7),
  Ans8 = rpc:call(Node1, logical_files_manager, list_xattr, [DirName]),
  ?assertEqual({ok, [{"name1", "other_value"}]}, Ans8),

  % test error reporting
  Ans9 = rpc:call(Node1, logical_files_manager, get_xattr, [DirName, "name3"]),
  ?assertEqual({logical_file_system_error, ?VENOATTR}, Ans9),

  % cleanup
  AnsDel = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
  ?assertEqual(ok, AnsDel),
  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

acl_test(Config) ->
    NodesUp = ?config(nodes, Config),
    [Node1 | _] = NodesUp,

    gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT]),
    ?assertEqual(ok, InsertStorageAns),

    Cert = ?COMMON_FILE("peer.pem"),
    UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
    [DN | _] = user_logic:get_dn_list(UserDoc),
    fslogic_context:set_user_dn(DN),

    DirName = "/spaces/" ++ ?TEST_USER ++ "/test_acl_dir",

    % make test file
    AnsDirCreate1 = rpc:call(Node1, fslogic_test_SUITE, make_dir, [DirName, DN]),
    ?assertEqual(ok, AnsDirCreate1),

    %test getting virtual acl
    VirtualAclAns = rpc:call(Node1, fslogic_test_SUITE, get_acl, [DirName, DN]),
    ?assertEqual({ok,[#accesscontrolentity{acetype = ?allow_mask, identifier = <<"global_id_for_", ?TEST_USER>>, aceflags = ?no_flags_mask, acemask = ?read_mask bor ?write_mask}]}
        ,VirtualAclAns),
    {ok, VirtualAcl} = VirtualAclAns,
    TestUserName = <<?TEST_USER, " ", ?TEST_USER>>,
    ?assertEqual([[{<<"acetype">>,<<"ALLOW">>},
        {<<"identifier">>, TestUserName},
        {<<"aceflags">>, <<"NO_FLAGS">>},
        {<<"acemask">>, <<"READ, WRITE">>}]],
        rpc:call(Node1, fslogic_acl,from_acl_to_json_format,[VirtualAcl])),

    GroupName = <<"name">>,
    GroupId = <<"Id">>,
    Group = #db_document{record = #group_details{name = GroupName, id= GroupId}},
    test_utils:ct_mock(Config, dao_groups, get_group_by_name, fun(_) -> {ok, [Group]} end),

    % test setting and getting acl
    TestUserNameWithNoHash = <<?TEST_USER, " ", ?TEST_USER>>,
    Acl = rpc:call(Node1, fslogic_acl, from_json_fromat_to_acl,[
        [
            [
                {<<"acetype">>, <<"ALLOW">>},
                {<<"identifier">>, TestUserNameWithNoHash},
                {<<"aceflags">>, <<"NO_FLAGS">>},
                {<<"acemask">>, <<"READ, WRITE">>}
            ],
            [
                {<<"acetype">>, <<"DENY">>},
                {<<"identifier">>, GroupId},
                {<<"aceflags">>, <<"IDENTIFIER_GROUP">>},
                {<<"acemask">>, <<"WRITE">>}
            ]
        ]
    ]),
    ?assertEqual(Acl, [
        #accesscontrolentity{acetype = ?allow_mask, identifier = <<"global_id_for_", ?TEST_USER>>, aceflags = ?no_flags_mask, acemask = ?read_mask bor ?write_mask},
        #accesscontrolentity{acetype = ?deny_mask, identifier = GroupId, aceflags = ?identifier_group_mask, acemask = ?write_mask}
    ]),

    Ans1 = rpc:call(Node1, logical_files_manager, set_acl, [DirName, Acl]),
    ?assertEqual(ok, Ans1),
    Ans2 = rpc:call(Node1, logical_files_manager, get_acl, [DirName]),
    ?assertEqual({ok,Acl}, Ans2),

    % cleanup
    AnsDel = rpc:call(Node1, logical_files_manager, rmdir, [DirName]),
    ?assertEqual(ok, AnsDel),
    RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
    ?assertEqual(ok, RemoveStorageAns).


get_file_local_location_test(Config) ->
    [Node1 | _] = ?config(nodes, Config),

    gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    FileId = "1234",
    Doc = #db_document{uuid = FileId, record = #file{}},
    Location = #file_location{file_id = FileId, storage_file_id = "123"},

    {ok, _} = rpc:call(Node1, dao_lib, apply, [dao_vfs, save_file_location, [Location], ?ProtocolVersion]),
    ?assertMatch(#file_location{storage_file_id = "123"}, rpc:call(Node1, fslogic_file, get_file_local_location, [Doc])),
    ?assertMatch(#file_location{storage_file_id = "123"}, rpc:call(Node1, fslogic_file, get_file_local_location, [FileId])).


block_creation_test(Config) ->
    FileName = "/block_creation_test",
    [Node1 | _] = ?config(nodes, Config),

    gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ?assertMatch({ok, _}, rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT])),

    ?assertEqual(ok, rpc:call(Node1, logical_files_manager, create, [FileName])),
    {ok, FileDoc} = rpc:call(Node1, fslogic_objects, get_file, [FileName]),
    #db_document{uuid = LocationId} = rpc:call(Node1, fslogic_file, get_file_local_location_doc, [FileDoc]),

    ExpectedBlock = #file_block{file_location_id = LocationId, offset = 0, size = ?FILE_BLOCK_SIZE_INF},
    {ok, Blocks} = rpc:call(Node1, dao_lib, apply, [dao_vfs, get_file_blocks, [LocationId], ?ProtocolVersion]),
    ?assertMatch([#db_document{record = ExpectedBlock}], Blocks).


block_registration_test(Config) ->
    FileName = "/block_registration_test",
    Cert = ?COMMON_FILE("peer.pem"),
    Host = "localhost",
    Port = ?config(port, Config),
    [Node1 | _] = ?config(nodes, Config),

    gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ?assertMatch({ok, _}, rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT])),

    UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
    {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),

    ?assertMatch({?VOK, _, _, _, ?VOK}, create_file(Socket, FileName)),
    ?assertEqual({?VOK, ok}, send_creation_ack(Socket, FileName)),
    {ok, FullFileName} = rpc:call(Node1, fslogic_path, get_full_file_name, [FileName, cluter_request, ok, UserDoc]),

    ?assertEqual({ok, 1}, rpc:call(Node1, fslogic_req_regular, register_file_block, [FullFileName, 9, 9])).


available_blocks_cache_test(Config) ->
    FileName = "/cache_test",
    Cert = ?COMMON_FILE("peer.pem"),
    Host = "localhost",
    Port = ?config(port, Config),
    [Node1 | _] = ?config(nodes, Config),

    gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ?assertMatch({ok, _}, rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT])),

    UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
    [DN | _] = user_logic:get_dn_list(UserDoc),
    {ok, Socket} = wss:connect(Host, Port, [{certfile, Cert}, {cacertfile, Cert}, auto_handshake]),

    ?assertMatch({?VOK, _, _, _, ?VOK}, create_file(Socket, FileName)),
    ?assertEqual({?VOK, ok}, send_creation_ack(Socket, FileName)),
    {ok, Uuid} = rpc:call(Node1, fslogic_test_SUITE, get_file_uuid, [FileName, DN]),

    Size = {100, 10},
    Blocks = #available_blocks{file_id = Uuid, file_parts = [], file_size = Size},
    rpc:call(Node1, fslogic_available_blocks, call, [{save_available_blocks, Blocks}]),
    ?assertMatch({ok, Size}, rpc:call(Node1, fslogic_available_blocks, call, [{get_file_size, Uuid}])),
    ?assertMatch({ok, [#db_document{record = Blocks}, _]}, rpc:call(Node1, fslogic_available_blocks, call, [{list_all_available_blocks, Uuid}])),
    ?assertMatch({ok, Size}, rpc:call(Node1, fslogic_available_blocks, call, [{get_file_size, Uuid}])).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(user_file_size_test, Config) ->
  ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [FSLogicNode | _] = NodesUp,

  DB_Node = ?DB_NODE,
  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}, {user_files_size_view_update_period, 2}, {heart_beat, 1}]]),
  ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config));

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1,true),
  [FSLogicNode | _] = NodesUp,

  DB_Node = ?DB_NODE,
  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}, {heart_beat, 1}]]),

  ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config)).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().

%% ====================================================================
%% Helper functions
%% ====================================================================

%% Each of following functions simulate one request from FUSE.
create_file(Socket, FileName) ->
  FslogicMessage = #getnewfilelocation{file_logic_name = FileName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getnewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getnewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "filelocation",
    answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

send_creation_ack(Socket, FileName) ->
  FslogicMessage = #createfileack{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createfileack(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createfileack", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

get_file_location(Socket, FileName) ->
  FslogicMessage = #getfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "filelocation",
    answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Location = fuse_messages_pb:decode_filelocation(Bytes),
  Location2 = records_translator:translate(Location, "fuse_messages"),
  {Status, Location2#filelocation.storage_helper_name, Location2#filelocation.file_id, Location2#filelocation.validity, Location2#filelocation.answer}.

renew_file_location(Socket, FileName) ->
  FslogicMessage = #renewfilelocation{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renewfilelocation(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renewfilelocation", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "filelocationvalidity",
    answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Validity = fuse_messages_pb:decode_filelocationvalidity(Bytes),
  Validity2 = records_translator:translate(Validity, "fuse_messages"),
  {Status, Validity2#filelocationvalidity.validity, Validity2#filelocationvalidity.answer}.

file_not_used(Socket, FileName) ->
  FslogicMessage = #filenotused{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_filenotused(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "filenotused", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

mkdir(Socket, DirName) ->
  FslogicMessage = #createdir{dir_logic_name = DirName, mode = 8#644},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createdir(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createdir", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

ls(Socket, Dir, Num, Offset) ->
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

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Files = fuse_messages_pb:decode_filechildren(Bytes),
  Files2 = records_translator:translate(Files, "fuse_messages"),
  FileList = lists:map(fun(#filechildren_direntry{name = Name}) -> Name end, Files2#filechildren.entry),
  {Status, FileList, Files2#filechildren.answer}.

delete_file(Socket, FileName) ->
  FslogicMessage = #deletefile{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_deletefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "deletefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

rename_file(Socket, FileName, NewName) ->
  FslogicMessage = #renamefile{from_file_logic_name = FileName, to_file_logic_name = NewName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renamefile(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "renamefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

change_file_perms(Socket, FileName, Perms) ->
  FslogicMessage = #changefileperms{file_logic_name = FileName, perms = Perms},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileperms(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "changefileperms", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

create_link(Socket, From, To) ->
  FslogicMessage = #createlink{from_file_logic_name = From, to_file_logic_name = To},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_createlink(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "createlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

get_link(Socket, FileName) ->
  FslogicMessage = #getlink{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getlink(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getlink", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "linkinfo",
    answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_linkinfo(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp#linkinfo.answer, Resp1#linkinfo.file_logic_name}.

get_file_attr(Socket, FileName) ->
  FslogicMessage = #getfileattr{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfileattr(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "getfileattr", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "fileattr",
    answer_decoder_name = "fuse_messages", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_fileattr(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp1}.

update_times(Socket, FileName, ATime, MTime) ->
  FslogicMessage = #updatetimes{file_logic_name = FileName, atime = ATime, mtime = MTime},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_updatetimes(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "updatetimes", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

chown(Socket, FileName, UID) ->
  FslogicMessage = #changefileowner{file_logic_name = FileName, uid = UID},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileowner(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "changefileowner", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, MessageBytes),
  {SendAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Answer = communication_protocol_pb:decode_atom(Bytes),
  Answer2 = records_translator:translate(Answer, "communication_protocol"),
  {Status, Answer2}.

%% chgrp(Socket, FileName, GID, GName) ->
%%   FslogicMessage = #changefilegroup{file_logic_name = FileName, gid = GID, gname = GName},
%%   FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefilegroup(FslogicMessage)),
%%
%%   FuseMessage = #fusemessage{message_type = "changefilegroup", input = FslogicMessageMessageBytes},
%%   FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),
%%
%%   Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
%%     message_decoder_name = "fuse_messages", answer_type = "atom",
%%     answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
%%   MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
%%
%%   wss:send(Socket, MessageBytes),
%%   {SendAns, Ans} = wss:recv(Socket, 5000),
%%   ?assertEqual(ok, SendAns),
%%
%%   #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
%%   Answer = communication_protocol_pb:decode_atom(Bytes),
%%   Answer2 = records_translator:translate(Answer, "communication_protocol"),
%%   {Status, Answer2}.

clear_old_descriptors(Node) ->
  {Megaseconds, Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000 * Megaseconds + Seconds + 60 * 15 + 1,
  gen_server:call({?Dispatcher_Name, Node}, {fslogic, 1, {delete_old_descriptors_test, Time}}, 1000),
  test_utils:wait_for_db_reaction().

create_standard_share(TestFile, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:create_standard_share(TestFile).

create_share(TestFile, Share_With, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:create_share(TestFile, Share_With).

get_share(Key, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:get_share(Key).

get_acl(FilePath, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:get_acl(FilePath).

make_dir(FilePath, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:mkdir(FilePath).

get_file_uuid(FilePath, DN) ->
  fslogic_context:set_user_dn(DN),
  logical_files_manager:get_file_uuid(FilePath).
