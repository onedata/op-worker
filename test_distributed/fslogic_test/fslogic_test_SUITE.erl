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

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([files_manager_standard_files_test/1, files_manager_tmp_files_test/1, fuse_requests_test/1, users_separation_test/1]).

all() -> [files_manager_tmp_files_test, files_manager_standard_files_test, fuse_requests_test, users_separation_test].

-define(SH, "DirectIO").
-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem
-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% Checks fslogic integration with dao and db
fuse_requests_test(Config) ->
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

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
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
  ?assertEqual(length(FilesInDir), length(Files7)),
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
  ?assertEqual(list_to_atom(?VEREMOTEIO), Answer8_1),

  {Status9, Files9, AnswerOpt9} = ls(Host, Cert, Port, DirName, 10, non),
  ?assertEqual("ok", Status9),
  ?assertEqual(?VOK, AnswerOpt9),
  ?assertEqual(length(FilesInDirTail), length(Files9)),
  lists:foreach(fun(Name9) ->
    ?assert(lists:member(Name9, Files9))
  end, FilesInDirNamesTail),

  [SecondFileInDir | FilesInDirTail2] = FilesInDirTail,
  [_ | FilesInDirNamesTail2] = FilesInDirNamesTail,

  {Status19, Attr3} = get_file_attr(Host, Cert, Port, SecondFileInDir),
  ?assertEqual("ok", Status19),


  %% updatetimes message test
  {Status20, Answer20} = update_times(Host, Cert, Port, SecondFileInDir, 1234, 5678),
  ?assertEqual("ok", Status20),
  ?assertEqual(list_to_atom(?VOK), Answer20),

  %% times update is async so we need to wait for it 
  timer:sleep(500),
  {Status21, Attr4} = get_file_attr(Host, Cert, Port, SecondFileInDir),
  ?assertEqual("ok", Status21),

  ?assertEqual(1234, Attr4#fileattr.atime),
  ?assertEqual(5678, Attr4#fileattr.mtime),
  %% updatetimes message test end


  timer:sleep(1100),
  {Status10, Answer10} = rename_file(Host, Cert, Port, SecondFileInDir, NewNameOfFIle),
  ?assertEqual("ok", Status10),
  ?assertEqual(list_to_atom(?VOK), Answer10),

  %% ctime update is async so we need to wait for it 
  timer:sleep(500),

  {Status17, Attr1} = get_file_attr(Host, Cert, Port, NewNameOfFIle),
  ?assertEqual("ok", Status17),

  %% Check if ctime was updated after rename
  ?assert(Attr1#fileattr.ctime > Attr3#fileattr.ctime),


  timer:sleep(1100),
  {Status10_2, Answer10_2} = change_file_perms(Host, Cert, Port, NewNameOfFIle, 8#400),
  ?assertEqual("ok", Status10_2),
  ?assertEqual(list_to_atom(?VOK), Answer10_2),

  %% ctime update is async so we need to wait for it 
  timer:sleep(500),

  {Status18, Attr2} = get_file_attr(Host, Cert, Port, NewNameOfFIle),
  ?assertEqual("ok", Status18),

  %% Check if ctime was updated after chmod
  ?assert(Attr2#fileattr.ctime > Attr1#fileattr.ctime),

  %% Check if perms are set
  ?assertEqual(8#400, Attr2#fileattr.mode),

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
  ?assertEqual(list_to_atom(?VEREMOTEIO), Answer14_1),

  {Status15, Answer15} = delete_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status15),
  ?assertEqual(list_to_atom(?VOK), Answer15),

  {Status16, Answer16} = delete_file(Host, Cert, Port, NewNameOfFIle),
  ?assertEqual("ok", Status16),
  ?assertEqual(list_to_atom(?VOK), Answer16),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns).

%% Checks fslogic integration with dao and db
%% This test also checks chown & chgrp behaviour
users_separation_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),
  Host = "localhost",
  Port = ?config(port, Config),
  [FSLogicNode | _] = NodesUp,

  TestFile = "users_separation_test_file",

  gen_server:cast({?Node_Manager_Name, FSLogicNode}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(FSLogicNode, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
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
  {CreateUserAns, #veil_document{uuid = UserID1}} = rpc:call(FSLogicNode, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),

  {ReadFileAns2, PemBin2} = file:read_file(Cert2),
  ?assertEqual(ok, ReadFileAns2),
  {ExtractAns2, RDNSequence2} = rpc:call(FSLogicNode, user_logic, extract_dn_from_cert, [PemBin2]),
  ?assertEqual(rdnSequence, ExtractAns2),
  {ConvertAns2, DN2} = rpc:call(FSLogicNode, user_logic, rdn_sequence_to_dn_string, [RDNSequence2]),
  ?assertEqual(ok, ConvertAns2),
  DnList2 = [DN2],

  Login2 = "user2",
  Name2 = "user2 user2",
  Teams2 = "user2 team",
  Email2 = "user2@email.net",
  {CreateUserAns2, #veil_document{uuid = UserID2}} = rpc:call(FSLogicNode, user_logic, create_user, [Login2, Name2, Teams2, Email2, DnList2]),
  ?assertEqual(ok, CreateUserAns2),

  %% Current time
  Time = fslogic_utils:time(),
  timer:sleep(1100),

  %% Users have different (and next to each other) IDs
  UID1 = list_to_integer(UserID1),
  UID2 = list_to_integer(UserID2),  
  ?assertEqual(UID2, UID1 + 1),

  {Status, Helper, Id, _Validity, AnswerOpt} = create_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status),
  ?assertEqual(?VOK, AnswerOpt),

  {Status2, Helper2, Id2, _Validity2, AnswerOpt2} = get_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status2),
  ?assertEqual(?VOK, AnswerOpt2),
  ?assertEqual(Helper, Helper2),
  ?assertEqual(Id, Id2),

  {Status3, _Helper3, _Id3, _Validity3, AnswerOpt3} = get_file_location(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status3),
  ?assertEqual(?VENOENT, AnswerOpt3),

  {Status4, Helper4, Id4, _Validity4, AnswerOpt4} = create_file(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status4),
  ?assertEqual(?VOK, AnswerOpt4),

  {Status5, Helper5, Id5, _Validity5, AnswerOpt5} = get_file_location(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status5),
  ?assertEqual(?VOK, AnswerOpt5),
  ?assertEqual(Helper4, Helper5),
  ?assertEqual(Id4, Id5),

  %% Check if owners are set correctly

  {Status21, Attr1} = get_file_attr(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status21),
  {Status22, Attr2} = get_file_attr(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status22),

  %% Check logins
  ?assertEqual(Login, Attr1#fileattr.uname),
  ?assertEqual(Login2, Attr2#fileattr.uname),

  %% Check UIDs
  ?assertEqual(UID1, Attr1#fileattr.uid),
  ?assertEqual(UID2, Attr2#fileattr.uid),

  timer:sleep(1100), 
  
  %% chown test
  {Status23, Answer23} = chown(Host, Cert, Port, TestFile, 77777, "unknown"),
  ?assertEqual("ok", Status23),
  ?assertEqual(list_to_atom(?VEINVAL), Answer23),

  {Status24, Answer24} = chown(Host, Cert, Port, TestFile, 0, Login2),
  ?assertEqual("ok", Status24),
  ?assertEqual(list_to_atom(?VOK), Answer24),

  {Status25, Answer25} = chown(Host, Cert2, Port, TestFile, UID1, "unknown"),
  ?assertEqual("ok", Status25),
  ?assertEqual(list_to_atom(?VOK), Answer25),

  %% Check if owners are set properly
  {Status26, Attr3} = get_file_attr(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status26),
  {Status27, Attr4} = get_file_attr(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status27),

  %% Check logins
  ?assertEqual(Login2, Attr3#fileattr.uname),
  ?assertEqual(Login, Attr4#fileattr.uname),

  %% Check UIDs
  ?assertEqual(UID2, Attr3#fileattr.uid),
  ?assertEqual(UID1, Attr4#fileattr.uid),

  %% Check if change time was updated and if times was setup correctly on file creation
  ?assert(Attr1#fileattr.atime > Time),
  ?assert(Attr1#fileattr.mtime > Time),
  ?assert(Attr1#fileattr.ctime > Time),
  ?assert(Attr2#fileattr.atime > Time),
  ?assert(Attr2#fileattr.mtime > Time),
  ?assert(Attr2#fileattr.ctime > Time),

  ?assert(Attr3#fileattr.ctime > Attr1#fileattr.ctime),
  ?assert(Attr4#fileattr.ctime > Attr2#fileattr.ctime),

  %% Check attrs in files_manager
  {FMStatys, FM_Attrs} = rpc:call(FSLogicNode, files_manager, getfileattr, [Login2 ++ "/" ++ TestFile]),
  ?assertEqual(ok, FMStatys),
  ?assertEqual(Attr4#fileattr.mode, FM_Attrs#fileattributes.mode),
  ?assertEqual(Attr4#fileattr.uid, FM_Attrs#fileattributes.uid),
  ?assertEqual(Attr4#fileattr.gid, FM_Attrs#fileattributes.gid),
  ?assertEqual(Attr4#fileattr.type, FM_Attrs#fileattributes.type),
  ?assertEqual(Attr4#fileattr.size, FM_Attrs#fileattributes.size),
  ?assertEqual(Attr4#fileattr.uname, FM_Attrs#fileattributes.uname),
  ?assertEqual(Attr4#fileattr.gname, FM_Attrs#fileattributes.gname),
  ?assertEqual(Attr4#fileattr.ctime, FM_Attrs#fileattributes.ctime),
  ?assertEqual(Attr4#fileattr.mtime, FM_Attrs#fileattributes.mtime),
  ?assertEqual(Attr4#fileattr.atime, FM_Attrs#fileattributes.atime),

  {Status6, Answer6} = delete_file(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status6),
  ?assertEqual(list_to_atom(?VOK), Answer6),

  {Status7, _Helper7, _Id7, _Validity7, AnswerOpt7} = get_file_location(Host, Cert, Port, TestFile),
  ?assertEqual("ok", Status7),
  ?assertEqual(?VENOENT, AnswerOpt7),

  {Status8, Helper8, Id8, _Validity8, AnswerOpt8} = get_file_location(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status8),
  ?assertEqual(?VOK, AnswerOpt8),
  ?assertEqual(Helper4, Helper8),
  ?assertEqual(Id4, Id8),

  {Status9, Answer9} = delete_file(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status9),
  ?assertEqual(list_to_atom(?VOK), Answer9),

  {Status10, _Helper10, _Id10, _Validity10, AnswerOpt10} = get_file_location(Host, Cert2, Port, TestFile),
  ?assertEqual("ok", Status10),
  ?assertEqual(?VENOENT, AnswerOpt10),

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

  {Status19_2, Answer19_2} = delete_file(Host, Cert, Port, "link_name"),
  ?assertEqual("ok", Status19_2),
  ?assertEqual(list_to_atom(?VOK), Answer19_2),

  % Try to fetch invalid link data
  {Status20, Answer20, _} = get_link(Host, Cert, Port, "link_name1"),
  ?assertEqual("ok", Status20),
  ?assertEqual(?VENOENT, Answer20),

  RemoveStorageAns = rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns),

  RemoveUserAns = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN}]),
  ?assertEqual(ok, RemoveUserAns),
  RemoveUserAns2 = rpc:call(FSLogicNode, user_logic, remove_user, [{dn, DN2}]),
  ?assertEqual(ok, RemoveUserAns2).

%% Checks files manager (manipulation on tmp files copies)
files_manager_tmp_files_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  SHInfo = #storage_helper_info{name = ?SH, init_args = ?TEST_ROOT},
  File = "files_manager_test_file1",
  NotExistingFile = "files_manager_test_not_existing_file",

  AnsCreate = rpc:call(Node1, files_manager, create_file_storage_system, [SHInfo, File]),
  ?assertEqual(ok, AnsCreate),

  AnsCreate2 = rpc:call(Node1, files_manager, create_file_storage_system, [SHInfo, File]),
  ?assertEqual({error,file_exists}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, files_manager, write_storage_system, [SHInfo, File, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),

  {StatusRead1, AnsRead1} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, files_manager, write_storage_system, [SHInfo, File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),

  {StatusRead3, AnsRead3} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, files_manager, write_storage_system, [SHInfo, File, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),

  {StatusRead4, AnsRead4} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, files_manager, truncate_storage_system, [SHInfo, File, 5]),
  ?assertEqual(ok, AnsTruncate),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, files_manager, read_storage_system, [SHInfo, NotExistingFile, 0, 100]),
  ?assertEqual(wrong_getatt_return_code, StatusRead6),
  ?assert(is_integer(AnsRead6)),

  AnsDel = rpc:call(Node1, files_manager, delete_file_storage_system, [SHInfo, File]),
  ?assertEqual(ok, AnsDel),

  {StatusDel2, AnsDel2}  = rpc:call(Node1, files_manager, delete_file_storage_system, [SHInfo, File]),
  ?assertEqual(wrong_getatt_return_code, StatusDel2),
  ?assert(is_integer(AnsDel2)).

%% Checks files manager (manipulation on users' files)
files_manager_standard_files_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node1}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {InsertStorageAns, StorageUUID} = rpc:call(Node1, fslogic_storage, insert_storage, ["DirectIO", ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  DirName = "fslogic_test_dir2",
  FileInDir = "files_manager_test_file2",
  FileInDir2 = "files_manager_test_file3",
  FileInDir2NewName = "files_manager_test_file3_new_name",
  File = DirName ++ "/" ++ FileInDir,
  File2 = DirName ++ "/" ++ FileInDir2,
  File2NewName = DirName ++ "/" ++ FileInDir2NewName,

  NotExistingFile = "files_manager_test_not_existing_file",

  MkDirAns = rpc:call(Node1, files_manager, mkdir, [DirName]),
  ?assertEqual(ok, MkDirAns),

  MkDirAns2 = rpc:call(Node1, files_manager, mkdir, [DirName]),
  ?assertEqual({logical_file_system_error, ?VEEXIST}, MkDirAns2),

  AnsCreate = rpc:call(Node1, files_manager, create, [File]),
  ?assertEqual(ok, AnsCreate),

  AnsCreate2 = rpc:call(Node1, files_manager, create, [File]),
  ?assertEqual({logical_file_system_error, ?VEEXIST}, AnsCreate2),

  AnsWrite1 = rpc:call(Node1, files_manager, write, [File, list_to_binary("abcdefgh")]),
  ?assertEqual(8, AnsWrite1),

  {StatusRead1, AnsRead1} = rpc:call(Node1, files_manager, read, [File, 2, 2]),
  ?assertEqual(ok, StatusRead1),
  ?assertEqual("cd", binary_to_list(AnsRead1)),

  {StatusRead2, AnsRead2} = rpc:call(Node1, files_manager, read, [File, 7, 2]),
  ?assertEqual(ok, StatusRead2),
  ?assertEqual("h", binary_to_list(AnsRead2)),

  AnsWrite2 = rpc:call(Node1, files_manager, write, [File, 3, list_to_binary("123")]),
  ?assertEqual(3, AnsWrite2),

  {StatusRead3, AnsRead3} = rpc:call(Node1, files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead3),
  ?assertEqual("c123g", binary_to_list(AnsRead3)),

  AnsWrite3 = rpc:call(Node1, files_manager, write, [File, list_to_binary("XYZ")]),
  ?assertEqual(3, AnsWrite3),

  {StatusRead4, AnsRead4} = rpc:call(Node1, files_manager, read, [File, 2, 5]),
  ?assertEqual(ok, StatusRead4),
  ?assertEqual("c123g", binary_to_list(AnsRead4)),

  {StatusRead5, AnsRead5} = rpc:call(Node1, files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5),
  ?assertEqual("abc123ghXYZ", binary_to_list(AnsRead5)),

  AnsTruncate = rpc:call(Node1, files_manager, truncate, [File, 5]),
  ?assertEqual(ok, AnsTruncate),

  {StatusRead5_1, AnsRead5_1} = rpc:call(Node1, files_manager, read, [File, 0, 100]),
  ?assertEqual(ok, StatusRead5_1),
  ?assertEqual("abc12", binary_to_list(AnsRead5_1)),

  {StatusRead6, AnsRead6} = rpc:call(Node1, files_manager, read, [NotExistingFile, 0, 100]),
  ?assertEqual(logical_file_system_error, StatusRead6),
  ?assertEqual(?VENOENT, AnsRead6),

  AnsCreate3 = rpc:call(Node1, files_manager, create, [File2]),
  ?assertEqual(ok, AnsCreate3),

  {StatusLs, AnsLs} = rpc:call(Node1, files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs),
  ?assertEqual(2, length(AnsLs)),
  ?assert(lists:member(FileInDir, AnsLs)),
  ?assert(lists:member(FileInDir2, AnsLs)),

  AnsMv = rpc:call(Node1, files_manager, mv, [File2, File2NewName]),
  ?assertEqual(ok, AnsMv),

  AnsChPerm = rpc:call(Node1, files_manager, change_file_perm, [File, 8#777]),
  ?assertEqual(ok, AnsChPerm),

  {StatusLs2, AnsLs2} = rpc:call(Node1, files_manager, ls, [DirName, 100, 0]),
  ?assertEqual(ok, StatusLs2),
  ?assertEqual(2, length(AnsLs2)),
  ?assert(lists:member(FileInDir, AnsLs2)),
  ?assert(lists:member(FileInDir2NewName, AnsLs2)),

  AnsDel = rpc:call(Node1, files_manager, delete, [File]),
  ?assertEqual(ok, AnsDel),

  AnsDel2 = rpc:call(Node1, files_manager, delete, [File2NewName]),
  ?assertEqual(ok, AnsDel2),

  AnsDel3 = rpc:call(Node1, files_manager, delete, [File2NewName]),
  ?assertEqual({logical_file_system_error, ?VENOENT}, AnsDel3),

  AnsDirDelete = rpc:call(Node1, files_manager, rmdir, [DirName]),
  ?assertEqual(ok, AnsDirDelete),

  AnsDirDelete2 = rpc:call(Node1, files_manager, rmdir, [DirName]),
  ?assertEqual({logical_file_system_error, ?VEREMOTEIO}, AnsDirDelete2),

  RemoveStorageAns = rpc:call(Node1, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
  ?assertEqual(ok, RemoveStorageAns).

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
  FslogicMessage = #changefileperms{file_logic_name = FileName, perms = Perms},
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

get_file_attr(Host, Cert, Port, FileName) ->
  FslogicMessage = #getfileattr{file_logic_name = FileName},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfileattr(FslogicMessage)),

  FuseMessage = #fusemessage{id = "1", message_type = "getfileattr", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "fileattr",
  answer_decoder_name = "fuse_messages", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {ConAns, Socket} = ssl:connect(Host, Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}, {cacertfile, Cert}]),
  ?assertEqual(ok, ConAns),
  ssl:send(Socket, MessageBytes),
  {SendAns, Ans} = ssl:recv(Socket, 0, 5000),
  ?assertEqual(ok, SendAns),

  #answer{answer_status = Status, worker_answer = Bytes} = communication_protocol_pb:decode_answer(Ans),
  Resp = fuse_messages_pb:decode_fileattr(Bytes),
  Resp1 = records_translator:translate(Resp, "fuse_messages"),
  {Status, Resp1}.

update_times(Host, Cert, Port, FileName, ATime, MTime) ->
    FslogicMessage = #updatetimes{file_logic_name = FileName, atime = ATime, mtime = MTime},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_updatetimes(FslogicMessage)),

    FuseMessage = #fusemessage{id = "1", message_type = "updatetimes", input = FslogicMessageMessageBytes},
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

chown(Host, Cert, Port, FileName, UID, UName) ->
    FslogicMessage = #changefileowner{file_logic_name = FileName, uid = UID, uname = UName},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefileowner(FslogicMessage)),

    FuseMessage = #fusemessage{id = "1", message_type = "changefileowner", input = FslogicMessageMessageBytes},
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

chgrp(Host, Cert, Port, FileName, GID, GName) ->
    FslogicMessage = #changefilegroup{file_logic_name = FileName, gid = GID, gname = GName},
    FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_changefilegroup(FslogicMessage)),

    FuseMessage = #fusemessage{id = "1", message_type = "changefilegroup", input = FslogicMessageMessageBytes},
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


clear_old_descriptors(Node) ->
  {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
  Time = 1000000*Megaseconds + Seconds + 60*15 + 1,
  gen_server:call({?Dispatcher_Name, Node}, {fslogic, 1, {delete_old_descriptors_test, Time}}),
  timer:sleep(500).
