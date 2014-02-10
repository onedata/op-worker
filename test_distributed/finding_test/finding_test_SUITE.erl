%% ===================================================================
%% @author Michal Sitko
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao.
%% It contains tests that base on ct.
%% @end
%% ===================================================================
-module(finding_test_SUITE).

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("files_common.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_vfs.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("logging.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

%% API
-export([dao_vfs_find_test/1]).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

all() -> [dao_vfs_find_test].

-define(RegularFile, 0).
-define(SH, "DirectIO").
-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem
-define(ProtocolVersion, 1).
-define(USER1, "dao_vfs_find_user1").
-define(USER2, "dao_vfs_find_user2").

-record(path_with_times, {path = [], times = {0, 0, 0}}).


%% @doc This is distributed test of dao_vfs:find_files.
%% It consists of series of dao_vfs:find_files with various file_criteria and comparing result to expected values.
%% Before testing it clears documents that may affect test and after testing it clears created documents.
%% @end
dao_vfs_find_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node1 | _] = NodesUp,
  start_cluster(Node1),

  % Delete all files - tests assumes that in db there is nothing else than prepared files
  clean_files(Node1),

  % preparing DB for tests
  Uid1 = "90",
  Uid2 = "92",

  FilesTree = get_test_files_tree(),
  Files1 = create_files_tree_for_user(Node1, FilesTree, Uid1, ?USER1),
  Files2 = create_files_tree_for_user(Node1, FilesTree, Uid2, ?USER2),
  FilesToUuidsPropList = Files1 ++ Files2,

  % verify if all files have been save properly
  verify_files_tree(Node1, FilesToUuidsPropList),

  % Find by exact filename
  ExpectedUuids1 = create_expected_uuids_list(?USER1, ["new/ab.txt", "old/ab.txt", "very_old/ab.txt"], FilesToUuidsPropList),
  ExpectedUuids2 = create_expected_uuids_list(?USER2, ["new/ab.txt", "old/ab.txt", "very_old/ab.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "ab.txt"}, ExpectedUuids1 ++ ExpectedUuids2),

  % Find by filename prefix
  ExpectedUuids3 = create_expected_uuids_list(?USER1, ["new/abba.txt", "old/abba.txt", "very_old/abba.txt"], FilesToUuidsPropList),
  ExpectedUuids4 = create_expected_uuids_list(?USER2, ["new/abba.txt", "old/abba.txt", "very_old/abba.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "ab*"}, ExpectedUuids1 ++ ExpectedUuids2 ++ ExpectedUuids3 ++ ExpectedUuids4),

  % Find by exact filename and user id
  test_find_usage(Node1, #file_criteria{file_pattern = "ab.txt", uid = Uid1}, ExpectedUuids1),

  % Find by filename prefix and user id
  test_find_usage(Node1, #file_criteria{file_pattern = "ab*", uid = Uid2}, ExpectedUuids2 ++ ExpectedUuids4),

  % Find by user id
  ExpectedUuids5 = create_expected_uuids_list(?USER1, ["very_old/acc.txt", "very_old/b.txt", "old/acc.txt", "old/b.txt", "new/acc.txt", "new/b.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{uid = Uid1}, ExpectedUuids1 ++ ExpectedUuids3 ++ ExpectedUuids5),

  % Find files with mtime older then specified time
  ExpectedOlderThan300_1 = create_expected_uuids_list(?USER1, ["very_old/ab.txt", "very_old/abba.txt", "very_old/acc.txt", "very_old/b.txt"], FilesToUuidsPropList),
  ExpectedOlderThan300_2 = create_expected_uuids_list(?USER2, ["very_old/ab.txt", "very_old/abba.txt", "very_old/acc.txt", "very_old/b.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{mtime = #time_criteria{time_relation = older_than, time = 300}}, ExpectedOlderThan300_1 ++ ExpectedOlderThan300_2),

  % Find files with mtime newer then specified time
  ExpectedNewerThan900_1 = create_expected_uuids_list(?USER1, ["new/ab.txt", "new/abba.txt", "new/acc.txt", "new/b.txt"], FilesToUuidsPropList),
  ExpectedNewerThan900_2 = create_expected_uuids_list(?USER2, ["new/ab.txt", "new/abba.txt", "new/acc.txt", "new/b.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{mtime = #time_criteria{time_relation = newer_than, time = 900}}, ExpectedNewerThan900_1 ++ ExpectedNewerThan900_2),

  % Corner case - newer_than is exclusive
  test_find_usage(Node1, #file_criteria{mtime = #time_criteria{time_relation = newer_than, time = 1000}}, []),

  % Find by exact filename and mtime
  ExpectedUuids6 = create_expected_uuids_list(?USER1, ["new/acc.txt"], FilesToUuidsPropList) ++ create_expected_uuids_list(?USER2, ["new/acc.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "acc.txt", mtime = #time_criteria{time_relation = newer_than, time = 900}}, ExpectedUuids6),

  % Find by user id and mtime
  ExpectedUuids7 = create_expected_uuids_list(?USER1, ["new/ab.txt", "new/abba.txt", "new/acc.txt", "new/b.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{uid = Uid1, mtime = #time_criteria{time_relation = newer_than, time = 900}}, ExpectedUuids7),

  % Find by exact filename, user id and mtime
  ExpectedUuids8 = create_expected_uuids_list(?USER2, ["new/acc.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "acc.txt", uid = Uid2, mtime = #time_criteria{time_relation = newer_than, time = 900}}, ExpectedUuids8),

  % Find by filename prefix, user id and mtime
  ExpectedUuids9 = create_expected_uuids_list(?USER2, ["new/ab.txt", "new/abba.txt"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "ab*", uid = Uid2, mtime = #time_criteria{time_relation = newer_than, time = 900}}, ExpectedUuids9),

  % Test include_dirs and include_files options
  ExpectedUuids10 = create_expected_uuids_list([?USER1 ++ "/new", ?USER2 ++ "/new"], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{file_pattern = "new", include_dirs = true, include_files = false}, ExpectedUuids10),

  ExpectedUuids11 = create_expected_uuids_list([?USER1 ++ "/new", ?USER1 ++ "/old", ?USER1 ++ "/very_old", ?USER1], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{uid = Uid1, include_dirs = true, include_files = false}, ExpectedUuids11),

  HomeUuids = create_expected_uuids_list([?USER1, ?USER2], FilesToUuidsPropList),
  test_find_usage(Node1, #file_criteria{mtime = #time_criteria{time_relation = newer_than, time = 900}, include_dirs = true, include_files = false}, ExpectedUuids10 ++ HomeUuids),

  % Corner case - with empty file_criteria we get all regular files
  {FindAns, FileUuids} = rpc:call(Node1, dao_vfs, find_files, [#file_criteria{}]),
  ?assertEqual(ok, FindAns),
  ?assertEqual(24, length(FileUuids)),

  % Corner case - with #file_criteria{include_dirs = true} we get all files and directories
  {FindAns2, FileUuids2} = rpc:call(Node1, dao_vfs, find_files, [#file_criteria{include_dirs = true}]),
  ?assertEqual(ok, FindAns2),
  ?assertEqual(32, length(FileUuids2)),

  delete_files_tree(Node1, FilesToUuidsPropList),
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

test_find_usage(Node, FileCriteria, ExpectedUuids) ->
  {FindAns, FileUuids} = rpc:call(Node, dao_vfs, find_files, [FileCriteria]),
  ?assertEqual(ok, FindAns),
  ?assert(same_list(ExpectedUuids, FileUuids)).

start_cluster(Node) ->
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  nodes_manager:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  nodes_manager:wait_for_cluster_init().

create_file(Node, #path_with_times{path = FilePath, times = {ATime, MTime, CTime}}, Uid, FileType) ->
  {ParentFound, ParentInfo} = rpc:call(Node, fslogic_utils, get_parent_and_name_from_path , [FilePath, ?ProtocolVersion]),
  ?assertEqual(ok, ParentFound),
  {FileName, Parent} = ParentInfo,
  File = #file{type = FileType, name = FileName, uid = Uid, parent = Parent#veil_document.uuid, perms = 8#600},
  FileDoc = rpc:call(Node, fslogic_utils, update_meta_attr, [File, times, {ATime, MTime, CTime}]),
  {SaveAns, FileUuid} = rpc:call(Node, dao_lib, apply, [dao_vfs, save_file, [FileDoc], ?ProtocolVersion]),
  ?assertEqual(ok, SaveAns),
  FileUuid.

delete_files_tree(Node, FilesToUuidsPropList) ->
  Uuids = lists:map(fun({_FileName, Uuid}) -> Uuid end, FilesToUuidsPropList),
  delete_files_by_uuid(Node, Uuids).

delete_files_by_uuid(Node, ListOfUuids) ->
  lists:foreach(
    fun(FileUuid) ->
      rpc:call(Node, dao_lib, apply, [dao_vfs, remove_file, [{uuid, FileUuid}], ?ProtocolVersion])
    end,
    ListOfUuids
  ).

get_test_files_tree() ->
  Filenames = ["ab.txt", "abba.txt", "acc.txt", "b.txt"],
  Directories =
    [#path_with_times{path = "new", times = {1000, 1000, 1000}},
     #path_with_times{path = "old", times = {600, 600, 600}},
     #path_with_times{path = "very_old", times = {200, 200, 200}}],

  Files = lists:append(lists:map(
    fun(#path_with_times{path = Dir, times = Times}) -> lists:map(
      fun(File) -> #path_with_times{path = filename:join(Dir, File), times = Times}
      end, Filenames)
    end, Directories)),

  {Directories, Files}.

-spec create_files_tree_for_user(
    Node :: node(), {Directories :: list(), Files :: list()}, UserId :: string(), UserName :: string()) ->
    list({FullPath :: string(), FileUuid :: uuid()}).
create_files_tree_for_user(Node, {Directories, Files}, UserId, UserName) ->
  HomePathWithTimes = #path_with_times{path = UserName, times = {1000, 1000, 1000}},
  Home = {UserName, create_file(Node, HomePathWithTimes, UserId, ?DIR_TYPE)},
  FileNamesToUuid1 = lists:map(
    fun(PathWithTimes) ->
      {filename:join(UserName, PathWithTimes#path_with_times.path), create_file(Node, PathWithTimes, UserId, ?DIR_TYPE)}
    end,
    Directories),
  FileNamesToUuid2 = lists:map(
    fun(PathWithTimes) ->
      {filename:join(UserName, PathWithTimes#path_with_times.path), create_file(Node, PathWithTimes, UserId, ?REG_TYPE)}
    end,
    Files),
  FileNamesToUuid1 ++ FileNamesToUuid2 ++ [Home].

verify_files_tree(Node, Files) ->
  lists:map(
    fun ({FilePath, Uuid}) ->
      {GetFileAns, FileDoc} = rpc:call(Node, dao_vfs, get_file, [{uuid, Uuid}]),
      ?assertEqual(ok, GetFileAns),
      FileNameFromDb = FileDoc#veil_document.record#file.name,
      ExpectedFileName = filename:basename(FilePath),
      ?assertEqual(ExpectedFileName, FileNameFromDb)
    end,
    Files).

clean_files(Node) ->
  QueryArgs = #view_query_args{start_key = [null, null], end_key = [null, {}]},
  case rpc:call(Node, dao, list_records, [?FILES_BY_UID_AND_FILENAME, QueryArgs]) of
    {ok, #view_result{rows = Rows}} ->
      Uuids = lists:map(fun(#view_row{id = Id}) -> Id end, Rows),
      delete_files_by_uuid(Node, Uuids);
    Error ->
      ?error("Canno clean_files. Invalid view response: ~p", [Error]),
      throw(invalid_data)
  end.

create_expected_uuids_list(UserName, ExpectedFilePaths, FilePathsToUuids) ->
  FullPaths = lists:map(fun(FilePath) -> filename:join(UserName, FilePath) end, ExpectedFilePaths),
  lists:map(fun(FullPath) -> proplists:get_value(FullPath, FilePathsToUuids) end, FullPaths).

create_expected_uuids_list(ExpectedFilePaths, FilePathsToUuids) ->
  lists:map(fun(FullPath) -> proplists:get_value(FullPath, FilePathsToUuids) end, ExpectedFilePaths).

same_list(List1, List2) ->
  (length(List1) == length(List2)) and (lists:subtract(List1, List2) == []).
