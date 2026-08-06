%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% This file contains tests of lfm API on posix storage.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_posix_test_SUITE).
-author("Michal Cwiertnia").

-include("lfm_files_test_base.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_acl_test/1,
    create_share_dir_test/1,
    create_share_file_test/1,
    remove_share_test/1,
    share_getattr_test/1,
    share_get_parent_test/1,
    share_list_test/1,
    share_read_test/1,
    share_child_getattr_test/1,
    share_child_list_test/1,
    share_child_read_test/1,
    share_permission_denied_test/1,
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
    rename_removed_opened_file_test/1,
    mkdir_removed_opened_file_test/1,
    rename_removed_opened_file_races_test/1,
    rename_removed_opened_file_races_test2/1,
    lfm_create_and_read_symlink/1,
    lfm_create_hardlink_to_symlink/1
]).


-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_acl_test,
    create_share_dir_test,
    create_share_file_test,
    remove_share_test,
    share_getattr_test,
    share_get_parent_test,
    share_list_test,
    share_read_test,
    share_child_getattr_test,
    share_child_list_test,
    share_child_read_test,
    share_permission_denied_test,
    new_file_should_not_have_popularity_doc,
    new_file_should_have_zero_popularity,
    opening_file_should_increase_file_popularity,
    file_popularity_should_have_correct_file_size,
    rename_removed_opened_file_test,
    mkdir_removed_opened_file_test,
    rename_removed_opened_file_races_test,
    rename_removed_opened_file_races_test2,
    lfm_create_and_read_symlink,
    lfm_create_hardlink_to_symlink
]).


all() ->
    ?ALL(?TEST_CASES).


%%%====================================================================
%%% Test function
%%%====================================================================


fslogic_new_file_test(Config) ->
    lfm_files_test_base:fslogic_new_file(Config).


lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).


create_share_dir_test(Config) ->
    lfm_files_test_base:create_share_dir(Config).


create_share_file_test(Config) ->
    lfm_files_test_base:create_share_file(Config).


remove_share_test(Config) ->
    lfm_files_test_base:remove_share(Config).


share_getattr_test(Config) ->
    lfm_files_test_base:share_getattr(Config).


share_get_parent_test(Config) ->
    lfm_files_test_base:share_get_parent(Config).


share_list_test(Config) ->
    lfm_files_test_base:share_list(Config).


share_read_test(Config) ->
    lfm_files_test_base:share_read(Config).


share_child_getattr_test(Config) ->
    lfm_files_test_base:share_child_getattr(Config).


share_child_list_test(Config) ->
    lfm_files_test_base:share_child_list(Config).


share_child_read_test(Config) ->
    lfm_files_test_base:share_child_read(Config).


share_permission_denied_test(Config) ->
    lfm_files_test_base:share_permission_denied(Config).


new_file_should_not_have_popularity_doc(Config) ->
    lfm_files_test_base:new_file_should_not_have_popularity_doc(Config).


new_file_should_have_zero_popularity(Config) ->
    lfm_files_test_base:new_file_should_have_zero_popularity(Config).


opening_file_should_increase_file_popularity(Config) ->
    lfm_files_test_base:opening_file_should_increase_file_popularity(Config).


file_popularity_should_have_correct_file_size(Config) ->
    lfm_files_test_base:file_popularity_should_have_correct_file_size(Config).


rename_removed_opened_file_test(Config) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    User = <<"user1">>,
    User2 = <<"user2">>,

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),
    Guid1String = binary_to_list(Guid1),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([Guid1String], ListAns3 -- InitialDeletedDir),
    RenamedStorageId = filename:join([?DELETED_OPENED_FILES_DIR, Guid1]),
    ?assertMatch({ok, #file_location{file_id = RenamedStorageId}},
        lfm_proxy:get_file_location(Worker, SessId(User), ?FILE_REF(Guid1))),
    ?assertMatch({error, ?EACCES}, lfm_proxy:get_file_location(Worker, SessId(User2), ?FILE_REF(Guid1))),

    lfm_proxy:close_all(Worker),
    {ok, ListAns4} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns4 -- InitialDeletedDir),

    ok.


mkdir_removed_opened_file_test(Config) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileName2 = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    FilePath2 = <<FilePath/binary, "/", FileName2/binary>>,
    User = <<"user1">>,

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),
    Guid1String = binary_to_list(Guid1),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([Guid1String], ListAns3 -- InitialDeletedDir),

    {ok, _} = lfm_proxy:mkdir(Worker, SessId(User), FilePath),
    {ok, _} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath2),
    {ok, ListAns4} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns4 -- InitialSpaceFiles),

    lfm_proxy:close_all(Worker),
    {ok, ListAns5} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns5 -- InitialDeletedDir),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath2})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    {ok, ListAns6} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns6 -- InitialSpaceFiles),
    ok.


rename_removed_opened_file_races_test(Config) ->
    rename_removed_opened_file_races_test_base(Config, before_mv).


rename_removed_opened_file_races_test2(Config) ->
    rename_removed_opened_file_races_test_base(Config, after_mv).


rename_removed_opened_file_races_test_base(Config, MockOpts) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    User = <<"user1">>,
    Master = self(),

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    case MockOpts of
        before_mv ->
            test_utils:mock_expect(Workers, storage_driver, mv,
                fun(Handle, TargetFileId) ->
                    case get(mv_test) of
                        undefined ->
                            put(mv_test, ok),
                            Master ! {mv_beg, self()},
                            receive
                                mv_start -> ok
                            end;
                        _ ->
                            ok
                    end,
                    meck:passthrough([Handle, TargetFileId])
                end);
        after_mv ->
            test_utils:mock_expect(Workers, storage_driver, mv,
                fun(Handle, TargetFileId) ->
                    case meck:passthrough([Handle, TargetFileId]) of
                        ok ->
                            Master ! {mv_beg, self()},
                            receive
                                mv_start -> ok
                            end;
                        Other3 ->
                            Other3
                    end
                end)
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    spawn(fun() ->
        Ans = lfm_proxy:unlink(Worker, SessId(User), {path, FilePath}),
        Master ! {unlink_ans, Ans}
    end),

    ?assertEqual(ok, receive
        {mv_beg, MvPid} ->
            lfm_proxy:close_all(Worker),
            MvPid ! mv_start,
            ok
    after
        5000 -> timeout
    end),

    ?assertEqual(ok, receive
        {unlink_ans, UnlinkAns} -> UnlinkAns
    after
        5000 -> timeout
    end),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns3 -- InitialDeletedDir),

    ok.


lfm_create_and_read_symlink(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    % Prepare test dir and link data
    TestDir = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, TestDir)),
    Path = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    LinkTarget = <<"test_link">>,
    LinkSize = byte_size(LinkTarget),

    % Create symlink and check its times
    {ok, LinkAttrs} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE, size = LinkSize, is_fully_replicated = undefined, parent_guid = DirGuid}},
        lfm_proxy:make_symlink(W, SessId, Path, LinkTarget)),
    ?assert(LinkAttrs#file_attr.atime > 0),
    ?assert(LinkAttrs#file_attr.mtime > 0),
    ?assert(LinkAttrs#file_attr.ctime > 0),
    ?assert(fslogic_file_id:is_symlink_uuid(file_id:guid_to_uuid(LinkAttrs#file_attr.guid))),

    % Read link and check it
    time_test_utils:simulate_seconds_passing(2), % ensure time change
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, Path})),
    {ok, LinkAttrs2} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE, size = LinkSize, is_fully_replicated = undefined, parent_guid = DirGuid}},
        lfm_proxy:stat(W, SessId, {path, Path})),
    ?assert(LinkAttrs2#file_attr.atime > LinkAttrs#file_attr.atime),
    ?assertMatch({ok, [LinkAttrs2], _}, lfm_proxy:get_children_attrs(W, SessId, ?FILE_REF(DirGuid), #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),

    % Unlink and check if symlink is deleted
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, Path})),
    ?assertEqual({error, enoent}, lfm_proxy:read_symlink(W, SessId, {path, Path})),
    ?assertMatch({ok, [], _}, lfm_proxy:get_children_attrs(W, SessId, ?FILE_REF(DirGuid), #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),

    % Delete test dir
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId, ?FILE_REF(DirGuid))),
    ok.


lfm_create_hardlink_to_symlink(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    % Prepare test dir and link data
    TestDir = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, TestDir)),
    SymlinkPath = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    HardlinkPath = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    LinkTarget = <<"test_link">>,

    % Create symlink and hardlink to this symlink
    {ok, #file_attr{guid = SymlinkGuid}} = ?assertMatch({ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_symlink(W, SessId, SymlinkPath, LinkTarget)),
    {ok, #file_attr{guid = HardlinkGuid}} = ?assertMatch({ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_link(W, SessId, HardlinkPath, SymlinkGuid)),

    % Verify links
    ?assertNotEqual(SymlinkGuid, HardlinkGuid),
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, SymlinkPath})),
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, HardlinkPath})),

    % Clean
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, SymlinkPath})),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, HardlinkPath})),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId, ?FILE_REF(DirGuid))),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    lfm_files_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) when
    Case =:= rename_removed_opened_file_races_test;
    Case =:= rename_removed_opened_file_races_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, storage_driver, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(lfm_create_and_read_symlink = Case, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) when
    Case =:= rename_removed_opened_file_races_test;
    Case =:= rename_removed_opened_file_races_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(lfm_create_and_read_symlink = Case, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).