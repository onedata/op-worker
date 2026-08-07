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
    rename_removed_opened_file_test/1,
    mkdir_removed_opened_file_test/1,
    rename_removed_opened_file_races_test/1,
    rename_removed_opened_file_races_test2/1
]).


-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_acl_test,
    rename_removed_opened_file_test,
    mkdir_removed_opened_file_test,
    rename_removed_opened_file_races_test,
    rename_removed_opened_file_races_test2
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

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) when
    Case =:= rename_removed_opened_file_races_test;
    Case =:= rename_removed_opened_file_races_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).