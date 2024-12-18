%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests that verify changes and possible races
%%% during file lifecycle (create/delete/rename/sync).
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    open_race_test/1, make_open_race_test/1, make_open_race_test2/1, create_open_race_test/1,
    create_open_race_test2/1, create_delete_race_test/1,
    rename_to_opened_file_test/1, create_file_existing_on_disk_test/1, open_delete_race_test/1,
    open_delete_race_test2/1
]).

-define(TEST_CASES, [
    open_race_test, make_open_race_test, make_open_race_test2, create_open_race_test,
    create_open_race_test2, create_delete_race_test,
    %%    rename_to_opened_file_test, % TODO VFS-5290
    %%    create_file_existing_on_disk_test, % TODO VFS-5271
    open_delete_race_test, open_delete_race_test2
]).

-define(PERFORMANCE_TEST_CASES, []).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%====================================================================
%%% Test function
%%%====================================================================

open_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    check_dir_init(W),

    test_utils:mock_new(W, sd_utils, [passthrough]),
    test_utils:mock_expect(W, sd_utils, generic_create_deferred,
        fun(UserCtx, FileCtx, IgnoreEexist) ->
            Ans = meck:passthrough([UserCtx, FileCtx, IgnoreEexist]),
            timer:sleep(2000),
            Ans
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath)),

    spawn(fun() ->
        Master ! {open_ans, lfm_proxy:open(W, SessId1, {path, FilePath}, read)}
    end),
    spawn(fun() ->
        Master ! {open_ans, lfm_proxy:open(W, SessId1, {path, FilePath}, read)}
    end),

    OpenAns1 = receive
        {open_ans, A} -> A
    after
        5000 -> timeout
    end,
    OpenAns2 = receive
        {open_ans, A2} -> A2
    after
        5000 -> timeout
    end,

    {ok, Open1} = ?assertMatch({ok, _}, OpenAns1),
    {ok, Open2} = ?assertMatch({ok, _}, OpenAns2),
    ?assertEqual(get_file_id(W, Open1), get_file_id(W, Open2)),

    check_dir(W, 1),
    ok.

make_open_race_test(Config) ->
    make_open_race_test(Config, file_req).

make_open_race_test2(Config) ->
    make_open_race_test(Config, file_meta).

make_open_race_test(Config, Mock) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    check_dir_init(W),

    case Mock of
        file_req ->
            test_utils:mock_new(W, file_req, [passthrough]),
            test_utils:mock_expect(W, file_req, create_file_doc,
                fun(UserCtx, ParentFileCtx, Name, Mode) ->
                    Ans = meck:passthrough([UserCtx, ParentFileCtx, Name, Mode]),

                    Master ! {open_file, self()},
                    ok = receive
                        file_opened -> ok
                    after
                        5000 -> timeout
                    end,
                    Ans
                end);
        file_meta ->
            test_utils:mock_new(W, file_meta, [passthrough]),
            test_utils:mock_expect(W, file_meta, save,
                fun(FileDoc) ->
                    Master ! {open_file, self()},
                    ok = receive
                        file_opened -> ok
                    after
                        5000 -> timeout
                    end,

                    meck:passthrough([FileDoc])
                end)
    end,

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create(W, SessId1, FilePath)}
    end),

    MockProc = receive
        {open_file, Proc} -> Proc
    after
        5000 -> timeout
    end,
    ?assert(is_pid(MockProc)),
    OpenAns = lfm_proxy:open(W, SessId1, {path, FilePath}, read),
    MockProc ! file_opened,

    CreateAns = receive
        {create_ans, A} -> A
    after
        5000 -> timeout
    end,
    ?assertMatch({ok, _}, CreateAns),

    case Mock of
        file_req ->
            {ok, Open1} = ?assertMatch({ok, _}, OpenAns),
            {ok, Open2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, read)),
            ?assertEqual(get_file_id(W, Open1), get_file_id(W, Open2)),
            check_dir(W, 1);
        file_meta ->
            ?assertMatch({error, ?ENOENT}, OpenAns),
            check_dir(W, 0)
    end,

    ok.

create_open_race_test(Config) ->
    create_open_race_test(Config, file_req).

create_open_race_test2(Config) ->
    create_open_race_test(Config, times_api).

create_open_race_test(Config, Mock) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    check_dir_init(W),

    case Mock of
        file_req ->
            test_utils:mock_new(W, file_req, [passthrough]),
            test_utils:mock_expect(W, file_req, create_file_doc,
                fun(UserCtx, ParentFileCtx, Name, Mode) ->
                    Ans = meck:passthrough([UserCtx, ParentFileCtx, Name, Mode]),

                    Master ! {open_file, self()},
                    ok = receive
                        file_opened -> ok
                    after
                        5000 -> timeout
                    end,
                    Ans
                end);
        times_api ->
            test_utils:mock_new(W, times_api, [passthrough]),
            test_utils:mock_expect(W, times_api, report_file_created,
                fun(FileCtx) ->
                    Master ! {open_file, self()},
                    ok = receive
                        file_opened -> ok
                    after
                        5000 -> timeout
                    end,
                    meck:passthrough([FileCtx])
                end)
    end,

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create_and_open(W, SessId1, FilePath)}
    end),

    MockProc = receive
        {open_file, Proc} -> Proc
    after
        5000 -> timeout
    end,
    ?assert(is_pid(MockProc)),
    {ok, Open1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),
    MockProc ! file_opened,

    CreateAns = receive
        {create_ans, A} -> A
    after
        5000 -> timeout
    end,
    ?assertMatch({ok, _}, CreateAns),
    {ok, {_, CreateHandle}} = CreateAns,

    {ok, Open2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),

    ?assertEqual(get_file_id(W, Open1), get_file_id(W, Open2)),
    ?assertEqual(get_file_id(W, Open1), get_file_id(W, CreateHandle)),

    check_dir(W, 1),
    ok.

create_delete_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    % Init storage dir
    lfm_proxy:create_and_open(W, SessId1, <<"/space_name1/", (generator:gen_name())/binary>>),
    check_dir_init(W),

    test_utils:mock_new(W, fslogic_event_emitter, [passthrough]),
    test_utils:mock_expect(W, fslogic_event_emitter, emit_file_attr_changed, fun(_, _, _) -> {error, not_found} end),

    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({error, ecanceled}, lfm_proxy:create_and_open(W, SessId1, FilePath)),

    check_dir(W, 0),
    % TODO VFS-5274 - vewrify if all documents (e.g., file_location) are cleared
    ok.

rename_to_opened_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    MountPoint = get_mount_point(W),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
        {ok, Dirs1} -> length(Dirs1);
        _ -> 0
    end,

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, FilePath)),
    {ok, {_, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, FilePath2)),
    ?assertMatch(ok, lfm_proxy:close(W, Handle)),

    ?assertMatch({ok, _}, lfm_proxy:mv(W, SessId1, {path, FilePath2}, FilePath)),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(2, length(Dirs2) - Dirs1Length),
    ok.

create_file_existing_on_disk_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileName = generator:gen_name(),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath0 = <<"/space_name1/", (generator:gen_name())/binary>>,
    lfm_proxy:create_and_open(W, SessId1, FilePath0), % To create storage dirs

    MountPoint = get_mount_point(W),
    StoragePath = filename:join([MountPoint, "space_id1", binary_to_list(FileName)]),

    {ok, FD} = ?assertMatch({ok, _}, rpc:call(W, file, open, [StoragePath, [write]])),
    rpc:call(W, file, close, [FD]),

    FilePath = <<"/space_name1/", FileName/binary>>,
    ?assertMatch({error, eexist}, lfm_proxy:create_and_open(W, SessId1, FilePath)),
    ok.

open_delete_race_test(Config) ->
    open_delete_race_test_base(Config, true).

open_delete_race_test2(Config) ->
    open_delete_race_test_base(Config, false).

open_delete_race_test_base(Config, MockDeletionLink) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    case MockDeletionLink of
        true ->
            check_dir_init(W),
            test_utils:mock_new(W, fslogic_delete, [passthrough]),
            test_utils:mock_expect(W, fslogic_delete, get_open_file_handling_method,
                fun(Ctx) -> {?SET_DELETION_MARKER, Ctx} end);
        _ ->
            check_dir_init(W, [?DELETED_OPENED_FILES_DIR_STRING])
    end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, open_on_storage,
        fun(UserCtx, FileCtx, SessId, Flag, HandleId) ->
            Ans = meck:passthrough([UserCtx, FileCtx, SessId, Flag, HandleId]),

            Master ! {delete_file, self()},
            ok = receive
                file_deleted -> ok
            after
                5000 -> timeout
            end,

            Ans
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath)),
    spawn(fun() ->
        Master ! {open_ans, lfm_proxy:open(W, SessId1, {path, FilePath}, read)}
    end),

    MakeProc = receive
        {delete_file, Proc} -> Proc
    after
        5000 -> timeout
    end,
    ?assert(is_pid(MakeProc)),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, FilePath})),
    MakeProc ! file_deleted,

    OpenAns = receive
        {open_ans, A} -> A
    after
        5000 -> timeout
    end,
    ?assertMatch({ok, _}, OpenAns),

    check_dir(W, 1),
    ?assertEqual(ok, lfm_proxy:close_all(W)),
    test_utils:mock_unload(W, [fslogic_delete]),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_unload(Workers,
        [communicator, file_meta, file_req, sd_utils, times_api, fslogic_delete, fslogic_event_emitter]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_dir_init(W) ->
    check_dir_init(W, ["space_id1"]).

check_dir_init(W, Path) ->
    MountPoint = get_mount_point(W),
    StorageSpacePath = filename:join([MountPoint | Path]),

    Size = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
        {ok, Dirs} -> length(Dirs);
        _ -> 0
    end,

    put(test_storage_space_path, StorageSpacePath),
    put(test_dir_size, Size).

check_dir(W, ExpectedSize) ->
    case rpc:call(W, file, list_dir, [get(test_storage_space_path)]) of
        {ok, Dirs} -> ?assertEqual(ExpectedSize, length(Dirs) - get(test_dir_size));
        _ -> ?assertEqual(ExpectedSize, 0 - get(test_dir_size))
    end.

get_file_id(W, OpenAns) ->
    lfm_context:get_file_id(rpc:call(W, ets, lookup_element, [lfm_handles, OpenAns, 2])).

get_mount_point(W) ->
    WorkerStorageId = initializer:get_storage_id(W),
    Helper = rpc:call(W, storage, get_helper, [WorkerStorageId]),
    #{<<"mountPoint">> := MountPoint} = helper:get_args(Helper),
    MountPoint.
