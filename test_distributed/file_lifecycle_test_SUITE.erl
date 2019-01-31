%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
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
    create_open_race_test2/1, create_open_race_test3/1, create_delete_race_test/1,
    rename_to_opened_file_test/1, create_file_existing_on_disk_test/1, open_delete_race_test/1
]).

-define(TEST_CASES, [
    open_race_test, make_open_race_test, make_open_race_test2, create_open_race_test,
    create_open_race_test2, create_open_race_test3, create_delete_race_test,
    rename_to_opened_file_test, create_file_existing_on_disk_test, open_delete_race_test
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

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, sfm_utils, [passthrough]),
    test_utils:mock_expect(W, sfm_utils, create_delayed_storage_file,
        fun(FileCtx) ->
            CreateOnStorageFun = fun(FileCtx2) ->
                FileCtx3 = sfm_utils:create_storage_file(
                    user_ctx:new(?ROOT_SESS_ID), FileCtx2),
                {StorageFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
                timer:sleep(2000),
                {StorageFileId, files_to_chown:chown_or_schedule_chowning(FileCtx4)}
            end,
            file_location_utils:create_file_location(FileCtx, CreateOnStorageFun)
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath, 8#777)),

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
    Context1 = rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
    Context2 = rpc:call(W, ets, lookup_element, [lfm_handles, Open2, 2]),
    ?assertEqual(lfm_context:get_file_id(Context1), lfm_context:get_file_id(Context2)),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, sfm_utils),
    ok.

make_open_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, make_file,
        fun(UserCtx, ParentFileCtx, Name, Mode) ->
            FileCtx = file_req:create_file_doc(UserCtx, ParentFileCtx, Name, Mode),

            Master ! {make_doc, self()},
            ok = receive
                file_opened -> ok
            after
                5000 -> timeout
            end,

            {_, FileCtx2} = file_location_utils:get_new_file_location_doc(FileCtx, false, true),
            fslogic_times:update_mtime_ctime(ParentFileCtx),
            attr_req:get_file_attr_insecure(UserCtx, FileCtx2)
        end),

    test_utils:mock_expect(W, file_req, open_file_with_extended_info,
        fun(_UserCtx, FileCtx, _Flag) ->
            {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
                storage_id = StorageId}}, _FileCtx2} =
                sfm_utils:create_delayed_storage_file(FileCtx),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_opened_extended{handle_id = undefined,
                    provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create(W, SessId1, FilePath, 8#777)}
    end),

    MakeProc = receive
        {make_doc, Proc} -> Proc
    after
        5000 -> timeout
    end,
    ?assert(is_pid(MakeProc)),
    {ok, Open1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, read)),
    MakeProc ! file_opened,

    CreateAns = receive
        {create_ans, A} -> A
    after
        5000 -> timeout
    end,
    ?assertMatch({ok, _}, CreateAns),

    {ok, Open2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, read)),
    ?assertEqual(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
        rpc:call(W, ets, lookup_element, [lfm_handles, Open2, 2])),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, file_req),
    ok.

make_open_race_test2(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_new(W, file_meta, [passthrough]),
    test_utils:mock_expect(W, file_meta, save,
        fun(FileDoc) ->
            Master ! {make_doc, self()},
            ok = receive
                     file_opened -> ok
                 after
                     5000 -> timeout
                 end,

            meck:passthrough([FileDoc])
        end),

    test_utils:mock_expect(W, file_req, open_file_with_extended_info,
        fun(_UserCtx, FileCtx, _Flag) ->
            {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
                storage_id = StorageId}}, _FileCtx2} =
                sfm_utils:create_delayed_storage_file(FileCtx),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_opened_extended{handle_id = undefined,
                    provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create(W, SessId1, FilePath, 8#777)}
          end),

    MakeProc = receive
                   {make_doc, Proc} -> Proc
               after
                   5000 -> timeout
               end,
    ?assert(is_pid(MakeProc)),
    ?assertMatch({error,enoent}, lfm_proxy:open(W, SessId1, {path, FilePath}, read)),
    MakeProc ! file_opened,

    CreateAns = receive
                    {create_ans, A} -> A
                after
                    5000 -> timeout
                end,
    ?assertMatch({ok, _}, CreateAns),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(0, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, [file_meta, file_req]),
    ok.

create_open_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, create_file,
        fun(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
            FileCtx = file_req:create_file_doc(UserCtx, ParentFileCtx, Name, Mode),

            Master ! {make_doc, self()},
            ok = receive
                     file_opened -> ok
                 after
                     5000 -> timeout
                 end,

            FileCtx2 = sfm_utils:create_storage_file(UserCtx, FileCtx),
            {FileLocation, FileCtx3} =
                file_location_utils:get_new_file_location_doc(FileCtx2, true, true),

            #fuse_response{fuse_response = FileAttr} =
                attr_req:get_file_attr_insecure(UserCtx, FileCtx3, false, false),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_created{
                    handle_id = undefined,
                    file_attr = FileAttr#file_attr{size = 0},
                    file_location = FileLocation
                }
            }
        end),

    test_utils:mock_expect(W, file_req, open_file_with_extended_info,
        fun(_UserCtx, FileCtx, _Flag) ->
            {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
                storage_id = StorageId}}, _FileCtx2} =
                sfm_utils:create_delayed_storage_file(FileCtx),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_opened_extended{handle_id = undefined,
                    provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)}
          end),

    MakeProc = receive
                   {make_doc, Proc} -> Proc
               after
                   5000 -> timeout
               end,
    ?assert(is_pid(MakeProc)),
    {ok, Open1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),
    MakeProc ! file_opened,

    CreateAns = receive
                    {create_ans, A} -> A
                after
                    5000 -> timeout
                end,
    ?assertMatch({ok, _}, CreateAns),
    {ok, {_, CreateHandle}} = CreateAns,

    {ok, Open2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),

    ?assertEqual(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
        rpc:call(W, ets, lookup_element, [lfm_handles, Open2, 2])),
    ?assertEqual(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
        rpc:call(W, ets, lookup_element, [lfm_handles, CreateHandle, 2])),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, file_req),
    ok.

create_open_race_test2(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, create_file,
        fun(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
            FileCtx = file_req:create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
            FileCtx2 = sfm_utils:create_storage_file(UserCtx, FileCtx),

            Master ! {make_doc, self()},
            ok = receive
                     file_opened -> ok
                 after
                     5000 -> timeout
                 end,

            {FileLocation, FileCtx3} =
                file_location_utils:get_new_file_location_doc(FileCtx2, true, true),

            #fuse_response{fuse_response = FileAttr} =
                attr_req:get_file_attr_insecure(UserCtx, FileCtx3, false, false),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_created{
                    handle_id = undefined,
                    file_attr = FileAttr#file_attr{size = 0},
                    file_location = FileLocation
                }
            }
        end),

    test_utils:mock_expect(W, file_req, open_file_with_extended_info,
        fun(_UserCtx, FileCtx, _Flag) ->
            {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
                storage_id = StorageId}}, _FileCtx2} =
                sfm_utils:create_delayed_storage_file(FileCtx),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_opened_extended{handle_id = undefined,
                    provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)}
          end),

    MakeProc = receive
                   {make_doc, Proc} -> Proc
               after
                   5000 -> timeout
               end,
    ?assert(is_pid(MakeProc)),
    {ok, Open1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),
    MakeProc ! file_opened,

    CreateAns = receive
                    {create_ans, A} -> A
                after
                    5000 -> timeout
                end,
    ?assertMatch({ok, _}, CreateAns),
    {ok, {_, CreateHandle}} = CreateAns,

    {ok, Open2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),

    ?assertEqual(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
        rpc:call(W, ets, lookup_element, [lfm_handles, Open2, 2])),
    ?assertEqual(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2]),
        rpc:call(W, ets, lookup_element, [lfm_handles, CreateHandle, 2])),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, file_req),
    ok.


create_open_race_test3(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, import_file,
        fun(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
            FileCtx = file_req:create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
            FileCtx2 = sfm_utils:create_storage_file(UserCtx, FileCtx),

            Master ! {make_doc, self()},
            timer:sleep(2000),

            {FileLocation, FileCtx3} =
                file_location_utils:get_new_file_location_doc(FileCtx2, true, true),

            #fuse_response{fuse_response = FileAttr} =
                attr_req:get_file_attr_insecure(UserCtx, FileCtx3, false, false),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_created{
                    handle_id = undefined,
                    file_attr = FileAttr#file_attr{size = 0},
                    file_location = FileLocation
                }
            }
        end),

    test_utils:mock_new(W, sfm_utils, [passthrough]),
    test_utils:mock_expect(W, sfm_utils, create_delayed_storage_file,
        fun(FileCtx) ->
            CreateOnStorageFun = fun(FileCtx2) ->
                FileCtx3 = sfm_utils:create_storage_file(
                    user_ctx:new(?ROOT_SESS_ID), FileCtx2),
                {StorageFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
                timer:sleep(5000),
                {StorageFileId, files_to_chown:chown_or_schedule_chowning(FileCtx4)}
            end,
            file_location_utils:create_file_location(FileCtx, CreateOnStorageFun)
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)}
          end),

    MakeProc = receive
                   {make_doc, Proc} -> Proc
               after
                   5000 -> timeout
               end,
    ?assert(is_pid(MakeProc)),
    {ok, Open1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath}, rdwr)),

    CreateAns = receive
                    {create_ans, A} -> A
                after
                    5000 -> timeout
                end,
    ?assertMatch({ok, _}, CreateAns),
    {ok, {_, CreateHandle}} = CreateAns,

    ?assertEqual(lfm_context:get_file_id(rpc:call(W, ets, lookup_element, [lfm_handles, Open1, 2])),
        lfm_context:get_file_id(rpc:call(W, ets, lookup_element, [lfm_handles, CreateHandle, 2]))),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, [file_req, sfm_utils]),
    ok.

create_delete_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, create_file,
        fun(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
            FileCtx = file_req:create_file_doc(UserCtx, ParentFileCtx, Name, Mode),

            Master ! {make_doc, self()},
            ok = receive
                     file_opened -> ok
                 after
                     5000 -> timeout
                 end,

            FileCtx2 = sfm_utils:create_storage_file(UserCtx, FileCtx),
            {FileLocation, FileCtx3} =
                file_location_utils:get_new_file_location_doc(FileCtx2, true, true),

            #fuse_response{fuse_response = FileAttr} =
                attr_req:get_file_attr_insecure(UserCtx, FileCtx3, false, false),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_created{
                    handle_id = undefined,
                    file_attr = FileAttr#file_attr{size = 0},
                    file_location = FileLocation
                }
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    spawn(fun() ->
        Master ! {create_ans, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)}
          end),

    MakeProc = receive
                   {make_doc, Proc} -> Proc
               after
                   5000 -> timeout
               end,
    ?assert(is_pid(MakeProc)),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, FilePath})),
    MakeProc ! file_opened,

    CreateAns = receive
                    {create_ans, A} -> A
                after
                    5000 -> timeout
                end,
    ?assertMatch({ok, _}, CreateAns),

    % TODO - sprawdzic czy nie zostaly smieciowe dokumenty typu file_location
    test_utils:mock_validate_and_unload(W, file_req),
    ok.

rename_to_opened_file_test(Config) ->
    % TODO a jak plik renameowany jest otwarty?
    [W | _] = ?config(op_worker_nodes, Config),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)),
    {ok, {_, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, FilePath2, 8#777)),
    ?assertMatch(ok, lfm_proxy:close(W, Handle)),

    % Co zrobic z move jak plik jest otwart?
    ?assertMatch({ok, _}, lfm_proxy:mv(W, SessId1, {path, FilePath2}, FilePath)),

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(2, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, file_req),
    ok.

% TODO - zakazac tworzenia pliku bez tworzenia na dysku? a co z sync?
% TODO - sync importuje plik jesli sie wstrzelimy z tworzeniem miedzy sprawdzanie a import
create_file_existing_on_disk_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileName = generator:gen_name(),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath0 = <<"/space_name1/", (generator:gen_name())/binary>>,
    lfm_proxy:create_and_open(W, SessId1, FilePath0, 8#777), % To create storage dirs

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StoragePath = filename:join([MountPoint, "space_id1", binary_to_list(FileName)]),

    ct:print("aaaaa ~p", [{StoragePath, rpc:call(W, file, list_dir, [filename:join([MountPoint, "space_id1"])])}]),

    {ok, FD} = ?assertMatch({ok, _}, rpc:call(W, file, open, [StoragePath, [write]])),
    rpc:call(W, file, close, [FD]),

    FilePath = <<"/space_name1/", FileName/binary>>,
    ?assertMatch({error, already_exists}, lfm_proxy:create_and_open(W, SessId1, FilePath, 8#777)),

    test_utils:mock_validate_and_unload(W, file_req),
    ok.

open_delete_race_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Master = self(),

    {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
    #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
    #{<<"mountPoint">> := MountPoint}= helper:get_args(Helpers),
    StorageSpacePath = filename:join([MountPoint, "space_id1"]),
    Dirs1Length = case rpc:call(W, file, list_dir, [StorageSpacePath]) of
                      {ok, Dirs1} -> length(Dirs1);
                      _ -> 0
                  end,

    test_utils:mock_new(W, file_req, [passthrough]),
    test_utils:mock_expect(W, file_req, open_file_with_extended_info,
        fun(UserCtx, FileCtx, Flag) ->
            SessId = user_ctx:get_session_id(UserCtx),
            {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
                storage_id = StorageId}}, FileCtx2} =
                sfm_utils:create_delayed_storage_file(FileCtx),
            {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
            SFMHandle2 = storage_file_manager:set_size(SFMHandle),
            {ok, Handle} = storage_file_manager:open(SFMHandle2, Flag),

            Master ! {delete_file, self()},
            ok = receive
                     file_deleted -> ok
                 after
                     5000 -> timeout
                 end,

            HandleId = base64:encode(crypto:strong_rand_bytes(20)),
            session_handles:add(SessId, HandleId, Handle),
            ok = file_handles:register_open(FileCtx3, SessId, 1),
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_opened_extended{handle_id = HandleId,
                    provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
            }
        end),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    FilePath = <<"/space_name1/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath, 8#777)),
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

    {ok, Dirs2} = rpc:call(W, file, list_dir, [StorageSpacePath]),
    ?assertEqual(1, length(Dirs2) - Dirs1Length),

    test_utils:mock_validate_and_unload(W, file_req),
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
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

