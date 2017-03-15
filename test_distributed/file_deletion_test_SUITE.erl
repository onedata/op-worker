%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(file_deletion_test_SUITE).
-author("Michal Wrona").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    counting_file_open_and_release_test/1,
    invalidating_session_open_files_test/1,
    init_should_clear_open_files_test/1,
    open_file_deletion_request_test/1,
    deletion_of_not_open_file_test/1,
    deletion_of_open_file_test/1]).

-define(TEST_CASES, [
    counting_file_open_and_release_test,
    invalidating_session_open_files_test,
    init_should_clear_open_files_test,
    open_file_deletion_request_test,
    deletion_of_not_open_file_test,
    deletion_of_open_file_test
]).

all() -> ?ALL(?TEST_CASES).

-define(FILE_UUID, <<"file_uuid">>).
-define(FILE_GUID, fslogic_uuid:uuid_to_guid(?FILE_UUID, <<"spaceid">>)).
-define(SESSION_ID_1, <<"session_id_1">>).
-define(SESSION_ID_2, <<"session_id_2">>).

-define(req(W, P), rpc:call(W, worker_proxy, call, [fslogic_deletion_worker, P])).

%%%===================================================================
%%% Test functions
%%%===================================================================

counting_file_open_and_release_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    init_session(Worker, ?SESSION_ID_1),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),

    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 70])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, ?SESSION_ID_1, 50])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, ?SESSION_ID_1, 20])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    %% Release of non existing file should not fail.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, ?SESSION_ID_1, 50])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 1])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    %% Release of file marked to remove should trigger call to file deletion worker.
    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, ?SESSION_ID_1, 1])),

    test_utils:mock_assert_num_calls(Worker, fslogic_deletion_worker, handle, 1, 1).

invalidating_session_open_files_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),

    init_session(Worker, ?SESSION_ID_1),
    %% With one active session entry for UUID should be removed after its expiration.
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    init_session(Worker, ?SESSION_ID_1),
    init_session(Worker, ?SESSION_ID_2),
    %% With few active sessions entry for UUID should be removed only after
    %% all of them expired.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_2, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_2])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    init_session(Worker, ?SESSION_ID_1),
    %% Last session expiration should trigger call to file deletion worker when
    %% file is marked to remove.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx])),
    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),

    test_utils:mock_assert_num_calls(Worker, fslogic_deletion_worker, handle, 1, 1),

    %% Invalidating session when file or session entry not exists should not fail.

    ?assertEqual(ok, rpc:call(Worker, file_handles, invalidate_session_entry,
        [FileCtx, ?SESSION_ID_1])),

    init_session(Worker, ?SESSION_ID_1),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, ?SESSION_ID_1, 30])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, invalidate_session_entry,
        [FileCtx, ?SESSION_ID_2])).

init_should_clear_open_files_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileGuid = create_test_file(Config, Worker, SessId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    FileCtx2 = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(<<"file_uuid2">>, <<"spaceid">>)),
    FileCtx3 = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(<<"file_uuid3">>, <<"spaceid">>)),
    FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 30])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx2, SessId, 30])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx3, SessId, 30])),

    %% One file should be also removed.
    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx])),

    {ok, OpenFiles} = rpc:call(Worker, file_handles, list, []),

    ?assertEqual(3, length(OpenFiles)),

    ?assertMatch({ok, _}, rpc:call(Worker, fslogic_deletion_worker, init, [args])),

    {ok, ClearedOpenFiles} = rpc:call(Worker, file_handles, list, []),
    ?assertEqual(0, length(ClearedOpenFiles)),

    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 2).

open_file_deletion_request_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileGuid = create_test_file(Config, Worker, SessId),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1),

    ?assertEqual(ok, ?req(Worker, {open_file_deletion_request, file_ctx:new_by_guid(FileGuid)})),

    test_utils:mock_assert_num_calls(Worker, rename_req, rename, 4, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 2).

deletion_of_not_open_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    FileGuid = create_test_file(Config, Worker, SessId),
    FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1),

    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [FileUuid])),
    ?assertEqual(ok, ?req(Worker, {fslogic_deletion_request, UserCtx, file_ctx:new_by_guid(FileGuid), false})),

    test_utils:mock_assert_num_calls(Worker, rename_req, rename, 4, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 2).

deletion_of_open_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileGuid = create_test_file(Config, Worker, SessId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 1])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [FileUuid])),

    ?assertEqual(ok, ?req(Worker, {fslogic_deletion_request, UserCtx, FileCtx, false})),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [FileUuid])),

    %% File should be marked to remove and renamed.
    test_utils:mock_assert_num_calls(Worker, rename_req, rename, 4, 1),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 0),
    %% Calls from rename
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, mv, 2, 1),

    %% Release of file mark to remove should remove it.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 1])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [FileUuid])),

    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    %% Calls rename
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 2).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [fslogic_uuid, file_ctx, fslogic_deletion_worker, file_meta],
        [passthrough]),

    test_utils:mock_expect(Worker, fslogic_deletion_worker, handle,
        fun({open_file_deletion_request, FileCtx}) ->
            true = file_ctx:is_file_ctx_const(FileCtx),
            ?FILE_UUID = file_ctx:get_uuid_const(FileCtx),
            ok
        end),
    Config;

init_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files_test;
    Case =:= open_file_deletion_request_test;
    Case =:= deletion_of_not_open_file_test;
    Case =:= deletion_of_open_file_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, [storage_file_manager, rename_req,
        worker_proxy, file_meta], [passthrough]),
    test_utils:mock_expect(Worker, worker_proxy, cast,
        fun(W, A) -> worker_proxy:call(W, A) end),

    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_validate_and_unload(Worker, [fslogic_deletion_worker, file_ctx, file_meta]),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [?SESSION_ID_2])),

    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files_test;
    Case =:= open_file_deletion_request_test;
    Case =:= deletion_of_not_open_file_test;
    Case =:= deletion_of_open_file_test ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),

    test_utils:mock_validate_and_unload(Worker, [storage_file_manager,
        rename_req]),
    test_utils:mock_unload(Worker, [worker_proxy, file_meta]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, OpenFiles} = rpc:call(Worker, file_handles, list, []),
    lists:foreach(fun(#document{key = Key}) ->
        rpc:call(Worker, file_handles, delete, [Key])
    end, OpenFiles).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_session(Worker, SessID) ->
    Self = self(),
    Iden = #user_identity{user_id = <<"u1">>},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [SessID, Iden, Self])).

create_test_file(Config, Worker, SessId) ->
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file0"])),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, File, 8#770)),
    FileGuid.