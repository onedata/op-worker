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
-define(SESSION_ID_1, <<"session_id_1">>).
-define(SESSION_ID_2, <<"session_id_2">>).

-define(req(W, P), rpc:call(W, worker_proxy, call, [file_deletion_worker, P])).


%%%===================================================================
%%% Test functions
%%%===================================================================

counting_file_open_and_release_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    init_session(Worker, ?SESSION_ID_1),

    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 70])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [?FILE_UUID, ?SESSION_ID_1, 50])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [?FILE_UUID, ?SESSION_ID_1, 20])),
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    %% Release of non existing file should not fail.
    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [?FILE_UUID, ?SESSION_ID_1, 50])),
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 1])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    %% Release of file marked to remove should trigger call to file deletion worker.
    ?assertEqual(ok, rpc:call(Worker, open_file, mark_to_remove, [?FILE_UUID])),
    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [?FILE_UUID, ?SESSION_ID_1, 1])),


    test_utils:mock_assert_num_calls(Worker, file_deletion_worker, handle, 1, 1).

invalidating_session_open_files_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    init_session(Worker, ?SESSION_ID_1),
    %% With one active session entry for UUID should be removed after its expiration.
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    init_session(Worker, ?SESSION_ID_1),
    init_session(Worker, ?SESSION_ID_2),
    %% With few active sessions entry for UUID should be removed only after
    %% all of them expired.
    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_2, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_2])),
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    init_session(Worker, ?SESSION_ID_1),
    %% Last session expiration should trigger call to file deletion worker when
    %% file is marked to remove.
    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, open_file, mark_to_remove, [?FILE_UUID])),
    ?assertEqual(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),

    test_utils:mock_assert_num_calls(Worker, file_deletion_worker, handle, 1, 1),

    %% Invalidating session when file or session entry not exists should not fail.

    ?assertEqual(ok, rpc:call(Worker, open_file, invalidate_session_entry,
        [?FILE_UUID, ?SESSION_ID_1])),

    init_session(Worker, ?SESSION_ID_1),
    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [?FILE_UUID, ?SESSION_ID_1, 30])),
    ?assertEqual(ok, rpc:call(Worker, open_file, invalidate_session_entry,
        [?FILE_UUID, ?SESSION_ID_2])).


init_should_clear_open_files_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileUUID = create_test_file(Config, Worker, SessId),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [FileUUID, SessId, 30])),
    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [<<"file_uuid2">>, SessId, 30])),
    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [<<"file_uuid3">>, SessId, 30])),

    %% One file should be also removed.
    ?assertEqual(ok, rpc:call(Worker, open_file, mark_to_remove, [FileUUID])),

    {ok, OpenFiles} = rpc:call(Worker, open_file, list, []),

    ?assertEqual(3, length(OpenFiles)),

    ?assertMatch({ok, _}, rpc:call(Worker, file_deletion_worker, init, [args])),

    {ok, ClearedOpenFiles} = rpc:call(Worker, open_file, list, []),
    ?assertEqual(0, length(ClearedOpenFiles)),

    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1).


open_file_deletion_request_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileUUID = create_test_file(Config, Worker, SessId),

    ?assertEqual(ok, ?req(Worker, {open_file_deletion_request, FileUUID})),

    test_utils:mock_assert_num_calls(Worker, fslogic_rename, rename, 3, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1).


deletion_of_not_open_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileUUID = create_test_file(Config, Worker, SessId),

    ?assertEqual(false, rpc:call(Worker, open_file, exists, [FileUUID])),
    ?assertEqual(ok, ?req(Worker, {fslogic_deletion_request,
        #fslogic_ctx{session_id = SessId, space_id = <<"SpaceId">>}, FileUUID, false})),

    test_utils:mock_assert_num_calls(Worker, fslogic_rename, rename, 3, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1).

deletion_of_open_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileUUID = create_test_file(Config, Worker, SessId),

    ?assertEqual(ok, rpc:call(Worker, open_file, register_open,
        [FileUUID, SessId, 1])),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [FileUUID])),

    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    ?assertEqual(ok, ?req(Worker, {fslogic_deletion_request, #fslogic_ctx{
        session_id = SessId, space_id = <<"SpaceId">>, session = Session}, FileUUID, false})),
    ?assertEqual(true, rpc:call(Worker, open_file, exists, [FileUUID])),

    %% File should be marked to remove and renamed.
    test_utils:mock_assert_num_calls(Worker, fslogic_rename, rename, 3, 1),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 0),
    %% Calls from rename
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 1),

    %% Release of file mark to remove should remove it.
    ?assertEqual(ok, rpc:call(Worker, open_file, register_release,
        [FileUUID, SessId, 1])),
    ?assertEqual(false, rpc:call(Worker, open_file, exists, [FileUUID])),

    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    %% Calls rename
    test_utils:mock_assert_num_calls(Worker, storage_file_manager, unlink, 1, 2).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, file_deletion_worker),
    test_utils:mock_expect(Worker, file_deletion_worker, handle,
        fun({open_file_deletion_request, ?FILE_UUID}) -> ok end),
    Config;

init_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files_test;
    Case =:= open_file_deletion_request_test;
    Case =:= deletion_of_not_open_file_test;
    Case =:= deletion_of_open_file_test ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, [storage_file_manager, fslogic_rename,
        worker_proxy]),
    test_utils:mock_expect(Worker, worker_proxy, cast,
        fun(W, A) -> worker_proxy:call(W, A) end),

    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_validate_and_unload(Worker, file_deletion_worker),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [?SESSION_ID_1])),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [?SESSION_ID_2])),

    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files_test;
    Case =:= open_file_deletion_request_test;
    Case =:= deletion_of_not_open_file_test;
    Case =:= deletion_of_open_file_test->
    [Worker | _] = ?config(op_worker_nodes, Config),

    lfm_proxy:teardown(Config),
    %% unload is in initializer:clean_test_users_and_spaces_no_validate
    test_utils:mock_validate(Worker, file_meta),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),

    test_utils:mock_validate_and_unload(Worker, [storage_file_manager,
        fslogic_rename]),
    test_utils:mock_unload(worker_proxy),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, OpenFiles} = rpc:call(Worker, open_file, list, []),
    lists:foreach(fun(#document{key = Key}) ->
        rpc:call(Worker, open_file, delete, [Key])
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

    {ok, GUID} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, File, 8#770)),
    fslogic_uuid:guid_to_uuid(GUID).
