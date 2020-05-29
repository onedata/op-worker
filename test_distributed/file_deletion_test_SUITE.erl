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

-include("fuse_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("proto/common/credentials.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    counting_file_open_and_release_test/1,
    invalidating_session_open_files_test/1,
    init_should_clear_open_files/1,
    init_should_clear_delayed_open_files/1,
    open_file_deletion_request/1,
    delayed_open_file_deletion_request/1,
    deletion_of_not_open_file/1,
    deletion_of_not_open_delayed_file/1,
    file_should_not_be_listed_after_deletion/1,
    file_stat_should_return_enoent_after_deletion/1,
    file_open_should_return_enoent_after_deletion/1,
    file_handle_should_work_after_deletion/1,
    remove_file_on_ceph_using_client/1,
    remove_opened_file_posix_test/1,
    remove_opened_file_ceph_test/1,
    correct_file_on_storage_is_deleted_new_file_first/1,
    correct_file_on_storage_is_deleted_old_file_first/1
]).

-define(TEST_CASES, [
    counting_file_open_and_release_test,
    invalidating_session_open_files_test,
    init_should_clear_open_files,
    init_should_clear_delayed_open_files,
    open_file_deletion_request,
    delayed_open_file_deletion_request,
    deletion_of_not_open_file,
    deletion_of_not_open_delayed_file,
    file_should_not_be_listed_after_deletion,
    file_stat_should_return_enoent_after_deletion,
    file_open_should_return_enoent_after_deletion,
    file_handle_should_work_after_deletion,
    remove_file_on_ceph_using_client,
    remove_opened_file_posix_test,
    remove_opened_file_ceph_test,
    correct_file_on_storage_is_deleted_new_file_first,
    correct_file_on_storage_is_deleted_old_file_first
]).

all() -> ?ALL(?TEST_CASES).

-define(FILE_UUID, <<"file_uuid">>).
-define(FILE_GUID, file_id:pack_guid(?FILE_UUID, <<"spaceid">>)).

-define(req(W, P), rpc:call(W, worker_proxy, call, [fslogic_deletion_worker, P])).

%%%===================================================================
%%% Test functions
%%%===================================================================

counting_file_open_and_release_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SessId} = init_session(Worker, <<"nonce">>),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),

    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 30, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 70, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 50])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 30])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 20])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    %% Release of non existing file should not fail.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 50])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 1, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    %% Release of file marked to remove should trigger call to file deletion worker.
    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx, ?LOCAL_REMOVE])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_release,
        [FileCtx, SessId, 1])),

    test_utils:mock_assert_num_calls(Worker, fslogic_delete, handle_release_of_deleted_file, 2, 1).

invalidating_session_open_files_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),

    {ok, SessId1} = init_session(Worker, <<"nonce_1">>),
    %% With one active session entry for UUID should be removed after its expiration.
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId1, 30, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId1])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    {ok, SessId1} = init_session(Worker, <<"nonce_1">>),
    {ok, SessId2} = init_session(Worker, <<"nonce_2">>),
    %% With few active sessions entry for UUID should be removed only after
    %% all of them expired.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId1, 30, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId2, 30, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId1])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId2])),
    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    {ok, SessId1} = init_session(Worker, <<"nonce_1">>),
    %% Last session expiration should trigger call to file deletion worker when
    %% file is marked to remove.
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId1, 30, undefined])),
    ?assertEqual(true, rpc:call(Worker, file_handles, exists, [?FILE_UUID])),

    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx, ?LOCAL_REMOVE])),
    ?assertEqual(ok, rpc:call(Worker, session, delete, [SessId1])),

    test_utils:mock_assert_num_calls(Worker, fslogic_delete, handle_release_of_deleted_file, 2, 1),

    %% Invalidating session when file or session entry not exists should not fail.

    ?assertEqual(ok, rpc:call(Worker, file_handles, invalidate_session_entry,
        [FileCtx, SessId1])),

    {ok, SessId1} = init_session(Worker, <<"nonce_1">>),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId1, 30, undefined])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, invalidate_session_entry,
        [FileCtx, SessId1])).

init_should_clear_open_files(Config) ->
    init_should_clear_open_files_test_base(Config, false).

init_should_clear_delayed_open_files(Config) ->
    init_should_clear_open_files_test_base(Config, true).

init_should_clear_open_files_test_base(Config, DelayedFileCreation) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileGuid = create_test_file(Config, Worker, SessId, DelayedFileCreation),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    FileCtx2 = file_ctx:new_by_guid(file_id:pack_guid(<<"file_uuid2">>, <<"spaceid">>)),
    FileCtx3 = file_ctx:new_by_guid(file_id:pack_guid(<<"file_uuid3">>, <<"spaceid">>)),

    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx, SessId, 30, undefined])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx2, SessId, 30, undefined])),
    ?assertEqual(ok, rpc:call(Worker, file_handles, register_open,
        [FileCtx3, SessId, 30, undefined])),

    %% One file should be also removed.
    ?assertEqual(ok, rpc:call(Worker, file_handles, mark_to_remove, [FileCtx, ?LOCAL_REMOVE])),

    {ok, OpenFiles} = rpc:call(Worker, file_handles, list_local, []),

    ?assertEqual(3, length(OpenFiles)),

    ?assertMatch(ok, rpc:call(Worker, fslogic_delete, cleanup_opened_files, [])),

    {ok, ClearedOpenFiles} = rpc:call(Worker, file_handles, list_local, []),
    ?assertEqual(0, length(ClearedOpenFiles)),

    test_utils:mock_assert_num_calls(Worker, file_meta, delete_without_link, 1, 1),
    case DelayedFileCreation of
        true -> ok;
        false ->
            test_utils:mock_assert_num_calls(Worker, storage_driver, unlink, 2, 1)
    end.

open_file_deletion_request(Config) ->
    open_file_deletion_request_test_base(Config, false).

delayed_open_file_deletion_request(Config) ->
    open_file_deletion_request_test_base(Config, true).

open_file_deletion_request_test_base(Config, DelayedFileCreation) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    FileGuid = create_test_file(Config, Worker, SessId, DelayedFileCreation),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = rpc:call(Worker, user_ctx, new, [<<"user1">>]),
    FileCtx2 = rpc:call(Worker, fslogic_delete, delete_parent_link, [FileCtx, UserCtx]),

    ?assertEqual(ok, rpc:call(Worker, fslogic_delete, handle_release_of_deleted_file, [FileCtx2, ?LOCAL_REMOVE])),

    test_utils:mock_assert_num_calls(Worker, rename_req, rename, 4, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete_without_link, 1, 1),
    case DelayedFileCreation of
        true -> ok;
        false ->
            test_utils:mock_assert_num_calls(Worker, storage_driver, unlink, 2, 1)
    end.

deletion_of_not_open_file(Config) ->
    deletion_of_not_open_file_test_base(Config, false).

deletion_of_not_open_delayed_file(Config) ->
    deletion_of_not_open_file_test_base(Config, true).

deletion_of_not_open_file_test_base(Config, DelayedFileCreation) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    FileGuid = create_test_file(Config, Worker, SessId, DelayedFileCreation),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    FileCtx = file_ctx:new_by_guid(FileGuid),

    ?assertEqual(false, rpc:call(Worker, file_handles, exists, [FileUuid])),
    ?assertEqual(ok, rpc:call(Worker, fslogic_delete, delete_file_locally, [UserCtx, FileCtx, false])),

    test_utils:mock_assert_num_calls(Worker, rename_req, rename, 4, 0),
    test_utils:mock_assert_num_calls(Worker, file_meta, delete, 1, 1),
    case DelayedFileCreation of
        true -> ok;
        false ->
            test_utils:mock_assert_num_calls(Worker, storage_driver, unlink, 2, 1)
    end.

file_should_not_be_listed_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/test_file">>, 8#770),
    {ok, #file_attr{guid = SpaceGuid}} = lfm_proxy:stat(Worker, SessId, {path, <<"/", SpaceName/binary>>}),
    {ok, Children} = lfm_proxy:get_children(Worker, SessId, {guid, SpaceGuid}, 0,10),
    ?assertMatch(#{FileGuid := <<"test_file">>}, maps:from_list(Children)),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),

    {ok, NewChildren} = lfm_proxy:get_children(Worker, SessId, {guid, SpaceGuid}, 0,10),
    ?assertNotMatch(#{FileGuid := <<"test_file">>}, maps:from_list(NewChildren)).

file_stat_should_return_enoent_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/test_file">>, 8#770),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId, {guid, FileGuid})).

file_open_should_return_enoent_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/test_file">>, 8#770),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:open(Worker, SessId, {guid, FileGuid}, read)).

file_handle_should_work_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/test_file">>, 8#770),
    {ok, Handle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, rdwr),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),

    ?assertEqual({ok, 12}, lfm_proxy:write(Worker, Handle, 0, <<"test_content">>)),
    ?assertEqual({ok, <<"test_content">>}, lfm_proxy:read(Worker, Handle, 0, 15)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).

remove_file_on_ceph_using_client(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user2">>, {0, 0, 0, 0}, 0),
    [Worker | _] = ?config(workers1, Config),
    SessionId = ?config(session, Config),
    SpaceName = <<"space2">>,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,

    [{_, Ceph} | _] = proplists:get_value(cephrados, ?config(storages, Config)),
    ContainerId = proplists:get_value(container_id, Ceph),

    {ok, Guid} = lfm_proxy:create(Worker, SessionId(Worker), FilePath, 8#755),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId(Worker), {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(Worker, Handle, 0, crypto:strong_rand_bytes(100)),
    ok = lfm_proxy:close(Worker, Handle),

    {ok, {Sock, _}} = fuse_test_utils:connect_as_user(Config, Worker, <<"user2">>, [{active, true}]),

    L = utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"]),
    ?assertEqual(true, length(L) > 0),

    ok = ssl:send(Sock, fuse_test_utils:generate_delete_file_message(Guid, <<"2">>)),
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"2">>}, fuse_test_utils:receive_server_message()),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(Worker, SessionId(Worker), {guid, Guid}, 0, 0), 60),

    ?assertMatch([], utils:cmd(["docker exec", atom_to_list(ContainerId), "rados -p onedata ls -"])).


remove_opened_file_posix_test(Config) ->
    remove_opened_file_test_base(Config, <<"space1">>).

remove_opened_file_ceph_test(Config) ->
    remove_opened_file_test_base(Config, <<"space2">>).

remove_opened_file_test_base(Config, SpaceName) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FilePath = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    
    User1 = <<"user1">>,
    User2 = <<"user2">>,

    Size = 100,
    Content1 = crypto:strong_rand_bytes(Size),
    Content2 = crypto:strong_rand_bytes(Size),

    % File1
    {ok, Guid1} = lfm_proxy:create(Worker, SessId(User1), FilePath, 8#777),
    {ok, Handle1} = lfm_proxy:open(Worker, SessId(User1), {path, FilePath}, rdwr),
    {ok, _} = lfm_proxy:write(Worker, Handle1, 0, Content1),

    % File2
    ok = lfm_proxy:unlink(Worker, SessId(User2), {path, FilePath}),
    {ok, _} = lfm_proxy:create(Worker, SessId(User2), FilePath, 8#777),
    {ok, Handle2} = lfm_proxy:open(Worker, SessId(User2), {path, FilePath}, rdwr),
    {ok, _} = lfm_proxy:write(Worker, Handle2, 0, Content2),
    ?assertEqual({ok, Content2}, lfm_proxy:read(Worker, Handle2, 0, Size)),

    % File1 
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User1), {guid, Guid1})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:open(Worker, SessId(User1), {guid, Guid1}, read)),
    ?assertEqual({ok, Content1}, lfm_proxy:read(Worker, Handle1, 0, Size)),
    
    {ok, _} = lfm_proxy:write(Worker, Handle1, Size, Content1),
    ?assertEqual({ok, <<Content1/binary, Content1/binary>>}, lfm_proxy:read(Worker, Handle1, 0, 2*Size)),
    
    ?assertEqual({ok, Content2}, lfm_proxy:read(Worker, Handle2, 0, Size)),
    lfm_proxy:close_all(Worker),
    {ok, Handle3} = lfm_proxy:open(Worker, SessId(User2), {path, FilePath}, read),
    ?assertEqual({ok, Content2}, lfm_proxy:read(Worker, Handle3, 0, Size)).


correct_file_on_storage_is_deleted_new_file_first(Config) ->
    correct_file_on_storage_is_deleted_test_base(Config, true).

correct_file_on_storage_is_deleted_old_file_first(Config) ->
    correct_file_on_storage_is_deleted_test_base(Config, false).

correct_file_on_storage_is_deleted_test_base(Config, DeleteNewFileFirst) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    SpaceId = <<"space1">>,
    ParentGuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    FileName = generator:gen_name(),
    FilePath = fslogic_path:join([<<"/">>, SpaceId, FileName]),
    
    {ok, {G1, H1}} = lfm_proxy:create_and_open(Worker, SessId, ParentGuid, FileName, 8#665),
    ok = lfm_proxy:unlink(Worker, SessId, {guid, G1}),
    {ok, G2} = lfm_proxy:create(Worker, SessId, FilePath, 8#665),
    {ok, H2} = lfm_proxy:open(Worker, SessId, {guid, G2}, rdwr),

    FileCtx1 = rpc:call(Worker, file_ctx, new_by_guid, [G1]),
    {SfmHandle1, _} = rpc:call(Worker, storage_driver, new_handle, [?ROOT_SESS_ID, FileCtx1]),
    FileCtx2 = rpc:call(Worker, file_ctx, new_by_guid, [G2]),
    {SfmHandle2, _} = rpc:call(Worker, storage_driver, new_handle, [?ROOT_SESS_ID, FileCtx2]),

    ?assertMatch({ok, _}, rpc:call(Worker, storage_driver, stat, [SfmHandle1]), 60),
    ?assertMatch({ok, _}, rpc:call(Worker, storage_driver, stat, [SfmHandle2]), 60),

    case DeleteNewFileFirst of
        true ->
            ok = lfm_proxy:unlink(Worker, SessId, {guid, G2}),
            ok = lfm_proxy:close(Worker, H2),
            ?assertMatch({ok, _}, rpc:call(Worker, storage_driver, stat, [SfmHandle1]), 60),
            ?assertEqual({error, ?ENOENT}, rpc:call(Worker, storage_driver, stat, [SfmHandle2]), 60),
            ok = lfm_proxy:close(Worker, H1);

        false ->
            ok = lfm_proxy:close(Worker, H1),
            ?assertEqual({error, ?ENOENT}, rpc:call(Worker, storage_driver, stat, [SfmHandle1]), 60),
            ?assertMatch({ok, _}, rpc:call(Worker, storage_driver, stat, [SfmHandle2]), 60),
            ok = lfm_proxy:unlink(Worker, SessId, {guid, G2}),
            ok = lfm_proxy:close(Worker, H2)
    end,

    ?assertEqual({error, ?ENOENT}, rpc:call(Worker, storage_driver, stat, [SfmHandle1]), 60),
    ?assertEqual({error, ?ENOENT}, rpc:call(Worker, storage_driver, stat, [SfmHandle2]), 60).
    

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base, fuse_test_utils]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test
->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [fslogic_uuid, file_ctx, fslogic_delete, file_meta],
        [passthrough]),

    test_utils:mock_expect(Worker, fslogic_delete, handle_release_of_deleted_file,
        fun(FileCtx, _RemovalStatus) ->
            true = file_ctx:is_file_ctx_const(FileCtx),
            ?FILE_UUID = file_ctx:get_uuid_const(FileCtx),
            ok
        end),

    initializer:mock_auth_manager(Config),
    Config;


init_per_testcase(remove_file_on_ceph_using_client, Config) ->
    initializer:remove_pending_messages(),
    ssl:start(),

    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo);
init_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files;
    Case =:= init_should_clear_delayed_open_files;
    Case =:= open_file_deletion_request;
    Case =:= delayed_open_file_deletion_request;
    Case =:= deletion_of_not_open_file;
    Case =:= deletion_of_not_open_delayed_file;
    Case =:= file_should_not_be_listed_after_deletion;
    Case =:= file_stat_should_return_enoent_after_deletion;
    Case =:= file_open_should_return_enoent_after_deletion;
    Case =:= file_handle_should_work_after_deletion
->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, [storage_driver, rename_req,
        worker_proxy, file_meta], [passthrough]),
    test_utils:mock_expect(Worker, worker_proxy, cast,
        fun(W, A) -> worker_proxy:call(W, A) end),

    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo);
init_per_testcase(Case, Config) when
    Case =:= remove_opened_file_posix_test;
    Case =:= remove_opened_file_ceph_test;
    Case =:= correct_file_on_storage_is_deleted_new_file_first;
    Case =:= correct_file_on_storage_is_deleted_old_file_first
->
    initializer:remove_pending_messages(),
    ssl:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when
    Case =:= counting_file_open_and_release_test;
    Case =:= invalidating_session_open_files_test
->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_validate_and_unload(Worker, [fslogic_delete, file_ctx, file_meta]),
    {ok, SessId1} = init_session(Worker, <<"nonce_1">>),
    {ok, SessId2} = init_session(Worker, <<"nonce_2">>),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [SessId1])),
    ?assertMatch(ok, rpc:call(Worker, session, delete, [SessId2])),

    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= init_should_clear_open_files;
    Case =:= init_should_clear_delayed_open_files;
    Case =:= open_file_deletion_request;
    Case =:= delayed_open_file_deletion_request;
    Case =:= deletion_of_not_open_file;
    Case =:= deletion_of_not_open_delayed_file;
    Case =:= file_should_not_be_listed_after_deletion;
    Case =:= file_stat_should_return_enoent_after_deletion;
    Case =:= file_open_should_return_enoent_after_deletion;
    Case =:= file_handle_should_work_after_deletion
->

    [Worker | _] = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),

    test_utils:mock_validate_and_unload(Worker, [storage_driver, rename_req]),
    test_utils:mock_unload(Worker, [worker_proxy, file_meta]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, OpenFiles} = rpc:call(Worker, file_handles, list_local, []),
    lists:foreach(fun(#document{key = Key}) ->
        rpc:call(Worker, file_handles, delete, [Key])
    end, OpenFiles).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_session(Worker, Nonce) ->
    fuse_test_utils:reuse_or_create_fuse_session(
        Worker, Nonce, ?SUB(user, <<"u1">>), undefined, self()
    ).

create_test_file(Config, Worker, SessId, DelayedFileCreation) ->
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = filename:join(["/", SpaceName, "file0"]),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, File, 8#770)),
    case DelayedFileCreation of
        true -> ok;
        false ->
            {ok, Handle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, read),
            lfm_proxy:close(Worker, Handle)
    end,
    FileGuid.
