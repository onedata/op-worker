%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains base functions for tests of lfm API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_test_base).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("lfm_files_test_base.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    fslogic_new_file/1,
    lfm_create_and_unlink/1,
    lfm_create_and_access/1,
    lfm_create_failure/1,
    lfm_basic_rename/1,
    lfm_renaming_space_directory_should_fail/1,
    lfm_basic_rdwr/1,
    lfm_basic_rdwr_opens_file_once/1,
    lfm_basic_rdwr_after_file_delete/1,
    lfm_write/1,
    lfm_stat/1,
    lfm_get_details/1,
    lfm_synch_stat/1,
    lfm_cp_file/1,
    lfm_cp_empty_dir/1,
    lfm_cp_dir_to_itself_should_fail/1,
    lfm_cp_dir_to_its_child_should_fail/1,
    lfm_cp_dir/1,
    lfm_truncate/1,
    lfm_truncate_and_write/1,
    lfm_acl/1,
    lfm_rmdir/1,
    lfm_rmdir_fails_with_eperm_on_space_directory/1,
    rm_recursive/1,
    rm_recursive_fails_with_eperm_on_space_directory/1,
    file_gap/1,
    ls/1, ls_base/1,
    ls_with_stats/1, ls_with_stats_base/1,
    create_share_dir/1,
    create_share_file/1,
    remove_share/1,
    share_getattr/1,
    share_get_parent/1,
    share_list/1,
    share_read/1,
    share_child_getattr/1,
    share_child_list/1,
    share_child_read/1,
    share_permission_denied/1,
    echo_loop/1,
    echo_loop_base/1,
    storage_file_creation_should_be_deferred_until_open/1,
    deferred_creation_should_not_prevent_mv/1,
    deferred_creation_should_not_prevent_truncate/1,
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
    readdir_plus_should_return_empty_result_for_empty_dir/1,
    readdir_plus_should_return_empty_result_zero_size/1,
    readdir_plus_should_work_with_zero_offset/1,
    readdir_plus_should_work_with_non_zero_offset/1,
    readdir_plus_should_work_with_size_greater_than_dir_size/1,
    readdir_should_work_with_token/4,
    readdir_should_work_with_startid/1,
    get_children_details_should_return_empty_result_for_empty_dir/1,
    get_children_details_should_return_empty_result_zero_size/1,
    get_children_details_should_work_with_zero_offset/1,
    get_children_details_should_work_with_non_zero_offset/1,
    get_children_details_should_work_with_size_greater_than_dir_size/1,
    get_children_details_should_work_with_startid/1,
    get_recursive_file_list/1,
    lfm_recreate_handle/3,
    lfm_open_failure/1,
    lfm_create_and_open_failure/1,
    lfm_open_in_direct_mode/1,
    lfm_mv_failure/1,
    lfm_open_multiple_times_failure/1,
    lfm_open_failure_multiple_users/1,
    lfm_open_and_create_open_failure/1,
    lfm_mv_failure_multiple_users/1,
    sparse_files_should_be_created/2,
    lfm_close_deleted_open_files/1
]).

-define(TIMEOUT, timer:seconds(10)).
-define(REPEATS, 3).
-define(SUCCESS_RATE, 100).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(file_req(W, SessId, ContextGuid, FileRequest), ?req(W, SessId,
    #file_request{context_guid = ContextGuid, file_request = FileRequest})).

-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

-define(cdmi_id(Guid), begin
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    FileId
end).

%%%====================================================================
%%% Test function
%%%====================================================================

lfm_recreate_handle(Config, CreatePerms, DeleteAfterOpen) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {ok, {FileGuid, Handle}} = lfm_proxy:create_and_open(W, SessId1, <<"/space_name1/test_read">>, CreatePerms),
    case DeleteAfterOpen of
        delete_after_open ->
            ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, ?FILE_REF(FileGuid))),
            ?assertEqual(ok, rpc:call(W, permissions_cache, invalidate, []));
        _ ->
            ok
    end,

    % remove handle before write to file so that handle has to be recreated
    Context = rpc:call(W, ets, lookup_element, [lfm_handles, Handle, 2]),
    HandleId = lfm_context:get_handle_id(Context),
    ?assertEqual({error, not_found}, rpc:call(W, session_handles, get, [SessId1, HandleId])),

    % try to write to file to confirm that handle has been recreated
    FileContent = <<"test_data">>,
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, FileContent)),
    verify_file_content(Config, Handle, FileContent),

    ?assertEqual(ok, lfm_proxy:close(W, Handle)),

    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ).

lfm_open_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),

    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, CacheEntriesBefore,
        MemEntriesAfter, CacheEntriesAfter).

lfm_create_and_open_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    ParentGuid = get_guid(W, SessId1, <<"/space_name1">>),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:create_and_open(
        W, SessId1, ParentGuid, <<"test_read">>, ?DEFAULT_FILE_PERMS)
    ),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
        W, SessId1, {path, <<"/space_name1/test_read">>})
    ),
    ?assertEqual({ok, []}, rpc:call(W, file_handles, list, [])),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_open_and_create_open_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    ParentGuid = get_guid(W, SessId1, <<"/space_name1">>),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:create_and_open(
        W, SessId1, ParentGuid, <<"test_read">>, ?DEFAULT_FILE_PERMS)
    ),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
        W, SessId1, {path, <<"/space_name1/test_read">>})
    ),
    ?assertEqual({ok, []}, rpc:call(W, file_handles, list, [])),

    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_open_multiple_times_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),

    % here all operations should succeed
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(
        W, SessId1, ?FILE_REF(FileGuid), rdwr)
    ),
    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),

    % unload mock for open so that it will succeed again
    test_utils:mock_unload(W, storage_driver),

    {ok, Handle2} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 11}, lfm_proxy:write(W, Handle2, 9, <<" test_data2">>)),
    verify_file_content(Config, Handle2, <<"test_data test_data2">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle2)),

    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_open_failure_multiple_users(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {SessId2, _UserId2} = {
        ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user2">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name2/test_read">>),

    % here all operations should succeed
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(
        W, SessId2, {path, <<"/space_name2/test_read">>}, rdwr)
    ),
    ?assertEqual(0, get_session_file_handles_num(W, FileGuid, SessId2)),

    % check that user1 handle still exists
    ?assertEqual(1, get_session_file_handles_num(W, FileGuid, SessId1)),

    % unload mock for open so that operations will succeed again
    test_utils:mock_unload(W, storage_driver),

    % check that user1 can still use his handle
    ?assertEqual({ok, 11}, lfm_proxy:write(W, Handle, 9, <<" test_data2">>)),
    verify_file_content(Config, Handle, <<"test_data test_data2">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)),

    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_open_in_direct_mode(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr)),

    Context = rpc:call(W, ets, lookup_element, [lfm_handles, Handle, 2]),
    HandleId = lfm_context:get_handle_id(Context),
    ?assertEqual({error, not_found}, rpc:call(
        W, session_handles, get, [SessId1, HandleId])
    ),
    ?assertEqual(1, get_session_file_handles_num(W, FileGuid, SessId1)),

    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_mv_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),

    % simulate open error so that mv function will fail
    open_failure_mock(W),

    % file has to be moved to different space in order to use copy / delete
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:mv(
        W, SessId1, ?FILE_REF(FileGuid), <<"/space_name2/test_read2">>)
    ),
    ?assertEqual({ok, []}, rpc:call(W, file_handles, list, [])),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

lfm_mv_failure_multiple_users(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {MemEntriesBefore, CacheEntriesBefore} = get_mem_and_disc_entries(W),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    {SessId2, _UserId2} = {
        ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user2">>}, Config)
    },
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name2/test_read">>),

    % user1 succeeds to write to file using handle
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),

    % simulate open error so that mv function will fail
    open_failure_mock(W),

    % user2 fails to move file
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:mv(
        W, SessId2, ?FILE_REF(FileGuid), <<"/space_name3/test_read2">>)
    ),
    ?assertEqual(0, get_session_file_handles_num(W, FileGuid, SessId2)),
    {ok, Docs} = rpc:call(W, file_handles, list, []),
    ?assertEqual(1, length(Docs)),

    % unload mock for open so that operations will succeed again
    test_utils:mock_unload(W, storage_driver),

    % user1 handle should still exists
    ?assertEqual(1, get_session_file_handles_num(W, FileGuid, SessId1)),

    % check that user1 can still write to file using his handle
    ?assertEqual({ok, 11}, lfm_proxy:write(W, Handle, 9, <<" test_data2">>)),
    verify_file_content(Config, Handle, <<"test_data test_data2">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)),

    ?assertEqual(false, rpc:call(
        W, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    ),
    {MemEntriesAfter, CacheEntriesAfter} = get_mem_and_disc_entries(W),
    print_mem_and_disc_docs_diff(W, MemEntriesBefore, MemEntriesAfter,
        CacheEntriesBefore, CacheEntriesAfter).

readdir_plus_should_return_empty_result_for_empty_dir(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 0),
    verify_attrs(Config, MainDirPath, Files, 10, 0).

readdir_plus_should_return_empty_result_zero_size(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 10),
    verify_attrs(Config, MainDirPath, Files, 0, 0).

readdir_plus_should_work_with_zero_offset(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_attrs(Config, MainDirPath, Files, 5, 5).

readdir_plus_should_work_with_non_zero_offset(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_attrs(Config, MainDirPath, Files, 3, 3, 2).

readdir_plus_should_work_with_size_greater_than_dir_size(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_attrs(Config, MainDirPath, Files, 10, 5).

readdir_should_work_with_token(Config, DirSize, Type, InitialToken) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {MainDirPath, Files} = generate_dir(Config, DirSize),
    VerifyFun = case Type of
        readdir_plus -> fun verify_attrs_with_token/8;
        readdir -> fun verify_with_token/8
    end,
    Token = VerifyFun(Config, MainDirPath, Files, max(0, min(DirSize - 0, 3)), 3, 0, false, InitialToken),
    {ok, FoldCacheTimeout} = erpc:call(Worker, application, get_env, [?CLUSTER_WORKER_APP_NAME, fold_cache_timeout]),
    case InitialToken of
        ?INITIAL_API_LS_TOKEN ->
            ok = erpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, fold_cache_timeout, 0]);
        _ ->
            % only API listing token supports continued listing after datastore token expiration
            ok
    end,
    Token2 = VerifyFun(Config, MainDirPath, Files, max(0, min(DirSize - 3, 3)), 3, 3, false, Token),
    timer:sleep(timer:seconds(5)), % wait for flush of expired tokens
    ok = erpc:call(Worker, application, set_env, [?CLUSTER_WORKER_APP_NAME, fold_cache_timeout, FoldCacheTimeout]),
    Token3 = VerifyFun(Config, MainDirPath, Files, max(0, min(DirSize - 6, 3)), 3, 6, false, Token2),
    VerifyFun(Config, MainDirPath, Files, max(0, min(DirSize - 9, 3)), 3, 9, true, Token3).

readdir_should_work_with_startid(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 10),

    % list all files in chunks (use 0 offset for each chunk)
    StartId1 = verify_with_startid(Config, MainDirPath, Files, 0, 4, 0, 4, undefined),
    StartId2 = verify_with_startid(Config, MainDirPath, Files, 3, 4, 0, 4, StartId1),
    StartId3 = verify_with_startid(Config, MainDirPath, Files, 6, 3, 0, 3, StartId2),
    StartId4 = verify_with_startid(Config, MainDirPath, Files, 8, 2, 0, 3, StartId3),
    ?assertEqual(lists:last(Files), StartId4),

    % test ls with startid and positive offset
    StartId5 = verify_with_startid(Config, MainDirPath, Files, 4, 2, 4, 2, undefined),
    StartId6 = verify_with_startid(Config, MainDirPath, Files, 7, 3, 2, 4, StartId5),
    ?assertEqual(lists:last(Files), StartId6),

    % test ls with startid and offset beyond files num
    verify_with_startid(Config, MainDirPath, Files, 0, 0, 20, 4, StartId5),

    % test ls with startid and negative offset
    StartId7 = verify_with_startid(Config, MainDirPath, Files, 3, 4, -2, 4, StartId5),
    verify_with_startid(Config, MainDirPath, Files, 0, 6, -10, 6, StartId7).

get_children_details_should_return_empty_result_for_empty_dir(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 0),
    verify_details(Config, MainDirPath, Files, 0, 0, 10).

get_children_details_should_return_empty_result_zero_size(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 10),
    verify_details(Config, MainDirPath, Files, 0, 0, 0).

get_children_details_should_work_with_zero_offset(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_details(Config, MainDirPath, Files, 5, 0, 5).

get_children_details_should_work_with_non_zero_offset(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_details(Config, MainDirPath, Files, 2, 3, 3).

get_children_details_should_work_with_size_greater_than_dir_size(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 5),
    verify_details(Config, MainDirPath, Files, 5, 0, 10).

get_children_details_should_work_with_startid(Config) ->
    {MainDirPath, Files} = generate_dir(Config, 10),

    % list all files in chunks (use 0 offset for each chunk)
    StartId1 = verify_details(Config, MainDirPath, Files, 0, 4, 0, 4, undefined),
    StartId2 = verify_details(Config, MainDirPath, Files, 3, 4, 0, 4, StartId1),
    StartId3 = verify_details(Config, MainDirPath, Files, 6, 3, 0, 3, StartId2),
    StartId4 = verify_details(Config, MainDirPath, Files, 8, 2, 0, 3, StartId3),
    ?assertEqual(lists:last(Files), StartId4),

    % test ls with startid and positive offset
    StartId5 = verify_details(Config, MainDirPath, Files, 4, 2, 4, 2, undefined),
    StartId6 = verify_details(Config, MainDirPath, Files, 7, 3, 2, 4, StartId5),
    ?assertEqual(lists:last(Files), StartId6),

    % test ls with startid and offset beyond files num
    verify_details(Config, MainDirPath, Files, 0, 0, 20, 4, StartId5),

    % test ls with startid and negative offset
    StartId7 = verify_details(Config, MainDirPath, Files, 3, 4, -2, 4, StartId5),
    verify_details(Config, MainDirPath, Files, 0, 6, -10, 6, StartId7).


% NOTE: this test must be run first as it requires empty space
get_recursive_file_list(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    
    MainDir = generator:gen_name(),
    MainDirPath = <<"/space_name1/", MainDir/binary, "/">>,
    {ok, MainDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, MainDirPath)),
    
    Dirs = lists:sort(lists_utils:generate(fun generator:gen_name/0, 4)),
    Files = lists:sort(lists_utils:generate(fun generator:gen_name/0, 8)),
    AllExpectedFiles = lists:foldl(fun(D, FilesTmp) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, filename:join([MainDirPath, D]))),
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, filename:join([MainDirPath, D, <<"empty_dir">>]))),
        FilesTmp ++ lists:map(fun(F) ->
            {ok, G} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, filename:join([MainDirPath, D, F]))),
            {G, filename:join([MainDir, D, F])}
        end, Files)
    end, [], Dirs),
    
    SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(file_id:guid_to_space_id(MainDirGuid)),
    ResultMapper = fun
        ({ok, Res, IsLast}) ->
            {ok, lists:map(fun({Path, #file_attr{guid = Guid}}) -> {Guid, Path} end, Res), IsLast};
        (Other) ->
            Other
    end,
    lists:foreach(fun(DirToListGuid) ->
        ?assertMatch({ok, AllExpectedFiles, _}, 
            ResultMapper(lfm_proxy:get_files_recursively(Worker, SessId1, ?FILE_REF(DirToListGuid), <<>>, length(AllExpectedFiles)))),
        lists:foreach(fun(Num) ->
            {_, StartAfter} = lists:nth(Num, AllExpectedFiles),
            ExpectedTail = lists:nthtail(Num, AllExpectedFiles),
            ?assertMatch({ok, ExpectedTail, _}, 
                ResultMapper(lfm_proxy:get_files_recursively(Worker, SessId1, ?FILE_REF(DirToListGuid), StartAfter, length(AllExpectedFiles))))
        end, lists:seq(1, length(AllExpectedFiles))),
        
        lists:foreach(fun(Num) ->
            {_, StartAfter} = lists:nth(Num, AllExpectedFiles),
            ExpectedRes = case lists:nthtail(Num, AllExpectedFiles) of
                [File | _] -> [File];
                [] -> []
            end,
            ?assertMatch({ok, ExpectedRes, _}, 
                ResultMapper(lfm_proxy:get_files_recursively(Worker, SessId1, ?FILE_REF(DirToListGuid), StartAfter, 1)))
        end, lists:seq(1, length(AllExpectedFiles)))
    end, [MainDirGuid, SpaceDirGuid]),
    
    % check that listing regular file returns this file
    {Num, {Guid, Path}} = lists_utils:random_element(lists:zip(lists:seq(1, length(AllExpectedFiles)), AllExpectedFiles)),
    ?assertMatch({ok, [{Guid, Path}], true}, ResultMapper(lfm_proxy:get_files_recursively(Worker, SessId1, ?FILE_REF(Guid), <<>>, 1))),
    
    % check listing after removing file that StartAfter points to 
    ok = lfm_proxy:unlink(Worker, SessId1, ?FILE_REF(Guid)),
    ExpectedTail = lists:nthtail(Num, AllExpectedFiles),
    ?assertMatch({ok, ExpectedTail, _}, 
        ResultMapper(lfm_proxy:get_files_recursively(Worker, SessId1, ?FILE_REF(MainDirGuid), Path, length(AllExpectedFiles)))).
    

echo_loop(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, writes_num}, {value, 1000}, {description, "Number of write operations during "}]
        ]},
        {description, "Simulates loop of echo operations done by client"},
        {config, [{name, performance},
            {parameters, [
                [{name, writes_num}, {value, 10000}]
            ]},
            {description, "Basic performance configuration"}
        ]}
    ]).
echo_loop_base(Config) ->
    WritesNum = ?config(writes_num, Config),

    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    wait_for_cache_dump(Workers),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    File = generator:gen_name(),
    FilePath = <<"/space_name1/", File/binary, "/">>,
    ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, FilePath)),

    {WriteTime, _} = measure_execution_time(fun() ->
        lists:foldl(fun(N, Offset) ->
            {ok, Handle} = ?assertMatch({ok, _},
                lfm_proxy:open(Worker, SessId1, {path, FilePath}, write)),
            Bytes = integer_to_binary(N),
            BufSize = size(Bytes),
            ?assertMatch({ok, BufSize}, lfm_proxy:write(Worker, Handle, Offset, Bytes)),
            lfm_proxy:close(Worker, Handle),
            Offset + BufSize
        end, 0, lists:seq(1, WritesNum))
    end),

    #parameter{name = echo_time, value = WriteTime, unit = "us",
        description = "Aggregated time of all operations"}.

ls_with_stats(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, proc_num}, {value, 1}, {description, "Number of threads used during the test."}],
            [{name, dir_level}, {value, 10}, {description, "Level of test directory."}],
            [{name, dirs_num_per_proc}, {value, 10}, {description, "Number of dirs tested by single thread."}]
        ]},
        {description, "Tests performance of ls with gettin stats operation"},
        {config, [{name, low_level_single_thread_small_dir},
            {parameters, [
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, low_level_single_thread_large_dir},
            {parameters, [
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 100}]
            ]},
            {description, ""}
        ]},
        {config, [{name, low_level_10_threads_large_dir},
            {parameters, [
                [{name, proc_num}, {value, 10}],
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 10}]
            ]},
            {description, ""}
        ]},
        %%        {config, [{name, low_level_many_threads_large_dir},
        %%            {parameters, [
        %%                [{name, proc_num}, {value, 100}],
        %%                [{name, dir_level}, {value, 1}],
        %%                [{name, dirs_num_per_proc}, {value, 1}]
        %%            ]},
        %%            {description, ""}
        %%        ]},
        {config, [{name, high_level_single_thread_small_dir},
            {parameters, [
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, high_level_single_thread_large_dir},
            {parameters, [
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 100}]
            ]},
            {description, ""}
        ]},
        {config, [{name, high_level_10_threads_large_dir},
            {parameters, [
                [{name, proc_num}, {value, 10}],
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 10}]
            ]},
            {description, ""}
        ]}
        %%        {config, [{name, high_level_many_threads_large_dir},
        %%            {parameters, [
        %%                [{name, proc_num}, {value, 100}],
        %%                [{name, dir_level}, {value, 100}],
        %%                [{name, dirs_num_per_proc}, {value, 1}]
        %%            ]},
        %%            {description, ""}
        %%        ]}
    ]).
ls_with_stats_base(Config) ->
    % Get test and environment description
    DirLevel = ?config(dir_level, Config),
    ProcNum = ?config(proc_num, Config),
    DirsNumPerProc = ?config(dirs_num_per_proc, Config),

    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    wait_for_cache_dump(Workers),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Master = self(),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    % Generate names of dirs in test directory tree
    [LastTreeDir | _] = TreeDirsReversed = lists:foldl(fun(_, [H | _] = Acc) ->
        NewDir = <<H/binary, "/", (generator:gen_name())/binary>>,
        [NewDir | Acc]
    end, [<<"/space_name1">>], lists:seq(1, DirLevel)),
    [_ | TreeDirs] = lists:reverse(TreeDirsReversed),

    % Create dirs tree
    {CreateTreeTime, _} = measure_execution_time(fun() ->
        lists:foreach(fun(D) ->
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, D))
        end, TreeDirs)
    end),

    % Create dirs at last level of tree (to be listed)
    {CreateDirsTime, _} = measure_execution_time(fun() ->
        Fun = fun() ->
            lists:foreach(fun(_) ->
                D = <<LastTreeDir/binary, "/", (generator:gen_name())/binary>>,
                ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, D))
            end, lists:seq(1, DirsNumPerProc))
        end,
        case ProcNum of
            1 ->
                [Fun()];
            _ ->
                lists:foreach(fun(_) ->
                    spawn(fun() ->
                        Fun(),
                        report_success(Master)
                    end)
                end, lists:seq(1, ProcNum)),
                check_run_parallel_ans(ProcNum)
        end
    end),

    % List directory
    {LsTime, LSDirs} = measure_execution_time(fun() ->
        {ok, ListedDirs} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Worker, SessId1, {path, LastTreeDir}, 0, DirsNumPerProc * ProcNum)),
        ?assertEqual(DirsNumPerProc * ProcNum, length(ListedDirs)),
        ListedDirs
    end),

    % Stat listed directories
    {StatTime, _} = measure_execution_time(fun() ->
        Fun = fun(Dirs) ->
            lists:foreach(fun({D, _}) ->
                ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker, SessId1, ?FILE_REF(D)))
            end, Dirs)
        end,
        case ProcNum of
            1 ->
                Fun(LSDirs);
            _ ->
                {Dirs, _} = lists:foldl(fun(D, {[H | T] = Acc, Count}) ->
                    case Count =< DirsNumPerProc of
                        true ->
                            {[[D | H] | T], Count + 1};
                        _ ->
                            {[[D] | Acc], 1}
                    end
                end, {[[]], 0}, LSDirs),

                lists:foreach(fun(ProcDirs) ->
                    spawn(fun() ->
                        Fun(ProcDirs),
                        Master ! run_parallel_ok
                    end)
                end, Dirs),
                check_run_parallel_ans(ProcNum)
        end
    end),

    LsWithStatTime = LsTime + StatTime,

    [
        #parameter{name = create_tree_time, value = CreateTreeTime, unit = "us",
            description = "Time of test tree creation"},
        #parameter{name = create_dirs_time, value = CreateDirsTime, unit = "us",
            description = "Time of test dirs creation"},
        #parameter{name = ls_time, value = LsTime, unit = "us",
            description = "Time of ls operation"},
        #parameter{name = stat_time, value = StatTime, unit = "us",
            description = "Time of all stat operations"},
        #parameter{name = ls_stat_time, value = LsWithStatTime, unit = "us",
            description = "Total time of ls and all stat operations"}
    ].

ls(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, dir_size_multiplier}, {value, 1}, {description, "Parametr for dir size tunning."}]
        ]},
        {description, "Tests ls operation"},
        {config, [{name, medium_dir},
            {parameters, [
                [{name, dir_size_multiplier}, {value, 1}]
            ]},
            {description, ""}
        ]},
        {config, [{name, large_dir},
            {parameters, [
                [{name, dir_size_multiplier}, {value, 10}]
            ]},
            {description, ""}
        ]}
    ]).
ls_base(Config) ->
    DSM = ?config(dir_size_multiplier, Config),

    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    wait_for_cache_dump(Workers),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    MainDir = generator:gen_name(),
    MainDirPath = <<"/space_name1/", MainDir/binary, "/">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, MainDirPath)),

    VerifyLS = fun(Offset0, Limit0, ElementsList) ->
        Offset = Offset0 * DSM,
        Limit = Limit0 * DSM,
        {ok, ListedElements} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Worker, SessId1, {path, MainDirPath}, Offset, Limit)),
        {ok, ListedElements2} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Worker, SessId1, {path, MainDirPath}, 0, Offset)),
        {ok, ListedElements3} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Worker, SessId1, {path, MainDirPath}, Offset + Limit, length(ElementsList))),

        ?assertEqual({min(Limit, max(length(ElementsList) - Offset, 0)), min(Offset, length(ElementsList)),
            max(length(ElementsList) - Offset - Limit, 0)},
            {length(ListedElements), length(ListedElements2), length(ListedElements3)}),
        ?assertEqual(ElementsList,
            lists:sort(lists:map(fun({_, Name}) ->
                Name end, ListedElements ++ ListedElements2 ++ ListedElements3)))
    end,

    Files = lists:sort(lists:map(fun(_) ->
        generator:gen_name() end, lists:seq(1, 30 * DSM))),
    lists:foreach(fun(F) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, <<MainDirPath/binary, F/binary>>))
    end, Files),

    VerifyLS(0, 30, Files),
    VerifyLS(0, 4, Files),
    VerifyLS(0, 15, Files),
    VerifyLS(0, 23, Files),
    VerifyLS(12, 11, Files),
    VerifyLS(20, 3, Files),
    VerifyLS(22, 8, Files),
    VerifyLS(0, 40, Files),
    VerifyLS(30, 10, Files),
    VerifyLS(35, 5, Files),

    Dirs = lists:map(fun(_) ->
        generator:gen_name() end, lists:seq(1, 30 * DSM)),
    lists:foreach(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, <<MainDirPath/binary, D/binary>>))
    end, Dirs),
    FandD = lists:sort(Files ++ Dirs),

    VerifyLS(0, 60, FandD),
    VerifyLS(0, 23, FandD),
    VerifyLS(12, 11, FandD),
    VerifyLS(20, 3, FandD),
    VerifyLS(22, 8, FandD),
    VerifyLS(22, 23, FandD),
    VerifyLS(45, 5, FandD),
    VerifyLS(45, 15, FandD),
    VerifyLS(10, 35, FandD),

    {FinalLSTime, _} = measure_execution_time(fun() ->
        VerifyLS(0, 80, FandD)
    end),

    #parameter{name = final_ls_time, value = FinalLSTime, unit = "us",
        description = "Time of last full dir listing"}.


fslogic_new_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} =
        {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    RootUuid1 = get_guid_privileged(Worker, SessId1, <<"/space_name1">>),
    RootUuid2 = get_guid_privileged(Worker, SessId2, <<"/space_name2">>),

    Resp11 = ?file_req(Worker, SessId1, RootUuid1, #create_file{name = <<"test">>}),
    Resp21 = ?file_req(Worker, SessId2, RootUuid2, #create_file{name = <<"test">>}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp11),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_created{}}, Resp21),

    #fuse_response{fuse_response = #file_created{
        file_location = #file_location{
            file_id = FileId11,
            storage_id = StorageId11,
            provider_id = ProviderId11,
            storage_file_created = true
        }
    }} = Resp11,

    #fuse_response{fuse_response = #file_created{
        file_location = #file_location{
            file_id = FileId21,
            storage_id = StorageId21,
            provider_id = ProviderId21,
            storage_file_created = true
        }
    }} = Resp21,

    ?assertNotMatch(undefined, FileId11),
    ?assertNotMatch(undefined, FileId21),

    TestStorageId1 = initializer:get_supporting_storage_id(Worker, ?SPACE_ID1),
    TestStorageId2 = initializer:get_supporting_storage_id(Worker, ?SPACE_ID2),
    ?assertMatch(TestStorageId1, StorageId11),
    ?assertMatch(TestStorageId2, StorageId21),

    TestProviderId = rpc:call(Worker, oneprovider, get_id, []),
    ?assertMatch(TestProviderId, ProviderId11),
    ?assertMatch(TestProviderId, ProviderId21).

lfm_create_and_access(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} =
        {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    FilePath1 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath3 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath4 = <<"/space_name3/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath1, 8#240)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath2, 8#640)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath3, 8#670)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath4, 8#540)),

    %% File #1
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath1}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath1}, read)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, FilePath1}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, read)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath1}, 10)),

    %% File #2
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath2}, read)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, FilePath2}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath2}, 10)),

    %% File #3
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, rdwr)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),

    %% File #4
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath4}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath4}, read)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId1, {path, FilePath4}, 10)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath4}, 10)).

lfm_create_and_unlink(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} =
        {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    FilePath11 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath12 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath21 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath22 = <<"/space_name2/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath11)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath12)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId1, FilePath11)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath21)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath22)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId2, FilePath21)),

    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, FilePath11})),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId2, {path, FilePath21})),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId1, {path, FilePath11})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId2, {path, FilePath21})),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath11)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath21)).

lfm_create_failure(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },
    % intentionally set mode of file to mode typical for directory so that it has execute right
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name1/test_create_fail_dir">>, ?DEFAULT_DIR_PERMS)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:create(W, SessId1, <<"/space_name1/test_create_fail_dir/file">>)).

lfm_basic_rename(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_rename">>),

    lfm_proxy:mv(W, SessId1, ?FILE_REF(FileGuid), <<"/space_name1/test_rename2">>),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name1/test_rename">>})),
    {ok, Stats} = ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId1, ?FILE_REF(FileGuid))),
    ?assertEqual({ok, Stats}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name1/test_rename2">>})).

lfm_renaming_space_directory_should_fail(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    ?assertMatch({error, ?EPERM},
        lfm_proxy:mv(W, SessId1, {path, <<"/space_name1">>}, <<"/other_space_name">>)).

lfm_basic_rdwr(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),

    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),
    verify_file_content(Config, Handle, <<"test_data">>),

    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

lfm_basic_rdwr_opens_file_once(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),
    test_utils:mock_new(W, storage_driver, [passthrough]),
    test_utils:mock_assert_num_calls(W, storage_driver, open, 2, 0),

    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    test_utils:mock_assert_num_calls(W, storage_driver, open, 2, 1),

    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle, 0, <<"11111">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle, 5, <<"22222">>)),

    verify_file_content(Config, Handle, <<"1111122222">>),
    verify_file_content(Config, Handle, <<"11111">>, 0, 5),

    ?assertEqual(ok, lfm_proxy:close(W, Handle)),
    test_utils:mock_assert_num_calls(W, storage_driver, open, 2, 1),
    test_utils:mock_validate_and_unload(W, storage_driver).

lfm_basic_rdwr_after_file_delete(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read">>),
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    FileContent = <<"test_data">>,

    %remove file
    FileCtx = rpc:call(W, file_ctx, new_by_guid, [FileGuid]),
    {SDHandle, _} = rpc:call(W, storage_driver, new_handle, [SessId1, FileCtx]),
    ok = rpc:call(W, storage_driver, unlink, [SDHandle, size(FileContent)]),

    %read opened file
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, FileContent)),
    verify_file_content(Config, Handle, FileContent),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

lfm_write(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} =
        {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name1/test3">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name1/test4">>)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/space_name2/test3">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/space_name2/test4">>)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name1/test3">>}, rdwr),
    O12 = lfm_proxy:open(W, SessId1, {path, <<"/space_name1/test4">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    ?assertMatch({ok, _}, O12),

    {ok, Handle11} = O11,
    {ok, Handle12} = O12,

    WriteAndTest =
        fun(Worker, Handle, Offset, Bytes) ->
            Size = size(Bytes),
            ?assertMatch({ok, Size}, lfm_proxy:write(Worker, Handle, Offset, Bytes)),
            for(Offset, Offset + Size - 1,
                fun(I) ->
                    for(1, Offset + Size - I,
                        fun(J) ->
                            SubBytes = binary:part(Bytes, I - Offset, J),
                            ?assertMatch({ok, SubBytes}, lfm_proxy:read(Worker, Handle, I, J))
                        end)
                end)
        end,

    WriteAndTest(W, Handle11, 0, <<"abc">>),
    WriteAndTest(W, Handle12, 0, <<"abc">>),

    WriteAndTest(W, Handle11, 3, <<"def">>),
    WriteAndTest(W, Handle12, 3, <<"def">>),

    WriteAndTest(W, Handle11, 2, <<"qwerty">>),
    WriteAndTest(W, Handle12, 2, <<"qwerty">>),

    WriteAndTest(W, Handle11, 8, <<"zxcvbnm">>),
    WriteAndTest(W, Handle12, 8, <<"zxcvbnm">>),

    WriteAndTest(W, Handle11, 6, <<"qwerty">>),
    WriteAndTest(W, Handle12, 6, <<"qwerty">>),

    WriteAndTest(W, Handle11, 10, crypto:strong_rand_bytes(40)).


lfm_stat(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test5">>)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test5">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 3, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 2, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 9}, lfm_proxy:write(W, Handle11, 1, <<"123456789">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10).

lfm_get_details(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test5">>)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test5">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_details{
        file_attr = #file_attr{name = <<"space_name2">>, size = 0},
        index_startid = <<"space_id2">>,
        active_permissions_type = posix,
        has_metadata = false
    }}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2">>})),

    ?assertMatch({ok, #file_details{
        file_attr = #file_attr{name = <<"test5">>, size = 0},
        index_startid = <<"test5">>,
        active_permissions_type = posix,
        has_metadata = false
    }}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_details{file_attr = #file_attr{size = 3}}}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 3, <<"abc">>)),
    ?assertMatch({ok, #file_details{file_attr = #file_attr{size = 6}}}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 2, <<"abc">>)),
    ?assertMatch({ok, #file_details{file_attr = #file_attr{size = 6}}}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 9}, lfm_proxy:write(W, Handle11, 1, <<"123456789">>)),
    ?assertMatch({ok, #file_details{file_attr = #file_attr{size = 10}}}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch(ok, lfm_proxy:set_xattr(W, SessId1, ?FILE_REF(FileGuid), #xattr{name = <<"123456789">>, value = <<"!@#">>})),
    ?assertMatch({ok, #file_details{has_metadata = true}}, lfm_proxy:get_details(W, SessId1, {path, <<"/space_name2/test5">>}), 10).

lfm_synch_stat(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test6">>)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test6">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test6">>})),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 3}}}, lfm_proxy:write_and_check(W, Handle11, 0, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 3, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 2, <<"abc">>)),

    ?assertMatch({ok, 9, {ok, #file_attr{size = 10}}}, lfm_proxy:write_and_check(W, Handle11, 1, <<"123456789">>)).

lfm_cp_file(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    SpaceName = <<"space_name2">>,
    TestCaseDir = ?FUNCTION_NAME,
    SourceFile = <<"test_cp_source_file">>,
    TargetParent1 = <<"test_cp_target_parent1">>,
    TargetParent2 = <<"test_cp_target_parent2">>,
    TargetFile1 = <<"test_cp_target_file1">>,
    TargetFile2 = <<"test_cp_target_file2">>,
    TestCaseDirPath = filename:join([<<?DIRECTORY_SEPARATOR>>, SpaceName, TestCaseDir]),
    SourceFilePath = filename:join([TestCaseDirPath, SourceFile]),
    TargetParentPath1 = filename:join([TestCaseDirPath, TargetParent1]),
    TargetParentPath2 = filename:join([TestCaseDirPath, TargetParent2]),
    TargetFilePath1 = filename:join([TargetParentPath1, TargetFile1]),
    TargetFilePath2 = filename:join([TargetParentPath2, TargetFile2]),

    {ok, _} = lfm_proxy:mkdir(W, SessId1, TestCaseDirPath),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(W, SessId1, SourceFilePath, ?DEFAULT_FILE_PERMS),
    TestData = <<"test data">>,
    {ok, _} = lfm_proxy:write(W, Handle, 0, TestData),
    lfm_proxy:close(W, Handle),

    % create target dirs
    {ok, TargetParentGuid1} = lfm_proxy:mkdir(W, SessId1, TargetParentPath1),
    {ok, TargetParentGuid2} = lfm_proxy:mkdir(W, SessId1, TargetParentPath2),

    % copy to first target
    {ok, TargetGuid1} = ?assertMatch({ok, _}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), {path, TargetParentPath1}, TargetFile1)),

    % verify copied file
    ?assertMatch({ok, [{TargetGuid1, TargetFile1}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetParentGuid1), #{offset => 0, size => 10})),
    ?assertMatch({ok, #file_attr{guid = TargetGuid1}},
        lfm_proxy:stat(W, SessId1, {path, TargetFilePath1})),
    {ok, Handle2} = lfm_proxy:open(W, SessId1, {path, TargetFilePath1}, read),
    ?assertMatch({ok, TestData}, lfm_proxy:read(W, Handle2, 0, byte_size(TestData))),
    ok = lfm_proxy:close(W, Handle2),

    % copy to second target
    {ok, TargetGuid2} = ?assertMatch({ok, _}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), TargetFilePath2)),

    % verify copied file
    ?assertMatch({ok, [{TargetGuid2, TargetFile2}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetParentGuid2), #{offset => 0, size => 10})),
    ?assertMatch({ok, #file_attr{guid = TargetGuid2}},
        lfm_proxy:stat(W, SessId1, {path, TargetFilePath2})),
    {ok, Handle3} = lfm_proxy:open(W, SessId1, {path, TargetFilePath2}, read),
    ?assertMatch({ok, TestData}, lfm_proxy:read(W, Handle3, 0, byte_size(TestData))),
    ok = lfm_proxy:close(W, Handle3).


lfm_cp_empty_dir(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    SpaceName = <<"space_name2">>,
    TestCaseDir = ?FUNCTION_NAME,
    SourceDir = <<"test_cp_source_dir">>,
    TargetParent1 = <<"test_cp_target_parent1">>,
    TargetParent2 = <<"test_cp_target_parent2">>,
    TargetDir1 = <<"test_cp_target_dir1">>,
    TargetDir2 = <<"test_cp_target_dir2">>,
    TestCaseDirPath = filename:join([<<?DIRECTORY_SEPARATOR>>, SpaceName, TestCaseDir]),
    SourceDirPath = filename:join([TestCaseDirPath, SourceDir]),
    TargetParentPath1 = filename:join([TestCaseDirPath, TargetParent1]),
    TargetParentPath2 = filename:join([TestCaseDirPath, TargetParent2]),
    TargetDirPath1 = filename:join([TargetParentPath1, TargetDir1]),
    TargetDirPath2 = filename:join([TargetParentPath2, TargetDir2]),

    {ok, _} = lfm_proxy:mkdir(W, SessId1, TestCaseDirPath),

    {ok, Guid} = lfm_proxy:mkdir(W, SessId1, SourceDirPath),

    % create target dirs
    {ok, TargetParentGuid1} = lfm_proxy:mkdir(W, SessId1, TargetParentPath1),
    {ok, TargetParentGuid2} = lfm_proxy:mkdir(W, SessId1, TargetParentPath2),

    % copy to first target
    {ok, TargetGuid1} = ?assertMatch({ok, _}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), {path, TargetParentPath1}, TargetDir1)),

    % verify copied dir
    ?assertMatch({ok, [{TargetGuid1, TargetDir1}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetParentGuid1), #{offset => 0, size => 10})),
    ?assertMatch({ok, #file_attr{guid = TargetGuid1}},
        lfm_proxy:stat(W, SessId1, {path, TargetDirPath1})),

    % copy to second target
    {ok, TargetGuid2} = ?assertMatch({ok, _}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), TargetDirPath2)),

    % verify copied dir
    ?assertMatch({ok, [{TargetGuid2, TargetDir2}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetParentGuid2), #{offset => 0, size => 10})),
    ?assertMatch({ok, #file_attr{guid = TargetGuid2}},
        lfm_proxy:stat(W, SessId1, {path, TargetDirPath2})).

lfm_cp_dir_to_itself_should_fail(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    SpaceName = <<"space_name2">>,
    TestCaseDir = ?FUNCTION_NAME,
    SourceDir = <<"test_cp_source_dir">>,
    TestCaseDirPath = filename:join([<<?DIRECTORY_SEPARATOR>>, SpaceName, TestCaseDir]),
    SourceDirPath = filename:join([TestCaseDirPath, SourceDir]),

    {ok, _} = lfm_proxy:mkdir(W, SessId1, TestCaseDirPath),
    {ok, Guid} = lfm_proxy:mkdir(W, SessId1, SourceDirPath),

    % try to copy file to itself
    ?assertMatch({error, ?EINVAL}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), {path, SourceDirPath}, SourceDir)).

lfm_cp_dir_to_its_child_should_fail(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    SpaceName = <<"space_name2">>,
    TestCaseDir = ?FUNCTION_NAME,
    SourceDir = <<"test_cp_source_dir">>,
    ChildDir1 = <<"test_cp_child_dir1">>,
    ChildDir2 = <<"test_cp_child_dir2">>,
    TestCaseDirPath = filename:join([<<?DIRECTORY_SEPARATOR>>, SpaceName, TestCaseDir]),
    SourceDirPath = filename:join([TestCaseDirPath, SourceDir]),
    ChildDirPath1 = filename:join([SourceDirPath, ChildDir1]),
    ChildDirPath2 = filename:join([ChildDirPath1, ChildDir2]),

    {ok, _} = lfm_proxy:mkdir(W, SessId1, TestCaseDirPath),
    {ok, Guid} = lfm_proxy:mkdir(W, SessId1, SourceDirPath),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, ChildDirPath1),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, ChildDirPath2),

    % try to copy file to child
    ?assertMatch({error, ?EINVAL}, lfm_proxy:cp(W, SessId1, ?FILE_REF(Guid), {path, ChildDirPath2}, SourceDir)).


lfm_cp_dir(Config) ->
    % In this test, environment variable `ls_batch_size`,
    % which is responsible for size of children batches used when listing a directory,
    % is decreased to ensure that copying directory, which has more children
    % than size of a single batch, is performed correctly.
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    SpaceName = <<"space_name2">>,
    TestCaseDir = ?FUNCTION_NAME,
    SourceDir = <<"test_cp_source_dir">>,
    Child1 = <<"test_cp_child1">>,
    Child2 = <<"test_cp_child2">>,
    TargetParent1 = <<"test_cp_target_parent1">>,
    TargetDir1 = <<"test_cp_target_dir1">>,
    TestCaseDirPath = filename:join([<<?DIRECTORY_SEPARATOR>>, SpaceName, TestCaseDir]),
    SourceDirPath = filename:join([TestCaseDirPath, SourceDir]),
    TargetParentPath1 = filename:join([TestCaseDirPath, TargetParent1]),
    TargetDirPath1 = filename:join([TargetParentPath1, TargetDir1]),
    TargetChildPath1 = filename:join([TargetDirPath1, Child1]),
    TargetChildPath2 = filename:join([TargetDirPath1, Child2]),
    TestData = <<"test data">>,

    {ok, _} = lfm_proxy:mkdir(W, SessId1, TestCaseDirPath),

    % create source dir and its children
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId1, SourceDirPath),
    {ok, {_, Handle1}} = lfm_proxy:create_and_open(W, SessId1, DirGuid, Child1, ?DEFAULT_FILE_PERMS),
    {ok, {_, Handle2}} = lfm_proxy:create_and_open(W, SessId1, DirGuid, Child2, ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(W, Handle1, 0, TestData),
    {ok, _} = lfm_proxy:write(W, Handle2, 0, TestData),
    lfm_proxy:close(W, Handle1),
    lfm_proxy:close(W, Handle2),
    % it should be possible to copy file with all its children, even without the write permission
    NewMode = 8#555,
    ok = lfm_proxy:set_perms(W, SessId1, ?FILE_REF(DirGuid), NewMode),

    % create target dir
    {ok, TargetParentGuid1} = lfm_proxy:mkdir(W, SessId1, TargetParentPath1),

    % decrease batch_size to ensure that all files will be correctly copied
    ok = test_utils:set_env(W, op_worker, ls_batch_size, 1),

    % copy to target
    {ok, TargetGuid1} = ?assertMatch({ok, _}, lfm_proxy:cp(W, SessId1, ?FILE_REF(DirGuid), {path, TargetParentPath1}, TargetDir1)),

    % verify copied dir
    ?assertMatch({ok, [{TargetGuid1, TargetDir1}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetParentGuid1), #{offset => 0, size => 10})),
    ?assertMatch({ok, #file_attr{guid = TargetGuid1, mode = NewMode}},
        lfm_proxy:stat(W, SessId1, {path, TargetDirPath1})),

    % verify children of copied dir
    ?assertMatch({ok, [{_, Child1}, {_, Child2}], _},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(TargetGuid1), #{offset => 0, size => 10})),

    ?assertMatch({ok, #file_attr{name = Child1}}, lfm_proxy:stat(W, SessId1, {path, TargetChildPath1})),
    ?assertMatch({ok, #file_attr{name = Child2}}, lfm_proxy:stat(W, SessId1, {path, TargetChildPath2})),

    {ok, Handle3} = lfm_proxy:open(W, SessId1, {path, TargetChildPath1}, read),
    ?assertMatch({ok, TestData}, lfm_proxy:read(W, Handle3, 0, byte_size(TestData))),
    ok = lfm_proxy:close(W, Handle3),

    {ok, Handle4} = lfm_proxy:open(W, SessId1, {path, TargetChildPath2}, read),
    ?assertMatch({ok, TestData}, lfm_proxy:read(W, Handle4, 0, byte_size(TestData))),
    ok = lfm_proxy:close(W, Handle4).


lfm_truncate(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test7">>)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test7">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 1)),
    ?assertMatch({ok, #file_attr{size = 1}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    verify_file_content(Config, Handle11, <<"a">>),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 10)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    verify_file_content(Config, Handle11, <<"a">>, 0, 1),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 1, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 5)),
    ?assertMatch({ok, #file_attr{size = 5}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    verify_file_content(Config, Handle11, <<"aabc">>, 0, 4).

lfm_truncate_and_write(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    FilePath = <<"/space_name2/lfm_truncate_and_write1">>,
    FileKey = {path, FilePath},

    % Prepare file
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, FileKey, rdwr)),
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId, FileKey)),
    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId, FileKey), 10),

    % Test write right after truncate
    % (aggregation of write and truncate resulted in wrong file size before fixing it)
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId, FileKey, 0)),
    ?assertMatch({ok, 1}, lfm_proxy:write(W, Handle, 0, <<"a">>)),
    ?assertMatch({ok, #file_attr{size = 1}}, lfm_proxy:stat(W, SessId, FileKey), 10),
    verify_file_content(Config, Handle, <<"a">>),
    ?assertMatch({ok, [#{<<"blocks">> := [[0, 1]], <<"totalBlocksSize">> := 1}]},
        lfm_proxy:get_file_distribution(W, SessId, FileKey)),

    % Test truncate between writes - blocks should not be aggregated
    % and first block should be trimmed
    ?assertMatch({ok, 4}, lfm_proxy:write(W, Handle, 1, <<"bcde">>)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId, FileKey, 3)),
    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle, 5, <<"fgh">>)),
    ?assertMatch({ok, #file_attr{size = 8}}, lfm_proxy:stat(W, SessId, FileKey), 10),
    verify_file_content(Config, Handle, <<"abc\0\0fgh">>),
    ?assertMatch({ok, [#{<<"blocks">> := [[0, 3], [5, 3]], <<"totalBlocksSize">> := 6}]},
        lfm_proxy:get_file_distribution(W, SessId, FileKey)),

    % Test truncate between writes - blocks should not be aggregated
    % and first block should not be included in final result
    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle, 8, <<"ijk">>)),
    ?assertMatch(ok, lfm_proxy:truncate(W, SessId, FileKey, 8)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W, Handle, 9, <<"xy">>)),
    ?assertMatch({ok, #file_attr{size = 11}}, lfm_proxy:stat(W, SessId, FileKey), 10),
    verify_file_content(Config, Handle, <<"abc\0\0fgh\0xy">>),

    ?assertMatch({ok, [#{<<"blocks">> := [[0, 3], [5, 3], [9, 2]], <<"totalBlocksSize">> := 8}]},
        lfm_proxy:get_file_distribution(W, SessId, FileKey), 10),

    % Test truncate between writes - blocks should not be aggregated
    % and first block should be trimmed
    % Truncate only with event to prevent events flush when truncating storage file
    % As a result only metadata is changed by truncate - data on storage system remains
    ?assertMatch({ok, 4}, lfm_proxy:write(W, Handle, 11, <<"bcde">>)),
    ?assertMatch(ok, produce_truncate_event(W, SessId, FileKey, 13)),
    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle, 15, <<"fgh">>)),
    ?assertMatch({ok, #file_attr{size = 18}}, lfm_proxy:stat(W, SessId, FileKey), 10),
    verify_file_content(Config, Handle, <<"abc\0\0fgh\0xybcdefgh">>),
    ?assertMatch({ok, [#{<<"blocks">> := [[0, 3], [5, 3], [9, 4], [15, 3]], <<"totalBlocksSize">> := 13}]},
        lfm_proxy:get_file_distribution(W, SessId, FileKey)),

    % Test truncate between writes - blocks should not be aggregated
    % and first block should not be included in final result
    % Truncate only with event to prevent events flush when truncating storage file
    % As a result only metadata is changed by truncate - data on storage system remains
    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle, 18, <<"ijk">>)),
    ?assertMatch(ok, produce_truncate_event(W, SessId, FileKey, 18)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W, Handle, 19, <<"xy">>)),
    ?assertMatch({ok, #file_attr{size = 21}}, lfm_proxy:stat(W, SessId, FileKey), 10),
    verify_file_content(Config, Handle, <<"abc\0\0fgh\0xybcdefghixy">>),

    ?assertMatch({ok, [#{<<"blocks">> := [[0, 3], [5, 3], [9, 4], [15, 3], [19, 2]], <<"totalBlocksSize">> := 15}]},
        lfm_proxy:get_file_distribution(W, SessId, FileKey), 10).

lfm_acl(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    UserName1 = ?config({user_name, <<"user1">>}, Config),
    [{GroupId1, GroupName1} | _] = ?config({groups, <<"user1">>}, Config),
    FileName = <<"/space_name2/test_file_acl">>,
    DirName = <<"/space_name2/test_dir_acl">>,

    {ok, FileGUID} = lfm_proxy:create(W, SessId1, FileName),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, DirName),

    % test setting and getting acl
    Acl = [
        #access_control_entity{acetype = ?allow_mask, identifier = UserId1, name = UserName1, aceflags = ?no_flags_mask, acemask =
        ?read_all_object_mask bor ?write_all_object_mask},
        #access_control_entity{acetype = ?deny_mask, identifier = GroupId1, name = GroupName1, aceflags = ?identifier_group_mask, acemask = ?write_all_object_mask}
    ],
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, ?FILE_REF(FileGUID), Acl)),
    ?assertEqual({ok, Acl}, lfm_proxy:get_acl(W, SessId1, ?FILE_REF(FileGUID))).

lfm_rmdir(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    DirPath = <<"/space_name1/dir1">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, DirPath)),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, DirPath})).

lfm_rmdir_fails_with_eperm_on_space_directory(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    ?assertMatch({error, ?EPERM}, lfm_proxy:unlink(W, SessId1, {path, <<"/space_name1">>})).


rm_recursive(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Attempts = 10,
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirA = <<"/space_name1/a">>,
    DirB = <<"/space_name1/a/b">>,
    DirC = <<"/space_name1/a/c">>,
    FileG = <<"/space_name1/a/c/g">>,
    FileH = <<"/space_name1/a/c/h">>,
    DirD = <<"/space_name1/a/d">>,
    FileI = <<"/space_name1/a/d/i">>,
    DirE = <<"/space_name1/a/d/e">>,
    FileF = <<"/space_name1/a/f">>,
    DirX = <<"/space_name1/a/x">>,
    FileJ = <<"/space_name1/a/x/j">>,
    {ok, DirAGuid} = lfm_proxy:mkdir(W, SessId, DirA, 8#700),
    {ok, _DirBGuid} = lfm_proxy:mkdir(W, SessId, DirB, 8#300), % B won't be deleted as user doesn't have permissions to list it
    {ok, DirCGuid} = lfm_proxy:mkdir(W, SessId, DirC, 8#700),
    {ok, _DirDGuid} = lfm_proxy:mkdir(W, SessId, DirD, 8#700),
    {ok, _DirEGuid} = lfm_proxy:mkdir(W, SessId, DirE, 8#000), % E won't be deleted as user doesn't have permissions to list it
    {ok, DirXGuid} = lfm_proxy:mkdir(W, SessId, DirX, 8#700),
    {ok, FileFGuid} = lfm_proxy:create(W, SessId, FileF, 8#000),
    {ok, FileGGuid} = lfm_proxy:create(W, SessId, FileG, 8#000),
    {ok, FileHGuid} = lfm_proxy:create(W, SessId, FileH, 8#000),
    {ok, FileIGuid} = lfm_proxy:create(W, SessId, FileI, 8#000),
    {ok, _FileJGuid} = lfm_proxy:create(W, SessId, FileJ, 8#000),
    ok = lfm_proxy:set_perms(W, SessId, ?FILE_REF(DirXGuid), 8#500), % X won't be deleted as user doesn't have permissions to remove it's children

    % when
    ?assertEqual(ok, lfm_proxy:rm_recursive(W, SessId, ?FILE_REF(DirAGuid))),
    % rm_recursive returns ok, despite tha fact that
    % some files won't be deleted as user doesn't have perms to remove or even list them.
    % That is because rm_recursive moves files to the trash and tries to delete them asynchronously.

    % then
    % TODO VFS-7348 uncomment below tests after scheduling deletion as user not by root
%%    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirAGuid)), Attempts),
%%    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirBGuid)), Attempts),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirCGuid)), Attempts),
    % TODO VFS-7348 uncomment below tests after scheduling deletion as user not by root
    % ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirDGuid)), Attempts),
    % ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirEGuid)), Attempts),
    % ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(DirXGuid)), Attempts),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileFGuid)), Attempts),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileGGuid)), Attempts),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileHGuid)), Attempts),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileIGuid)), Attempts).
    % TODO VFS-7348 uncomment below tests after scheduling deletion as user not by root
    % ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileJGuid)), Attempts).

rm_recursive_fails_with_eperm_on_space_directory(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    ?assertMatch({error, ?EPERM}, lfm_proxy:rm_recursive(W, SessId1, {path, <<"/space_name1">>})).

file_gap(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {ok, Guid} = lfm_proxy:create(W, SessId, <<"/space_name2/f">>),
    {ok, Handle} = lfm_proxy:open(W, SessId, ?FILE_REF(Guid), rdwr),

    % when
    {ok, 3} = lfm_proxy:write(W, Handle, 3, <<"abc">>),
    ok = lfm_proxy:fsync(W, Handle),

    % then
    verify_file_content(Config, Handle, <<0, 0, 0, $a, $b, $c>>, 0, 6),
    verify_file_content(Config, Handle, <<0, 0, 0, $a, $b, $c>>),

    % when
    {ok, 4} = lfm_proxy:write(W, Handle, 8, <<"defg">>),
    ok = lfm_proxy:fsync(W, Handle),

    % then
    verify_file_content(Config, Handle,
        <<0, 0, 0, $a, $b, $c, 0, 0, $d, $e, $f, $g>>, 0, 12).

create_share_dir(Config) ->
    [W | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId = ?config({user_id, <<"user1">>}, Config),
    Path = <<"/space_name1/share_dir">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessId, Path, 8#700),
    SpaceId = file_id:guid_to_space_id(Guid),

    % Make sure SPACE_MANAGE_SHARES priv is accounted
    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin() -- [?SPACE_MANAGE_SHARES]
    ),
    ?assertMatch(?ERROR_POSIX(?EPERM), opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)),

    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin()
    ),

    % User root dir can not be shared
    ?assertMatch(
        ?ERROR_POSIX(?EPERM),
        opt_shares:create(W, SessId, ?FILE_REF(fslogic_uuid:user_root_dir_guid(UserId)), <<"share_name">>)
    ),
    % But space dir can
    ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)), <<"share_name">>)
    ),
    % As well as normal directory
    {ok, ShareId1} = ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)
    ),
    % Multiple times at that
    {ok, ShareId2} = ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)
    ),
    ?assertNotEqual(ShareId1, ShareId2).

create_share_file(Config) ->
    [W | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId = ?config({user_id, <<"user1">>}, Config),
    Path = <<"/space_name1/share_file">>,
    {ok, Guid} = lfm_proxy:create(W, SessId, Path, 8#700),
    SpaceId = file_id:guid_to_space_id(Guid),

    % Make sure SPACE_MANAGE_SHARES priv is accounted
    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin() -- [?SPACE_MANAGE_SHARES]
    ),
    ?assertMatch(?ERROR_POSIX(?EPERM), opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)),

    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin()
    ),
    {ok, ShareId1} = ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)
    ),
    % File can be shared multiple times
    {ok, ShareId2} = ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)
    ),
    ?assertNotEqual(ShareId1, ShareId2).

remove_share(Config) ->
    [W | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId = ?config({user_id, <<"user1">>}, Config),
    DirPath = <<"/space_name1/share_dir">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#704),
    SpaceId = file_id:guid_to_space_id(Guid),
    {ok, ShareId1} = opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>),

    % Make sure SPACE_MANAGE_SHARES priv is accounted
    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin() -- [?SPACE_MANAGE_SHARES]
    ),
    ?assertMatch(?ERROR_POSIX(?EPERM), opt_shares:remove(W, SessId, ShareId1)),

    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin()
    ),

    % Remove share by share Id
    ?assertMatch(ok, opt_shares:remove(W, SessId, ShareId1)),
    % ShareId no longer exists -> {error, not_found}
    ?assertMatch(?ERROR_NOT_FOUND, opt_shares:remove(W, SessId, ShareId1)).

share_getattr(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    ProviderId = ?GET_DOMAIN_BIN(W),
    OwnerSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirPath = <<SpaceName/binary, "/share_dir2">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, OwnerSessId, DirPath, 8#704),
    {ok, ShareId1} = opt_shares:create(W, OwnerSessId, ?FILE_REF(DirGuid), <<"share_name">>),
    {ok, ShareId2} = opt_shares:create(W, OwnerSessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ?assertNotEqual(ShareId1, ShareId2),

    ShareGuid = file_id:guid_to_share_guid(DirGuid, ShareId1),

    {ok, #file_attr{uid = Uid, gid = Gid}} = ?assertMatch(
        {ok, #file_attr{
            mode = 8#704,
            name = <<"share_dir2">>,
            type = ?DIRECTORY_TYPE,
            guid = DirGuid,
            parent_guid = SpaceGuid,
            owner_id = UserId,
            provider_id = ProviderId,
            shares = [ShareId2, ShareId1]}
        },
        lfm_proxy:stat(W, OwnerSessId, ?FILE_REF(DirGuid))
    ),
    ?assertNotMatch({Uid, Gid}, {?SHARE_UID, ?SHARE_GID}),

    lists:foreach(fun(SessId) ->
        ?assertMatch(
            {ok, #file_attr{
                mode = 8#004,                 % only 'other' bits should be shown
                name = <<"share_dir2">>,
                type = ?DIRECTORY_TYPE,
                guid = ShareGuid,
                uid = ?SHARE_UID,
                gid = ?SHARE_GID,
                parent_guid = undefined,      % share root should not point to any parent
                owner_id = <<"unknown">>,
                provider_id = <<"unknown">>,
                shares = [ShareId1]}          % other shares shouldn't be shown
            },
            lfm_proxy:stat(W, SessId, ?FILE_REF(ShareGuid))
        )
    end, [OwnerSessId, ?GUEST_SESS_ID]).

share_get_parent(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirPath = <<SpaceName/binary, "/share_get_parent">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, FileGuid} = lfm_proxy:create(W, SessId, <<DirPath/binary, "/file">>, 8#700),

    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Getting parent of dir should return space guid
    ?assertMatch({ok, SpaceGuid}, lfm_proxy:get_parent(W, SessId, ?FILE_REF(DirGuid))),
    % Getting parent of dir when accessing it in share mode should return undefined
    % as dir is share root
    ?assertMatch({ok, undefined}, lfm_proxy:get_parent(W, SessId, ?FILE_REF(ShareDirGuid))),

    % Getting file parent in normal mode should return dir guid
    ?assertMatch({ok, DirGuid}, lfm_proxy:get_parent(W, SessId, ?FILE_REF(FileGuid))),
    % Getting file parent in share mode should return share dir guid
        ?assertMatch({ok, ShareDirGuid}, lfm_proxy:get_parent(W, SessId, ?FILE_REF(ShareFileGuid))).

share_list(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir3">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    {ok, Guid1} = lfm_proxy:mkdir(W, SessId, <<"/space_name1/share_dir3/1">>, 8#700),
    {ok, Guid2} = lfm_proxy:mkdir(W, SessId, <<"/space_name1/share_dir3/2">>, 8#700),
    {ok, Guid3} = lfm_proxy:create(W, SessId, <<"/space_name1/share_dir3/3">>, 8#700),
    ChildrenShareGuids = lists:map(fun({Guid, Name}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name}
    end, [{Guid1, <<"1">>}, {Guid2, <<"2">>}, {Guid3, <<"3">>}]),

    {ok, Result} = ?assertMatch({ok, _}, lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareDirGuid), 0, 10)),
    ?assertMatch(ChildrenShareGuids, Result).

share_read(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir4">>,
    FilePath = <<"/space_name1/share_dir4/share_file">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, FileGuid} = lfm_proxy:create(W, SessId, FilePath, 8#707),
    {ok, Handle} = lfm_proxy:open(W, SessId, ?FILE_REF(FileGuid), write),
    {ok, 4} = lfm_proxy:write(W, Handle, 0, <<"data">>),
    ok = lfm_proxy:close(W, Handle),
    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ShareGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    {ok, [{ShareChildGuid, <<"share_file">>}]} = lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareGuid), 0, 10),

    {ok, FileShareHandle} =
        ?assertMatch({ok, <<_/binary>>}, lfm_proxy:open(W, ?GUEST_SESS_ID, ?FILE_REF(ShareChildGuid), read)),
    verify_file_content(Config, FileShareHandle, <<"data">>, 0, 4),
    ?assertEqual(ok, lfm_proxy:close(W, FileShareHandle)).

share_child_getattr(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir5">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, _} = lfm_proxy:create(W, SessId, <<"/space_name1/share_dir5/file">>, 8#700),
    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>),
    ShareDirGuid = file_id:guid_to_share_guid(Guid, ShareId),

    {ok, [{ShareChildGuid, _}]} = lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareDirGuid), 0, 1),

    ?assertMatch(
        {ok, #file_attr{
            mode = 8#000,                   % only 'other' bits should be shown
            name = <<"file">>,
            type = ?REGULAR_FILE_TYPE,
            guid = ShareChildGuid,
            parent_guid = ShareDirGuid,
            shares = []
        }},
        lfm_proxy:stat(W, ?GUEST_SESS_ID, ?FILE_REF(ShareChildGuid))
    ).

share_child_list(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir6">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    {ok, Guid1} = lfm_proxy:mkdir(W, SessId, <<"/space_name1/share_dir6/1">>, 8#707),
    {ok, [{ShareChildGuid, _}]} = lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareDirGuid), 0, 1),
    ExpShareChildGuid = file_id:guid_to_share_guid(Guid1, ShareId),
    ?assertMatch(ExpShareChildGuid, ShareChildGuid),

    {ok, Guid2} = lfm_proxy:mkdir(W, SessId, <<"/space_name1/share_dir6/1/2">>, 8#707),
    {ok, Guid3} = lfm_proxy:create(W, SessId, <<"/space_name1/share_dir6/1/3">>, 8#707),
    ShareChildrenShareGuids = lists:map(fun({Guid, Name}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name}
    end, [{Guid2, <<"2">>}, {Guid3, <<"3">>}]),

    ?assertMatch(
        {ok, ShareChildrenShareGuids},
        lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareChildGuid), 0, 10)
    ).

share_child_read(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir7">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>),
    ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),

    Path = <<"/space_name1/share_dir7/file">>,
    {ok, FileGuid} = lfm_proxy:create(W, SessId, Path, 8#707),
    {ok, Handle} = lfm_proxy:open(W, SessId, ?FILE_REF(FileGuid), write),
    {ok, 4} = lfm_proxy:write(W, Handle, 0, <<"data">>),
    ok = lfm_proxy:close(W, Handle),
    {ok, [{ShareFileGuid, _}]} = lfm_proxy:get_children(W, ?GUEST_SESS_ID, ?FILE_REF(ShareGuid), 0, 1),

    {ok, ShareHandle} =
        ?assertMatch({ok, <<_/binary>>}, lfm_proxy:open(W, ?GUEST_SESS_ID, ?FILE_REF(ShareFileGuid), read)),
    verify_file_content(Config, ShareHandle, <<"data">>, 0, 4),
    ?assertEqual(ok, lfm_proxy:close(W, ShareHandle)).

share_permission_denied(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirPath = <<"/space_name1/share_dir8">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(W, ?GUEST_SESS_ID, ?FILE_REF(Guid))).

storage_file_creation_should_be_deferred_until_open(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_read1">>),
    FileCtx = rpc:call(W, file_ctx, new_by_guid, [FileGuid]),
    {SDHandle, _} = rpc:call(W, storage_driver, new_handle, [SessId1, FileCtx]),

    % verify that storage file does not exist
    ?assertEqual({error, ?ENOENT}, rpc:call(W, storage_driver, stat, [SDHandle])),

    % open file
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),

    % verify that storage file exists
    ?assertMatch({ok, _}, rpc:call(W, storage_driver, stat, [SDHandle])),
    verify_file_content(Config, Handle, <<"test_data">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

deferred_creation_should_not_prevent_mv(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_move">>),

    % move empty file
    lfm_proxy:mv(W, SessId1, ?FILE_REF(FileGuid), <<"/space_name1/test_move2">>),

    % verify rdwr
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),
    verify_file_content(Config, Handle, <<"test_data">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

deferred_creation_should_not_prevent_truncate(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    ProviderId = rpc:call(W, oneprovider, get_id, []),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_truncate">>),

    % truncate file not existing on storage
    ?assertEqual(ok, lfm_proxy:truncate(W, SessId1, ?FILE_REF(FileGuid), 10)),
    ?assertEqual(ok, lfm_proxy:fsync(W, SessId1, ?FILE_REF(FileGuid), ProviderId)),

    % verify rdwr
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, ?FILE_REF(FileGuid))),
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),
    verify_file_content(Config, Handle, <<"test_data">>),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

new_file_should_not_have_popularity_doc(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    % when
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_no_popularity">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % then
    ?assertEqual(
        {error, not_found},
        rpc:call(W, file_popularity, get, [FileUuid])
    ).

new_file_should_have_zero_popularity(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    % when
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_zero_popularity">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    % then
    ?assertMatch(
        {ok, #document{
            key = FileUuid,
            value = #file_popularity{
                file_uuid = FileUuid,
                space_id = SpaceId,
                last_open = 0,
                open_count = 0,
                hr_mov_avg = 0.0,
                dy_mov_avg = 0.0,
                mth_mov_avg = 0.0
            }
        }},
        rpc:call(W, file_popularity, get_or_default, [file_ctx:new_by_uuid(FileUuid, SpaceId)])
    ).

opening_file_should_increase_file_popularity(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/test_increased_popularity">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    ok = rpc:call(W, file_popularity_api, enable, [SpaceId]),

    % when
    TimeBeforeFirstOpen = rpc:call(W, global_clock, timestamp_hours, []),
    {ok, Handle1} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), read),
    lfm_proxy:close(W, Handle1),

    % then
    {ok, Doc} = ?assertMatch(
        {ok, #document{
            key = FileUuid,
            value = #file_popularity{
                file_uuid = FileUuid,
                space_id = SpaceId,
                open_count = 1,
                hr_hist = [1 | _],
                dy_hist = [1 | _],
                mth_hist = [1 | _]
            }
        }},
        rpc:call(W, file_popularity, get_or_default, [file_ctx:new_by_uuid(FileUuid, SpaceId)])
    ),
    ?assert(TimeBeforeFirstOpen =< Doc#document.value#file_popularity.last_open),

    % when
    TimeBeforeSecondOpen = rpc:call(W, global_clock, timestamp_hours, []),
    lists:foreach(fun(_) ->
        {ok, Handle2} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), read),
        lfm_proxy:close(W, Handle2)
    end, lists:seq(1, 23)),

    % then
    {ok, Doc2} = ?assertMatch(
        {ok, #document{
            value = #file_popularity{
                open_count = 24,
                hr_mov_avg = 1.0,
                dy_mov_avg = 0.8,
                mth_mov_avg = 2.0
            }
        }},
        rpc:call(W, file_popularity, get_or_default, [file_ctx:new_by_uuid(FileUuid, SpaceId)])
    ),
    ?assert(TimeBeforeSecondOpen =< Doc2#document.value#file_popularity.last_open),
    [FirstHour, SecondHour | _] = Doc2#document.value#file_popularity.hr_hist,
    [FirstDay, SecondDay | _] = Doc2#document.value#file_popularity.hr_hist,
    [FirstMonth, SecondMonth | _] = Doc2#document.value#file_popularity.hr_hist,
    ?assertEqual(24, FirstHour + SecondHour),
    ?assertEqual(24, FirstDay + SecondDay),
    ?assertEqual(24, FirstMonth + SecondMonth).

file_popularity_should_have_correct_file_size(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/file_to_check_size">>),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    ok = rpc:call(W, file_popularity_api, enable, [SpaceId]),

    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), write),
    {ok, 5} = lfm_proxy:write(W, Handle, 0, <<"01234">>),
    ok = lfm_proxy:close(W, Handle),

    FileUuid = file_id:guid_to_uuid(FileGuid),
    ?assertMatch(
        {ok, #document{value = #file_popularity{size = 5}}},
        rpc:call(W, file_popularity, get, [FileUuid])
    ),

    {ok, Handle2} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), write),
    {ok, 5} = lfm_proxy:write(W, Handle2, 5, <<"01234">>),
    ok = lfm_proxy:close(W, Handle2),

    ?assertMatch(
        {ok, #document{value = #file_popularity{size = 10}}},
        rpc:call(W, file_popularity, get, [FileUuid])
    ),

    ok = lfm_proxy:truncate(W, SessId1, ?FILE_REF(FileGuid), 1),
    {ok, Handle3} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(W, Handle3),

    ?assertMatch(
        {ok, #document{value = #file_popularity{size = 1}}},
        rpc:call(W, file_popularity, get, [FileUuid])
    ).

sparse_files_should_be_created(Config, ReadFun) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    ProviderId = rpc:call(W, oneprovider, get_id, []),

    % Hole between not empty blocks
    {ok, FileGuid1} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid1, 0),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid1, 10),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid1, 11, [[0, 1], [10, 1]]),

    % Hole before single block
    {ok, FileGuid2} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid2, 10),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid2, 11, [[10, 1]]),

    % Empty block write to not empty file
    {ok, FileGuid3} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid3, 0),
    file_ops_test_utils:empty_write_to_file(W, SessId1, FileGuid3, 10),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid3, 10, [[0, 1]]),

    % Empty block write to empty file
    {ok, FileGuid4} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:empty_write_to_file(W, SessId1, FileGuid4, 10),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid4, 10, []),

    % Empty block write in the middle of not empty file
    {ok, FileGuid5} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid5, 10),
    file_ops_test_utils:empty_write_to_file(W, SessId1, FileGuid5, 5),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid5, 11, [[10, 1]]),

    % Creation of hole using truncate on not empty file
    {ok, FileGuid6} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    file_ops_test_utils:write_byte_to_file(W, SessId1, FileGuid6, 0),
    ?assertEqual(ok, lfm_proxy:truncate(W, SessId1, ?FILE_REF(FileGuid6), 10)),
    ?assertEqual(ok, lfm_proxy:fsync(W, SessId1, ?FILE_REF(FileGuid6), ProviderId)),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid6, 10, [[0, 1]]),

    % Creation of hole using truncate on empty file
    {ok, FileGuid7} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    ?assertEqual(ok, lfm_proxy:truncate(W, SessId1, ?FILE_REF(FileGuid7), 10)),
    ?assertEqual(ok, lfm_proxy:fsync(W, SessId1, ?FILE_REF(FileGuid7), ProviderId)),
    verify_sparse_file(ReadFun, W, SessId1, FileGuid7, 10, []).

lfm_close_deleted_open_files(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    
    {ok, DirGuid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1,
        <<"/space_name1/", (generator:gen_name())/binary>>)),
    {ok, DirGuid2} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, DirGuid1, generator:gen_name(), ?DEFAULT_DIR_MODE)),
    {ok, {FileGuid1, Handle1}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, DirGuid2, generator:gen_name(), ?DEFAULT_FILE_MODE)),
    {ok, {_FileGuid2, Handle2}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(W, SessId1, DirGuid2, generator:gen_name(), ?DEFAULT_FILE_MODE)),
    {ok, _} = lfm_proxy:write(W, Handle1, 0, <<"some_text">>),
    {ok, _} = lfm_proxy:write(W, Handle2, 0, <<"another_text">>),
    
    ok = ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, ?FILE_REF(FileGuid1))),
    ok = ?assertEqual(ok, lfm_proxy:rm_recursive(W, SessId1, ?FILE_REF(DirGuid1))),
    
    ok = ?assertEqual(ok, lfm_proxy:close(W, Handle1)),
    ok = ?assertEqual(ok, lfm_proxy:close(W, Handle2)).
    

%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_sparse_file(ReadFun, W, SessId, FileGuid, FileSize, ExpectedBlocks) ->
    BlocksSize = lists:foldl(fun([_, Size], Acc) -> Acc + Size end, 0, ExpectedBlocks),
    ?assertMatch({ok, [#{<<"blocks">> := ExpectedBlocks, <<"totalBlocksSize">> := BlocksSize}]},
        lfm_proxy:get_file_distribution(W, SessId, ?FILE_REF(FileGuid))),

    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(W, SessId, ?FILE_REF(FileGuid))),

    ExpectedFileContent = file_ops_test_utils:get_sparse_file_content(ExpectedBlocks, FileSize),
    {ok, Handle} = lfm_proxy:open(W, SessId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, ExpectedFileContent}, lfm_proxy:ReadFun(W, Handle, 0, 100)),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

open_failure_mock(Worker) ->
    % mock for open error - note that error is raised after
    % register_open is performed
    test_utils:mock_expect(Worker, storage_driver, open,
        fun(SDHandle2, Flag) ->
            meck:passthrough([SDHandle2, Flag]),
            throw(error)
        end).

print_mem_and_disc_docs_diff(Worker, MemEntriesBefore, CacheEntriesBefore,
    MemEntriesAfter, CacheEntriesAfter) ->
    MemDiff = pool_utils:get_documents_diff(Worker, MemEntriesAfter,
        MemEntriesBefore),
    CacheDiff = pool_utils:get_documents_diff(Worker, CacheEntriesAfter,
        CacheEntriesBefore),
    ct:pal("~n MemRes: ~p ~n~n CacheRes: ~p ~n", [MemDiff, CacheDiff]).

get_mem_and_disc_entries(Worker) ->
    {MemEntries, _} = pool_utils:get_pools_entries_and_sizes(Worker, memory),
    {DiscEntries, _} = pool_utils:get_pools_entries_and_sizes(Worker, disc),
    {MemEntries, DiscEntries}.

get_session_file_handles_num(W, FileGuid, SessionId) ->
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, [#document{key = FileUuid, value = FileHandlesRec} | _]} = rpc:call(
        W, file_handles, list, []
    ),
    Descriptors = FileHandlesRec#file_handles.descriptors,
    case maps:find(SessionId, Descriptors) of
        {ok, HandlesNum} ->
            HandlesNum;
        error ->
            0
    end.

%% Get guid of given by path file. Possible as root to bypass permissions checks.
get_guid_privileged(Worker, SessId, Path) ->
    get_guid(Worker, SessId, Path).

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.

for(From, To, Fun) ->
    for(From, To, 1, Fun).
for(From, To, Step, Fun) ->
    [Fun(I) || I <- lists:seq(From, To, Step)].

measure_execution_time(Fun) ->
    Stopwatch = stopwatch:start(),
    Ans = Fun(),
    {stopwatch:read_micros(Stopwatch), Ans}.

check_run_parallel_ans(0) ->
    ok;
check_run_parallel_ans(Num) ->
    RStatus = receive
        run_parallel_ok ->
            ok
    after
        100000 ->
            timeout
    end,
    ?assertEqual(ok, RStatus),
    check_run_parallel_ans(Num - 1).

report_success(Master) ->
    Master ! run_parallel_ok.

wait_for_cache_dump(Workers) ->
    lists:foreach(fun(W) ->
        rpc:call(W, caches_controller, wait_for_cache_dump, [])
    end, Workers).

generate_dir(Config, Size) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    MainDir = generator:gen_name(),
    MainDirPath = <<"/space_name1/", MainDir/binary, "/">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, MainDirPath)),

    case Size of
        0 ->
            {MainDirPath, []};
        _ ->
            Files = lists:sort(lists:map(fun(_) ->
                generator:gen_name() end, lists:seq(1, Size))),
            lists:foreach(fun(F) ->
                ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, <<MainDirPath/binary, F/binary>>))
            end, Files),

            {MainDirPath, Files}
    end.

verify_attrs(Config, MainDirPath, Files, Limit, ExpectedSize) ->
    verify_attrs(Config, MainDirPath, Files, Limit, ExpectedSize, 0).

verify_attrs(Config, MainDirPath, Files, Limit, ExpectedSize, Offset) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    Ans = lfm_proxy:get_children_attrs(Worker, SessId1, {path, MainDirPath}, #{offset => Offset, size => Limit}),
    {ok, List, _} = ?assertMatch({ok, _, _}, Ans),
    ?assertEqual(ExpectedSize, length(List)),

    lists:foreach(fun({F1, F2}) ->
        ?assertEqual(F1#file_attr.name, F2)
    end, lists:zip(List, lists:sublist(Files, Offset + 1, ExpectedSize))).

verify_attrs_with_token(Config, MainDirPath, Files, ExpectedSize, Limit, Offset, IsLast, Token) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    Ans = lfm_proxy:get_children_attrs(Worker, SessId1, {path, MainDirPath}, #{size => Limit, token => Token}),
    {ok, List, #{token := Token2, is_last := IL}} = ?assertMatch({ok, _, _}, Ans),
    ?assertEqual(ExpectedSize, length(List)),

    lists:foreach(fun({F1, F2}) ->
        ?assertEqual(F1#file_attr.name, F2)
    end, lists:zip(List, lists:sublist(Files, Offset + 1, ExpectedSize))),
    ?assertEqual(IsLast, IL),
    Token2.

verify_with_token(Config, MainDirPath, Files, ExpectedSize, Limit, Offset, IsLast, Token) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    Ans = lfm_proxy:get_children(Worker, SessId1, {path, MainDirPath}, #{size => Limit, token => Token}),
    {ok, List, #{token := Token2, is_last := IL}} = ?assertMatch({ok, _, _}, Ans),
    ?assertEqual(ExpectedSize, length(List)),

    lists:foreach(fun({{_, F1}, F2}) ->
        ?assertEqual(F1, F2)
    end, lists:zip(List, lists:sublist(Files, Offset + 1, ExpectedSize))),
    ?assertEqual(IsLast, IL),
    Token2.

verify_with_startid(Config, MainDirPath, Files, FilesOffset, ExpectedSize, Offset, Limit, StartId) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    Ans = lfm_proxy:get_children(Worker, SessId1, {path, MainDirPath}, #{
        offset => Offset,
        size => Limit,
        last_name => StartId
    }),
    {ok, List, _} = ?assertMatch({ok, _, _}, Ans),
    ?assertEqual(ExpectedSize, length(List)),

    lists:foreach(fun({{_, F1}, F2}) ->
        ?assertEqual(F1, F2)
    end, lists:zip(List, lists:sublist(Files, FilesOffset + 1, ExpectedSize))),

    case List of
        [_ | _] ->
            {_, NextStartId} = lists:last(List),
            NextStartId;
        _ ->
            undefined
    end.

verify_details(Config, MainDirPath, Files, ExpectedSize, Offset, Limit) ->
    verify_details(Config, MainDirPath, Files, Offset, ExpectedSize, Offset, Limit, undefined).

verify_details(Config, MainDirPath, Files, FilesOffset, ExpectedSize, Offset, Limit, StartId) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    {ok, List, _} = ?assertMatch(
        {ok, _, _},
        lfm_proxy:get_children_details(Worker, SessId1, {path, MainDirPath}, #{offset => Offset, size => Limit, last_name => StartId})
    ),
    ?assertEqual(ExpectedSize, length(List)),

    lists:foreach(fun({F1, F2}) ->
        ?assertEqual(F1#file_details.file_attr#file_attr.name, F2)
    end, lists:zip(List, lists:sublist(Files, FilesOffset + 1, ExpectedSize))),

    case List of
        [_ | _] ->
            LastFile = lists:last(List),
            LastFile#file_details.index_startid;
        _ ->
            undefined
    end.

verify_file_content(Config, Handle, FileContent) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, Handle, 0, size(FileContent))).

verify_file_content(Config, Handle, FileContent, From, To) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, Handle, From, To)).

produce_truncate_event(Worker, SessId, FileKey, Size) ->
    {ok, FileGuid} = rpc:call(Worker, lfm_file_key, ensure_guid, [SessId, FileKey]),
    ok = rpc:call(Worker, lfm_event_emitter, emit_file_truncated, [FileGuid, Size, SessId]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils, ?MODULE]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    initializer:unmock_auth_manager(Config).


init_per_testcase(Case, Config) when
    Case =:= lfm_open_in_direct_mode_test;
    Case =:= lfm_recreate_handle_test;
    Case =:= lfm_write_after_create_no_perms_test;
    Case =:= lfm_recreate_handle_after_delete_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_ctx, [passthrough]),
    test_utils:mock_expect(Workers, user_ctx, is_direct_io,
        fun(_, _) ->
            true
        end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);


init_per_testcase(Case, Config) when
    Case =:= lfm_open_failure_test;
    Case =:= lfm_create_and_open_failure_test;
    Case =:= lfm_mv_failure_test;
    Case =:= lfm_open_multiple_times_failure_test;
    Case =:= lfm_open_failure_multiple_users_test;
    Case =:= lfm_open_and_create_open_failure_test;
    Case =:= lfm_mv_failure_multiple_users_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, storage_driver, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(ShareTest, Config) when
    ShareTest =:= create_share_dir_test;
    ShareTest =:= create_share_file_test;
    ShareTest =:= remove_share_test;
    ShareTest =:= share_getattr_test;
    ShareTest =:= share_get_parent_test;
    ShareTest =:= share_list_test;
    ShareTest =:= share_read_test;
    ShareTest =:= share_child_getattr_test;
    ShareTest =:= share_child_list_test;
    ShareTest =:= share_child_read_test;
    ShareTest =:= share_permission_denied_test
    ->
    initializer:mock_share_logic(Config),
    init_per_testcase(?DEFAULT_CASE(ShareTest), Config);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(Case, Config) when
    Case =:= lfm_open_in_direct_mode_test;
    Case =:= lfm_recreate_handle_test;
    Case =:= lfm_write_after_create_no_perms_test;
    Case =:= lfm_recreate_handle_after_delete_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [user_ctx]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= lfm_open_failure_test;
    Case =:= lfm_create_and_open_failure_test;
    Case =:= lfm_mv_failure_test;
    Case =:= lfm_open_multiple_times_failure_test;
    Case =:= lfm_open_failure_multiple_users_test;
    Case =:= lfm_open_and_create_open_failure_test;
    Case =:= lfm_mv_failure_multiple_users_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(ShareTest, Config) when
    ShareTest =:= create_share_dir_test;
    ShareTest =:= create_share_file_test;
    ShareTest =:= remove_share_test;
    ShareTest =:= share_getattr_test;
    ShareTest =:= share_get_parent_test;
    ShareTest =:= share_list_test;
    ShareTest =:= share_read_test;
    ShareTest =:= share_child_getattr_test;
    ShareTest =:= share_child_list_test;
    ShareTest =:= share_child_read_test;
    ShareTest =:= share_permission_denied_test
    ->
    initializer:unmock_share_logic(Config),

    end_per_testcase(?DEFAULT_CASE(ShareTest), Config);

end_per_testcase(Case, Config) when
    Case =:= opening_file_should_increase_file_popularity;
    Case =:= file_popularity_should_have_correct_file_size
    ->
    [W | _] = ?config(op_worker_nodes, Config),
    rpc:call(W, file_popularity_api, disable, [?SPACE_ID1]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = lfm_cp_dir, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    % set default value of ls_batch_size env
    test_utils:set_env(W, op_worker, ls_batch_size, 5000),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID1, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID2, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID3, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID4, 30),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).