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
    lfm_acl/1,
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
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
    lfm_recreate_handle/3,
    lfm_open_failure/1,
    lfm_create_and_open_failure/1,
    lfm_open_in_direct_mode/1,
    lfm_mv_failure/1,
    lfm_open_multiple_times_failure/1,
    lfm_open_failure_multiple_users/1,
    lfm_open_and_create_open_failure/1,
    lfm_mv_failure_multiple_users/1
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
    Filename = generator:gen_name(),
    {ok, {FileGuid, Handle}} = lfm_proxy:create_and_open(W, SessId1, <<"/space_name1/", Filename/binary>>, CreatePerms),
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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/", Filename/binary>>),

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
    
    Filename = generator:gen_name(),
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:create_and_open(
        W, SessId1, ParentGuid, Filename, ?DEFAULT_FILE_PERMS)
    ),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
        W, SessId1, {path, <<"/space_name1/", Filename/binary>>})
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
    
    Filename = generator:gen_name(),
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:create_and_open(
        W, SessId1, ParentGuid, Filename, ?DEFAULT_FILE_PERMS)
    ),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
        W, SessId1, {path, <<"/space_name1/", Filename/binary>>})
    ),
    ?assertEqual({ok, []}, rpc:call(W, file_handles, list, [])),

    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/", Filename/binary>>),
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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/", Filename/binary>>),

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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name2/", Filename/binary>>),

    % here all operations should succeed
    {ok, Handle} = lfm_proxy:open(W, SessId1, ?FILE_REF(FileGuid), rdwr),
    ?assertEqual({ok, 9}, lfm_proxy:write(W, Handle, 0, <<"test_data">>)),

    % simulate open error
    open_failure_mock(W),

    ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(
        W, SessId2, {path, <<"/space_name2/", Filename/binary>>}, rdwr)
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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/", Filename/binary>>),

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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name1/", Filename/binary>>),

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
    Filename = generator:gen_name(),
    {ok, FileGuid} = lfm_proxy:create(W, SessId1, <<"/space_name2/", Filename/binary>>),

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
    ?assertMatch(?ERR_POSIX(?EPERM), opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)),

    initializer:testmaster_mock_space_user_privileges(
        Workers, SpaceId, UserId, privileges:space_admin()
    ),

    % User root dir can not be shared
    ?assertMatch(
        ?ERROR_NOT_SUPPORTED,
        opt_shares:create(W, SessId, ?FILE_REF(user_root_dir:guid(UserId)), <<"share_name">>)
    ),
    % But space dir can
    ?assertMatch(
        {ok, <<_/binary>>},
        opt_shares:create(W, SessId, ?FILE_REF(space_dir:guid(SpaceId)), <<"share_name">>)
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
    ?assertMatch(?ERR_POSIX(?EPERM), opt_shares:create(W, SessId, ?FILE_REF(Guid), <<"share_name">>)),

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
    ?assertMatch(?ERR_POSIX(?EPERM), opt_shares:remove(W, SessId, ShareId1)),

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
    SpaceDirGuid = space_dir:guid(SpaceId),
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
            parent_guid = SpaceDirGuid,
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
                owner_id = undefined,
                provider_id = undefined,
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

    SpaceDirGuid = space_dir:guid(SpaceId),
    DirPath = <<SpaceName/binary, "/share_get_parent">>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, SessId, DirPath, 8#707),
    {ok, FileGuid} = lfm_proxy:create(W, SessId, <<DirPath/binary, "/file">>, 8#700),

    {ok, ShareId} = opt_shares:create(W, SessId, ?FILE_REF(DirGuid), <<"share_name">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Getting parent of dir should return space guid
    ?assertMatch({ok, SpaceDirGuid}, lfm_proxy:get_parent(W, SessId, ?FILE_REF(DirGuid))),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    MemDiff = datastore_pool_test_utils:get_documents_diff(Worker, MemEntriesAfter,
        MemEntriesBefore),
    CacheDiff = datastore_pool_test_utils:get_documents_diff(Worker, CacheEntriesAfter,
        CacheEntriesBefore),
    ct:pal("~n MemRes: ~tp ~n~n CacheRes: ~tp ~n", [MemDiff, CacheDiff]).

get_mem_and_disc_entries(Worker) ->
    {MemEntries, _} = datastore_pool_test_utils:get_pools_entries_and_sizes(Worker, memory),
    {DiscEntries, _} = datastore_pool_test_utils:get_pools_entries_and_sizes(Worker, disc),
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

verify_file_content(Config, Handle, FileContent) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, Handle, 0, size(FileContent))).

verify_file_content(Config, Handle, FileContent, From, To) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, Handle, From, To)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, datastore_pool_test_utils, ?MODULE]} | Config].


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

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID1, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID2, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID3, 30),
    lfm_test_utils:clean_space(Workers, ?SPACE_ID4, 30),
    lfm_proxy:teardown(Config),
    lfm_ct:clear_context(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).