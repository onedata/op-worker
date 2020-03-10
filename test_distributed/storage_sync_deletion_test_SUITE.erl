%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync_deletion module.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_deletion_test_SUITE).
-author("Jakub Kudzia").

-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    delete_child_file_basic_test/1,
    empty_child_dir_should_not_be_deleted_test/1,
    delete_child_subtree_test/1,
    delete_nested_child_on_object_storage_test/1,
    delete_nested_child_on_block_storage_test/1,
    do_not_delete_child_file_basic_test/1,
    do_not_delete_child_without_location_test/1,
    delete_children_test/1, delete_children2_test/1,
    delete_children3_test/1, delete_children4_test/1,
    delete_children5_test/1, delete_children6_test/1
]).

-define(TEST_CASES, [
    delete_child_file_basic_test,
    empty_child_dir_should_not_be_deleted_test,
    delete_child_subtree_test,
    delete_nested_child_on_object_storage_test,
    delete_nested_child_on_block_storage_test,
    do_not_delete_child_file_basic_test,
    do_not_delete_child_without_location_test,
    delete_children_test,
    delete_children2_test,
    delete_children3_test,
    delete_children4_test,
    delete_children5_test,
    delete_children6_test
]).

all() -> ?ALL(?TEST_CASES).

-define(POOL, ?MODULE).

-define(RAND_STR, <<(crypto:strong_rand_bytes(16))/binary>>).
-define(USER, <<"user1">>).

-define(SPACE_ID1, <<"space1">>).   % space supported by posix
-define(SPACE_ID2, <<"space2">>).   % space supported by s3
-define(SPACE_ID3, <<"space3">>).   % space supported by posix and mounted in root
-define(SPACE_ID4, <<"space4">>).   % space supported by s3 and mounted in root

-define(SPACE_IDS, [
    ?SPACE_ID1,
    ?SPACE_ID2,
    ?SPACE_ID3,
    ?SPACE_ID4
]).

-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).

-define(SPACE_CTX(SpaceId), file_ctx:new_by_guid(?SPACE_GUID(SpaceId))).
-define(SPACE_STORAGE_CTX(Worker, SpaceId, ImportedStorage), begin
    storage_file_ctx:new(space_storage_file_id(SpaceId, ImportedStorage), SpaceId,
        initializer:get_supporting_storage_id(W, SpaceId))
end).
-define(SESSION_ID(Config, Worker), ?config({session_id, {?USER, ?GET_DOMAIN(Worker)}}, Config)).

% {StorageType, ImportedStorage}
-define(BLOCK_STORAGE_CONFIGS, [{block_storage, false}, {block_storage, true}]).
-define(OBJECT_STORAGE_CONFIGS, [{object_storage, false}, {object_storage, true}]).

-define(STORAGE_CONFIGS, ?BLOCK_STORAGE_CONFIGS ++ ?OBJECT_STORAGE_CONFIGS).

-define(STORAGE_TO_SPACE_ID(StorageType, ImportedStorage),
    case {StorageType, ImportedStorage} of
        {block_storage, false} -> ?SPACE_ID1;
        {object_storage, false} -> ?SPACE_ID2;
        {block_storage, true} -> ?SPACE_ID3;
        {object_storage, true} -> ?SPACE_ID4
    end).

-define(FOR_ALL_STORAGE_CONFIGS(TestFun, Args),
    ?FOR_EACH_STORAGE_CONFIG(TestFun, Args, ?STORAGE_CONFIGS)).

-define(FOR_EACH_STORAGE_CONFIG(TestFun, Args, StorageConfigs),
    run_test_for_all_storage_configs(?FUNCTION, TestFun, Args, StorageConfigs)).

-define(TIMEOUT, 10).

%%%==================================================================
%%% Test functions
%%%===================================================================

delete_child_file_basic_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_child_file_basic_test_base/1, [Config]).

empty_child_dir_should_not_be_deleted_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun empty_child_dir_should_not_be_deleted_test_base/1, [Config]).

delete_child_subtree_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_child_subtree_test_base/1, [Config]).

delete_nested_child_on_object_storage_test(Config) ->
    ?FOR_EACH_STORAGE_CONFIG(fun delete_nested_child_on_object_storage_test_base/1, [Config], ?OBJECT_STORAGE_CONFIGS).

delete_nested_child_on_block_storage_test(Config) ->
    ?FOR_EACH_STORAGE_CONFIG(fun delete_nested_child_on_block_storage_test_base/1, [Config],
        [{block_storage, false}, {block_storage, true}]).

do_not_delete_child_file_basic_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun do_not_delete_child_file_basic_test_base/1, [Config]).

do_not_delete_child_without_location_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun do_not_delete_child_file_without_location_test_base/1, [Config]).

delete_children_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base/3, [Config, 100, 100]).

delete_children2_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base/3, [Config, 10, 100]).

delete_children3_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base/3, [Config, 100, 10]).

delete_children4_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base2/3, [Config, 100, 100]).

delete_children5_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base2/3, [Config, 10, 100]).

delete_children6_test(Config) ->
    ?FOR_ALL_STORAGE_CONFIGS(fun delete_children_files_test_base2/3, [Config, 100, 100]).

%===================================================================
% Test bases functions
%===================================================================

delete_child_file_basic_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    SessionId = ?SESSION_ID(Config, W),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    Child = <<"child1">>,

    {ok, Guid} = lfm_proxy:create(W, SessionId, SpaceGuid, Child, 8#664),
    {ok, H} = lfm_proxy:open(W, SessionId, {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, <<"test_data">>),

    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessionId, {guid, Guid}), ?TIMEOUT),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

empty_child_dir_should_not_be_deleted_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    SessionId = ?SESSION_ID(Config, W),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    Child = <<"child">>,
    {ok, Guid} = lfm_proxy:mkdir(W, SessionId, SpaceGuid, Child, 8#775),

    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W, SessionId, {guid, Guid}), ?TIMEOUT),
    ?assertMatch({ok, [{Guid, _}]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

delete_child_subtree_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    SessionId = ?SESSION_ID(Config, W),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    ChildDir1 = <<"child_dir1">>,
    ChildDir2 = <<"child_dir1">>,
    ChildDir3 = <<"child_dir3">>,
    ChildFile = <<"child_file">>,

    {ok, DirGuid1} = lfm_proxy:mkdir(W, SessionId, SpaceGuid, ChildDir1, 8#775),
    {ok, DirGuid2} = lfm_proxy:mkdir(W, SessionId, DirGuid1, ChildDir2, 8#775),
    {ok, DirGuid3} = lfm_proxy:mkdir(W, SessionId, DirGuid2, ChildDir3, 8#775),
    {ok, FileGuid} = lfm_proxy:create(W, SessionId, DirGuid3, ChildFile, 8#664),
    {ok, H} = lfm_proxy:open(W, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, <<"test_data">>),

    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessionId, {guid, FileGuid}), ?TIMEOUT),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid1}, 0, 1), ?TIMEOUT),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid2}, 0, 1), ?TIMEOUT),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid3}, 0, 1), ?TIMEOUT),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

delete_nested_child_on_object_storage_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    SessionId = ?SESSION_ID(Config, W),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    ChildDir1 = <<"child_dir1">>,
    ChildDir2 = <<"child_dir2">>,
    ChildDir3 = <<"child_dir3">>,
    ChildFile1 = <<"child_file1">>,
    ChildFile2 = <<"child_file2">>,
    SpaceStorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    SpaceStorageFileId = storage_file_ctx:get_storage_file_id_const(SpaceStorageFileCtx),

    ct:pal("SpaceStorageFIleId: ~p", [SpaceStorageFileId]),

    {ok, DirGuid1} = lfm_proxy:mkdir(W, SessionId, SpaceGuid, ChildDir1, 8#775),
    {ok, DirGuid2} = lfm_proxy:mkdir(W, SessionId, DirGuid1, ChildDir2, 8#775),
    {ok, DirGuid3} = lfm_proxy:mkdir(W, SessionId, DirGuid2, ChildDir3, 8#775),
    {ok, FileGuid} = lfm_proxy:create(W, SessionId, DirGuid3, ChildFile1, 8#664),
    {ok, FileGuid2} = lfm_proxy:create(W, SessionId, DirGuid3, ChildFile2, 8#664),
    {ok, H} = lfm_proxy:open(W, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, <<"test_data">>),
    {ok, H2} = lfm_proxy:open(W, SessionId, {guid, FileGuid2}, write),
    {ok, _} = lfm_proxy:write(W, H2, 0, <<"test_data">>),
    ?assertMatch({ok, [{DirGuid1, _}]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT),
    Child1FilePath = filename:join([SpaceStorageFileId, ChildDir1, ChildDir2, ChildDir3, ChildFile1]),
    ok = add_storage_sync_link(W, SpaceStorageFileId, Child1FilePath, StorageId, true),

    run_deletion(W, SpaceStorageFileCtx, ?SPACE_CTX(SpaceId)),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W, SessionId, {guid, FileGuid}), ?TIMEOUT),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessionId, {guid, FileGuid2}), ?TIMEOUT),
    ?assertMatch({ok, [{FileGuid, _}]}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid3}, 0, 1), ?TIMEOUT),
    ?assertMatch({ok, [{DirGuid3, _}]}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid2}, 0, 1), ?TIMEOUT),
    ?assertMatch({ok, [{DirGuid2, _}]}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid1}, 0, 1), ?TIMEOUT),
    ?assertMatch({ok, [{DirGuid1, _}]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

delete_nested_child_on_block_storage_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    SessionId = ?SESSION_ID(Config, W),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    ChildDir = <<"child_dir">>,
    ChildFile = <<"child_file">>,
    DirStorageFileCtx = storage_file_ctx:get_child_ctx_const(StorageFileCtx, ChildDir),

    {ok, DirGuid} = lfm_proxy:mkdir(W, SessionId, SpaceGuid, ChildDir, 8#775),
    {ok, FileGuid} = lfm_proxy:create(W, SessionId, DirGuid, ChildFile, 8#664),
    {ok, H} = lfm_proxy:open(W, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, <<"test_data">>),
    DirCtx = file_ctx:new_by_guid(DirGuid),

    run_deletion(W, DirStorageFileCtx, DirCtx),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessionId, {guid, FileGuid}), ?TIMEOUT),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, SessionId, {guid, DirGuid}, 0, 1), ?TIMEOUT),
    ?assertMatch({ok, [{DirGuid, _}]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

do_not_delete_child_file_basic_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    StorageType = ?config(storage_type, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    MarkLeaves = should_mark_leaves(StorageType),
    SessionId = ?SESSION_ID(Config, W),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    Child = <<"child1">>,
    {ok, Guid} = lfm_proxy:create(W, SessionId, SpaceGuid, Child, 8#664),
    {ok, H} = lfm_proxy:open(W, SessionId, {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, <<"test_data">>),

    ChildStorageFileId = filename:join([RootStorageFileId, Child]),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves),
    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({ok, [_]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

do_not_delete_child_file_without_location_test_base(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    StorageType = ?config(storage_type, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    MarkLeaves = should_mark_leaves(StorageType),
    SessionId = ?SESSION_ID(Config, W),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),
    Child = <<"child1">>,
    {ok, _} = lfm_proxy:create(W, SessionId, SpaceGuid, Child, 8#664),

    ChildStorageFileId = filename:join([RootStorageFileId, Child]),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves),
    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({ok, [_]}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, 1), ?TIMEOUT).

delete_children_files_test_base(Config, ChildrenToStayNum, ChildrenToDeleteNum) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    StorageType = ?config(storage_type, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    MarkLeaves = should_mark_leaves(StorageType),
    SessionId = ?SESSION_ID(Config, W),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),

    lists:foldl(fun(N, {ToStayIn, ToDeleteIn})->
        Child = <<"child", (integer_to_binary(N))/binary>>,
        case {ToStayIn < ChildrenToStayNum, ToDeleteIn < ChildrenToDeleteNum, N rem 2 =:= 0} of
            {_, true, true} ->
                create_file(W, SpaceGuid, Child, SessionId),
                {ToStayIn, ToDeleteIn + 1};
            {true, _, false} ->
                add_storage_sync_link(W, RootStorageFileId, Child, StorageId, MarkLeaves),
                {ToStayIn + 1, ToDeleteIn};
            {false, true, _} ->
                create_file(W, SpaceGuid, Child, SessionId),
                {ToStayIn, ToDeleteIn + 1};
            {true, false, _} ->
                add_storage_sync_link(W, RootStorageFileId, Child, StorageId, MarkLeaves),
                {ToStayIn + 1, ToDeleteIn}
        end
    end, {0, 0}, lists:seq(1, ChildrenToStayNum + ChildrenToDeleteNum)),

    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, ChildrenToStayNum + ChildrenToDeleteNum), 5 * ?TIMEOUT).

delete_children_files_test_base2(Config, ChildrenToStayNum, ChildrenToDeleteNum) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    ImportedStorage = ?config(imported_storage, Config),
    SpaceGuid = ?SPACE_GUID(SpaceId),
    StorageType = ?config(storage_type, Config),
    MarkLeaves = should_mark_leaves(StorageType),
    SessionId = ?SESSION_ID(Config, W),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    StorageFileCtx = ?SPACE_STORAGE_CTX(W, SpaceId, ImportedStorage),

    lists:foldl(fun(N, {ToStayIn, ToDeleteIn})->
        Child = <<"child", (integer_to_binary(N))/binary>>,
        case {ToStayIn < ChildrenToStayNum, ToDeleteIn < ChildrenToDeleteNum, rand:uniform() < 0.5} of
            {_, true, true} ->
                create_file(W, SpaceGuid, Child, SessionId),
                {ToStayIn, ToDeleteIn + 1};
            {true, _, false} ->
                add_storage_sync_link(W, RootStorageFileId, Child, StorageId, MarkLeaves),
                {ToStayIn + 1, ToDeleteIn};
            {false, true, _} ->
                create_file(W, SpaceGuid, Child, SessionId),
                {ToStayIn, ToDeleteIn + 1};
            {true, false, _} ->
                add_storage_sync_link(W, RootStorageFileId, Child, StorageId, MarkLeaves),
                {ToStayIn + 1, ToDeleteIn}
        end
    end, {0, 0}, lists:seq(1, ChildrenToStayNum + ChildrenToDeleteNum)),

    run_deletion(W, StorageFileCtx, ?SPACE_CTX(SpaceId)),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, SessionId, {guid, SpaceGuid}, 0, ChildrenToStayNum + ChildrenToDeleteNum), 5 * ?TIMEOUT).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        [W | _] = ?config(op_worker_nodes, NewConfig2),
        rpc:call(W, storage_sync_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = worker_pool:stop_sup_pool(?POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    clean_spaces(W),
    clean_storage_sync_links(W),
    lfm_proxy:teardown(Config).

%===================================================================
% Internal functions
%===================================================================

space_storage_file_id(_SpaceId, true) ->
    <<"/">>;
space_storage_file_id(SpaceId, false) ->
    <<"/", SpaceId/binary>>.

run_deletion(Worker, StorageFileCtx, FileCtx) ->
    ok = rpc:call(Worker, storage_sync_traverse, run_deletion_scan, [StorageFileCtx, 0, #{max_depth => 1000000000000000000000}, FileCtx, false]).

clean_spaces(Worker) ->
    lfm_proxy:close_all(Worker),
    lists:foreach(fun(SpaceId) ->
        clean_space(Worker, SpaceId)
    end, ?SPACE_IDS).

clean_space(Worker, SpaceId) ->
    SpaceGuid = ?SPACE_GUID(SpaceId),
    clean_space(Worker, SpaceGuid, 0, 1000),
    ?assertMatch({ok, []}, lfm_proxy:get_children(Worker, <<"0">>, {guid, SpaceGuid}, 0, 1000)).


clean_space(Worker, SpaceGuid, Offset, Count) ->
    {ok, Children} = lfm_proxy:get_children(Worker, <<"0">>, {guid, SpaceGuid}, Offset, Count),
    lists:foreach(fun({ChildGuid, _}) ->
        lfm_proxy:rm_recursive(Worker, <<"0">>, {guid, ChildGuid}),
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, <<"0">>,  {guid, ChildGuid}))
    end, Children),
    case length(Children) < Count of
        true ->
            ok;
        false ->
            clean_space(Worker, SpaceGuid, Offset + length(Children), Count)
    end.

clean_storage_sync_links(Worker) ->
    lists:foreach(fun(SpaceId) ->
        clean_storage_sync_links(Worker, SpaceId)
    end, ?SPACE_IDS).

clean_storage_sync_links(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_sync_links_test_utils:delete_recursive(Worker, space_storage_file_id(SpaceId, true), StorageId),
    storage_sync_links_test_utils:delete_recursive(Worker, space_storage_file_id(SpaceId, false), StorageId).

create_file(Worker, ParentGuid, ChildName, SessionId) ->
    {ok, Guid} = lfm_proxy:create(Worker, SessionId, ParentGuid, ChildName, 8#664),
    {ok, H} = lfm_proxy:open(Worker, SessionId, {guid, Guid}, write),
    {ok, _} = lfm_proxy:write(Worker, H, 0, <<"test_data">>),
    lfm_proxy:close(Worker, H).

add_storage_sync_link(Worker, RootStorageFileId, ChildName, StorageId, MarkLeaves) ->
    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
    ok = storage_sync_links_test_utils:add_link(Worker, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves).

should_mark_leaves(object_storage = _StorageType) -> true;
should_mark_leaves(block_storage = _StorageType) -> false.

run_test_for_all_storage_configs(Testcase, TestFun, Args, StorageConfigs) ->
    Results = lists:map(fun(StorageConfig) ->
        run_test(TestFun, StorageConfig, Args)
    end, StorageConfigs),
    case lists:all(fun(E) -> E =:= ok end, Results) of
        false -> ct:fail("Testcase ~p failed", [Testcase]);
        true -> ok
    end.

run_test(TestFun, StorageConfig = {StorageType, ImportedStorage}, [Config | OtherArgs]) ->
    try
        FinalConfig = [
            {storage_type, StorageType}, {imported_storage, ImportedStorage},
            {space_id, ?STORAGE_TO_SPACE_ID(StorageType, ImportedStorage)} | Config
        ],
        FinalArgs = [FinalConfig | OtherArgs],
        apply(TestFun, FinalArgs),
        ok
    catch
        E:R ->
            ct:pal("Testcase ~p failed due to ~p for storage config ~p~n"
            "Stacktrace: ~p", [TestFun, {E, R}, StorageConfig, erlang:get_stacktrace()]),
            error
    after
        [W | _] = ?config(op_worker_nodes, Config),
        storage_sync_test_base:clean_traverse_tasks(W)
    end.