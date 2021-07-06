%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Replication tests.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    dbsync_trigger_should_not_create_local_file_location/1,
    local_file_location_should_have_correct_uid_for_local_user/1,
    local_file_location_should_be_chowned_when_missing_user_appears/1,
    write_should_add_blocks_to_file_location/1,
    truncate_should_change_size_and_blocks/1,
    write_and_truncate_should_not_update_remote_file_location/1,
    update_should_bump_replica_version/1,
    read_should_synchronize_file/1,
    external_change_should_invalidate_blocks/1,
    update_should_save_recent_changes/1,
    remote_change_should_invalidate_only_updated_part_of_file/1,
    remote_change_without_history_should_invalidate_whole_data/1,
    remote_change_of_size_should_notify_clients/1,
    remote_change_of_blocks_should_notify_clients/1,
    remote_irrelevant_change_should_not_notify_clients/1,
    conflicting_remote_changes_should_be_reconciled/1,
    replica_invalidate_should_migrate_unique_data/1,
    replica_invalidate_should_truncate_storage_file_to_zero_size/1,
    dir_replica_invalidate_should_invalidate_all_children/1
]).

all() ->
    ?ALL([
        dbsync_trigger_should_not_create_local_file_location,
        local_file_location_should_have_correct_uid_for_local_user,
        local_file_location_should_be_chowned_when_missing_user_appears,
        write_should_add_blocks_to_file_location,
        truncate_should_change_size_and_blocks,
        write_and_truncate_should_not_update_remote_file_location,
        update_should_bump_replica_version,
        read_should_synchronize_file,
        external_change_should_invalidate_blocks,
        update_should_save_recent_changes,
        remote_change_should_invalidate_only_updated_part_of_file,
        remote_change_without_history_should_invalidate_whole_data,
        remote_change_of_size_should_notify_clients,
        remote_change_of_blocks_should_notify_clients,
        remote_irrelevant_change_should_not_notify_clients,
        conflicting_remote_changes_should_be_reconciled
        %% @TODO VFS-3728
        %% replica_invalidate_should_migrate_unique_data,
        %% replica_invalidate_should_truncate_storage_file_to_zero_size,
        %% dir_replica_invalidate_should_invalidate_all_children
    ]).


-define(call_store(Model, F, A), rpc:call(
    W1, model, execute_with_default_context, [Model, F, A])).
-define(call_store(Model, F, A, O), rpc:call(
    W1, model, execute_with_default_context, [Model, F, A, O])).
-define(extract_key(Result), datastore_runner:extract_key(Result)).

-define(SUPPORT_SIZE, 1000000000).

%%%===================================================================
%%% Test functions
%%%===================================================================

dbsync_trigger_should_not_create_local_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    CTime = global_clock:timestamp_millis(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #document{value = #file_meta{
        mode = 8#777,
        name = <<"file">>,
        type = ?REGULAR_FILE_TYPE,
        owner = UserId
    }},
    {ok, #document{key = FileUuid}} = ?assertMatch(
        {ok, _},
        rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])
    ),
    ?assertMatch(
        {ok, _},
        rpc:call(W1, times, save, [#document{
            key = FileUuid,
            value = #times{
                atime = CTime,
                ctime = CTime,
                mtime = CTime
            }
        }])
    ),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #document{key = FileUuid, value = FileMeta}]),

    %then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    ?assertMatch({error, not_found}, rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])),
    {ok, Handle} = ?assertMatch(
        {ok, _},
        lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr)
    ),
    ?assertMatch({ok, 3}, lfm_proxy:write(W1, Handle, 0, <<"aaa">>)),
    ?assertMatch({ok, <<"aaa">>}, lfm_proxy:read(W1, Handle, 0, 3)),
    ?assertEqual(ok, lfm_proxy:close(W1, Handle)).

local_file_location_should_have_correct_uid_for_local_user(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    {ok, StorageId} = storage_test_utils:get_supporting_storage_id(W1, SpaceId),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    StorageDir = ?config({storage_dir, ?GET_DOMAIN(W1)}, Config),
    CTime = global_clock:timestamp_millis(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"local_file_location_should_have_correct_uid_for_local_user">>,
        type = ?REGULAR_FILE_TYPE,
        owner = UserId
    },
    {ok, #document{key = FileUuid}} = ?assertMatch(
        {ok, _},
        rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, #document{value = FileMeta}])
    ),
    ?assertMatch(
        {ok, _},
        rpc:call(W1, times, save, [#document{
            key = FileUuid,
            value = #times{
                atime = CTime,
                ctime = CTime,
                mtime = CTime
            }
        }])
    ),

    {ok, FileToCompareGUID} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/file_to_compare">>),
    {ok, Handle} =
        lfm_proxy:open(W1, SessionId, ?FILE_REF(FileToCompareGUID), read), % open in order to create storage file
    lfm_proxy:close(W1, Handle),
    FileToCompareUUID = file_id:guid_to_uuid(FileToCompareGUID),

    [$/ | FileToCompareFID] =
        binary_to_list(get_storage_file_id_by_uuid(W1, FileToCompareUUID)),
    [$/ | FileFID] = binary_to_list(get_storage_file_id_by_uuid(W1, FileUuid)),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #document{key = FileUuid, value = FileMeta}]),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {ok, Handle2} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), read),
    lfm_proxy:close(W1, Handle2),

    %then
    {ok, #{<<"uid">> := UidBin, <<"gid">> := GidBin}} =
        rpc:call(W1, luma, map_to_storage_credentials, [UserId, SpaceId, StorageId]),
    Uid = binary_to_integer(UidBin),
    Gid = binary_to_integer(GidBin),
    {ok, CorrectFileInfo} =
        rpc:call(W1, file, read_file_info, [filename:join([StorageDir, FileToCompareFID])]),
    {ok, FileInfo} =
        rpc:call(W1, file, read_file_info, [filename:join([StorageDir, FileFID])]),
    ?assertEqual(Uid, FileInfo#file_info.uid),
    ?assertNotEqual(0, FileInfo#file_info.uid),
    ?assertEqual(Gid, FileInfo#file_info.gid),
    ?assertNotEqual(0, FileInfo#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.uid, FileInfo#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.gid, FileInfo#file_info.gid).

local_file_location_should_be_chowned_when_missing_user_appears(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, StorageId} = storage_test_utils:get_supporting_storage_id(W1, SpaceId),
    ExternalUser = <<"external_user_id">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    StorageDir = ?config({storage_dir, ?GET_DOMAIN(W1)}, Config),
    CTime = global_clock:timestamp_millis(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"local_file_location_should_be_chowned_when_missing_user_appears1">>,
        type = ?REGULAR_FILE_TYPE,
        owner = ExternalUser
    },
    {ok, #document{key = FileUuid}} = ?assertMatch(
        {ok, _},
        rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, #document{value = FileMeta}])
    ),
    ?assertMatch(
        {ok, _},
        rpc:call(W1, times, save, [#document{
            key = FileUuid,
            value = #times{
                atime = CTime,
                ctime = CTime,
                mtime = CTime
            }
        }])
    ),

    {ok, FileToCompareGUID} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/file_to_compare">>),
    {ok, Handle} =
        lfm_proxy:open(W1, SessionId, ?FILE_REF(FileToCompareGUID), read), % open in order to create storage file
    lfm_proxy:close(W1, Handle),
    FileToCompareUUID = file_id:guid_to_uuid(FileToCompareGUID),

    [$/ | FileToCompareFID] =
        binary_to_list(get_storage_file_id_by_uuid(W1, FileToCompareUUID)),
    [$/ | File1FID] = binary_to_list(get_storage_file_id_by_uuid(W1, FileUuid)),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #document{key = FileUuid, value = FileMeta}]),

    FileGuid1 = file_id:pack_guid(FileUuid, SpaceId), % create deferred storage files
    {ok, Handle1} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid1), read),
    lfm_proxy:close(W1, Handle1),

    % Simulate new user appearing
    rpc:call(W1, od_user, run_after, [create, [], {ok, #document{key = ExternalUser, value = #od_user{}}}]),

    %then
    {ok, #{<<"uid">> := UidBin, <<"gid">> := GidBin}} =
        rpc:call(W1, luma, map_to_storage_credentials, [ExternalUser, SpaceId, StorageId]),
    Uid = binary_to_integer(UidBin),
    Gid = binary_to_integer(GidBin),
    {ok, CorrectFileInfo} =
        rpc:call(W1, file, read_file_info, [filename:join([StorageDir, FileToCompareFID])]),
    {ok, FileInfo1} =
        rpc:call(W1, file, read_file_info, [filename:join([StorageDir, File1FID])]),
    ?assertEqual(Uid, FileInfo1#file_info.uid, 10),
    ?assertEqual(Gid, FileInfo1#file_info.gid, 10),
    ?assertNotEqual(CorrectFileInfo#file_info.uid, FileInfo1#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.gid, FileInfo1#file_info.gid, 10).

write_should_add_blocks_to_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),

    %when
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    {ok, LocationDoc = #document{
        value = Location = #file_location{
            blocks = Blocks,
            size = Size,
            provider_id = ProviderId
        }}
    } = ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])),
    ?assertEqual(initializer:domain_to_provider_id(?GET_DOMAIN(W1)), ProviderId),
    ?assertEqual(10, Size),
    [Block] = ?assertMatch([#file_block{offset = 0, size = 10}], Blocks),

    % when
    LocationWithoutBeginning = LocationDoc#document{
        value = Location#file_location{
            blocks = [Block#file_block{offset = 5, size = 5}]
        }
    },
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [LocationWithoutBeginning])),
    ?assertMatch({ok, 5}, lfm_proxy:write(W1, Handle, 0, <<"11111">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    {ok, #document{
        value = #file_location{
            blocks = Blocks2,
            size = Size2
        }
    }} = ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])),
    ?assertEqual(10, Size2),
    ?assertMatch([Block], Blocks2).

truncate_should_change_size_and_blocks(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 6)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    {ok, #document{
        value = #file_location{
            blocks = Blocks,
            size = Size
        }
    }} = ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])),
    ?assertEqual(6, Size),
    ?assertMatch([#file_block{offset = 0, size = 6}], Blocks).

write_and_truncate_should_not_update_remote_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,
    ExternalBlocks = [#file_block{offset = 0, size = 10}],
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    RemoteLocation = #file_location{
        size = 10,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = #{}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location,
            [#document{value = RemoteLocation}]))
    ),

    % when
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 1, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 8)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    ?assertMatch({ok, #document{value = RemoteLocation}},
        rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid])).

update_should_bump_replica_version(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(W1)),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),

    %when
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"01">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 2, <<"23">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 4, <<"45">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 6, <<"67">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 8, <<"78">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    VV1 = maps:put({ProviderId, LocationId}, 5, #{}),
    ?assertMatch(
        {ok, #document{
            value = #file_location{
                version_vector = VV1,
                blocks = [#file_block{offset = 0, size = 10}]
            }
        }},
        rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])
    ),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 2)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 0)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    VV2 = maps:put({ProviderId, LocationId}, 9, #{}),
    ?assertMatch({ok, #document{value = #file_location{version_vector = VV2}}},
        rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])).

read_should_synchronize_file(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    LocalProviderId = rpc:call(W1, oneprovider, get_id, []),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    override_space_providers_mock(Config, Workers, SpaceId, [LocalProviderId, ExternalProviderId]),

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % attach external location
    ExternalBlocks = [#file_block{offset = 0, size = 10}],
    RemoteLocationId = file_location:id(FileUuid, ExternalProviderId),
    RemoteLocation = #document{
        key = RemoteLocationId,
        value = #file_location{
            size = 10,
            space_id = SpaceId,
            storage_id = <<"external_storage_id">>,
            provider_id = ExternalProviderId,
            blocks = ExternalBlocks,
            file_id = ExternalFileId,
            uuid = FileUuid,
            version_vector = #{}
        }
    },
    ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location, [RemoteLocation]))
    ),

    LocalLocationId = file_location:id(FileUuid, LocalProviderId),
    %pretend that file_location size has been updated by dbsync
    ?assertMatch(
        {ok, _},
        rpc:call(W1, fslogic_location_cache, update_location, [FileUuid, LocalLocationId, fun(FL) ->
            {ok, FL#file_location{size = 10}}
        end, false])
    ),

    % mock rtransfer_link
    test_utils:mock_new(Workers, rtransfer_link, [passthrough]),
    test_utils:mock_expect(Workers, rtransfer_link, fetch,
        fun(#{offset := 1, size := S, provider_id := PID, file_guid := FG},
            _TransferData, NotifyFun, OnCompleteFun)
              when PID == ExternalProviderId, FG == FileGuid, S >= 3 ->
            NotifyFun(ref, 1, 3),
            OnCompleteFun(ref, {ok, 3}),
            {ok, ref}
        end
    ),

    % when
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    Ans = lfm_proxy:read(W1, Handle, 1, 3),

    % then
    ?assertEqual({ok, <<>>}, Ans),
    ?assertEqual(1, rpc:call(W1, meck, num_calls, [rtransfer_link, fetch, '_'])),
    ?assert(rpc:call(W1, meck, validate, [rtransfer_link])),
    test_utils:mock_validate_and_unload(Workers, [rtransfer_link]),
    ?assertMatch({
        #document{
            value = #file_location{
                blocks = [#file_block{offset = 1, size = 3}]
            }
        },
        _
    },
        rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)])
    ).


external_change_should_invalidate_blocks(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    {#document{
        value = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [#file_block{offset = 2, size = 5}],
    RemoteLocation = #file_location{
        size = 10,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        recent_changes = {[], [ExternalBlocks]},
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location,
            [#document{value = RemoteLocation}]))
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = #document{
        value = #file_location{
            version_vector = VV
        }
    } = version_vector:bump_version(RemoteLocationDoc),
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    ?assertMatch({#document{
        value = #file_location{
            version_vector = VV,
            blocks = [
                #file_block{offset = 0, size = 2},
                #file_block{offset = 7, size = 3}
            ]
        }
    }, _},
        rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)])).

update_should_save_recent_changes(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),

    %when
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"01">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 2, <<"23">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 4, <<"45">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 6, <<"67">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 8, <<"78">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch(
        {ok, [_]},
        rpc:call(W1, file_meta, get_locations_by_uuid, [FileUuid])
    ),
    ?assertMatch(
        {ok, #document{
            value = #file_location{
                blocks = [#file_block{offset = 0, size = 10}]
            }
        }},
        rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])
    ),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 2)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, ?FILE_REF(FileGuid), 0)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    ?assertMatch({ok, #document{value = #file_location{recent_changes = {[],
        [
            [#file_block{offset = 0, size = 2}],
            {shrink, 0},
            [#file_block{offset = 0, size = 2}],
            {shrink, 2},
            [#file_block{offset = 8, size = 2}],
            [#file_block{offset = 6, size = 2}],
            [#file_block{offset = 4, size = 2}],
            [#file_block{offset = 2, size = 2}],
            [#file_block{offset = 0, size = 2}]
        ]}
    }}},
        rpc:call(W1, fslogic_location_cache, get_location, [LocationId, FileUuid])).

remote_change_should_invalidate_only_updated_part_of_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    {LocalDoc = #document{
        value = LocalLocation = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [#file_block{offset = 2, size = 5}],
    ExternalChanges = [
        [#file_block{offset = 2, size = 2}],
        [#file_block{offset = 7, size = 3}],
        [#file_block{offset = 0, size = 10}],
        [#file_block{offset = 1, size = 5}]
    ],
    RemoteLocation = #file_location{
        size = 10,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], ExternalChanges}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        rpc:call(W1, fslogic_location_cache, create_location,
            [#document{key = datastore_key:new(), value = RemoteLocation}, true])
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = #document{
        value = #file_location{
            version_vector = VV
        }
    } = bump_version(RemoteLocationDoc, 4),

    % prepare local doc version
    #document{
        value = #file_location{
            version_vector = NewLocalVV
        }
    } = bump_version(RemoteLocationDoc, 2),
    rpc:call(W1, fslogic_location_cache, save_location, [LocalDoc#document{
        value = LocalLocation#file_location{
            version_vector = NewLocalVV
        }
    }]),

    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    ?assertMatch(
        {#document{
            value = #file_location{
                version_vector = VV,
                blocks = [
                    #file_block{offset = 0, size = 2},
                    #file_block{offset = 4, size = 3}
                ]
            }
        }, _},
        rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)])
    ).

remote_change_without_history_should_invalidate_whole_data(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceId = <<"space_id1">>,
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    {#document{
        value = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [
        #file_block{offset = 1, size = 1},
        #file_block{offset = 5, size = 1}
    ],
    ExternalSize = 8,
    RemoteLocation = #file_location{
        size = ExternalSize,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], []}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location,
            [#document{value = RemoteLocation}]))
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = #document{
        value = #file_location{
            version_vector = VV
        }
    } = bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    ?assertMatch(
        {#document{
            value = #file_location{
                version_vector = VV,
                size = ExternalSize,
                blocks = [
                    #file_block{offset = 0, size = 1},
                    #file_block{offset = 2, size = 3},
                    #file_block{offset = 6, size = 2}
                ]
            }
        }, _},
        rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)])
    ).

remote_change_of_size_should_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    {#document{
        value = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [],
    ExternalSize = 8,
    RemoteLocation = #file_location{
        size = ExternalSize,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], [{shrink, 8}]}
    },
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location,
            [#document{value = RemoteLocation}]))),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % mock events
    test_utils:mock_new(W1, [fslogic_event_emitter], [passthrough]),
    test_utils:mock_expect(W1, fslogic_event_emitter, emit_file_attr_changed,
        fun(_Entry, _ExcludedSessions) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    TheFileCtxWithGuid = fun(FileCtx) ->
        FileGuid =:= file_ctx:get_logical_guid_const(FileCtx)
    end,
    ?assert(rpc:call(W1, meck, called, [fslogic_event_emitter, emit_file_attr_changed,
        [meck:is(TheFileCtxWithGuid), []]])),
    test_utils:mock_validate_and_unload(W1, fslogic_event_emitter).

remote_change_of_blocks_should_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    {#document{
        value = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [#file_block{offset = 1, size = 1}],
    ExternalSize = 10,
    RemoteLocation = #file_location{
        size = ExternalSize,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], [[#file_block{offset = 1, size = 1}]]}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        rpc:call(W1, fslogic_location_cache, create_location, [#document{key = datastore_key:new(), value = RemoteLocation}, true])
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % mock events
    test_utils:mock_new(W1, [fslogic_cache], [passthrough]),
    test_utils:mock_expect(W1, fslogic_cache, cache_location_change, fun(_, _) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    ?assert(rpc:call(W1, meck, called, [fslogic_cache, cache_location_change, 2])),
    test_utils:mock_validate_and_unload(W1, fslogic_cache).

remote_irrelevant_change_should_not_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % invalidate half of file
    {LocalDoc = #document{
        value = LocalLoc = #file_location{
            blocks = [Block]
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    rpc:call(W1, fslogic_location_cache, save_location, [LocalDoc#document{
        value = LocalLoc#file_location{
            blocks = [Block#file_block{offset = 0, size = 5}]
        }
    }]),

    % prepare external location
    {#document{
        value = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [#file_block{offset = 5, size = 5}],
    ExternalSize = 10,
    RemoteLocation = #file_location{
        size = ExternalSize,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], [ExternalBlocks, {shrink, 7}]}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location,
            [#document{value = RemoteLocation}]))
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 2),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % mock events
    test_utils:mock_new(W1, [fslogic_event_emitter], [passthrough]),
    test_utils:mock_expect(W1, fslogic_event_emitter, emit_file_location_changed,
        fun(_Entry, _ExcludedSessions) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
%%    ?assertEqual(0, rpc:call(W1, meck, num_calls, [fslogic_event_emitter, emit_file_location_changed, ['_', '_']])), %todo VFS-2132
    ?assertEqual(0, rpc:call(W1, meck, num_calls,
        [fslogic_event_emitter, emit_file_attr_changed, ['_', '_']])),
    test_utils:mock_validate_and_unload(W1, fslogic_event_emitter).

conflicting_remote_changes_should_be_reconciled(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"zzz_external_provider_id">>, % should be greater than LocalId
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    {LocalDoc = #document{
        value = LocalLocation = #file_location{
            version_vector = VVLocal
        }
    }, _} = rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)]),
    ExternalBlocks = [#file_block{offset = 2, size = 5}],
    ExternalChanges = [
        [#file_block{offset = 0, size = 2}],
        [#file_block{offset = 2, size = 2}],
        {shrink, 8}
    ],
    RemoteLocation = #file_location{
        size = 8,
        space_id = SpaceId,
        storage_id = <<"external_storage_id">>,
        provider_id = ExternalProviderId,
        blocks = ExternalBlocks,
        file_id = ExternalFileId,
        uuid = FileUuid,
        version_vector = VVLocal,
        recent_changes = {[], ExternalChanges}
    },
    {ok, RemoteLocationId} = ?assertMatch(
        {ok, _},
        rpc:call(W1, fslogic_location_cache, create_location,
            [#document{key = datastore_key:new(),value = RemoteLocation}, true])
    ),
    {ok, RemoteLocationDoc} = rpc:call(W1, fslogic_location_cache, get_location, [RemoteLocationId, FileUuid]),
    UpdatedRemoteLocationDoc = #document{
        value = #file_location{
            version_vector = ExternalVV
        }
    } = bump_version(RemoteLocationDoc, 3),
    ?assertMatch({ok, _}, rpc:call(W1, fslogic_location_cache, save_location, [UpdatedRemoteLocationDoc])),

    % update local location
    #document{value = #file_location{version_vector = NewLocalVV}} =
        bump_version(LocalDoc, 3),
    LocalChanges = [
        [#file_block{offset = 2, size = 2}],
        {shrink, 6},
        [#file_block{offset = 5, size = 1}]
    ],
    rpc:call(W1, fslogic_location_cache, save_location, [LocalDoc#document{
        value = LocalLocation#file_location{
            version_vector = NewLocalVV,
            recent_changes = {[], LocalChanges}
        }
    }]),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, UpdatedRemoteLocationDoc]),

    % then
    #document{value = #file_location{
        version_vector = MergedVV
    }} = bump_version(LocalDoc#document{value = LocalLocation#file_location{
        version_vector = ExternalVV
    }}, 3),

    ?assertMatch(
        {#document{
            value = #file_location{
                version_vector = MergedVV,
                blocks = [#file_block{offset = 4, size = 4}]
            }
        }, _},
        rpc:call(W1, file_ctx, get_local_file_location_doc, [file_ctx:new_by_guid(FileGuid)])
    ).

replica_invalidate_should_migrate_unique_data(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    LocalProviderId = rpc:call(W1, oneprovider, get_id, []),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, 10} = lfm_proxy:write(W1, Handle, 0, <<"0123456789">>),
    ok = lfm_proxy:close(W1, Handle),

    FileUuid = file_id:guid_to_uuid(FileGuid),

    % attach external location
    ExternalBlocks = [],
    RemoteLocationId = file_location:id(FileUuid, ExternalProviderId),
    RemoteLocation = #document{
        key = RemoteLocationId,
        value = #file_location{
            size = 10,
            space_id = SpaceId,
            storage_id = <<"external_storage_id">>,
            provider_id = ExternalProviderId,
            blocks = ExternalBlocks,
            file_id = ExternalFileId,
            uuid = FileUuid,
            version_vector = #{}
        }
    },
    ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location, [RemoteLocation]))
    ),

    override_space_providers_mock(Config, Workers, SpaceId, [LocalProviderId, ExternalProviderId]),

    test_utils:mock_new(Workers, lfm, [passthrough]),
    test_utils:mock_expect(Workers, lfm, schedule_file_replication,
        fun(_SessId, _FileKey, _ProviderId) -> ok end),

    % when
    ok = lfm_proxy:schedule_file_replica_eviction(W1, SessionId, ?FILE_REF(FileGuid), LocalProviderId, ExternalProviderId),
    {ok, Handle2} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, 10} = lfm_proxy:write(W1, Handle2, 0, <<"0123456789">>),
    ok = lfm_proxy:close(W1, Handle2),

    % then
    test_utils:mock_assert_num_calls(W1, lfm, schedule_file_replication, [SessionId, ?FILE_REF(FileGuid), ExternalProviderId], 1),

    % when
    ok = lfm_proxy:schedule_file_replica_eviction(W1, SessionId, ?FILE_REF(FileGuid), LocalProviderId, undefined),

    % then
    test_utils:mock_assert_num_calls(W1, lfm, schedule_file_replication, [SessionId, ?FILE_REF(FileGuid), ExternalProviderId], 1),
    test_utils:mock_validate_and_unload(Workers, [od_space, lfm]).

replica_invalidate_should_truncate_storage_file_to_zero_size(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    LocalProviderId = rpc:call(W1, oneprovider, get_id, []),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGuid} =
        lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, 10} = lfm_proxy:write(W1, Handle, 0, <<"0123456789">>),
    ok = lfm_proxy:close(W1, Handle),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {SDHandle, _} = rpc:call(W1, storage_driver, new_handle, [SessionId, FileCtx]),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % attach external location
    ExternalBlocks = [#file_block{offset = 0, size = 10}],
    RemoteLocationId = file_location:id(FileUuid, ExternalProviderId),
    RemoteLocation = #document{
        key = RemoteLocationId,
        value = #file_location{
            size = 10,
            space_id = SpaceId,
            storage_id = <<"external_storage_id">>,
            provider_id = ExternalProviderId,
            blocks = ExternalBlocks,
            file_id = ExternalFileId,
            uuid = FileUuid,
            version_vector = #{}
        }
    },
    ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location, [RemoteLocation]))
    ),

    override_space_providers_mock(Config, Workers, SpaceId, [LocalProviderId, ExternalProviderId]),

    test_utils:mock_new(Workers, lfm, [passthrough]),
    test_utils:mock_expect(Workers, lfm, schedule_file_replication,
        fun(_SessId, _FileKey, _ProviderId) -> ok end),

    % when
    ?assertMatch({ok, #statbuf{st_size = 10}}, rpc:call(W1, storage_driver, stat, [SDHandle])),
    ok = lfm_proxy:schedule_file_replica_eviction(W1, SessionId, ?FILE_REF(FileGuid), LocalProviderId, ExternalProviderId),

    % then
    ?assertMatch({undefined, _}, rpc:call(W1, file_ctx, get_local_file_location_doc, [FileCtx])),
    ?assertMatch({ok, #statbuf{st_size = 0}}, rpc:call(W1, storage_driver, stat, [SDHandle])),
    test_utils:mock_validate_and_unload(W1, [lfm]).

dir_replica_invalidate_should_invalidate_all_children(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    LocalProviderId = rpc:call(W1, oneprovider, get_id, []),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test files
    {ok, DirGuid} = lfm_proxy:mkdir(W1, SessionId, <<SpaceName/binary, "/dir">>),

    {ok, FileGuid1} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/dir/file1">>),
    {ok, Handle1} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid1), write),
    {ok, 10} = lfm_proxy:write(W1, Handle1, 0, <<"0123456789">>),
    ok = lfm_proxy:close(W1, Handle1),

    {ok, _Dir2Guid} = lfm_proxy:mkdir(W1, SessionId, <<SpaceName/binary, "/dir/dir2">>),

    {ok, FileGuid2} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/dir/dir2/file2">>),
    {ok, Handle2} = lfm_proxy:open(W1, SessionId, ?FILE_REF(FileGuid2), write),
    {ok, 10} = lfm_proxy:write(W1, Handle2, 0, <<"0123456789">>),
    ok = lfm_proxy:close(W1, Handle2),

    FileCtx1 = file_ctx:new_by_guid(FileGuid1),
    FileCtx2 = file_ctx:new_by_guid(FileGuid2),
    {SDHandle1, _} = rpc:call(W1, storage_driver, new_handle, [SessionId, FileCtx1]),
    {SDHandle2, _} = rpc:call(W1, storage_driver, new_handle, [SessionId, FileCtx2]),
    FileUuid1 = file_id:guid_to_uuid(FileGuid1),
    FileUuid2 = file_id:guid_to_uuid(FileGuid2),

    % attach external location
    ExternalBlocks = [#file_block{offset = 0, size = 10}],
    RemoteLocation1 = #document{
        key = file_location:id(FileUuid1, ExternalProviderId),
        value = #file_location{
            size = 10,
            space_id = SpaceId,
            storage_id = <<"external_storage_id">>,
            provider_id = ExternalProviderId,
            blocks = ExternalBlocks,
            file_id = ExternalFileId,
            uuid = FileUuid1,
            version_vector = #{}
        }
    },
    RemoteLocation2 = #document{
        key = file_location:id(FileUuid2, ExternalProviderId),
        value = #file_location{
            size = 10,
            space_id = SpaceId,
            storage_id = <<"external_storage_id">>,
            provider_id = ExternalProviderId,
            blocks = ExternalBlocks,
            file_id = ExternalFileId,
            uuid = FileUuid1,
            version_vector = #{}
        }
    },
    ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location, [RemoteLocation1]))
    ),
    ?assertMatch(
        {ok, _},
        ?extract_key(rpc:call(W1, fslogic_location_cache, create_location, [RemoteLocation2]))
    ),

    override_space_providers_mock(Config, Workers, SpaceId, [LocalProviderId, ExternalProviderId]),

    test_utils:mock_new(Workers, lfm, [passthrough]),
    test_utils:mock_expect(Workers, lfm, schedule_file_replication,
        fun(_SessId, _FileKey, _ProviderId) -> ok end),

    % when
    ?assertMatch({ok, #statbuf{st_size = 10}}, rpc:call(W1, storage_driver, stat, [SDHandle1])),
    ?assertMatch({ok, #statbuf{st_size = 10}}, rpc:call(W1, storage_driver, stat, [SDHandle2])),
    ok = lfm_proxy:schedule_file_replica_eviction(W1, SessionId, ?FILE_REF(DirGuid), LocalProviderId, ExternalProviderId),

    % then
    ?assertMatch({ok, #statbuf{st_size = 0}}, rpc:call(W1, storage_driver, stat, [SDHandle1])),
    ?assertMatch({ok, #statbuf{st_size = 0}}, rpc:call(W1, storage_driver, stat, [SDHandle2])),
    test_utils:mock_validate_and_unload(W1, [lfm]).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(local_file_location_should_be_chowned_when_missing_user_appears, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(W1, sd_utils, [passthrough]),
    test_utils:mock_expect(W1, sd_utils, create_deferred,
        fun(FileCtx, UserCtx, VerifyLink, CheckLocationExists) ->
            {Doc, FileCtx2} = meck:passthrough([FileCtx, UserCtx, VerifyLink, CheckLocationExists]),
            {Doc, files_to_chown:chown_or_defer(FileCtx2)}
        end),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    ssl:start(),
    application:ensure_all_started(hackney),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(local_file_location_should_be_chowned_when_missing_user_appears, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(W1, [sd_utils]),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    initializer:unload_quota_mocks(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
     application:stop(hackney),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

bump_version(LocationDoc, 0) ->
    LocationDoc;
bump_version(LocationDoc, N) when N > 0 ->
    bump_version(version_vector:bump_version(LocationDoc), N - 1).

get_storage_file_id_by_uuid(Worker, FileUuid) ->
    FileGuid = rpc:call(Worker, fslogic_uuid, uuid_to_guid, [FileUuid]),
    FileCtx = rpc:call(Worker, file_ctx, new_by_guid, [FileGuid]),
    {StorageFileId, _} = rpc:call(Worker, file_ctx, get_storage_file_id, [FileCtx]),
    StorageFileId.

% space_logic is mocked in initializer to return data from default test
% setup, modify this mock so that user's space has more providers.
% Given that this just overrides the mock from initializer, no need to unmock
% (this will be done in test cleanup).
override_space_providers_mock(Config, Workers, SpaceId, Providers) ->
    UserSpaces = ?config({spaces, <<"user1">>}, Config),
    SpaceName = ?config(SpaceId, UserSpaces),

    test_utils:mock_unload(Workers, [space_logic]),
    test_utils:mock_new(Workers, space_logic, []),
    test_utils:mock_expect(Workers, space_logic, is_owner, fun(_, _) -> false end),
    test_utils:mock_expect(Workers, space_logic, has_eff_user,
        fun(_Client, SpId, UsId) ->
            SpId =:= SpaceId andalso UsId =:= <<"user1">>
        end),
    test_utils:mock_expect(Workers, space_logic, has_eff_privilege,
        fun(SpId, UsId, Privilege) ->
            SpId =:= SpaceId andalso UsId =:= <<"user1">> andalso lists:member(Privilege, privileges:space_privileges())
        end),
    test_utils:mock_expect(Workers, space_logic, get_eff_privileges,
        fun(_SpId, _UsId) ->
            {ok, privileges:space_privileges()}
        end),
    test_utils:mock_expect(Workers, space_logic, get_name,
        fun(_, SpId) when SpId =:= SpaceId ->
            {ok, SpaceName}
        end),
    test_utils:mock_expect(Workers, space_logic, get_provider_ids,
        fun(_Client, SpId) when SpId =:= SpaceId ->
            {ok, Providers}
        end),
    test_utils:mock_expect(Workers, space_logic, is_supported,
        fun(_Client, SpId, ProvId) when SpId =:= SpaceId ->
            lists:member(ProvId, Providers)
        end),
    test_utils:mock_expect(Workers, space_logic, get_support_size,
        fun(SpId, _ProvId) when SpId =:= SpaceId ->
            {ok, ?SUPPORT_SIZE}
        end),
    test_utils:mock_expect(Workers, space_logic, get_local_storages,
        fun(SpId) when SpId =:= SpaceId ->
            {ok, lists:foldl(fun(Worker, Acc) ->
                case ?config({storage_id, ?GET_DOMAIN(Worker)}, Config) of
                    undefined -> Acc;
                    StorageId -> [StorageId | Acc]
                end
            end, [], Workers)}
        end).

