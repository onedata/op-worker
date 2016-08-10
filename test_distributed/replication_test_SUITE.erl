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
-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    dbsync_trigger_should_create_local_file_location/1,
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
    rtransfer_config_should_work/1
]).


-performance({test_cases, []}).
all() ->
    ?ALL([
        dbsync_trigger_should_create_local_file_location,
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
        conflicting_remote_changes_should_be_reconciled,
        rtransfer_config_should_work
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

dbsync_trigger_should_create_local_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    CTime = erlang:monotonic_time(micro_seconds),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"file">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = UserId
    },
    {ok, FileUuid} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #change{model = file_meta, doc = #document{key = FileUuid, value = FileMeta}}]),

    %then
    ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessionId, {uuid, FileUuid}, rdwr)),
    ?assertMatch({ok, 3}, lfm_proxy:write(W1, Handle, 0, <<"aaa">>)),
    ?assertMatch({ok, <<"aaa">>}, lfm_proxy:read(W1, Handle, 0, 3)).

local_file_location_should_have_correct_uid_for_local_user(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    StorageDir = ?config({storage_dir, ?GET_DOMAIN(W1)}, Config),
    CTime = erlang:monotonic_time(micro_seconds),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"test_file">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = UserId
    },
    {ok, FileUuid} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])),
    {ok, FileToCompareUUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/file_to_compare">>, 8#777),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #change{model = file_meta, doc = #document{key = FileUuid, value = FileMeta}}]),

    %then
    Uid = rpc:call(W1, luma_utils, gen_storage_uid, [UserId]),
    {ok, CorrectFileInfo} = rpc:call(W1, file, read_file_info, [filename:join([StorageDir, SpaceId, fslogic_utils:gen_storage_file_id({uuid, FileToCompareUUID})])]),
    {ok, FileInfo} = rpc:call(W1, file, read_file_info, [filename:join([StorageDir, SpaceId, fslogic_utils:gen_storage_file_id({uuid, FileUuid})])]),
    ?assertEqual(Uid, FileInfo#file_info.uid),
    ?assertNotEqual(0, FileInfo#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.uid, FileInfo#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.gid, FileInfo#file_info.gid).

local_file_location_should_be_chowned_when_missing_user_appears(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalUser = <<"external_user_id">>,
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    StorageDir = ?config({storage_dir, ?GET_DOMAIN(W1)}, Config),
    CTime = erlang:monotonic_time(micro_seconds),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"test_file">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = ExternalUser
    },
    FileMeta2 = #file_meta{
        mode = 8#777,
        name = <<"test_file2">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = ExternalUser
    },
    {ok, FileUuid} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])),
    {ok, FileUuid2} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta2])),
    {ok, _} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/file_to_compare">>, 8#777),

    %when
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #change{model = file_meta, doc = #document{key = FileUuid, value = FileMeta}}]),
    rpc:call(W1, dbsync_events, change_replicated,
        [SpaceId, #change{model = file_meta, doc = #document{key = FileUuid2, value = FileMeta2}}]),
    rpc:call(W1, onedata_user, create, [#document{key = ExternalUser, value = #onedata_user{name = <<"User">>, spaces = [{SpaceId, SpaceName}]}}]),
    timer:sleep(timer:seconds(1)), % need to wait for asynchronous trigger

    %then
    Uid = rpc:call(W1, luma_utils, gen_storage_uid, [ExternalUser]),
    {ok, CorrectFileInfo} = rpc:call(W1, file, read_file_info, [filename:join([StorageDir, SpaceId, <<"file_to_compare::1">>])]),
    {ok, FileInfo1} = rpc:call(W1, file, read_file_info, [filename:join([StorageDir, SpaceId, fslogic_utils:gen_storage_file_id({uuid, FileUuid})])]),
    {ok, FileInfo2} = rpc:call(W1, file, read_file_info, [filename:join([StorageDir, SpaceId, fslogic_utils:gen_storage_file_id({uuid, FileUuid2})])]),
    ?assertEqual(Uid, FileInfo1#file_info.uid),
    ?assertEqual(Uid, FileInfo2#file_info.uid),
    ?assertNotEqual(CorrectFileInfo#file_info.uid, FileInfo1#file_info.uid),
    ?assertNotEqual(CorrectFileInfo#file_info.uid, FileInfo2#file_info.uid),
    ?assertEqual(CorrectFileInfo#file_info.gid, FileInfo1#file_info.gid),
    ?assertEqual(CorrectFileInfo#file_info.gid, FileInfo2#file_info.gid).

write_should_add_blocks_to_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),

    %when
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, LocationDoc = #document{value = Location = #file_location{blocks = Blocks, size = Size, provider_id = ProviderId}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(initializer:domain_to_provider_id(?GET_DOMAIN(W1)), ProviderId),
    ?assertEqual(10, Size),
    [Block] = ?assertMatch([#file_block{offset = 0, size = 10}], Blocks),

    % when
    LocationWithoutBeginning =
        LocationDoc#document{value = Location#file_location{blocks = [Block#file_block{offset = 5, size = 5}]}},
    ?assertMatch({ok, _},
        rpc:call(W1, file_location, save, [LocationWithoutBeginning])),
    ?assertMatch({ok, 5}, lfm_proxy:write(W1, Handle, 0, <<"11111">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, #document{value = #file_location{blocks = Blocks2, size = Size2}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(10, Size2),
    ?assertMatch([Block], Blocks2).

truncate_should_change_size_and_blocks(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 6)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    {ok, #document{value = #file_location{blocks = Blocks, size = Size}}} =
        ?assertMatch({ok, _}, rpc:call(W1, file_location, get, [LocationId])),
    ?assertEqual(6, Size),
    ?assertMatch([#file_block{offset = 0, size = 6}], Blocks).

write_and_truncate_should_not_update_remote_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,
    ExternalBlocks = [#file_block{offset = 0, size = 10, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    RemoteLocation = #file_location{size = 10, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = #{}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % when
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 1, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 8)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % then
    ?assertMatch({ok, #document{value = RemoteLocation}},
        rpc:call(W1, file_location, get, [RemoteLocationId])).

update_should_bump_replica_version(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(W1)),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),

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
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    VV1 = maps:put({ProviderId, LocationId}, 5, #{}),
    ?assertMatch({ok, #document{value = #file_location{version_vector = VV1, blocks = [#file_block{offset = 0, size = 10}]}}},
        rpc:call(W1, file_location, get, [LocationId])),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 2)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 0)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    %then
    VV2 = maps:put({ProviderId, LocationId}, 9, #{}),
    ?assertMatch({ok, #document{value = #file_location{version_vector = VV2}}},
        rpc:call(W1, file_location, get, [LocationId])).

read_should_synchronize_file(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),

    % attach external location
    ExternalBlocks = [#file_block{offset = 0, size = 10, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    RemoteLocation = #file_location{size = 10, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = #{}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % mock rtransfer
    test_utils:mock_new(Workers, rtransfer),
    test_utils:mock_expect(Workers, rtransfer, prepare_request,
        fun(_ProvId, _Uuid, 1, Size) when Size >= 3 ->
            ref
        end
    ),
    test_utils:mock_expect(Workers, rtransfer, fetch,
        fun(ref, _NotifyFun, OnCompleteFun) ->
            OnCompleteFun(ref, {ok, 3}),
            ref
        end
    ),

    % when
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    Ans = lfm_proxy:read(W1, Handle, 1, 3),

    % then
    ?assertEqual({ok, <<>>}, Ans),
    ?assertEqual(1, rpc:call(W1, meck, num_calls, [rtransfer, prepare_request, '_'])),
    ?assert(rpc:call(W1, meck, called, [rtransfer, prepare_request, [ExternalProviderId, FileGUID, 1, '_']])),
    ?assertEqual(1, rpc:call(W1, meck, num_calls, [rtransfer, fetch, '_'])),
    ?assert(rpc:call(W1, meck, called, [rtransfer, fetch, [ref, '_', '_']])),
    test_utils:mock_validate_and_unload(Workers, rtransfer),
    ?assertMatch(#document{value = #file_location{blocks = [#file_block{offset = 1, size = 3}]}},
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}])).

external_change_should_invalidate_blocks(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    #document{value = #file_location{version_vector = VVLocal}} = rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 2, size = 5, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    RemoteLocation = #file_location{size = 10, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, recent_changes = {[], [ExternalBlocks]}, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = #document{value = #file_location{version_vector = VV}} =
        version_vector:bump_version(RemoteLocationDoc),
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    ?assertMatch(#document{value = #file_location{version_vector = VV, blocks = [#file_block{offset = 0, size = 2}, #file_block{offset = 7, size = 3}]}},
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}])).


update_should_save_recent_changes(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),

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
    {ok, [LocationId]} = ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUuid}])),
    ?assertMatch({ok, #document{value = #file_location{blocks = [#file_block{offset = 0, size = 10}]}}},
        rpc:call(W1, file_location, get, [LocationId])),

    %when
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 2)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch({ok, 2}, lfm_proxy:write(W1, Handle, 0, <<"00">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),
    ?assertMatch(ok, lfm_proxy:truncate(W1, SessionId, {guid, FileGUID}, 0)),
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
        rpc:call(W1, file_location, get, [LocationId])).

remote_change_should_invalidate_only_updated_part_of_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    LocalDoc = #document{value = LocalLocation = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 2, size = 5, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    ExternalChanges = [
        [#file_block{offset = 2, size = 2}],
        [#file_block{offset = 7, size = 3}],
        [#file_block{offset = 0, size = 10}],
        [#file_block{offset = 1, size = 5}]
    ],
    RemoteLocation = #file_location{size = 10, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], ExternalChanges}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = #document{value = #file_location{version_vector = VV}} =
        bump_version(RemoteLocationDoc, 4),

    % prepare local doc version
    #document{value = #file_location{version_vector = NewLocalVV}} = bump_version(RemoteLocationDoc, 2),
    rpc:call(W1, file_location, save, [LocalDoc#document{value = LocalLocation#file_location{version_vector = NewLocalVV}}]),

    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    ?assertMatch(#document{value = #file_location{version_vector = VV, blocks = [#file_block{offset = 0, size = 2}, #file_block{offset = 4, size = 3}]}},
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}])).

remote_change_without_history_should_invalidate_whole_data(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceId = <<"space_id1">>,
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    #document{value = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 1, size = 1}, #file_block{offset = 5, size = 1}],
    ExternalSize = 8,
    RemoteLocation = #file_location{size = ExternalSize, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], []}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = #document{value = #file_location{version_vector = VV}} =
        bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    ?assertMatch(#document{value = #file_location{version_vector = VV, size = ExternalSize,
        blocks = [
            #file_block{offset = 0, size = 1},
            #file_block{offset = 2, size = 3},
            #file_block{offset = 6, size = 2}
        ]}},
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}])).

remote_change_of_size_should_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    #document{value = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [],
    ExternalSize = 8,
    RemoteLocation = #file_location{size = ExternalSize, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], [{shrink, 8}]}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % mock events
    test_utils:mock_new(W1, [fslogic_event]),
    test_utils:mock_expect(W1, fslogic_event, emit_file_attr_update,
        fun(_Entry, _ExcludedSessions) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    ?assert(rpc:call(W1, meck, called, [fslogic_event, emit_file_attr_update, [{uuid, FileUuid}, []]])),
    test_utils:mock_validate_and_unload(W1, fslogic_event).

remote_change_of_blocks_should_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % prepare external location
    #document{value = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 1, size = 1}],
    ExternalSize = 10,
    RemoteLocation = #file_location{size = ExternalSize, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], [[#file_block{offset = 1, size = 1}]]}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 1),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % mock events
    test_utils:mock_new(W1, [fslogic_event]),
    test_utils:mock_expect(W1, fslogic_event, emit_file_location_update,
        fun(_Entry, _ExcludedSessions) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    ?assert(rpc:call(W1, meck, called, [fslogic_event, emit_file_location_update, [{uuid, FileUuid}, []]])),
    test_utils:mock_validate_and_unload(W1, fslogic_event).

remote_irrelevant_change_should_not_notify_clients(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"external_provider_id">>,
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % invalidate half of file
    LocalDoc = #document{value = LocalLoc = #file_location{blocks = [Block]}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    rpc:call(W1, file_location, save, [LocalDoc#document{value = LocalLoc#file_location{blocks = [Block#file_block{offset = 0, size = 5}]}}]),

    % prepare external location
    #document{value = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 5, size = 5}],
    ExternalSize = 10,
    RemoteLocation = #file_location{size = ExternalSize, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], [ExternalBlocks, {shrink, 7}]}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = bump_version(RemoteLocationDoc, 2),

    % attach external location
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % mock events
    test_utils:mock_new(W1, [fslogic_event]),
    test_utils:mock_expect(W1, fslogic_event, emit_file_location_update,
        fun(_Entry, _ExcludedSessions) -> ok end),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
%%    ?assertEqual(0, rpc:call(W1, meck, num_calls, [fslogic_event, emit_file_location_update, ['_', '_']])), %todo VFS-2132
    ?assertEqual(0, rpc:call(W1, meck, num_calls, [fslogic_event, emit_file_attr_update, ['_', '_']])),
    test_utils:mock_validate_and_unload(W1, fslogic_event).


conflicting_remote_changes_should_be_reconciled(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ExternalProviderId = <<"zzz_external_provider_id">>, % should be greater than LocalId
    ExternalFileId = <<"external_file_id">>,

    % create test file
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),
    FileUuid = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, Handle} = lfm_proxy:open(W1, SessionId, {guid, FileGUID}, rdwr),
    ?assertMatch({ok, 10}, lfm_proxy:write(W1, Handle, 0, <<"0123456789">>)),
    ?assertMatch(ok, lfm_proxy:fsync(W1, Handle)),

    % attach external location
    LocalDoc = #document{value = LocalLocation = #file_location{version_vector = VVLocal}} =
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}]),
    ExternalBlocks = [#file_block{offset = 2, size = 5, file_id = ExternalFileId, storage_id = <<"external_storage_id">>}],
    ExternalChanges = [
        [#file_block{offset = 0, size = 2}],
        [#file_block{offset = 2, size = 2}],
        {shrink, 8}
    ],
    RemoteLocation = #file_location{size = 8, space_id = SpaceId,
        storage_id = <<"external_storage_id">>, provider_id = ExternalProviderId,
        blocks = ExternalBlocks, file_id = ExternalFileId, uuid = FileUuid,
        version_vector = VVLocal, recent_changes = {[], ExternalChanges}},
    {ok, RemoteLocationId} = ?assertMatch({ok, _},
        rpc:call(W1, file_location, create, [#document{value = RemoteLocation}])),
    {ok, RemoteLocationDoc} = rpc:call(W1, file_location, get, [RemoteLocationId]),
    UpdatedRemoteLocationDoc = #document{value = #file_location{version_vector = ExternalVV}} =
        bump_version(RemoteLocationDoc, 3),
    ?assertMatch({ok, _}, rpc:call(W1, file_location, save, [UpdatedRemoteLocationDoc])),
    ?assertEqual(ok, rpc:call(W1, file_meta, attach_location,
        [{uuid, FileUuid}, RemoteLocationId, ExternalProviderId])),

    % update local location
    #document{value = #file_location{version_vector = NewLocalVV}} = bump_version(LocalDoc, 3),
    LocalChanges = [
        [#file_block{offset = 2, size = 2}],
        {shrink, 6},
        [#file_block{offset = 5, size = 1}]
    ],
    rpc:call(W1, file_location, save, [LocalDoc#document{value =
    LocalLocation#file_location{version_vector = NewLocalVV, recent_changes = {[], LocalChanges}}}]),

    % when
    rpc:call(W1, dbsync_events, change_replicated, [SpaceId,
        #change{model = file_location, doc = UpdatedRemoteLocationDoc}]),

    % then
    #document{value = #file_location{version_vector = MergedVV}} =
        bump_version(LocalDoc#document{value = LocalLocation#file_location{version_vector = ExternalVV}}, 3),
    ?assertMatch(#document{value = #file_location{
        version_vector = MergedVV,
        blocks = [#file_block{offset = 4, size = 4}]}},
        rpc:call(W1, fslogic_utils, get_local_file_location, [{uuid, FileUuid}])).


rtransfer_config_should_work(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    {ok, FileGUID} = lfm_proxy:create(W1, SessionId, <<SpaceName/binary, "/test_file">>, 8#777),

    ?assertEqual(ok, rpc:call(W1, erlang, apply, [
        fun() ->
            Opts = rtransfer_config:rtransfer_opts(),
            Open = proplists:get_value(open_fun, Opts),
            Read = proplists:get_value(read_fun, Opts),
            Write = proplists:get_value(write_fun, Opts),
            {ok, WriteHandle} = erlang:apply(Open, [FileGUID, write]),
            {ok, _, 4} = erlang:apply(Write, [WriteHandle, 0, <<"data">>]),
            {ok, ReadHandle} = erlang:apply(Open, [FileGUID, read]),
            {ok, _, <<"data">>} = erlang:apply(Read, [ReadHandle, 0, 10]),
            ok
        end, []
    ])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer, ?MODULE]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:unload_quota_mocks(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(etls).

%%%===================================================================
%%% Internal functions
%%%===================================================================

bump_version(LocationDoc, 0) ->
    LocationDoc;
bump_version(LocationDoc, N) when N > 0 ->
    bump_version(version_vector:bump_version(LocationDoc), N - 1).