%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider rest tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_rest_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    get_simple_file_distribution/1,
    replicate_file/1,
    replicate_already_replicated_file/1,
    transfers_should_be_ordered_by_timestamps/1,
    replicate_not_synced_file/1,
    replicate_file_to_source_provider/1,
    restart_file_replication/1,
    cancel_file_replication/1,
    replicate_dir/1,
    restart_dir_replication/1,
    replicate_file_by_id/1,
    replicate_to_missing_provider/1,
    replicate_to_nonsupporting_provider/1,
    invalidate_file_replica/1,
    invalidate_file_replica_with_migration/1,
    restart_invalidation_of_file_replica_with_migration/1,
    invalidate_dir_replica/1,
    automatic_cleanup_should_invalidate_unpopular_files/1,
    posix_mode_get/1,
    posix_mode_put/1,
    attributes_list/1,
    xattr_get/1,
    xattr_put/1,
    xattr_list/1,
    metric_get/1,
    list_file/1,
    list_dir/1,
    list_dir_range/1,
    changes_stream_file_meta_test/1,
    changes_stream_xattr_test/1,
    changes_stream_json_metadata_test/1,
    changes_stream_times_test/1,
    changes_stream_file_location_test/1,
    changes_stream_on_multi_provider_test/1,
    list_spaces/1,
    get_space/1,
    set_get_json_metadata/1,
    set_get_json_metadata_id/1,
    set_get_rdf_metadata/1,
    set_get_rdf_metadata_id/1,
    remove_index/1,
    create_list_index/1,
    create_geospatial_index/1,
    query_geospatial_index/1,
    query_file_popularity_index/1,
    set_get_json_metadata_inherited/1,
    set_get_xattr_inherited/1,
    set_get_json_metadata_using_filter/1,
    primitive_json_metadata_test/1,
    empty_metadata_invalid_json_test/1,
    spatial_flag_test/1,
    quota_exceeded_during_file_replication/1,
    quota_decreased_after_invalidation/1,
    file_replication_failures_should_fail_whole_transfer/1,
    replicate_big_dir/1,
    replicate_big_file/1,
    invalidate_big_dir/1,
    many_simultaneous_transfers/1,
    many_simultaneous_failed_transfers/1]).

%utils
-export([verify_file/3, create_file/3, create_dir/3,
    create_nested_directory_tree/4, sync_file_counter/3, create_file_counter/4]).

all() ->
    ?ALL([
        get_simple_file_distribution,
        replicate_file,
        replicate_already_replicated_file,
        transfers_should_be_ordered_by_timestamps,
        replicate_not_synced_file,
        replicate_file_to_source_provider,
        restart_file_replication,
        cancel_file_replication,
        replicate_dir,
        restart_dir_replication,
        replicate_file_by_id,
        replicate_to_missing_provider,
        replicate_to_nonsupporting_provider,
        invalidate_file_replica,
        invalidate_file_replica_with_migration,
        restart_invalidation_of_file_replica_with_migration,
        invalidate_dir_replica,
        automatic_cleanup_should_invalidate_unpopular_files,
        posix_mode_get,
        posix_mode_put,
        attributes_list,
        xattr_get,
        xattr_put,
        xattr_list,
        metric_get,
        list_file,
        list_dir,
        list_dir_range,
        changes_stream_file_meta_test,
        changes_stream_xattr_test,
        changes_stream_json_metadata_test,
        changes_stream_times_test,
        changes_stream_file_location_test,
        changes_stream_on_multi_provider_test, %todo fix VFS-2864
        list_spaces,
        get_space,
        set_get_json_metadata,
        set_get_json_metadata_id,
        set_get_rdf_metadata,
        set_get_rdf_metadata_id,
        remove_index,
        create_list_index,
        create_geospatial_index,
        query_geospatial_index,
        query_file_popularity_index,
        set_get_json_metadata_inherited,
        set_get_xattr_inherited,
        set_get_json_metadata_using_filter,
        primitive_json_metadata_test,
        empty_metadata_invalid_json_test,
        spatial_flag_test,
        many_simultaneous_failed_transfers,
        many_simultaneous_transfers,
        quota_exceeded_during_file_replication,
        quota_decreased_after_invalidation,
        file_replication_failures_should_fail_whole_transfer,
        replicate_big_dir,
        replicate_big_file,
        invalidate_big_dir
    ]).

-define(LIST_TRANSFER, fun(Id, Acc) -> [Id | Acc] end).
-define(ATTEMPTS, 60).

-define(assertDistribution(Worker, ExpectedDistribution, Config, File),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        case do_request(Worker, <<"replicas", File/binary>>, get,
            [user_1_token_header(Config)], []
        ) of
            {ok, 200, _, __Body} ->
                lists:sort(json_utils:decode_map(__Body));
            Error ->
                Error
        end
    end, ?ATTEMPTS)).

-define(assertDistributionProxyByGuid(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        {ok, __FileBlocks} = lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}),
        lists:sort(__FileBlocks)
    end, ?ATTEMPTS)
).

-define(assertDistributionById(Worker, ExpectedDistribution, Config, FileId),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        case do_request(Worker, <<"replicas-id/", FileId/binary>>, get,
            [user_1_token_header(Config)], []
        ) of
            {ok, 200, _, __Body} ->
                lists:sort(json_utils:decode_map(__Body));
            Error ->
                Error
        end
    end, ?ATTEMPTS)).

-define(assertTransferStatus(ExpectedStatus, Worker, Tid, Config),
    ?assertTransferStatus(ExpectedStatus, Worker, Tid, Config, ?ATTEMPTS)).

-define(assertTransferStatus(ExpectedStatus, Worker, Tid, Config, Attempts),
    ?assertMatch(ExpectedStatus,
        case do_request(Worker, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, __TransferStatus} ->
                json_utils:decode_map(__TransferStatus);
            Error -> Error
        end, Attempts)
).

-define(absPath(SpaceId, Path), <<"/", SpaceId/binary, "/", Path/binary>>).

-define(TEST_DATA, <<"test">>).
-define(TEST_DATA_SIZE, byte_size(?TEST_DATA)).
-define(TEST_DATA2, <<"test01234">>).
-define(TEST_DATA_SIZE2, byte_size(?TEST_DATA2)).

-define(TARGET, 30).
-define(THRESHOLD, 35).
-define(LOWER_SIZE_LIMIT, 5).
-define(UPPER_SIZE_LIMIT, 10).
-define(MAX_INACTIVE_TIME, 15). %hours

-define(AUTOCLEANING_SETTINGS, #{
    enabled => true,
    lower_file_size_limit => ?LOWER_SIZE_LIMIT,
    upper_file_size_limit => ?UPPER_SIZE_LIMIT,
    max_file_not_opened_hours => ?MAX_INACTIVE_TIME,
    target => ?TARGET,
    threshold => ?THRESHOLD
}).

-define(CREATE_FILE_COUNTER, create_file_counter).
-define(SYNC_FILE_COUNTER, sync_file_counter).
-define(VERIFY_POOL, verify_pool).
-define(ZERO_SOFT_QUOTA, 0).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_simple_file_distribution(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file0_gsfd"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    ExpectedDistribution = [#{
        <<"providerId">> => domain(WorkerP1),
        <<"blocks">> => [[0, 4]]
    }],

    % then
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File).

replicate_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file">>),
    Size = ?TEST_DATA_SIZE,
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesTransferred">> := 1,
        <<"filesInvalidated">> := 0,
        <<"failedFiles">> := 0,
        <<"bytesTransferred">> := Size,
        <<"minHist">> := #{DomainP1 := [Size | _]},
        <<"hrHist">> := #{DomainP1 := [Size | _]},
        <<"dyHist">> := #{DomainP1 := [Size | _]},
        <<"mthHist">> := #{DomainP1 := [Size | _]}
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, ?TEST_DATA_SIZE]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, ?TEST_DATA_SIZE]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_already_replicated_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file_already_replicated">>),
    Size = ?TEST_DATA_SIZE,
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesTransferred">> := 1,
        <<"filesInvalidated">> := 0,
        <<"failedFiles">> := 0,
        <<"bytesTransferred">> := Size,
        <<"minHist">> := #{DomainP1 := [Size | _]},
        <<"hrHist">> := #{DomainP1 := [Size | _]},
        <<"dyHist">> := #{DomainP1 := [Size | _]},
        <<"mthHist">> := #{DomainP1 := [Size | _]}
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, ?TEST_DATA_SIZE]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, ?TEST_DATA_SIZE]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    Tid2 = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesTransferred">> := 0,
        <<"filesInvalidated">> := 0,
        <<"bytesTransferred">> := 0,
        <<"minHist">> := #{},
        <<"hrHist">> := #{},
        <<"dyHist">> := #{},
        <<"mthHist">> := #{}
    }, WorkerP1, Tid2, Config),

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

transfers_should_be_ordered_by_timestamps(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file_sorted">>),
    Size = 1024 * 1024,
    FileGuid = create_test_file(WorkerP1, SessionId, File, crypto:strong_rand_bytes(Size)),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    File2 = ?absPath(SpaceId, <<"file_sorted2">>),
    Size2 = 1024 * 1024 * 1024,
    FileGuid2 = create_test_file(WorkerP1, SessionId, File2, crypto:strong_rand_bytes(Size2)),
    {ok, FileObjectId2} = cdmi_id:guid_to_objectid(FileGuid2),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    Tid2 = schedule_file_replication(WorkerP1, DomainP2, File2, Config),
    timer:sleep(timer:seconds(1)),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesTransferred">> := 1,
        <<"filesInvalidated">> := 0,
        <<"bytesTransferred">> := Size,
        <<"dyHist">> := #{DomainP1 := [Size | _]},
        <<"mthHist">> := #{DomainP1 := [Size | _]}
    }, WorkerP1, Tid, Config),

    % and
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesTransferred">> := 1,
        <<"filesInvalidated">> := 0,
        <<"bytesTransferred">> := Size2,
        <<"dyHist">> := #{DomainP1 := [Size2 | _]},
        <<"mthHist">> := #{DomainP1 := [Size2 | _]}
    }, WorkerP1, Tid2, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assert(get_finish_time(WorkerP2, Tid, Config) < get_finish_time(WorkerP2, Tid2, Config)).

replicate_not_synced_file(Config) ->
    % list on Dir1 is mocked to return not existing file
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SpaceId = ?config(space_id, Config),
    Dir1 = ?absPath(SpaceId, <<"dir1_not_synced">>),
    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    DomainP2 = domain(WorkerP2),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),

    % when
    Tid = schedule_file_replication(WorkerP1, DomainP2, Dir1, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 2,
        <<"filesProcessed">> := 2,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 0
    }, WorkerP1, Tid, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_file_to_source_provider(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),

    File = ?absPath(SpaceId, <<"file_schedule_provider">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    Tid = schedule_file_replication(WorkerP2, DomainP1, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP1,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

restart_file_replication(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file_restart_replication">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    mock_file_replication_failure(WorkerP2),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 1,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 0,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId
    }, WorkerP1, Tid, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    unmock_file_replication(WorkerP2),
    restart_file_replication(WorkerP2, Tid, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 4,
        <<"minHist">> := #{DomainP1 := [4 | _]},
        <<"hrHist">> := #{DomainP1 := [4 | _]},
        <<"dyHist">> := #{DomainP1 := [4 | _]},
        <<"mthHist">> := #{DomainP1 := [4 | _]}
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

cancel_file_replication(Config) ->
    ct:timetrap({minutes, 10}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = <<"space3">>,
    DomainP2 = domain(WorkerP2),

    Size = 1024 * 1024 * 1024,
    File = <<"/space3/file_cancel_replication">>,
    FileGuid = create_test_file(WorkerP1, SessionId, File, crypto:strong_rand_bytes(Size)),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),
    tracer:start([WorkerP1, WorkerP2]),
    tracer:trace_calls(transfer_links, umerge),

    ?assertEqual([Tid], list_active_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_active_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    % then
    ?assertTransferStatus(#{
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"fileId">> := FileObjectId,
        <<"transferStatus">> := <<"active">>
    }, WorkerP2, Tid, Config),

    %% cancel transfer
    cancel_transfer(WorkerP2, Tid, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"cancelled">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"fileId">> := FileObjectId,
        <<"invalidationStatus">> := <<"skipped">>
    }, WorkerP2, Tid, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_dir(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP2 = domain(WorkerP2),

    Dir1 = ?absPath(SpaceId, <<"dir1_rd">>),
    Dir2 = ?absPath(SpaceId, <<"dir1_rd/dir2">>),
    File1 = ?absPath(SpaceId, <<"dir1_rd/file1">>),
    File2 = ?absPath(SpaceId, <<"dir1_rd/file2">>),
    File3 = ?absPath(SpaceId, <<"dir1_rd/dir2/file3">>),

    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    create_test_file(WorkerP1, SessionId, File1, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File2, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File3, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, Dir1, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 5,
        <<"filesProcessed">> := 5,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 3,
        <<"bytesTransferred">> := 12
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File3),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File3),


    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

restart_dir_replication(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    Dir1 = ?absPath(SpaceId, <<"dir1_rd_restart">>),
    Dir2 = ?absPath(SpaceId, <<"dir1_rd_restart/dir2">>),
    File1 = ?absPath(SpaceId, <<"dir1_rd_restart/file1">>),
    File2 = ?absPath(SpaceId, <<"dir1_rd_restart/file2">>),
    File3 = ?absPath(SpaceId, <<"dir1_rd_restart/dir2/file3">>),
    DomainP2 = domain(WorkerP2),

    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    create_test_file(WorkerP1, SessionId, File1, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File2, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File3, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),

    % when
    mock_file_replication_failure(WorkerP2),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, Dir1, Config),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),
    DomainP2 = domain(WorkerP2),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId
    }, WorkerP1, Tid, Config),

    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),

    unmock_file_replication(WorkerP2),
    %% restart transfer

    restart_file_replication(WorkerP2, Tid, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 5,
        <<"filesProcessed">> := 5,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 3,
        <<"bytesTransferred">> := 12
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File3),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File3),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_file_by_id(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"replicate_file_by_id">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication_by_id(WorkerP1, DomainP2, FileObjectId, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 4
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],

    ?assertDistributionById(WorkerP1, ExpectedDistribution, Config, FileObjectId),
    ?assertDistributionById(WorkerP2, ExpectedDistribution, Config, FileObjectId),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_to_missing_provider(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"replicate_to_missing_provider">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    timer:sleep(timer:seconds(30)), % for hooks todo VFS-3462
    ?assertMatch({ok, 400, _, _},
        do_request(WorkerP1, <<"replicas", File/binary, "?provider_id=missing_id">>,
            post, [user_1_token_header(Config)], [])
    ).

replicate_to_nonsupporting_provider(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space1/file">>,
    create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),

    % when
    {ok, 400, _, _} = do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []
    ),
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File).

invalidate_file_replica(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_invalidate">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    DomainP2 = domain(WorkerP2),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 4
    }, WorkerP1, Tid, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    Tid1 = schedule_replica_invalidation(WorkerP1, DomainP2, File, Config),
    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"skipped">>,
        <<"targetProviderId">> := null,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"completed">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> :=0,
        <<"filesTransferred">> := 0,
        <<"filesInvalidated">> := 1,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid1, Config),

    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

invalidate_file_replica_with_migration(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_invalidate_migration">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    % when
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    % then
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    Tid = schedule_replica_invalidation(WorkerP1, DomainP1, DomainP2, File, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"completed">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 2,
        <<"filesProcessed">> := 2,
        <<"failedFiles">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 4,
        <<"filesInvalidated">> := 1,
        <<"callback">> := null
    }, WorkerP1, Tid, Config),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

restart_invalidation_of_file_replica_with_migration(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_invalidate_migration_restart">>),
    create_test_file(WorkerP1, SessionId, File, ?TEST_DATA),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    mock_file_replication_failure(WorkerP2),
    Tid1 = schedule_replica_invalidation(WorkerP1, DomainP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"invalidationStatus">> := <<"failed">>,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 1,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid1, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    unmock_file_replication(WorkerP2),
    restart_file_replication(WorkerP2, Tid1, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"invalidationStatus">> := <<"completed">>,
        <<"filesToProcess">> := 2,
        <<"filesProcessed">> := 2,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 4
    }, WorkerP1, Tid1, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid1], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File).

invalidate_dir_replica(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP2 = domain(WorkerP2),

    Dir1 = ?absPath(SpaceId, <<"dir1_invalidate">>),
    Dir2 = ?absPath(SpaceId, <<"dir1_invalidate/dir2">>),
    File1 = ?absPath(SpaceId, <<"dir1_invalidate/file1">>),
    File2 = ?absPath(SpaceId, <<"dir1_invalidate/file2">>),
    File3 = ?absPath(SpaceId, <<"dir1_invalidate/dir2/file3">>),
    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    create_test_file(WorkerP1, SessionId, File1, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File2, ?TEST_DATA),
    create_test_file(WorkerP1, SessionId, File3, ?TEST_DATA),

    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),

    % when
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),
    Tid = schedule_file_replication(WorkerP1, DomainP2, Dir1, Config),

    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),
    DomainP2 = domain(WorkerP2),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 5,
        <<"filesProcessed">> := 5,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 3,
        <<"bytesTransferred">> := 12
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File3),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File1),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File3),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    Tid2 = schedule_replica_invalidation(WorkerP1, DomainP2, Dir1, Config),

    ?assertTransferStatus(#{
        <<"path">> := Dir1,
        <<"callback">> := null,
        <<"transferStatus">> := <<"skipped">>,
        <<"invalidationStatus">> := <<"completed">>,
        <<"targetProviderId">> := null,
        <<"fileId">> := FileObjectId,
        <<"filesToProcess">> := 5,
        <<"filesProcessed">> := 5,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 3,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid2, Config),

    %then
    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File1),
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File2),
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File3),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File1),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File3),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

automatic_cleanup_should_invalidate_unpopular_files(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionIdP1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionIdP2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SmallSize = byte_size(?TEST_DATA),
    BigSize = ?UPPER_SIZE_LIMIT + 1,
    NormalSize = byte_size(?TEST_DATA2),
    BigData = crypto:strong_rand_bytes(BigSize),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File1 = <<"/space5/file1_too_small">>,
    File2 = <<"/space5/file2_popular">>,
    File3 = <<"/space5/file3_unpopular">>,
    File4 = <<"/space5/file4_too_big">>,
    File5 = <<"/space5/file5_to_write">>,
    % threshold is 35
    % size of files File1-4 is 4 + 9 + 9 + 11 = 33
    % target is 30

    File1Guid = create_test_file(WorkerP1, SessionIdP1, File1, ?TEST_DATA),
    File2Guid = create_test_file(WorkerP1, SessionIdP1, File2, ?TEST_DATA2),
    File3Guid = create_test_file(WorkerP1, SessionIdP1, File3, ?TEST_DATA2),
    File4Guid = create_test_file(WorkerP1, SessionIdP1, File4, BigData),
    File5Guid = create_test_file(WorkerP1, SessionIdP1, File5, ?TEST_DATA2),

    % synchronize files
    {ok, ReadHandle1} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File1Guid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(WorkerP2, ReadHandle1, 0, SmallSize), ?ATTEMPTS),
    lfm_proxy:close(WorkerP2, ReadHandle1),
    {ok, ReadHandle2} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File2Guid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(WorkerP2, ReadHandle2, 0, NormalSize), ?ATTEMPTS),
    lfm_proxy:close(WorkerP2, ReadHandle2),
    {ok, ReadHandle3} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File3Guid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(WorkerP2, ReadHandle3, 0, NormalSize), ?ATTEMPTS),
    lfm_proxy:close(WorkerP2, ReadHandle3),
    {ok, ReadHandle4} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File4Guid}, read), ?ATTEMPTS),
    ?assertMatch({ok, BigData}, lfm_proxy:read(WorkerP2, ReadHandle4, 0, BigSize), ?ATTEMPTS),
    lfm_proxy:close(WorkerP2, ReadHandle4),

    ExpectedDistribution1 = [
        #{<<"providerId">> => DomainP1, <<"blocks">> => [[0, NormalSize]]},
        #{<<"providerId">> => DomainP2, <<"blocks">> => [[0, NormalSize]]}
    ],
    ExpectedDistribution2 = [
        #{<<"providerId">> => DomainP1, <<"blocks">> => [[0, SmallSize]]},
        #{<<"providerId">> => DomainP2, <<"blocks">> => [[0, SmallSize]]}
    ],
    ExpectedDistribution3 = [
        #{<<"providerId">> => DomainP1, <<"blocks">> => [[0, BigSize]]},
        #{<<"providerId">> => DomainP2, <<"blocks">> => [[0, BigSize]]}
    ],
    ExpectedDistribution4 = [
        #{<<"providerId">> => DomainP1, <<"blocks">> => [[0, NormalSize]]}
    ],

    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution2, File1Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution1, File2Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution1, File3Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution3, File4Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution4, File5Guid),

    % pretend File3 hasn't been opened for 24 hours
    File3Uuid = fslogic_uuid:guid_to_uuid(File3Guid),
    {ok, _} = rpc:call(WorkerP2, file_popularity, update, [File3Uuid,
        fun(FP = #file_popularity{last_open = LastOpen}) ->
            {ok, FP#file_popularity{last_open = LastOpen - 24}}
        end
    ]),

    timer:sleep(timer:seconds(5)),

    %synchronize file5
    {ok, ReadHandle5} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, File5Guid}, read),
    {ok, ?TEST_DATA2} = lfm_proxy:read(WorkerP2, ReadHandle5, 0, NormalSize),
    lfm_proxy:close(WorkerP2, ReadHandle5),

    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution2, File1Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution1, File2Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution4, File3Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution3, File4Guid),
    ?assertDistributionProxyByGuid(WorkerP2, SessionIdP2, ExpectedDistribution1, File5Guid),

    [Report] = rpc:call(WorkerP2, autocleaning, list_reports_since, [<<"space5">>, 0]),
    ?assertMatch(#{
        bytesToRelease := 12,
        releasedBytes := NormalSize,
        filesNumber := 1,
        startedAt := _,
        stoppedAt := _
    }, maps:from_list(Report)).

posix_mode_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_pmg"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"mode">> => <<"0", (integer_to_binary(Mode, 8))/binary>>
        },
        DecodedBody
    ).

posix_mode_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2_pmp"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    NewMode = 8#777,
    Body = json_utils:encode_map(#{<<"mode">> => <<"0", (integer_to_binary(NewMode, 8))/binary>>}),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary>>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=mode">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(RespBody),
    ?assertEqual(
        #{
            <<"mode">> => <<"0", (integer_to_binary(NewMode, 8))/binary>>
        },
        DecodedBody
    ).

attributes_list(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_al"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary>>, get, [user_1_token_header(Config)], []),

    % then
    {ok, #file_attr{
        atime = ATime,
        ctime = CTime,
        mtime = MTime,
        gid = Gid,
        uid = Uid
    }} = lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid}),
    {ok, CdmiObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"mode">> => <<"0700">>,
            <<"size">> => 0,
            <<"atime">> => ATime,
            <<"ctime">> => CTime,
            <<"mtime">> => MTime,
            <<"storage_group_id">> => Gid,
            <<"storage_user_id">> => Uid,
            <<"name">> => <<"file1_al">>,
            <<"owner_id">> => UserId1,
            <<"shares">> => [],
            <<"type">> => <<"reg">>,
            <<"file_id">> => CdmiObjectId
        },
        DecodedBody
    ).

xattr_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_xg"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertEqual(
        #{
            <<"k1">> => <<"v1">>
        },
        DecodedBody
    ).

xattr_put(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file2_xp"])),
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    Body = json_utils:encode_map(#{<<"k1">> => <<"v1">>}),
    {ok, 204, _, _} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, put,
        [user_1_token_header(Config), {<<"Content-Type">>, <<"application/json">>}], Body),

    % then
    {ok, 200, _, RespBody} = do_request(WorkerP1, <<"attributes", File/binary, "?attribute=k1&extended=true">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(RespBody),
    ?assertEqual(
        #{
            <<"k1">> => <<"v1">>
        },
        DecodedBody
    ).

xattr_list(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_xl"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k1">>, value = <<"v1">>}),
    ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"k2">>, value = <<"v2">>}),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"attributes", File/binary, "?extended=true">>, get, [user_1_token_header(Config)], []),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(#{
        <<"k1">> := <<"v1">>,
        <<"k2">> := <<"v2">>
    }, DecodedBody).

metric_get(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_id, []),

    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = <<"space3">>,
        metric_type = storage_quota,
        provider_id = Prov1ID
    },

    ?assertMatch(ok, rpc:call(WorkerP1, monitoring_utils, create, [<<"space3">>, MonitoringId, time_utils:system_time_seconds()])),
    {ok, #document{value = State}} = rpc:call(WorkerP1, monitoring_state, get, [MonitoringId]),
    ?assertMatch({ok, _}, rpc:call(WorkerP1, monitoring_state, save, [
        #document{
            key = monitoring_state:encode_id(MonitoringId#monitoring_id{
                provider_id = Prov2ID
            }),
            value = State
        }
    ])),

    % when
    {ok, 200, _, Body} = do_request(WorkerP1, <<"metrics/space/space3?metric=storage_quota">>, get, [user_1_token_header(Config)], []),
    DecodedBody = json_utils:decode_map(Body),

    % then
    ?assertMatch([
        #{<<"providerId">> := _, <<"rrd">> := _},
        #{<<"providerId">> := _, <<"rrd">> := _}
    ], DecodedBody),
    [Elem1, Elem2] = DecodedBody,
    ?assertNotEqual(maps:get(<<"providerId">>, Elem1), maps:get(<<"providerId">>, Elem2)).

list_file(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    File = <<"/space3/file1_lf">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files/space3/file1_lf">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    ?assertEqual(
        [#{<<"id">> => FileObjectId, <<"path">> => File}],
        DecodedBody
    ).

list_dir(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space1">>},
            #{<<"id">> := _, <<"path">> := <<"/space10">>},
            #{<<"id">> := _, <<"path">> := <<"/space2">>},
            #{<<"id">> := _, <<"path">> := <<"/space3">>},
            #{<<"id">> := _, <<"path">> := <<"/space4">>},
            #{<<"id">> := _, <<"path">> := <<"/space5">>},
            #{<<"id">> := _, <<"path">> := <<"/space6">>},
            #{<<"id">> := _, <<"path">> := <<"/space7">>},
            #{<<"id">> := _, <<"path">> := <<"/space8">>},
            #{<<"id">> := _, <<"path">> := <<"/space9">>}
        ],
        lists:sort(fun(#{<<"path">> := Path1}, #{<<"path">> := Path2}) ->
            Path1 =< Path2
        end, DecodedBody)
    ).

list_dir_range(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files?offset=0&limit=1">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space1">>}
        ],
        DecodedBody
    ).

changes_stream_file_meta_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3_csfmt"])),
    File2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3_csfmt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, rdwr),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:write(WorkerP1, Handle, 0, <<"data">>),
        lfm_proxy:read(WorkerP1, Handle, 0, 2),
        lfm_proxy:set_perms(WorkerP1, SessionId, {guid, FileGuid}, 8#777),
        lfm_proxy:fsync(WorkerP1, Handle),
        lfm_proxy:create(WorkerP1, SessionId, File2, Mode)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    ?assert(length(binary:split(Body, <<"\r\n">>, [global])) >= 2).

changes_stream_xattr_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csxt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid}, #xattr{name = <<"name">>, value = <<"value">>})
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),

    [_ | Changes2] = lists:reverse(Changes),
    [LastChange | _] = lists:filtermap(fun(Change) ->
        DecodedChange = json_utils:decode_map(Change),
        case maps:get(<<"name">>, DecodedChange) of
            <<"file4_csxt">> -> {true, DecodedChange};
            _ -> false
        end
    end, Changes2),

    Metadata = maps:get(<<"changes">>, LastChange),
    ?assertEqual(#{<<"name">> => <<"value">>}, maps:get(<<"xattrs">>, Metadata)).

changes_stream_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csjmt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    Json = #{<<"k1">> => <<"v1">>, <<"k2">> => [<<"v2">>, <<"v3">>], <<"k3">> => #{<<"k31">> => <<"v31">>}},
    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, FileGuid}, json, Json, [])
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),

    ?assert(lists:any(fun(Change) ->
        Json == maps:get(<<"onedata_json">>, maps:get(<<"xattrs">>, maps:get(<<"changes">>, Change)), undefined)
    end, DecodedChanges)).

changes_stream_times_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_cstt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, 1000, 1000, 1000)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),
    ?assert(lists:any(fun(Change) ->
        1000 == maps:get(<<"atime">>, maps:get(<<"changes">>, Change)) andalso
            1000 == maps:get(<<"mtime">>, maps:get(<<"changes">>, Change)) andalso
            1000 == maps:get(<<"ctime">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

changes_stream_file_location_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csflt"])),
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        {ok, 5} = lfm_proxy:write(WorkerP1, Handle, 0, <<"01234">>)
    end),
    {ok, 200, _, Body} = do_request(WorkerP1, <<"changes/metadata/space1?timeout=10000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 40000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),
    ?assert(lists:any(fun(Change) ->
        5 == maps:get(<<"size">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

changes_stream_on_multi_provider_test(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionIdP2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    [_, _, {_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csompt"])),
    Mode = 8#700,
    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        lfm_proxy:write(WorkerP1, Handle, 0, <<"data">>)
    end),
    ?assertMatch({ok, _}, lfm_proxy:open(WorkerP2, SessionIdP2, {guid, FileGuid}, write), 20),
    {ok, 200, _, Body} = do_request(WorkerP2, <<"changes/metadata/space3?timeout=20000">>,
        get, [user_1_token_header(Config)], [], [{recv_timeout, 60000}]),

    ?assertNotEqual(<<>>, Body),
    Changes = binary:split(Body, <<"\r\n">>, [global]),
    ?assert(length(Changes) >= 1),
    [_ | AllChanges] = lists:reverse(Changes),
    DecodedChanges =
        lists:map(fun(Change) ->
            json_utils:decode_map(Change)
        end, AllChanges),

    ?assert(lists:any(fun(Change) ->
        <<"file4_csompt">> == maps:get(<<"name">>, Change) andalso
            4 == maps:get(<<"size">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"atime">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"ctime">>, maps:get(<<"changes">>, Change)) andalso
            0 < maps:get(<<"mtime">>, maps:get(<<"changes">>, Change))
    end, DecodedChanges)).

list_spaces(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"spaces">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"name">> := <<"space1">>, <<"spaceId">> := <<"space1">>},
            #{<<"name">> := <<"space10">>, <<"spaceId">> := <<"space10">>},
            #{<<"name">> := <<"space2">>, <<"spaceId">> := <<"space2">>},
            #{<<"name">> := <<"space3">>, <<"spaceId">> := <<"space3">>},
            #{<<"name">> := <<"space4">>, <<"spaceId">> := <<"space4">>},
            #{<<"name">> := <<"space5">>, <<"spaceId">> := <<"space5">>},
            #{<<"name">> := <<"space6">>, <<"spaceId">> := <<"space6">>},
            #{<<"name">> := <<"space7">>, <<"spaceId">> := <<"space7">>},
            #{<<"name">> := <<"space8">>, <<"spaceId">> := <<"space8">>},
            #{<<"name">> := <<"space9">>, <<"spaceId">> := <<"space9">>}
        ],
        lists:sort(DecodedBody)
    ).

get_space(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"spaces/space3">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"name">> := <<"space3">>,
            <<"providers">> := [
                #{
                    <<"providerId">> := PID1,
                    <<"providerName">> := PID1
                },
                #{
                    <<"providerId">> := PID2,
                    <<"providerName">> := PID2
                }
            ],
            <<"spaceId">> := <<"space3">>
        },
        DecodedBody
    ).

set_get_json_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata/space3?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])).

set_get_json_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file_sgjmi">>, 8#777),
    {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?filter_type=keypath&filter=key">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])).

set_get_rdf_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

set_get_rdf_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space3/file_sgrmi">>, 8#777),
    {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/rdf+xml">>}], "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata-id/", ObjectId/binary, "?metadata_type=rdf">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/rdf+xml">>}], [])),
    ?assertMatch(<<"some_xml">>, Body).

remove_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
                  return meta['onedata_json']['meta']['color'];
              }
              return null;
        }">>,
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assertMatch([_], IndexList),

    %when
    ?assertMatch({ok, 204, _, _}, do_request(WorkerP1, <<"index/", Id/binary>>, delete, [user_1_token_header(Config)], [])),

    %then
    ?assertMatch({ok, 200, _, <<"[]">>}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])).

create_list_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
                  return meta['onedata_json']['meta']['color'];
              }
              return null;
        }">>,
    ?assertMatch({ok, 200, _, <<"[]">>}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),

    % when
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),

    % then
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assertMatch([#{<<"spaceId">> := <<"space1">>, <<"name">> := <<"name">>, <<"indexId">> := Id, <<"spatial">> := false}], IndexList),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"index/", Id/binary>>, get, [user_1_token_header(Config), {<<"accept">>, <<"application/javascript">>}], [])),

    % when
    {ok, 303, _, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name2">>, post,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),

    % then
    {ok, _, _, ListBody2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get,
        [user_1_token_header(Config)], [])),
    IndexList2 = json_utils:decode_map(ListBody2),
    ?assertMatch([#{}, #{}], IndexList2).

create_geospatial_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['loc']) {
                  return meta['onedata_json']['loc'];
              }
              return null;
        }">>,

    % when
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name&spatial=true">>, post, [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),

    % then
    {ok, _, _, ListBody} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"index">>, get, [user_1_token_header(Config)], [])),
    IndexList = json_utils:decode_map(ListBody),
    ?assert(lists:member(#{<<"spaceId">> => <<"space1">>, <<"name">> => <<"name">>, <<"indexId">> => Id, <<"spatial">> => true}, IndexList)),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"index/", Id/binary>>, get, [user_1_token_header(Config), {<<"accept">>, <<"application/javascript">>}], [])).

query_geospatial_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    Function =
        <<"function (meta) {
              if(meta['onedata_json'] && meta['onedata_json']['loc']) {
                  return meta['onedata_json']['loc'];
              }
              return null;
        }">>,
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Path1 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f1"])),
    Path2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f2"])),
    Path3 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f3"])),
    {ok, Guid1} = lfm_proxy:create(WorkerP1, SessionId, Path1, 8#777),
    {ok, Guid2} = lfm_proxy:create(WorkerP1, SessionId, Path2, 8#777),
    {ok, Guid3} = lfm_proxy:create(WorkerP1, SessionId, Path3, 8#777),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid1}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [5.1, 10.22]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid2}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [0, 0]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid3}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [10, 5]}, [<<"loc">>]),
    {ok, 303, Headers, _} = ?assertMatch({ok, 303, _, _},
        do_request(WorkerP1, <<"index?space_id=space1&name=name&spatial=true">>, post,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/javascript">>}], Function)),
    <<"/api/v3/oneprovider/index/", Id/binary>> = proplists:get_value(<<"location">>, Headers),
    timer:sleep(timer:seconds(5)), % let the data be stored in db todo VFS-3462

    % when
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/", Id/binary, "?spatial=true&stale=false">>, get, [user_1_token_header(Config)], [])),

    % then
    Guids = lists:map(fun(X) ->
        {ok, ObjId} = cdmi_id:objectid_to_guid(X),
        ObjId
    end, json_utils:decode_map(Body)),
    ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), lists:sort(Guids)),

    % when
    {ok, 200, _, Body2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/", Id/binary, "?spatial=true&stale=false&start_range=[0,0]&end_range=[5.5,10.5]">>, get, [user_1_token_header(Config)], [])),

    % then
    Guids2 = lists:map(fun(X) -> {ok, ObjId} = cdmi_id:objectid_to_guid(X),
        ObjId end, json_utils:decode_map(Body2)),
    ?assertEqual(lists:sort([Guid1, Guid2]), lists:sort(Guids2)).

query_file_popularity_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true&stale=false">>, get, [user_1_token_header(Config)], [])).

set_get_json_metadata_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\": {\"a1\": \"b1\"}, \"b\": \"c\", \"e\": \"f\"}">>)),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir">>),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\": {\"a2\": \"b2\"}, \"b\": \"d\"}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3/dir?metadata_type=json&inherited=true">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        #{
            <<"a">> := #{<<"a1">> := <<"b1">>, <<"a2">> := <<"b2">>},
            <<"b">> := <<"d">>,
            <<"e">> := <<"f">>
        },
        DecodedBody
    ).

set_get_xattr_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir_test">>),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space3/dir_test/child">>),

    % when
    XattrSpace = json_utils:encode_map(#{<<"k1">> => <<"v1">>}),
    XattrDir = json_utils:encode_map(#{<<"k2">> => <<"v2">>}),
    XattrChild = json_utils:encode_map(#{<<"k2">> => <<"v22">>}),
    XattrChild2 = json_utils:encode_map(#{<<"k3">> => <<"v3">>}),

    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"a\":5}">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrSpace)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrDir)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrChild)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?extended=true">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], XattrChild2)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"attributes/space3/dir_test/child?inherited=true&extended=true">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(#{
        <<"k1">> := <<"v1">>,
        <<"k2">> := <<"v22">>,
        <<"k3">> := <<"v3">>,
        <<"onedata_json">> := #{<<"a">> := 5}
    }, DecodedBody).

set_get_json_metadata_using_filter(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"key1\": \"value1\", \"key2\": \"value2\", \"key3\": [\"v1\", \"v2\"]}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(<<"value1">>, DecodedBody),
    {_, _, _, Body2} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key3.[1]">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    DecodedBody2 = json_utils:decode_map(Body2),
    ?assertMatch(<<"v2">>, DecodedBody2),

    %when
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key1">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"\"value11\"">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key2">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"{\"key22\": \"value22\"}">>)),
    ?assertMatch({ok, 204, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json&filter_type=keypath&filter=key3.[0]">>, put,
            [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], <<"\"v11\"">>)),

    %then
    {_, _, _, ReponseBody} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
            [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], [])),
    ?assertMatch(
        #{
            <<"key1">> := <<"value11">>,
            <<"key2">> := #{<<"key22">> := <<"value22">>},
            <<"key3">> := [<<"v11">>, <<"v2">>]
        },
        json_utils:decode_map(ReponseBody)).

primitive_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Primitives = [<<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
        <<"null">>, <<"\"string\"">>],

    lists:foreach(fun(Primitive) ->
        ?assertMatch({ok, 204, _, _},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
                [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], Primitive)),
        ?assertMatch({ok, 200, _, Primitive},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, get,
                [user_1_token_header(Config), {<<"accept">>, <<"application/json">>}], []))
    end, Primitives).

empty_metadata_invalid_json_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    InvalidJsons = [<<"">>, <<"aaa">>, <<"{">>, <<"{\"aaa\": aaa}">>],

    lists:foreach(fun(InvalidJson) ->
        ?assertMatch({ok, 400, _, _},
            do_request(WorkerP1, <<"metadata/space3?metadata_type=json">>, put,
                [user_1_token_header(Config), {<<"content-type">>, <<"application/json">>}], InvalidJson))
    end, InvalidJsons).

spatial_flag_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    ?assertMatch({ok, 404, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary>>, get, [user_1_token_header(Config)], [])),
    ?assertMatch({ok, 400, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial">>, get, [user_1_token_header(Config)], [])),
    ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true">>, get, [user_1_token_header(Config)], [])).

quota_exceeded_during_file_replication(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_quota_exceeded">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, <<"0123456789">>),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    DomainP2 = domain(WorkerP2),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    ExpectedDistribution0 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistributionProxyByGuid(WorkerP2, SessionId2, ExpectedDistribution0, FileGuid),

    % when
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 1,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid, Config),

    FailedTransfers = list_past_transfers(WorkerP2, <<"space4">>),
    ?assert(lists:member(Tid, FailedTransfers)),
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
    ],

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File).

many_simultaneous_failed_transfers(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space4">>,
    DomainP2 = domain(WorkerP2),

    RootDir = filename:join(Space, <<"simultaneous_transfers_failed">>),
    FilesNum = 100,
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, RootDir),

    FileGuids = lists:map(fun(N) ->
        File = filename:join(RootDir, integer_to_binary(N)),
        create_test_file(WorkerP1, SessionId, File, ?TEST_DATA)
    end, lists:seq(1, FilesNum)),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, RootDir}), ?ATTEMPTS),
    lists:foreach(fun(FileGuid) ->
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS)
    end, FileGuids),

    TidsAndGuids = lists:map(fun(FileGuid) ->
        {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
        Tid = schedule_file_replication_by_id(WorkerP1, DomainP2, FileObjectId, Config),
        {Tid, FileGuid}
    end, FileGuids),

    % then
    lists:foreach(fun({Tid, FileGuid}) ->
        {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
        ?assertTransferStatus(#{
            <<"transferStatus">> := <<"failed">>,
            <<"targetProviderId">> := DomainP2,
            <<"invalidationStatus">> := <<"skipped">>,
            <<"fileId">> := FileObjectId,
            <<"callback">> := null,
            <<"filesToProcess">> := 1,
            <<"filesProcessed">> := 1,
            <<"failedFiles">> := 1,
            <<"filesTransferred">> := 0,
            <<"bytesTransferred">> := 0
        }, WorkerP1, Tid, Config)
    end, TidsAndGuids).

many_simultaneous_transfers(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space10">>,
    RootDir = filename:join(Space, <<"simultaneous_transfers">>),
    FilesNum = 1000,
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, RootDir),
    DomainP2 = domain(WorkerP2),

    FileGuids = lists:map(fun(N) ->
        File = filename:join(RootDir, integer_to_binary(N)),
        create_test_file(WorkerP1, SessionId, File, ?TEST_DATA)
    end, lists:seq(1, FilesNum)),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, RootDir}), ?ATTEMPTS),
    lists:foreach(fun(FileGuid) ->
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS)
    end, FileGuids),

    TidsAndIds = lists:map(fun(FileGuid) ->
        {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
        Tid = schedule_file_replication_by_id(WorkerP1, DomainP2, FileObjectId, Config),
        {Tid, FileObjectId}
    end, FileGuids),

    % then
    lists:foreach(fun({Tid, FileObjectId}) ->
        ?assertTransferStatus(#{
            <<"transferStatus">> := <<"completed">>,
            <<"targetProviderId">> := DomainP2,
            <<"invalidationStatus">> := <<"skipped">>,
            <<"fileId">> := FileObjectId,
            <<"callback">> := null,
            <<"filesToProcess">> := 1,
            <<"filesProcessed">> := 1,
            <<"failedFiles">> := 0,
            <<"filesInvalidated">> := 0,
            <<"filesTransferred">> := 1,
            <<"bytesTransferred">> := 4
        }, WorkerP1, Tid, Config)
    end, TidsAndIds).

quota_decreased_after_invalidation(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file_quota_decreased">>),
    File2 = ?absPath(SpaceId, <<"file_quota_decreased2">>),
    FileGuid = create_test_file(WorkerP1, SessionId, File, <<"0123456789">>),
    FileGuid2 = create_test_file(WorkerP1, SessionId, File2, <<"9876543210">>),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    {ok, FileObjectId2} = cdmi_id:guid_to_objectid(FileGuid2),

    ExpectedDistribution0 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistributionProxyByGuid(WorkerP2, SessionId2, ExpectedDistribution0, FileGuid),

    % when
    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 10
    }, WorkerP1, Tid, Config),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 10]]}
    ],

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    % when
    Tid2 = schedule_file_replication(WorkerP1, DomainP2, File2, Config),

    % then file cannot be replicated because of quota
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 1,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid2, Config),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File2),

    Tid3 = schedule_replica_invalidation(WorkerP2, DomainP2, File, Config),

    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"skipped">>,
        <<"targetProviderId">> := null,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"completed">>,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 1,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid3, Config),

    % File replica is invalidated
    ExpectedDistribution3 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution3, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution3, Config, File),

    %File2 can now be replicated
    % when
    Tid4 = schedule_file_replication(WorkerP1, DomainP2, File2, Config),

    {ok, FileObjectId2} = cdmi_id:guid_to_objectid(FileGuid2),
    DomainP2 = domain(WorkerP2),
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesInvalidated">> := 0,
        <<"filesTransferred">> := 1,
        <<"bytesTransferred">> := 10
    }, WorkerP1, Tid4, Config),

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File2),

    ?assertEqual([], list_scheduled_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid4, Tid3, Tid2, Tid], list_past_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_scheduled_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_current_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid4, Tid3, Tid2, Tid], list_past_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

file_replication_failures_should_fail_whole_transfer(Config) ->
    %soft quota on WorkerP2 is set to 0, so every write on WorkerP2 will fail
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Dir = <<"/space8/fail_failures_exceeded">>,
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir),
    {ok, OverallFileFailuresLimit} = test_utils:get_env(WorkerP2, op_worker, max_file_transfer_failures_per_transfer),
    FilesNum = OverallFileFailuresLimit + 1,

    {ok, DirObjectGuid} = cdmi_id:guid_to_objectid(DirGuid),
    DomainP2 = domain(WorkerP2),

    FileGuids = lists:map(fun(I) ->
        File = <<Dir/binary, "/", (integer_to_binary(I))/binary>>,
        create_test_file(WorkerP1, SessionId, File, ?TEST_DATA)
    end, lists:seq(1, FilesNum)),

    % when
    lists:foreach(fun(FileGuid) ->
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS)
    end, FileGuids),
    Tid = schedule_file_replication(WorkerP1, DomainP2, Dir, Config),

    % then
    %replication of files will fail because space quota is set to 0 on WorkerP2
    FilesToProcess = FilesNum + 1,
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := DirObjectGuid,
        <<"callback">> := null,
        <<"filesToProcess">> := FilesToProcess,
        <<"filesProcessed">> := FilesToProcess,
        <<"failedFiles">> := FilesNum,
        <<"filesTransferred">> := 0,
        <<"bytesTransferred">> := 0
    }, WorkerP1, Tid, Config).

replicate_big_dir(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space3">>,
    RootDir = filename:join(Space, <<"big_dir_replication">>),
    Structure = [10, 10], % last level are files
    FilesToCreate = lists:foldl(fun(N, AccIn) -> 1 + AccIn * N end, 1, Structure),
    RegularFilesNum = lists:foldl(fun(N, AccIn) -> N * AccIn end, 1, Structure),
    BytesSum = byte_size(?TEST_DATA) * lists:foldl(fun(N, AccIn) ->
        AccIn * N end, 1, Structure),

    true = register(?CREATE_FILE_COUNTER, spawn_link(?MODULE, create_file_counter, [0, FilesToCreate, self(), []])),
    true = register(?SYNC_FILE_COUNTER, spawn_link(?MODULE, sync_file_counter, [0, FilesToCreate, self()])),

    create_nested_directory_tree(WorkerP1, SessionId, Structure, RootDir),

    FileGuids = receive {create, FileGuids0} -> FileGuids0 end,

    lists:foreach(fun(FileGuid) ->
        worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file, [WorkerP2, SessionId2, FileGuid]})
    end, FileGuids),

    receive files_synchronized -> ok end,
    DomainP2 = domain(WorkerP2),

    Tid = schedule_file_replication(WorkerP1, DomainP2, RootDir, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := RootDir,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"callback">> := null,
        <<"filesToProcess">> := FilesToCreate,
        <<"filesProcessed">> := FilesToCreate,
        <<"filesTransferred">> := RegularFilesNum,
        <<"bytesTransferred">> := BytesSum
    }, WorkerP1, Tid, Config, 600).

replicate_big_file(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space7">>,
    DomainP2 = domain(WorkerP2),
    File = filename:join(Space, <<"big_file">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    BaseSize = 1024 * 1024 * 1024,
    Size = 2 * BaseSize,
    lfm_proxy:write(WorkerP1, Handle, 0, crypto:strong_rand_bytes(BaseSize)),
    lfm_proxy:write(WorkerP1, Handle, BaseSize, crypto:strong_rand_bytes(BaseSize)),
    lfm_proxy:fsync(WorkerP1, Handle),
    lfm_proxy:close(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    Tid = schedule_file_replication(WorkerP1, DomainP2, File, Config),

    % then
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesTransferred">> := 1,
        <<"failedFiles">> := 0,
        <<"filesInvalidated">> := 0,
        <<"bytesTransferred">> := Size
    }, WorkerP1, Tid, Config, 3600).

invalidate_big_dir(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space3">>,
    RootDir = filename:join(Space, <<"big_dir_invalidation">>),
    Structure = [10, 10], % last level are files
    FilesToCreate = lists:foldl(fun(N, AccIn) -> 1 + AccIn * N end, 1, Structure),
    RegularFilesNum = lists:foldl(fun(N, AccIn) -> N * AccIn end, 1, Structure),
    DomainP2 = domain(WorkerP2),
    DomainP1 = domain(WorkerP1),

    true = register(?CREATE_FILE_COUNTER, spawn_link(?MODULE, create_file_counter, [0, FilesToCreate, self(), []])),
    true = register(?SYNC_FILE_COUNTER, spawn_link(?MODULE, sync_file_counter, [0, FilesToCreate, self()])),

    create_nested_directory_tree(WorkerP1, SessionId, Structure, RootDir),

    FileGuids = receive {create, FileGuids0} -> FileGuids0 end,

    lists:foreach(fun(FileGuid) ->
        worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file, [WorkerP2, SessionId2, FileGuid]})
    end, FileGuids),

    receive files_synchronized -> ok end,

    Tid = schedule_replica_invalidation(WorkerP2, DomainP1, DomainP2, RootDir, Config),

    % then
    FilesToProcess = 2 * FilesToCreate,
    ?assertTransferStatus(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := RootDir,
        <<"invalidationStatus">> := <<"completed">>,
        <<"callback">> := null,
        <<"filesToProcess">> := FilesToProcess,
        <<"filesProcessed">> := FilesToProcess,
        <<"filesTransferred">> := RegularFilesNum,
        <<"filesInvalidated">> := RegularFilesNum,
        <<"failedFiles">> := 0
    }, WorkerP1, Tid, Config, 600).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        NewConfig3
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, multi_provider_rest_test_SUITE]}
        | Config
    ].


end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    true = worker_pool:stop_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= replicate_file;
    Case =:= replicate_already_replicated_file;
    Case =:= replicate_file_to_source_provider;
    Case =:= transfers_should_be_ordered_by_timestamps;
    Case =:= restart_file_replication;
    Case =:= cancel_file_replication;
    Case =:= replicate_dir;
    Case =:= restart_dir_replication;
    Case =:= replicate_to_missing_provider;
    Case =:= replicate_file_by_id;
    Case =:= invalidate_file_replica;
    Case =:= invalidate_file_replica_with_migration;
    Case =:= restart_invalidation_of_file_replica_with_migration;
    Case =:= invalidate_dir_replica;
    Case =:= many_simultaneous_transfers
    ->
    init_per_testcase(all, [{space_id, <<"space3">>} | Config]);

init_per_testcase(Case, Config) when Case =:= replicate_not_synced_file ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(WorkerP2, sync_req),
    ok = test_utils:mock_expect(WorkerP2, sync_req, get_file_children, fun(_, _, _, _) ->
        {[file_ctx:new_by_guid(<<"test">>)], undefined}
    end),
    init_per_testcase(all, [{space_id, <<"space3">>} | Config]);

init_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    start_monitoring_worker(WorkerP1),
    test_utils:mock_new(WorkerP1, rrd_utils),
    test_utils:mock_expect(WorkerP1, rrd_utils, export_rrd, fun(_, _, _) ->
        {ok, <<"{\"test\":\"rrd\"}">>}
    end),
    init_per_testcase(all, Config);

init_per_testcase(Case, Config) when
    Case =:= query_file_popularity_index;
    Case =:= spatial_flag_test
    ->
    [_, WorkerP1] = ?config(op_worker_nodes, Config),
    {ok, _} = rpc:call(WorkerP1, space_storage, enable_file_popularity, [<<"space1">>]),
    init_per_testcase(all, Config);

init_per_testcase(Case, Config) when
    Case =:= quota_exceeded_during_file_replication;
    Case =:= file_replication_failures_should_fail_whole_transfer;
    Case =:= many_simultaneous_failed_transfers
    ->
    [WorkerP2, _WorkerP1] = ?config(op_worker_nodes, Config),
    OldSoftQuota = rpc:call(WorkerP2, application, get_env, [op_worker, soft_quota_limit_size]),
    rpc:call(WorkerP2, application, set_env, [op_worker, soft_quota_limit_size, ?ZERO_SOFT_QUOTA]),
    Config2 = [{old_soft_quota, OldSoftQuota}, {space_id, <<"space4">>} | Config],
    init_per_testcase(all, Config2);

init_per_testcase(Case, Config) when
    Case =:= quota_decreased_after_invalidation
    ->
    [WorkerP2, _WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space6">>,
    clean_monitoring_dir(WorkerP2, SpaceId),
    OldSoftQuota = rpc:call(WorkerP2, application, get_env, [op_worker, soft_quota_limit_size]),
    ok = rpc:call(WorkerP2, application, set_env, [op_worker, soft_quota_limit_size, 15]),
    Config2 = [{old_soft_quota, OldSoftQuota}, {space_id, SpaceId} | Config],
    init_per_testcase(all, Config2);

init_per_testcase(automatic_cleanup_should_invalidate_unpopular_files, Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    {ok, _} = rpc:call(WorkerP1, space_storage, enable_file_popularity, [<<"space5">>]),
    {ok, _} = rpc:call(WorkerP2, space_storage, enable_file_popularity, [<<"space5">>]),
    {ok, _} = rpc:call(WorkerP1, space_cleanup_api, configure_autocleaning, [<<"space5">>, ?AUTOCLEANING_SETTINGS]),
    {ok, _} = rpc:call(WorkerP2, space_cleanup_api, configure_autocleaning, [<<"space5">>, ?AUTOCLEANING_SETTINGS]),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config).

end_per_testcase(metric_get = Case, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(WorkerP1, rrd_utils),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = automatic_cleanup_should_invalidate_unpopular_files, Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    rpc:call(WorkerP1, space_storage, disable_file_popularity, [<<"space5">>]),
    rpc:call(WorkerP2, space_storage, disable_file_popularity, [<<"space5">>]),
    rpc:call(WorkerP2, space_cleanup_api, disable_autocleaning, [<<"space5">>]),
    rpc:call(WorkerP2, space_cleanup_api, disable_autocleaning, [<<"space5">>]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= quota_exceeded_during_file_replication;
    Case =:= quota_decreased_after_invalidation;
    Case =:= file_replication_failures_should_fail_whole_transfer;
    Case =:= many_simultaneous_failed_transfers
    ->
    [WorkerP2, _WorkerP1] = ?config(op_worker_nodes, Config),
    {ok, OldSoftQuota} = ?config(old_soft_quota, Config),
    rpc:call(WorkerP2, application, set_env, [op_worker, soft_quota_limit_size, OldSoftQuota]),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= query_file_popularity_index;
    Case =:= spatial_flag_test
    ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    rpc:call(WorkerP1, space_storage, disable_file_popularity, [<<"space1">>]),
    rpc:call(WorkerP2, space_storage, disable_file_popularity, [<<"space1">>]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);


end_per_testcase(Case, Config) when
    Case =:= replicate_file;
    Case =:= replicate_already_replicated_file;
    Case =:= transfers_should_be_ordered_by_timestamps;
    Case =:= restart_file_replication;
    Case =:= cancel_file_replication;
    Case =:= replicate_file_to_source_provider;
    Case =:= replicate_dir;
    Case =:= restart_dir_replication;
    Case =:= replicate_file_by_id;
    Case =:= replicate_to_missing_provider;
    Case =:= invalidate_file_replica;
    Case =:= invalidate_file_replica_with_migration;
    Case =:= restart_invalidation_of_file_replica_with_migration;
    Case =:= invalidate_dir_replica;
    Case =:= many_simultaneous_transfers
    ->
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when Case =:= replicate_not_synced_file ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(WorkerP2, sync_req),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    remove_transfers(Config),
    ensure_transfers_removed(Config),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request(Node, URL, Method, Headers, Body) ->
    do_request(Node, URL, Method, Headers, Body, [{recv_timeout, 15000}]).
do_request(Node, URL, Method, Headers, Body, Opts) ->
    CaCerts = rpc:call(Node, gui_listener, get_cert_chain, []),
    Opts2 = [{ssl_options, [{cacerts, CaCerts}]} | Opts],
    Result = http_client:request(
        Method, <<(rest_endpoint(Node))/binary, URL/binary>>,
        maps:from_list(Headers), Body, Opts2
    ),
    case Result of
        {ok, RespCode, RespHeaders, RespBody} ->
            {ok, RespCode, maps:to_list(RespHeaders), RespBody};
        Other ->
            Other
    end.

rest_endpoint(Node) ->
    Port = case get(port) of
        undefined ->
            {ok, P} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),
            PStr = case P of
                443 -> <<"">>;
                _ -> <<":", (integer_to_binary(P))/binary>>
            end,
            put(port, PStr),
            PStr;
        P -> P
    end,
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),
    <<"https://", (str_utils:to_binary(Domain))/binary, Port/binary, "/api/v3/oneprovider/">>.


user_1_token_header(Config) ->
    #macaroon_auth{macaroon = Macaroon} = ?config({auth, <<"user1">>}, Config),
    {<<"Macaroon">>, Macaroon}.

domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).

mock_file_replication_failure(Node) ->
    test_utils:mock_new(Node, replica_synchronizer),
    test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(_, _, _, _, _) ->
            throw(test_error)
        end
    ).

unmock_file_replication(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

create_nested_directory_tree(Node, SessionId, [SubFilesNum], Root) ->
    create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        FilePath = filename:join([Root, integer_to_binary(N)]),
        ok = worker_pool:cast(?VERIFY_POOL, {?MODULE, create_file, [Node, SessionId, FilePath]})
    end, lists:seq(1, SubFilesNum));
create_nested_directory_tree(Node, SessionId, [SubDirsNum | Rest], Root) ->
    create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = filename:join([Root, NBin]),
        ok = worker_pool:cast(?VERIFY_POOL,
            {?MODULE, create_nested_directory_tree, [Node, SessionId, Rest, DirPath]}
        )
    end, lists:seq(1, SubDirsNum)).

create_file(Node, SessionId, FilePath) ->
    FileGuid = create_test_file(Node, SessionId, FilePath, ?TEST_DATA),
    ?CREATE_FILE_COUNTER ! {created, FileGuid}.

create_dir(Node, SessionId, DirPath) ->
    {ok, DirGuid} = lfm_proxy:mkdir(Node, SessionId, DirPath),
    ?CREATE_FILE_COUNTER ! {created, DirGuid}.

sync_file_counter(FilesToVerify, FilesToVerify, ParentPid) ->
    ParentPid ! files_synchronized;
sync_file_counter(N, FilesToVerify, ParentPid) ->
    receive
        verified ->
            sync_file_counter(N + 1, FilesToVerify, ParentPid)
    end.

create_file_counter(FilesToCreate, FilesToCreate, ParentPid, Files) ->
    ParentPid ! {create, Files};
create_file_counter(N, FilesToCreate, ParentPid, Files) ->
    receive
        {created, FileGuid} ->
            create_file_counter(N + 1, FilesToCreate, ParentPid, [FileGuid | Files])
    end.

verify_file(Worker, SessionId, FileGuid) ->
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Worker, SessionId, {guid, FileGuid}), 3600),
    ?SYNC_FILE_COUNTER ! verified.

clean_monitoring_dir(Worker, SpaceId) ->
    RootSessionId = <<"0">>,
    SpaceGuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    RRDDir = rpc:call(Worker, file_meta, hidden_file_name, [<<"rrd">>]),
    case rpc:call(Worker, logical_file_manager, get_child_attr, [RootSessionId, SpaceGuid, RRDDir]) of
        {ok, #file_attr{
            guid = RRDGuid}
        } ->
            lfm_proxy:rm_recursive(Worker, RootSessionId, {guid, RRDGuid});
        _ -> ok
    end.

list_active_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_scheduled_and_current_transfers, [SpaceId]),
    Transfers.

list_past_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_past_transfers, [SpaceId]),
    Transfers.

list_current_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_current_transfers, [SpaceId]),
    Transfers.

list_scheduled_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_scheduled_transfers, [SpaceId]),
    Transfers.

start_monitoring_worker(Node) ->
    Args = [
        {supervisor_flags, rpc:call(Node, monitoring_worker, supervisor_flags, [])},
        {supervisor_children_spec, rpc:call(Node, monitoring_worker, supervisor_children_spec, [])}
    ],
    ok = rpc:call(Node, node_manager, start_worker, [monitoring_worker, Args]).

remove_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            Current = list_current_transfers(Worker, SpaceId),
            Past = list_past_transfers(Worker, SpaceId),
            Scheduled = list_scheduled_transfers(Worker, SpaceId),
            lists:foreach(fun(Tid) ->
                rpc:call(Worker, transfer, delete, [Tid])
            end, lists:umerge([Current, Past, Scheduled]))
        end, SpaceIds)
    end, Workers).

ensure_transfers_removed(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            ?assertMatch([], list_past_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_current_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_scheduled_transfers(Worker, SpaceId), ?ATTEMPTS)
        end, SpaceIds)
    end, Workers).

get_finish_time(Worker, Tid, Config) ->
    Status = get_status(Worker, Tid, Config),
    maps:get(<<"finishTime">>, Status).

get_status(Worker, Tid, Config) ->
    {ok, 200, _, TransferStatus} = do_request(Worker, <<"transfers/", Tid/binary>>,
        get, [user_1_token_header(Config)], []),
    json_utils:decode_map(TransferStatus).

schedule_file_replication(Worker, ProviderId, File, Config) ->
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(Worker,
        <<"replicas/", File/binary, "?provider_id=", ProviderId/binary>>,
        post, [user_1_token_header(Config)], []
    ), ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

schedule_file_replication_by_id(Worker, ProviderId, FileId, Config) ->
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(Worker,
        <<"replicas-id/", FileId/binary, "?provider_id=", ProviderId/binary>>,
        post, [user_1_token_header(Config)], []
    ), ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

restart_file_replication(Worker, Tid, Config) ->
    {ok, 204, _, _} =
        do_request(Worker, <<"transfers/", Tid/binary>>, patch, [user_1_token_header(Config)], []).

schedule_replica_invalidation(Worker, ProviderId, File, Config) ->
    schedule_replica_invalidation(Worker, ProviderId, undefined, File, Config).

schedule_replica_invalidation(Worker, ProviderId, undefined, File, Config) ->
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(Worker,
        <<"replicas/", File/binary, "?provider_id=", ProviderId/binary>>,
        delete, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid;
schedule_replica_invalidation(Worker, ProviderId, MigrationProviderId, File, Config) ->
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(Worker, <<"replicas", File/binary, "?provider_id=",
        ProviderId/binary, "&migration_provider_id=", MigrationProviderId/binary>>,
        delete, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

cancel_transfer(Worker, Tid, Config) ->
    {ok, 204, _, _} = do_request(Worker, <<"transfers/", Tid/binary>>, delete, [user_1_token_header(Config)], []).


create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.
