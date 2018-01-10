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
    replicate_dir/1,
    restart_file_replication/1,
    restart_dir_replication/1,
    cancel_file_replication/1,
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
    create_list_index/1,
    remove_index/1,
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
    replicate_big_dir/1,
    quota_decreased_after_invalidation/1,
    replicate_big_file/1,
    file_replication_failures_should_fail_whole_transfer/1
]).

%utils
-export([kill/1, verify_file/3, create_file/3, create_dir/3,
    create_nested_directory_tree/4, sync_file_counter/3, create_file_counter/4]).

all() ->
    ?ALL([
        get_simple_file_distribution,
        replicate_file,
        restart_file_replication,
        cancel_file_replication,
        replicate_dir,
        restart_dir_replication,
        replicate_file_by_id,
        replicate_to_missing_provider,
        replicate_to_nonsupporting_provider,
        invalidate_file_replica,
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
        quota_exceeded_during_file_replication,
        quota_decreased_after_invalidation,
        file_replication_failures_should_fail_whole_transfer,
        replicate_big_dir,
        replicate_big_file
    ]).

-define(LIST_TRANSFER, fun(Id, Acc) -> [Id | Acc] end).
-define(ATTEMPTS, 30).

-define(assertDistribution(Worker, ExpectedDistribution, Config, File),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        {ok, 200, _, __Body} = do_request(Worker, <<"replicas", File/binary>>, get,
            [user_1_token_header(Config)], []
        ),
        lists:sort(json_utils:decode_map(__Body))
    end, ?ATTEMPTS)).

-define(assertDistributionProxyByGuid(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        {ok, __FileBlocks} = lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}),
        lists:sort(__FileBlocks)
    end, ?ATTEMPTS)
).

-define(assertDistributionById(Worker, ExpectedDistribution, Config, FileId),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        {ok, 200, _, __Body} = do_request(Worker, <<"replicas-id/", FileId/binary>>, get,
            [user_1_token_header(Config)], []
        ),
        lists:sort(json_utils:decode_map(__Body))
    end, ?ATTEMPTS)).

-define(assertEqualList(L1, L2, Attempts), 
    ?assertEqual(lists:sort(L1), lists:sort(L2), Attempts)).

-define(absPath(SpaceId, Path), <<"/", SpaceId/binary, "/", Path/binary>>).

-define(TEST_DATA, <<"test">>).
-define(TEST_DATA2, <<"test01234">>).

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
    File = ?absPath(SpaceId, <<"file">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []
    ), ?ATTEMPTS),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 4,
        <<"bytesTransferred">> := 4,
        <<"minHist">> := #{DomainP1 := [4 | _]},
        <<"hrHist">> := #{DomainP1 := [4 | _]},
        <<"dyHist">> := #{DomainP1 := [4 | _]},
        <<"mthHist">> := #{DomainP1 := [4 | _]}
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

restart_file_replication(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_restart_replication">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    lengthen_file_replication_time(WorkerP2, 60),
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),

    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    Pid = check_pid(WorkerP2, ?ATTEMPTS, Tid),
    %% stop transfer
    true = rpc:call(WorkerP2, multi_provider_rest_test_SUITE, kill, [Pid]),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    ?assertMatch(#{
        <<"transferStatus">> := <<"active">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ?assertEqualList([], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    resume_file_replication_time(WorkerP2),
    rpc:call(WorkerP1, transfer, restart_unfinished_transfers, [SpaceId]),

    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 4,
        <<"bytesTransferred">> := 4,
        <<"minHist">> := #{DomainP1 := [4 | _]},
        <<"hrHist">> := #{DomainP1 := [4 | _]},
        <<"dyHist">> := #{DomainP1 := [4 | _]},
        <<"mthHist">> := #{DomainP1 := [4 | _]}
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

cancel_file_replication(Config) ->
    ct:timetrap({minutes, 10}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),

    File = <<"/space3/file_cancel_replication">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, crypto:strong_rand_bytes(200 * 1024 * 1024)),
    lfm_proxy:fsync(WorkerP1, Handle),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS),

    % when
    lengthen_file_replication_time(WorkerP2, 60),
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),

    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"transferStatus">> := <<"active">>
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    %% cancel transfer
    ?assertMatch({ok, 204, _, _}, do_request(WorkerP2, <<"transfers/", Tid/binary>>, delete, [user_1_token_header(Config)], [])),

    % then
    DomainP2 = domain(WorkerP2),

    ?assertMatch(#{
        <<"transferStatus">> := <<"cancelled">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),
    resume_file_replication_time(WorkerP2).

replicate_dir(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    Dir1 = ?absPath(SpaceId, <<"dir1_rd">>),
    Dir2 = ?absPath(SpaceId, <<"dir1_rd/dir2">>),
    File1 = ?absPath(SpaceId, <<"dir1_rd/file1">>),
    File2 = ?absPath(SpaceId, <<"dir1_rd/file2">>),
    File3 = ?absPath(SpaceId, <<"dir1_rd/dir2/file3">>),

    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionId, File1, 8#700),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionId, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle1),
    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionId, File2, 8#700),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionId, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle2),
    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionId, File3, 8#700),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionId, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle3),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),

    % when
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", Dir1/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 5,
        <<"filesTransferred">> := 5,
        <<"bytesToTransfer">> := 12,
        <<"bytesTransferred">> := 12
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

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

    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

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
    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionId, File1, 8#700),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionId, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle1),
    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionId, File2, 8#700),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionId, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle2),
    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionId, File3, 8#700),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionId, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle3),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),

    lengthen_file_replication_time(WorkerP2, 60),

    % when
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1, <<"replicas", Dir1/binary,
        "?provider_id=", (domain(WorkerP2))/binary>>, post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    Pid = check_pid(WorkerP2, ?ATTEMPTS, Tid),

    %% stop transfer
    true = rpc:call(WorkerP2, multi_provider_rest_test_SUITE, kill, [Pid]),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),
    DomainP2 = domain(WorkerP2),

    ?assertMatch(#{
        <<"transferStatus">> := <<"active">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ?assertEqualList([], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    resume_file_replication_time(WorkerP2),
    %% restart transfer
    rpc:call(WorkerP1, transfer, restart_unfinished_transfers, [SpaceId]),

    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 5,
        <<"filesTransferred">> := 5,
        <<"bytesToTransfer">> := 12,
        <<"bytesTransferred">> := 12
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

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

    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

replicate_file_by_id(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"replicate_file_by_id">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas-id/", FileObjectId/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 4,
        <<"bytesTransferred">> := 4
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistributionById(WorkerP1, ExpectedDistribution, Config, FileObjectId),
    ?assertDistributionById(WorkerP2, ExpectedDistribution, Config, FileObjectId),

    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

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
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),

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
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 4,
        <<"bytesTransferred">> := 4
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    % then
    {ok, 200, _, Body1} = do_request(WorkerP2,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        delete, [user_1_token_header(Config)], []
    ),

    DecodedBody1 = json_utils:decode_map(Body1),
    #{<<"transferId">> := Tid1} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody1),

    ?assertMatch(#{
        <<"transferStatus">> := <<"skipped">>,
        <<"invalidationStatus">> := <<"completed">>
    },
        case do_request(WorkerP1, <<"transfers/", Tid1/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File),

    ?assertEqualList([Tid, Tid1], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid, Tid1], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

invalidate_file_replica_with_migration(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_invalidate_migration">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    % then
    {ok, 200, _, Body} = do_request(WorkerP1, <<"replicas", File/binary, "?provider_id=",
        (domain(WorkerP1))/binary, "&migration_provider_id=", (domain(WorkerP2))/binary>>,
        delete, [user_1_token_header(Config)], []
    ),

    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"invalidationStatus">> := <<"completed">>
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => []},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File),

    ?assertEqualList([Tid, Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid, Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

restart_invalidation_of_file_replica_with_migration(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_invalidate_migration_restart">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    % when
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 4]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),

    lengthen_file_replication_time(WorkerP2, 60),
    % then
    {ok, 200, _, Body1} = do_request(WorkerP1, <<"replicas", File/binary, "?provider_id=",
        (domain(WorkerP1))/binary, "&migration_provider_id=", (domain(WorkerP2))/binary>>,
        delete, [user_1_token_header(Config)], []
    ),
    DecodedBody1 = json_utils:decode_map(Body1),
    #{<<"transferId">> := Tid1} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody1),

    Pid = check_pid(WorkerP2, ?ATTEMPTS, Tid1),
    true = rpc:call(WorkerP2, multi_provider_rest_test_SUITE, kill, [Pid]),

    ?assertMatch(#{
        <<"transferStatus">> := <<"active">>,
        <<"invalidationStatus">> := <<"scheduled">>
    },
        case do_request(WorkerP1, <<"transfers/", Tid1/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    resume_file_replication_time(WorkerP2),
    rpc:call(WorkerP2, transfer, restart_unfinished_transfers, [SpaceId]),

    ?assertEqualList([Tid1], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid1], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

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
    Dir1 = ?absPath(SpaceId, <<"dir1_invalidate">>),
    Dir2 = ?absPath(SpaceId, <<"dir1_invalidate/dir2">>),
    File1 = ?absPath(SpaceId, <<"dir1_invalidate/file1">>),
    File2 = ?absPath(SpaceId, <<"dir1_invalidate/file2">>),
    File3 = ?absPath(SpaceId, <<"dir1_invalidate/dir2/file3">>),
    {ok, Dir1Guid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir1),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir2),
    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionId, File1, 8#700),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionId, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle1),
    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionId, File2, 8#700),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionId, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle2),
    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionId, File3, 8#700),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionId, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle3),

    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File3}), ?ATTEMPTS),

    % when
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", Dir1/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),

    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(Dir1Guid),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 5,
        <<"filesTransferred">> := 5,
        <<"bytesToTransfer">> := 12,
        <<"bytesTransferred">> := 12
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

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
    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    {ok, 200, _, Body2} = do_request(WorkerP2,
        <<"replicas", Dir1/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        delete, [user_1_token_header(Config)], []
    ),
    DecodedBody2 = json_utils:decode_map(Body2),
    #{<<"transferId">> := Tid2} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody2),

    ?assertMatch(#{
        <<"transferStatus">> := <<"skipped">>,
        <<"targetProviderId">> := <<"undefined">>,
        <<"path">> := Dir1,
        <<"invalidationStatus">> := <<"completed">>,
        <<"callback">> := null
    },
        case do_request(WorkerP1, <<"transfers/", Tid2/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),


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
    ?assertEqualList([Tid, Tid2], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid, Tid2], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

automatic_cleanup_should_invalidate_unpopular_files(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionIdP1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionIdP2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SmallSize = byte_size(?TEST_DATA),
    BigSize = ?UPPER_SIZE_LIMIT + 1,
    NormalSize = byte_size(?TEST_DATA2),
    BigData = crypto:strong_rand_bytes(BigSize),
    
    
    File1 = <<"/space5/file1_too_small">>,
    File2 = <<"/space5/file2_popular">>,
    File3 = <<"/space5/file3_unpopular">>,
    File4 = <<"/space5/file4_too_big">>,
    File5 = <<"/space5/file5_to_write">>,
    % threshold is 35
    % size of files File1-4 is 4 + 9 + 9 + 11 = 33
    % target is 30

    {ok, File1Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File1, 8#777),
    {ok, Handle1} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File1Guid}, write),
    lfm_proxy:write(WorkerP1, Handle1, 0, ?TEST_DATA),
    lfm_proxy:close(WorkerP1, Handle1),

    {ok, File2Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File2, 8#777),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File2Guid}, write),
    lfm_proxy:write(WorkerP1, Handle2, 0, ?TEST_DATA2),
    lfm_proxy:close(WorkerP1, Handle2),

    {ok, File3Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File3, 8#777),
    {ok, Handle3} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File3Guid}, write),
    lfm_proxy:write(WorkerP1, Handle3, 0, ?TEST_DATA2),
    lfm_proxy:close(WorkerP1, Handle3),
    
    {ok, File4Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File4, 8#777),
    {ok, Handle4} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File4Guid}, write),
    lfm_proxy:write(WorkerP1, Handle4, 0, BigData),
    lfm_proxy:close(WorkerP1, Handle4),


    {ok, File5Guid} = lfm_proxy:create(WorkerP1, SessionIdP1, File5, 8#777),
    {ok, Handle5} = lfm_proxy:open(WorkerP1, SessionIdP1, {guid, File5Guid}, write),
    lfm_proxy:write(WorkerP1, Handle5, 0, ?TEST_DATA2),
    lfm_proxy:close(WorkerP1, Handle5),

    Provider1Id = rpc:call(WorkerP1, oneprovider, get_id, []),
    Provider2Id = rpc:call(WorkerP2, oneprovider, get_id, []),

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
        #{<<"providerId">> => Provider1Id, <<"blocks">> => [[0, NormalSize]]},
        #{<<"providerId">> => Provider2Id, <<"blocks">> => [[0, NormalSize]]}
    ],
    ExpectedDistribution2 = [
        #{<<"providerId">> => Provider1Id, <<"blocks">> => [[0, SmallSize]]},
        #{<<"providerId">> => Provider2Id, <<"blocks">> => [[0, SmallSize]]}
    ],
    ExpectedDistribution3 = [
        #{<<"providerId">> => Provider1Id, <<"blocks">> => [[0, BigSize]]},
        #{<<"providerId">> => Provider2Id, <<"blocks">> => [[0, BigSize]]}
    ],
    ExpectedDistribution4 = [
        #{<<"providerId">> => Provider1Id, <<"blocks">> => [[0, NormalSize]]}
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
            #{<<"id">> := _, <<"path">> := <<"/space2">>},
            #{<<"id">> := _, <<"path">> := <<"/space3">>},
            #{<<"id">> := _, <<"path">> := <<"/space4">>},
            #{<<"id">> := _, <<"path">> := <<"/space5">>},
            #{<<"id">> := _, <<"path">> := <<"/space6">>},
            #{<<"id">> := _, <<"path">> := <<"/space7">>},
            #{<<"id">> := _, <<"path">> := <<"/space8">>},
            #{<<"id">> := _, <<"path">> := <<"/space9">>}
        ],
        DecodedBody
    ).

list_dir_range(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        do_request(WorkerP1, <<"files?offset=1&limit=1">>, get, [user_1_token_header(Config)], [])),

    % then
    DecodedBody = json_utils:decode_map(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space2">>}
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
            #{<<"name">> := <<"space2">>, <<"spaceId">> := <<"space2">>},
            #{<<"name">> := <<"space3">>, <<"spaceId">> := <<"space3">>},
            #{<<"name">> := <<"space4">>, <<"spaceId">> := <<"space4">>},
            #{<<"name">> := <<"space5">>, <<"spaceId">> := <<"space5">>},
            #{<<"name">> := <<"space6">>, <<"spaceId">> := <<"space6">>},
            #{<<"name">> := <<"space7">>, <<"spaceId">> := <<"space7">>},
            #{<<"name">> := <<"space8">>, <<"spaceId">> := <<"space8">>},
            #{<<"name">> := <<"space9">>, <<"spaceId">> := <<"space9">>}
        ],
        DecodedBody
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
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, 10} = lfm_proxy:write(WorkerP1, Handle, 0, <<"0123456789">>),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),

    ExpectedDistribution0 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistributionProxyByGuid(WorkerP2, SessionId2, ExpectedDistribution0, FileGuid),

    % when
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
            <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []), ?ATTEMPTS),

    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 0,
        <<"failedFiles">> := 1
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    {ok, FailedTransfers} = rpc:call(WorkerP2, transfer, for_each_past_transfer, [
        fun(Id, Acc) -> [Id | Acc] end, [], <<"space4">>
    ]),
    ?assert(lists:member(Tid, FailedTransfers)),
    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
    ],

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File).

quota_decreased_after_invalidation(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    File = ?absPath(SpaceId, <<"file_quota_decreased">>),
    File2 = ?absPath(SpaceId, <<"file_quota_decreased2">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileGuid2} = lfm_proxy:create(WorkerP1, SessionId, File2, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, Handle2} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid2}, write),
    {ok, 10} = lfm_proxy:write(WorkerP1, Handle, 0, <<"0123456789">>),
    {ok, 10} = lfm_proxy:write(WorkerP1, Handle2, 0, <<"9876543210">>),
    lfm_proxy:fsync(WorkerP1, Handle),
    lfm_proxy:fsync(WorkerP1, Handle2),

    ExpectedDistribution0 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistributionProxyByGuid(WorkerP2, SessionId2, ExpectedDistribution0, FileGuid),

    % when
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []), ?ATTEMPTS),

    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 10,
        <<"bytesTransferred">> := 10
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ExpectedDistribution = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => [[0, 10]]}
    ],

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    % when
    {ok, 200, _, Body2} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File2/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []), ?ATTEMPTS),

    DecodedBody2 = json_utils:decode_map(Body2),
    #{<<"transferId">> := Tid2} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody2),

    % then file cannot be replicated because of quota
    {ok, FileObjectId2} = cdmi_id:guid_to_objectid(FileGuid2),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"failedFiles">> := 1,
        <<"filesTransferred">> := 0
    },
        case do_request(WorkerP1, <<"transfers/", Tid2/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    {ok, FailedTransfers} = rpc:call(WorkerP2, transfer, for_each_past_transfer, [
        fun(Id, Acc) -> [Id | Acc] end, [], <<"space6">>
    ]),
    ?assert(lists:member(Tid2, FailedTransfers)),

    ?assertEqualList([Tid, Tid2], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid, Tid2], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),

    ExpectedDistribution2 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]},
        #{<<"providerId">> => domain(WorkerP2), <<"blocks">> => []}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution2, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File2),

    {ok, 200, _, Body3} = do_request(WorkerP2,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        delete, [user_1_token_header(Config)], []
    ),
    DecodedBody3 = json_utils:decode_map(Body3),
    #{<<"transferId">> := Tid3} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody3),

    ?assertMatch(#{
        <<"transferStatus">> := <<"skipped">>,
        <<"targetProviderId">> := <<"undefined">>,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"completed">>,
        <<"callback">> := null
    },
        case do_request(WorkerP1, <<"transfers/", Tid3/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),


    % File replica is invalidated
    ExpectedDistribution3 = [
        #{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, 10]]}
    ],
    ?assertDistribution(WorkerP1, ExpectedDistribution3, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution3, Config, File),

    %File2 can now be replicated
    % when
    {ok, 200, _, Body4} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File2/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []), ?ATTEMPTS),

    DecodedBody4 = json_utils:decode_map(Body4),
    #{<<"transferId">> := Tid4} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody4),

    {ok, FileObjectId2} = cdmi_id:guid_to_objectid(FileGuid2),
    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := 10,
        <<"bytesTransferred">> := 10
    },
        case do_request(WorkerP1, <<"transfers/", Tid4/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS),

    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File2),

    ?assertEqualList([Tid, Tid2, Tid3, Tid4], list_finished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqualList([Tid, Tid2, Tid3, Tid4], list_finished_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqualList([], list_unfinished_transfers(WorkerP2, SpaceId), ?ATTEMPTS).

file_replication_failures_should_fail_whole_transfer(Config) ->
    %soft quota on WorkerP2 is set to 0, so every write on WorkerP2 will fail
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Dir = <<"/space8/fail_failures_exceeded">>,
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, Dir),
    {ok, OverallFileFailuresLimit} = rpc:call(WorkerP2, application, get_env, [op_worker, max_file_transfer_retries_per_transfer]),
    FilesNum = OverallFileFailuresLimit + 1,

    FileGuids = lists:map(fun(I) ->
        File = <<Dir/binary, "/", (integer_to_binary(I))/binary>>,
        {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
        lfm_proxy:fsync(WorkerP1, Handle),
        FileGuid
    end, lists:seq(1, FilesNum)),

    lists:foreach(fun(FileGuid) ->
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS)
    end, FileGuids),

    %replication of files will fail because space quota is set to 0 on WorkerP2
    % when
    {ok, 200, _, Body0} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", Dir/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []
    ), ?ATTEMPTS),
    DecodedBody0 = json_utils:decode_map(Body0),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody0),

    % then
    {ok, DirObjectGuid} = cdmi_id:guid_to_objectid(DirGuid),
    DomainP2 = domain(WorkerP2),

    FilesToTransfer = FilesNum + 1,

    ?assertMatch(#{
        <<"transferStatus">> := <<"failed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := Dir,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"fileId">> := DirObjectGuid,
        <<"callback">> := null,
        <<"filesToTransfer">> := FilesToTransfer,
        <<"filesTransferred">> := 1,
        <<"failedFiles">> := FilesNum
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, ?ATTEMPTS).

replicate_big_dir(Config) ->
    ct:timetrap({minutes, 10}),
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space3">>,
    RootDir = filename:join(Space, <<"big_dir_replication">>),
    Structure = [10, 10],   % last level are files
    FilesToCreate = lists:foldl(fun(N, AccIn) ->
        1 + AccIn * N end, 1, Structure),
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

    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", RootDir/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    % then
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"targetProviderId">> := DomainP2,
        <<"path">> := RootDir,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"callback">> := null,
        <<"filesToTransfer">> := FilesToCreate,
        <<"filesTransferred">> := FilesToCreate,
        <<"bytesToTransfer">> := BytesSum,
        <<"bytesTransferred">> := BytesSum
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, 600),
    worker_pool:stop_pool(?VERIFY_POOL).

replicate_big_file(Config) ->
    ct:timetrap({hours, 1}),
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    Space = <<"/space7">>,
    File = filename:join(Space, <<"big_file">>),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    BaseSize = 1024 * 1024 * 1024,
    Size = 2 * BaseSize,
    lfm_proxy:write(WorkerP1, Handle, 0, crypto:strong_rand_bytes(BaseSize)),
    lfm_proxy:write(WorkerP1, Handle, BaseSize, crypto:strong_rand_bytes(BaseSize)),
    lfm_proxy:fsync(WorkerP1, Handle),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),


    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _}, do_request(WorkerP1,
        <<"replicas", File/binary, "?provider_id=", (domain(WorkerP2))/binary>>,
        post, [user_1_token_header(Config)], []),
        ?ATTEMPTS),
    DecodedBody = json_utils:decode_map(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),

    % then
    ?assertMatch(#{
        <<"transferStatus">> := <<"completed">>,
        <<"path">> := File,
        <<"invalidationStatus">> := <<"skipped">>,
        <<"callback">> := null,
        <<"filesToTransfer">> := 1,
        <<"filesTransferred">> := 1,
        <<"bytesToTransfer">> := Size,
        <<"bytesTransferred">> := Size
    },
        case do_request(WorkerP1, <<"transfers/", Tid/binary>>, get, [user_1_token_header(Config)], []) of
            {ok, 200, _, TransferStatus} ->
                json_utils:decode_map(TransferStatus);
            Error -> Error
        end, 3600).

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
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)) % TODO - change to 2 seconds
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, multi_provider_rest_test_SUITE]}
        | Config
    ].


end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:=  replicate_file;
    Case =:=  restart_file_replication;
    Case =:=  replicate_dir;
    Case =:=  cancel_file_replication;
    Case =:=  restart_dir_replication;
    Case =:=  replicate_to_missing_provider;
    Case =:=  replicate_file_by_id;
    Case =:=  invalidate_file_replica;
    Case =:=  invalidate_file_replica_with_migration;
    Case =:=  restart_invalidation_of_file_replica_with_migration;
    Case =:= invalidate_dir_replica
    ->
    init_per_testcase(all, [{space_id, <<"space3">>} | Config]);

init_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

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
    Case =:= file_replication_failures_should_fail_whole_transfer
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
    Case =:= file_replication_failures_should_fail_whole_transfer
    ->
    [WorkerP2, _WorkerP1] = ?config(op_worker_nodes, Config),
    {ok, OldSoftQuota} = ?config(old_soft_quota, Config),
    SpaceId = ?config(space_id, Config),
    delete_transfers(WorkerP2, SpaceId),
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
    Case =:= restart_file_replication;
    Case =:= replicate_dir;
    Case =:= cancel_file_replication;
    Case =:= restart_dir_replication;
    Case =:= replicate_file_by_id;
    Case =:= replicate_to_missing_provider;
    Case =:= invalidate_file_replica;
    Case =:= invalidate_file_replica_with_migration;
    Case =:= restart_invalidation_of_file_replica_with_migration;
    Case =:= invalidate_dir_replica
    ->
    [WorkerP2, _WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(space_id, Config),
    delete_transfers(WorkerP2, SpaceId),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request(Node, URL, Method, Headers, Body) ->
    do_request(Node, URL, Method, Headers, Body, [{recv_timeout, 15000}]).
do_request(Node, URL, Method, Headers, Body, Opts) ->
    CaCertsDir = rpc:call(Node, oz_plugin, get_cacerts_dir, []),
    CaCerts = rpc:call(Node, cert_utils, load_ders_in_dir, [CaCertsDir]),
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

lengthen_file_replication_time(Node, SleepTimeInSeconds) ->
    test_utils:mock_new(Node, replica_synchronizer),
    test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
            timer:sleep(timer:seconds(SleepTimeInSeconds)),    %lengthen replication time
            meck:passthrough([UserCtx, FileCtx, Block, Prefetch, TransferId])
        end
    ).

resume_file_replication_time(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

kill(Pid) ->
    true = exit(list_to_pid(binary_to_list(Pid)), kill).

check_pid(_Node, 0, _Tid) ->
    undefined;
check_pid(Node, Attempts, Tid) ->
    case rpc:call(Node, transfer, get, [Tid]) of
        {ok, #document{value = #transfer{pid = undefined}}} ->
            timer:sleep(timer:seconds(1)),
            check_pid(Node, Attempts - 1, Tid);
        {ok, #document{value = #transfer{pid = Pid}}} ->
            Pid;
        _Other ->
            timer:sleep(timer:seconds(1)),
            check_pid(Node, Attempts - 1, Tid)
    end.

create_nested_directory_tree(Node, SessionId, [SubFilesNum], Root) ->
    create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        FilePath = filename:join([Root, integer_to_binary(N)]),
        worker_pool:cast(?VERIFY_POOL, {?MODULE, create_file, [Node, SessionId, FilePath]})
    end, lists:seq(1, SubFilesNum));
create_nested_directory_tree(Node, SessionId, [SubDirsNum | Rest], Root) ->
    create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = filename:join([Root, NBin]),
        worker_pool:cast(?VERIFY_POOL,
            {?MODULE, create_nested_directory_tree, [Node, SessionId, Rest, DirPath]}
        )
    end, lists:seq(1, SubDirsNum)).

create_file(Node, SessionId, FilePath) ->
    DataSize = byte_size(?TEST_DATA),
    {ok, FileGuid} = lfm_proxy:create(Node, SessionId, FilePath, 8#700),
    {ok, Handle} = lfm_proxy:open(Node, SessionId, {guid, FileGuid}, write),
    ?CREATE_FILE_COUNTER ! {created, FileGuid},
    ?assertEqual({ok, DataSize}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)).

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

delete_transfers(Worker, SpaceId) ->
    SuccessfulTransfers = list_finished_transfers(Worker, SpaceId),
    UnfinishedTransfers = list_unfinished_transfers(Worker, SpaceId),
    Transfers = SuccessfulTransfers ++ UnfinishedTransfers ,
    lists:foreach(fun(Tid) ->
        rpc:call(Worker, transfer, delete, [Tid])
    end, Transfers).

list_finished_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, for_each_past_transfer, [?LIST_TRANSFER, [], SpaceId]),
    Transfers.

list_unfinished_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, for_each_current_transfer, [?LIST_TRANSFER, [], SpaceId]),
    Transfers.
