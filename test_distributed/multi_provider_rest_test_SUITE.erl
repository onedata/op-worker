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
-include("http/rest.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_file_objectid/1,
    get_simple_file_distribution/1,
    transfers_should_be_ordered_by_timestamps/1,
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
    list_spaces/1,
    get_space/1,
    create_share/1,
    get_share/1,
    get_file_shares/1,
    update_share_name/1,
    delete_share/1,
    set_get_json_metadata/1,
    set_get_json_metadata_id/1,
    set_get_rdf_metadata/1,
    set_get_rdf_metadata_id/1,
    set_get_json_metadata_inherited/1,
    set_get_xattr_inherited/1,
    set_get_json_metadata_using_filter/1,
    primitive_json_metadata_test/1,
    empty_metadata_invalid_json_test/1,
    list_transfers/1,
    track_transferred_files/1
]).

%utils
-export([
    verify_file/3, create_file/3, create_dir/3,
    create_nested_directory_tree/4, sync_file_counter/3, create_file_counter/4,
    verify_distribution/6
]).

all() ->
    ?ALL([
        get_file_objectid,
        get_simple_file_distribution,
        transfers_should_be_ordered_by_timestamps,
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
        list_spaces,
        get_space,
        create_share,
        get_share,
        get_file_shares,
        update_share_name,
        delete_share,
        set_get_json_metadata,
        set_get_json_metadata_id,
        set_get_rdf_metadata,
        set_get_rdf_metadata_id,
        set_get_json_metadata_inherited,
        set_get_xattr_inherited,
        set_get_json_metadata_using_filter,
        primitive_json_metadata_test,
        empty_metadata_invalid_json_test,
        list_transfers,
        track_transferred_files
    ]).

-define(ATTEMPTS, 100).

-define(SPACE1_ID, <<"space1">>).

-define(normalizeDistribution(__Distributions), lists:sort(lists:map(fun(__Distribution) ->
    __Distribution#{
        <<"totalBlocksSize">> => lists:foldl(fun([_Offset, __Size], __SizeAcc) ->
            __SizeAcc + __Size
        end, 0, maps:get(<<"blocks">>, __Distribution))
    }
end, __Distributions))).

-define(assertDistribution(Worker, ExpectedDistribution, Config, File),
    ?assertEqual(?normalizeDistribution(ExpectedDistribution), begin
        case rest_test_utils:request(Worker, <<"replicas", File/binary>>, get,
            ?USER_1_AUTH_HEADERS(Config), []
        ) of
            {ok, 200, _, __Body} ->
                lists:sort(json_utils:decode(__Body));
            Error ->
                Error
        end
    end, ?ATTEMPTS)).

-define(assertTransferStatus(ExpectedStatus, Worker, Tid, Config),
    ?assertTransferStatus(ExpectedStatus, Worker, Tid, Config, ?ATTEMPTS)).

-define(assertTransferStatus(ExpectedStatus, Worker, Tid, Config, Attempts),
    ?assertMatch(ExpectedStatus,
        case rest_test_utils:request(Worker, <<"transfers/", Tid/binary>>, get, ?USER_1_AUTH_HEADERS(Config), []) of
            {ok, 200, _, __TransferStatus} ->
                json_utils:decode(__TransferStatus);
            Error -> Error
        end, Attempts)
).

-define(absPath(SpaceId, Path), <<"/", SpaceId/binary, "/", Path/binary>>).

-define(TEST_DATA, <<"test">>).
-define(TEST_DATA_SIZE, 4).
-define(TEST_DATA2, <<"test01234">>).
-define(TEST_DATA_SIZE2, 9).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).

-define(CREATE_FILE_COUNTER, create_file_counter).
-define(SYNC_FILE_COUNTER, sync_file_counter).
-define(VERIFY_POOL, verify_pool).

-define(SHARE_PUBLIC_URL(__ShareId), <<__ShareId/binary, "_public_url">>).
-define(SHARE_HANDLE_ID(__ShareId), <<__ShareId/binary, "_handle_id">>).

%%%===================================================================
%%% Test functions
%%%===================================================================


get_file_objectid(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "get_file_objectid"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, 8#700),
    {ok, 200, _, Response} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
        WorkerP1, <<"file-id/", FilePath/binary>>, get,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), []
    )),
    #{<<"fileId">> := ObjectId} = json_utils:decode(Response),
    ?assertMatch({ok, ObjectId}, file_id:guid_to_objectid(FileGuid)).


get_simple_file_distribution(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file0_gsfd"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, Handle, 0, ?TEST_DATA),
    lfm_proxy:fsync(WorkerP1, Handle),

    % when
    ExpectedDistribution = [#{
        <<"providerId">> => domain(WorkerP1),
        <<"blocks">> => [[0, 4]]
    }],

    % then
    ?assertDistribution(WorkerP1, ExpectedDistribution, Config, File).

transfers_should_be_ordered_by_timestamps(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),

    File = ?absPath(SpaceId, <<"file_sorted">>),
    Size = 1,
    FileGuid = create_test_file(WorkerP1, SessionId, File, crypto:strong_rand_bytes(Size)),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    File2 = ?absPath(SpaceId, <<"file_sorted2">>),
    Size2 = 3 * 1024 * 1024 * 1024,
    FileGuid2 = create_test_file_by_size(WorkerP1, SessionId, File2, Size2),
    {ok, FileObjectId2} = file_id:guid_to_objectid(FileGuid2),

    % when
    ?assertMatch({ok, #file_attr{size = Size}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = Size2}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, File2}), ?ATTEMPTS),
    ExpectedDistribution = [#{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, Size]]}],
    ExpectedDistribution2 = [#{<<"providerId">> => domain(WorkerP1), <<"blocks">> => [[0, Size2]]}],
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, File),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, File2),

    Tid2 = schedule_file_replication(WorkerP2, DomainP2, File2, Config),
    ?assertEqual([Tid2], get_ongoing_transfers_for_file(WorkerP2, FileGuid2), ?ATTEMPTS),
    ?assertEqual([], get_ended_transfers_for_file(WorkerP2, FileGuid2), ?ATTEMPTS),
    % Wait 1 second to be sure that transfer Tid will have greater timestamp than transfer Tid2
    timer:sleep(timer:seconds(1)),
    Tid = schedule_file_replication(WorkerP2, DomainP2, File, Config),
    ?assertEqual([Tid], get_ongoing_transfers_for_file(WorkerP2, FileGuid), ?ATTEMPTS),
    ?assertEqual([], get_ended_transfers_for_file(WorkerP2, FileGuid), ?ATTEMPTS),

    % then
    ?assertTransferStatus(#{
        <<"replicationStatus">> := <<"completed">>,
        <<"replicatingProviderId">> := DomainP2,
        <<"path">> := File,
        <<"replicaEvictionStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"failedFiles">> := 0,
        <<"filesReplicated">> := 1,
        <<"fileReplicasEvicted">> := 0,
        <<"bytesReplicated">> := Size,
        <<"mthHist">> := #{DomainP1 := [Size | _]}
    }, WorkerP1, Tid, Config),

    ?assertTransferStatus(#{
        <<"replicationStatus">> := <<"completed">>,
        <<"replicatingProviderId">> := DomainP2,
        <<"path">> := File2,
        <<"replicaEvictionStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesReplicated">> := 1,
        <<"fileReplicasEvicted">> := 0,
        <<"bytesReplicated">> := Size2,
        <<"mthHist">> := #{DomainP1 := [Size2 | _]}
    }, WorkerP1, Tid2, Config),

    ?assertEqual([], list_waiting_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_ongoing_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_ended_transfers(WorkerP1, SpaceId), ?ATTEMPTS),
    ?assertEqual([], get_ongoing_transfers_for_file(WorkerP1, FileGuid), ?ATTEMPTS),
    ?assertEqual([], get_ongoing_transfers_for_file(WorkerP1, FileGuid2), ?ATTEMPTS),
    ?assertEqual([Tid], get_ended_transfers_for_file(WorkerP1, FileGuid), ?ATTEMPTS),
    ?assertEqual([Tid2], get_ended_transfers_for_file(WorkerP1, FileGuid2), ?ATTEMPTS),

    ?assertEqual([], list_waiting_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], list_ongoing_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([Tid2, Tid], list_ended_transfers(WorkerP2, SpaceId), ?ATTEMPTS),
    ?assertEqual([], get_ongoing_transfers_for_file(WorkerP2, FileGuid), ?ATTEMPTS),
    ?assertEqual([], get_ongoing_transfers_for_file(WorkerP2, FileGuid2), ?ATTEMPTS),
    ?assert(get_finish_time(WorkerP2, Tid, Config) =< get_finish_time(WorkerP2, Tid2, Config)).

posix_mode_get(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1_pmg"])),
    Mode = 8#700,
    {ok, _FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {ok, 200, _, Body} = rest_test_utils:request(WorkerP1, <<"metadata/attrs", File/binary, "?attribute=mode">>, get, ?USER_1_AUTH_HEADERS(Config), []),

    % then
    DecodedBody = json_utils:decode(Body),
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
    Body = json_utils:encode(#{<<"mode">> => <<"0", (integer_to_binary(NewMode, 8))/binary>>}),
    {ok, 204, _, _} = rest_test_utils:request(WorkerP1, <<"metadata/attrs", File/binary>>, put,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), Body),

    % then
    {ok, 200, _, RespBody} = rest_test_utils:request(WorkerP1, <<"metadata/attrs", File/binary, "?attribute=mode">>, get, ?USER_1_AUTH_HEADERS(Config), []),
    DecodedBody = json_utils:decode(RespBody),
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
    {ok, 200, _, Body} = rest_test_utils:request(WorkerP1, <<"metadata/attrs", File/binary>>, get, ?USER_1_AUTH_HEADERS(Config), []),

    % then
    {ok, #file_attr{
        atime = ATime,
        ctime = CTime,
        mtime = MTime,
        gid = Gid,
        uid = Uid
    }} = lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid}),
    {ok, CdmiObjectId} = file_id:guid_to_objectid(FileGuid),
    DecodedBody = json_utils:decode(Body),
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
    {ok, 200, _, Body} = rest_test_utils:request(WorkerP1, <<"metadata/xattrs", File/binary, "?attribute=k1">>, get, ?USER_1_AUTH_HEADERS(Config), []),

    % then
    DecodedBody = json_utils:decode(Body),
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
    Body = json_utils:encode(#{<<"k1">> => <<"v1">>}),
    {ok, 204, _, _} = rest_test_utils:request(WorkerP1, <<"metadata/xattrs", File/binary>>, put,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), Body),

    % then
    {ok, 200, _, RespBody} = rest_test_utils:request(WorkerP1, <<"metadata/xattrs", File/binary, "?attribute=k1">>, get, ?USER_1_AUTH_HEADERS(Config), []),
    DecodedBody = json_utils:decode(RespBody),
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
    {ok, 200, _, Body} = rest_test_utils:request(WorkerP1, <<"metadata/xattrs", File/binary>>, get, ?USER_1_AUTH_HEADERS(Config), []),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(#{
        <<"k1">> := <<"v1">>,
        <<"k2">> := <<"v2">>
    }, DecodedBody).

metric_get(Config) ->
    Workers = [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space2">>,
    UserId = <<"user1">>,

    Prov1ID = rpc:call(WorkerP1, oneprovider, get_id, []),
    Prov2ID = rpc:call(WorkerP2, oneprovider, get_id, []),

    MonitoringId = #monitoring_id{
        main_subject_type = space,
        main_subject_id = SpaceId,
        metric_type = storage_quota,
        provider_id = Prov1ID
    },

    ?assertMatch(ok, rpc:call(WorkerP1, monitoring_utils, create, [SpaceId, MonitoringId, time_utils:system_time_seconds()])),
    {ok, #document{value = State}} = rpc:call(WorkerP1, monitoring_state, get, [MonitoringId]),
    ?assertMatch({ok, _}, rpc:call(WorkerP1, monitoring_state, save, [
        #document{
            key = monitoring_state:encode_id(MonitoringId#monitoring_id{
                provider_id = Prov2ID
            }),
            value = State
        }
    ])),

    AllPrivs = privileges:space_privileges(),

    % viewing metrics without SPACE_VIEW_STATISTICS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, SpaceId, UserId, AllPrivs -- [?SPACE_VIEW_STATISTICS]),
    ?assertMatch(
        {ok, 403, _, _},
        rest_test_utils:request(WorkerP1, <<"metrics/space/space2?metric=storage_quota">>, get, ?USER_1_AUTH_HEADERS(Config), [])
    ),

    % viewing metrics with SPACE_VIEW_STATISTICS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, SpaceId, UserId, [?SPACE_VIEW_STATISTICS]),
    {ok, 200, _, Body} = ?assertMatch(
        {ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metrics/space/space2?metric=storage_quota">>, get, ?USER_1_AUTH_HEADERS(Config), [])
    ),
    DecodedBody = json_utils:decode(Body),

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
    File = <<"/space2/file1_lf">>,
    Mode = 8#700,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, Mode),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"files/space2/file1_lf">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    DecodedBody = json_utils:decode(Body),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    ?assertEqual(
        [#{<<"id">> => FileObjectId, <<"path">> => File}],
        DecodedBody
    ).

list_dir(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"files">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space1">>},
            #{<<"id">> := _, <<"path">> := <<"/space2">>},
            #{<<"id">> := _, <<"path">> := <<"/space3">>},
            #{<<"id">> := _, <<"path">> := <<"/space4">>}
        ],
        lists:sort(fun(#{<<"path">> := Path1}, #{<<"path">> := Path2}) ->
            Path1 =< Path2
        end, DecodedBody)
    ).

list_dir_range(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"files?offset=0&limit=1">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            #{<<"id">> := _, <<"path">> := <<"/space1">>}
        ],
        DecodedBody
    ).

list_spaces(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"spaces">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        [
            #{<<"name">> := <<"space1">>, <<"spaceId">> := <<"space1">>},
            #{<<"name">> := <<"space2">>, <<"spaceId">> := <<"space2">>},
            #{<<"name">> := <<"space3">>, <<"spaceId">> := <<"space3">>},
            #{<<"name">> := <<"space4">>, <<"spaceId">> := <<"space4">>}
        ],
        lists:sort(DecodedBody)
    ).

get_space(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"spaces/space2">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        #{
            <<"name">> := <<"space2">>,
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
            <<"spaceId">> := <<"space2">>
        },
        DecodedBody
    ).


create_share(Config) ->
    {SupportingProviderNode, OtherProviderNode} = get_op_nodes(Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SupportingProviderNode)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),

    % create directory
    DirPath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "shared_dir"])),
    {ok, DirGuid} = lfm_proxy:mkdir(SupportingProviderNode, SessionId, DirPath, 8#700),

    % create regular file
    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file1"])),
    {ok, FileGuid} = lfm_proxy:create(SupportingProviderNode, SessionId, FilePath, 8#600),

    RestPath = <<"shares/">>,
    ShareName = <<"Share name">>,

    lists:foreach(fun(Guid) ->
        PayloadWithNameOnly = json_utils:encode(#{
            <<"name">> => ShareName
        }),
        FullPayload = json_utils:encode(#{
            <<"name">> => ShareName,
            <<"fileId">> => element(2, {ok, _} = file_id:guid_to_objectid(Guid))
        }),

        % request without share name should fail
        ?assertMatch(true, rest_test_utils:assert_request_error(
            ?ERROR_MISSING_REQUIRED_VALUE(<<"fileId">>),
            {SupportingProviderNode, RestPath, post, Headers, PayloadWithNameOnly}
        )),

        % creating share from provider that does not support space should fail
        ?assertMatch(true, rest_test_utils:assert_request_error(
            ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(OtherProviderNode)),
            {OtherProviderNode, RestPath, post, Headers, FullPayload}
        )),

        % creating with share name in request should succeed
        % and return share id in response
        {ok, 201, _, Response1} = ?assertMatch(
            {ok, 201, _, _},
            rest_test_utils:request(SupportingProviderNode, RestPath, post, Headers, FullPayload)
        ),
        #{<<"shareId">> := ShareId1} = json_utils:decode(Response1),
        ShareGuid = file_id:guid_to_share_guid(Guid, ShareId1),

        % check that share with given name and id has been created
        ?assertMatch(
            {ok, #document{key = ShareId1, value = #od_share{
                root_file = ShareGuid, name = ShareName, space = SpaceId
            }}},
            rpc:call(SupportingProviderNode, share_logic, get, [?ROOT_SESS_ID, ShareId1])
        ),

        % file can be shared multiple times
        {ok, 201, _, Response2} = ?assertMatch(
            {ok, 201, _, _},
            rest_test_utils:request(SupportingProviderNode, RestPath, post, Headers, FullPayload)
        ),
        #{<<"shareId">> := ShareId2} = json_utils:decode(Response2),

        ?assertNot(ShareId1 == ShareId2)
    end, [DirGuid, FileGuid]).


get_share(Config) ->
    {SupportingProviderNode, OtherProviderNode} = get_op_nodes(Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SupportingProviderNode)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),

    % create directory
    SharedDir = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "shared_dir"])),
    {ok, SharedDirGuid} = lfm_proxy:mkdir(SupportingProviderNode, SessionId, SharedDir, 8#700),

    % get invalid rest path
    InvalidRestPath = str_utils:format_bin("shares/~s", [<<"invalid_share_id">>]),

    % create share for directory
    ShareName = <<"Share name">>,
    {ok, {ShareId, ShareGuid }} = ?assertMatch({ok, {_, _ }},
        lfm_proxy:create_share(SupportingProviderNode, SessionId, {guid, SharedDirGuid}, ShareName)),
    ExpectedPublicUrl = ?SHARE_PUBLIC_URL(ShareId),
    ExpectedHandleId = ?SHARE_HANDLE_ID(ShareId),
    {ok, ExpectedRootFileObjectId} = file_id:guid_to_objectid(ShareGuid),

    % getting not existing share should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_NOT_FOUND,
        {SupportingProviderNode, InvalidRestPath, get, Headers, <<"">>}
    )),

    % get valid rest path
    RestPath = str_utils:format_bin("shares/~s", [ShareId]),

    % this mock is needed as remove_share calls share_logic:get() under the hood
    % and share_logic mock from initializer does not propagate information to other providers
    mock_get_share_on_other_node(OtherProviderNode, SupportingProviderNode, SessionId, ShareId),

    % getting share from provider that does not support space should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(OtherProviderNode)),
        {OtherProviderNode, RestPath, get, Headers, <<>>}
    )),

    % getting share for directory should succeed
    {ok, 200, _, Response} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(SupportingProviderNode, RestPath, get, Headers, <<>>)),

    ?assertMatch(
        #{
            <<"shareId">> := ShareId,
            <<"name">> := ShareName,
            <<"publicUrl">> := ExpectedPublicUrl,
            <<"fileType">> := <<"dir">>,
            <<"rootFileId">> := ExpectedRootFileObjectId,
            <<"spaceId">> := ?SPACE1_ID,
            <<"handleId">> := ExpectedHandleId
        },
        json_utils:decode(Response)
    ),

    ?assertMatch(
        {ok, #document{key = ShareId, value = #od_share{
            root_file = ShareGuid,
            name = ShareName,
            space = ?SPACE1_ID,
            public_url = ExpectedPublicUrl,
            handle = ExpectedHandleId
        }}},
        rpc:call(SupportingProviderNode, share_logic, get, [?ROOT_SESS_ID, ShareId])
    ).


get_file_shares(Config) ->
    {SupportingProviderNode, OtherProviderNode} = get_op_nodes(Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SupportingProviderNode)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),

    % create file
    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "shared_file"])),
    {ok, SharedFileGuid} = lfm_proxy:create(SupportingProviderNode, SessionId, FilePath, 8#700),
    {ok, SharedFileObjectId} = file_id:guid_to_objectid(SharedFileGuid),

    RestPath1 = str_utils:format_bin("file-shares/~s", [FilePath]),
    RestPath2 = str_utils:format_bin("file-id-shares/~s", [SharedFileObjectId]),

    lists:foreach(fun(RestPath) ->
        % getting share from provider that does not support space should fail
        ?assertMatch(true, rest_test_utils:assert_request_error(
            ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(OtherProviderNode)),
            {OtherProviderNode, RestPath, get, Headers, <<>>}
        )),

        % getting shares for file not yet shared should return empty list
        ?assertMatch(
            {ok, 200, _, <<"[]">>},
            rest_test_utils:request(SupportingProviderNode, RestPath, get, Headers, <<>>)
        )
    end, [RestPath1, RestPath2]),

    ShareIds = lists:map(fun(_) ->
        {ok, {ShareId, _}} = ?assertMatch(
            {ok, {_, _ }},
            lfm_proxy:create_share(SupportingProviderNode, SessionId, {guid, SharedFileGuid}, <<"share">>)
        ),
        ShareId
    end, lists:seq(1, 10)),
    ExpShareIds = lists:reverse(ShareIds),

    lists:foreach(fun(RestPath) ->
        % getting shares for file not yet shared should return empty list
        {ok, 200, _, Response} = ?assertMatch(
            {ok, 200, _, _},
            rest_test_utils:request(SupportingProviderNode, RestPath, get, Headers, <<>>)
        ),
        ?assertEqual(ExpShareIds, json_utils:decode(Response))
    end, [RestPath1, RestPath2]).


update_share_name(Config) ->
    {SupportingProviderNode, OtherProviderNode} = get_op_nodes(Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SupportingProviderNode)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),

    % create directory
    SharedDir = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "shared_dir"])),
    {ok, SharedDirGuid} = lfm_proxy:mkdir(SupportingProviderNode, SessionId, SharedDir, 8#700),

    % get invalid rest paths
    InvalidRestPath = str_utils:format_bin("shares/~s", [<<"invalid_share_id">>]),
    NewShareName = <<"NewShareName">>,
    Payload = json_utils:encode(#{<<"name">> => NewShareName}),

    % updating share should fail when share does not exists
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_NOT_FOUND,
        {SupportingProviderNode, InvalidRestPath, patch, Headers, Payload}
    )),

    % create share for directory
    {ok, {ShareId, _ShareGuid}} = ?assertMatch({ok, {_, _ }},
        lfm_proxy:create_share(SupportingProviderNode, SessionId, {guid, SharedDirGuid}, <<"Share name">>)),

    % get valid rest path
    RestPath = str_utils:format_bin("shares/~s", [ShareId]),

    % this mock is needed as remove_share calls share_logic:get() under the hood
    % and share_logic mock from initializer does not propagate information to other providers
    mock_get_share_on_other_node(OtherProviderNode, SupportingProviderNode, SessionId, ShareId),

    % request without new share name should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_MISSING_REQUIRED_VALUE(<<"name">>),
        {SupportingProviderNode, RestPath, patch, Headers, <<>>}
    )),

    % updating share from provider that does not support space should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(OtherProviderNode)),
        {OtherProviderNode, RestPath, patch, Headers, Payload}
    )),

    % updating share name should succeed
    {ok, 204, _, _} = ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(SupportingProviderNode, RestPath, patch, Headers, Payload)),

    % check that share has been renamed
    ?assertMatch(
        {ok, #document{key = ShareId, value = #od_share{name = NewShareName}}},
        rpc:call(SupportingProviderNode, share_logic, get, [?ROOT_SESS_ID, ShareId])
    ).


delete_share(Config) ->
    {SupportingProviderNode, OtherProviderNode} = get_op_nodes(Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SupportingProviderNode)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),

    % create directory
    SharedDir = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "shared_dir"])),
    {ok, SharedDirGuid} = lfm_proxy:mkdir(SupportingProviderNode, SessionId, SharedDir, 8#700),

    % get invalid rest paths
    InvalidRestPath = str_utils:format_bin("shares/~s", [<<"invalid_share_id">>]),

    % delete not existing share should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_NOT_FOUND,
        {SupportingProviderNode, InvalidRestPath, delete, Headers, <<>>}
    )),

    % create share for directory
    {ok, {ShareId, _ShareGuid}} = ?assertMatch({ok, {_, _ }},
        lfm_proxy:create_share(SupportingProviderNode, SessionId, {guid, SharedDirGuid}, <<"Share name">>)),

    % get valid rest path
    RestPath = str_utils:format_bin("shares/~s", [ShareId]),

    % this mock is needed as remove_share calls share_logic:get() under the hood
    % and share_logic mock from initializer does not propagate information to other providers
    mock_get_share_on_other_node(OtherProviderNode, SupportingProviderNode, SessionId, ShareId),

    % deleting share from provider that does not support space should fail
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(OtherProviderNode)),
        {OtherProviderNode, RestPath, delete, Headers, <<>>}
    )),

    % deleting share for directory should succeed
    {ok, 204, _, _} = ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(SupportingProviderNode, RestPath, delete, Headers, <<>>)),

    ?assertMatch({error, not_found}, rpc:call(SupportingProviderNode, share_logic, get, [?ROOT_SESS_ID, ShareId])),
    ?assertMatch(true, rest_test_utils:assert_request_error(
        ?ERROR_NOT_FOUND,
        {SupportingProviderNode, RestPath, delete, Headers, <<>>}
    )),

    % recreating share after delete should succeed
    ?assertMatch(
        {ok, {_, _ }},
        lfm_proxy:create_share(SupportingProviderNode, SessionId, {guid, SharedDirGuid}, <<"Share name">>)
    ).


set_get_json_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])).

set_get_json_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space2/file_sgjmi">>, 8#777),
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata-id/json/", ObjectId/binary>>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), "{\"key\": \"value\"}")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata-id/json/", ObjectId/binary>>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        #{
            <<"key">> := <<"value">>
        },
        DecodedBody
    ),

    % then
    ?assertMatch({ok, 200, _, <<"\"value\"">>},
        rest_test_utils:request(WorkerP1, <<"metadata-id/json/", ObjectId/binary, "?filter_type=keypath&filter=key">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])).

set_get_rdf_metadata(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/rdf/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/rdf+xml">>}]), "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/rdf/space2">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/rdf+xml">>}]), [])),
    ?assertMatch(<<"some_xml">>, Body).

set_get_rdf_metadata_id(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    {ok, Guid} = lfm_proxy:create(WorkerP1, SessionId, <<"/space2/file_sgrmi">>, 8#777),
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    % when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata-id/rdf/", ObjectId/binary>>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/rdf+xml">>}]), "some_xml")),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata-id/rdf/", ObjectId/binary>>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/rdf+xml">>}]), [])),
    ?assertMatch(<<"some_xml">>, Body).

set_get_json_metadata_inherited(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),

    % when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"{\"a\": {\"a1\": \"b1\"}, \"b\": \"c\", \"e\": \"f\"}">>)),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space2/dir">>),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2/dir">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"{\"a\": {\"a2\": \"b2\"}, \"b\": \"d\"}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2/dir?inherited=true">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody = json_utils:decode(Body),
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
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space2/dir_test">>),
    {ok, _} = lfm_proxy:mkdir(WorkerP1, SessionId, <<"/space2/dir_test/child">>),

    % when
    XattrSpace = json_utils:encode(#{<<"k1">> => <<"v1">>}),
    XattrDir = json_utils:encode(#{<<"k2">> => <<"v2">>}),
    XattrChild = json_utils:encode(#{<<"k2">> => <<"v22">>}),
    XattrChild2 = json_utils:encode(#{<<"k3">> => <<"v3">>}),

    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"{\"a\":5}">>)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/xattrs/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), XattrSpace)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/xattrs/space2/dir_test">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), XattrDir)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/xattrs/space2/dir_test/child">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), XattrChild)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/xattrs/space2/dir_test/child">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), XattrChild2)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/xattrs/space2/dir_test/child?inherited=true">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody = json_utils:decode(Body),
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
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"{\"key1\": \"value1\", \"key2\": \"value2\", \"key3\": [\"v1\", \"v2\"]}">>)),

    % then
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key1">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(<<"value1">>, DecodedBody),
    {_, _, _, Body2} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key3.[1]">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    DecodedBody2 = json_utils:decode(Body2),
    ?assertMatch(<<"v2">>, DecodedBody2),

    %when
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key1">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"\"value11\"">>)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key2">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"{\"key22\": \"value22\"}">>)),
    ?assertMatch({ok, 204, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2?filter_type=keypath&filter=key3.[0]">>, put,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), <<"\"v11\"">>)),

    %then
    {_, _, _, ReponseBody} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, get,
            ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), [])),
    ?assertMatch(
        #{
            <<"key1">> := <<"value11">>,
            <<"key2">> := #{<<"key22">> := <<"value22">>},
            <<"key3">> := [<<"v11">>, <<"v2">>]
        },
        json_utils:decode(ReponseBody)).

primitive_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Primitives = [<<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
        <<"null">>, <<"\"string\"">>],

    lists:foreach(fun(Primitive) ->
        ?assertMatch({ok, 204, _, _},
            rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
                ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), Primitive)),
        ?assertMatch({ok, 200, _, Primitive},
            rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, get,
                ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]), []))
    end, Primitives).

empty_metadata_invalid_json_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    InvalidJsons = [<<"aaa">>, <<"{">>, <<"{\"aaa\": aaa}">>],

    lists:foreach(fun(InvalidJson) ->
        ?assertMatch({ok, 400, _, _},
            rest_test_utils:request(WorkerP1, <<"metadata/json/space2">>, put,
                ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), InvalidJson))
    end, InvalidJsons).

list_transfers(Config) ->
    ct:timetrap({hours, 1}),
    Workers = [P2, P1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(P1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(P2)}}, Config),
    Space = <<"space4">>,
    RootDir = filename:join([<<"/">>, Space, <<"list_transfers">>]),
    FilesNum = 100,
    Structure = [FilesNum],
    FilesToCreate = lists:foldl(fun(N, AccIn) ->
        1 + AccIn * N end, 1, Structure),
    DomainP2 = domain(P2),

    true = register(?CREATE_FILE_COUNTER, spawn_link(?MODULE, create_file_counter, [0, FilesToCreate, self(), []])),
    true = register(?SYNC_FILE_COUNTER, spawn_link(?MODULE, sync_file_counter, [0, FilesToCreate, self()])),

    create_nested_directory_tree(P1, SessionId, Structure, RootDir),
    FileGuidsAndPaths = receive {create, FileGuidsAndPaths0} ->
        FileGuidsAndPaths0 end,

    lists:foreach(fun({FileGuid, _FilePath}) ->
        worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file, [P2, SessionId2, FileGuid]})
    end, FileGuidsAndPaths),
    receive files_synchronized -> ok end,

    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(P2, SessionId2, {path, RootDir})),

    FileGuidsAndPaths2 = proplists:delete(DirGuid, FileGuidsAndPaths),

    AllTransfers = lists:sort(lists:map(fun({FileGuid, _FilePath}) ->
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        schedule_file_replication_by_id(P1, DomainP2, FileObjectId, Config)
    end, FileGuidsAndPaths2)),

    % List using random chunk sizes
    Waiting = fun(Worker) ->
        list_all_transfers_via_rest(Config, Worker, Space, <<"waiting">>, rand:uniform(FilesNum))
    end,
    Ongoing = fun(Worker) ->
        list_all_transfers_via_rest(Config, Worker, Space, <<"ongoing">>, rand:uniform(FilesNum))
    end,
    Ended = fun(Worker) ->
        list_all_transfers_via_rest(Config, Worker, Space, <<"ended">>, rand:uniform(FilesNum))
    end,
    All = fun(Worker) ->
        lists:usort(Waiting(Worker) ++ Ongoing(Worker) ++ Ended(Worker)) end,

    OneFourth = FilesNum div 4,

    % Check if listing different
    ?assertMatch(AllTransfers, All(P1), ?ATTEMPTS),
    ?assertMatch(AllTransfers, All(P2), ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P1)) > OneFourth, ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P2)) > OneFourth, ?ATTEMPTS),

    ?assertMatch(AllTransfers, All(P1), ?ATTEMPTS),
    ?assertMatch(AllTransfers, All(P2), ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P1)) > 2 * OneFourth, ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P2)) > 2 * OneFourth, ?ATTEMPTS),

    ?assertMatch(AllTransfers, All(P1), ?ATTEMPTS),
    ?assertMatch(AllTransfers, All(P2), ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P1)) > 3 * OneFourth, ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P2)) > 3 * OneFourth, ?ATTEMPTS),

    ?assertMatch(AllTransfers, All(P1), ?ATTEMPTS),
    ?assertMatch(AllTransfers, All(P2), ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P1)) =:= FilesNum, ?ATTEMPTS),
    ?assertMatch(true, length(Ended(P2)) =:= FilesNum, ?ATTEMPTS),

    ?assertMatch(AllTransfers, lists:sort(Ended(P1)), ?ATTEMPTS),
    ?assertMatch(AllTransfers, lists:sort(Ended(P2)), ?ATTEMPTS),

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),

    % listing transfers without SPACE_VIEW_TRANSFERS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, <<"space4">>, <<"user1">>, AllPrivs -- [?SPACE_VIEW_TRANSFERS]),
    ?assertMatch(ErrorForbidden, Ended(P1), ?ATTEMPTS),
    ?assertMatch(ErrorForbidden, Ended(P2), ?ATTEMPTS).

track_transferred_files(Config) ->
    [Provider1, Provider2] = ?config(op_worker_nodes, Config),
    FileUuid = <<"file1">>,
    SpaceId = <<"space2">>,
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    ScheduleTime = 100,
    FinishTime = 150,
    Transfer1 = <<"transfer1">>,
    Transfer2 = <<"transfer2">>,
    Transfer3 = <<"transfer3">>,
    Transfer4 = <<"transfer4">>,

    % Check if counting ongoing transfers works as expected and is synchronized
    % by another provider
    rpc:call(Provider1, transferred_file, report_transfer_start, [
        FileGuid, Transfer1, ScheduleTime + 10
    ]),
    ?assertMatch([Transfer1], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer1], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    rpc:call(Provider1, transferred_file, report_transfer_start, [
        FileGuid, Transfer2, ScheduleTime + 20
    ]),
    ?assertMatch([Transfer2, Transfer1], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer2, Transfer1], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    rpc:call(Provider1, transferred_file, report_transfer_start, [
        FileGuid, Transfer2, ScheduleTime + 20
    ]),
    ?assertMatch([Transfer2, Transfer1], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer2, Transfer1], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    rpc:call(Provider1, transferred_file, report_transfer_finish, [
        FileGuid, Transfer1, ScheduleTime + 10, FinishTime + 10
    ]),
    ?assertMatch([Transfer2], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer2], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer1], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer1], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    rpc:call(Provider1, transferred_file, report_transfer_finish, [
        FileGuid, Transfer2, ScheduleTime + 20, FinishTime + 20
    ]),
    ?assertMatch([], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer2, Transfer1], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer2, Transfer1], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    % Check if changes from concurrent operations on two providers are reconciled correctly
    rpc:call(Provider1, transferred_file, report_transfer_start, [
        FileGuid, Transfer3, ScheduleTime + 30
    ]),
    rpc:call(Provider2, transferred_file, report_transfer_finish, [
        FileGuid, Transfer3, ScheduleTime + 30, FinishTime + 30
    ]),
    % Cross checks to make sure the changes have been propagated
    ?assertMatch([], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    ?assertMatch([Transfer3, Transfer2, Transfer1], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([Transfer3, Transfer2, Transfer1], get_ended_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),

    % Check if record cleanup works as expected
    rpc:call(Provider1, transferred_file, report_transfer_start, [
        FileGuid, Transfer4, ScheduleTime + 40
    ]),
    ?assertEqual([Transfer4], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertEqual([Transfer4], get_ongoing_transfers_for_file(Provider2, FileGuid), ?ATTEMPTS),
    rpc:call(Provider1, transferred_file, clean_up, [FileGuid]),
    ?assertMatch([], get_ongoing_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),
    ?assertMatch([], get_ended_transfers_for_file(Provider1, FileGuid), ?ATTEMPTS),

    ok.

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
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),
        application:start(ssl),
        hackney:start(),
        NewConfig2
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
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(transfers_should_be_ordered_by_timestamps, Config) ->
    init_per_testcase(all, [{space_id, <<"space2">>} | Config]);

init_per_testcase(metric_get, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    start_monitoring_worker(WorkerP1),
    test_utils:mock_new(WorkerP1, rrd_utils),
    test_utils:mock_expect(WorkerP1, rrd_utils, export_rrd, fun(_, _, _) ->
        {ok, <<"{\"test\":\"rrd\"}">>}
    end),
    OldPrivs = rpc:call(WorkerP1, initializer, node_get_mocked_space_user_privileges, [<<"space2">>, <<"user1">>]),
    init_per_testcase(all, [{old_privs, OldPrivs} | Config]);

init_per_testcase(list_transfers, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    OldPrivs = rpc:call(WorkerP1, initializer, node_get_mocked_space_user_privileges, [<<"space4">>, <<"user1">>]),
    init_per_testcase(all, [{old_privs, OldPrivs} | Config]);

init_per_testcase(Case, Config) when
    Case =:= create_share;
    Case =:= get_share;
    Case =:= get_file_shares;
    Case =:= update_share_name;
    Case =:= delete_share
->
    initializer:mock_share_logic(Config),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    Config2 = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config2).

end_per_testcase(metric_get = Case, Config) ->
    Workers = [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(WorkerP1, rrd_utils),
    OldPrivs = ?config(old_privs, Config),
    initializer:testmaster_mock_space_user_privileges(Workers, <<"space2">>, <<"user1">>, OldPrivs),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(list_transfers = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    OldPrivs = ?config(old_privs, Config),
    initializer:testmaster_mock_space_user_privileges(Workers, <<"space2">>, <<"user1">>, OldPrivs),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(changes_stream_closed_on_disconnection, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, changes),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= create_share;
    Case =:= get_share;
    Case =:= get_file_shares;
    Case =:= update_share_name;
    Case =:= delete_share
->
    initializer:unmock_share_logic(Config),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    remove_transfers(Config),
    ensure_transfers_removed(Config),
    test_utils:mock_unload(Workers, [sync_req, replica_deletion_req]),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).

create_nested_directory_tree(Node, SessionId, [SubFilesNum], Root) ->
    DirGuid = create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        FilePath = filename:join([Root, integer_to_binary(N)]),
        ok = worker_pool:cast(?VERIFY_POOL, {?MODULE, create_file, [Node, SessionId, FilePath]})
    end, lists:seq(1, SubFilesNum)),
    DirGuid;
create_nested_directory_tree(Node, SessionId, [SubDirsNum | Rest], Root) ->
    DirGuid = create_dir(Node, SessionId, Root),
    lists:foreach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = filename:join([Root, NBin]),
        ok = worker_pool:cast(?VERIFY_POOL,
            {?MODULE, create_nested_directory_tree, [Node, SessionId, Rest, DirPath]}
        )
    end, lists:seq(1, SubDirsNum)),
    DirGuid.

create_file(Node, SessionId, FilePath) ->
    FileGuid = create_test_file(Node, SessionId, FilePath, ?TEST_DATA),
    ?CREATE_FILE_COUNTER ! {created, {FileGuid, FilePath}}.

create_dir(Node, SessionId, DirPath) ->
    {ok, DirGuid} = lfm_proxy:mkdir(Node, SessionId, DirPath),
    ?CREATE_FILE_COUNTER ! {created, {DirGuid, DirPath}},
    DirGuid.

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
        {created, FileGuidAndPath} ->
            create_file_counter(N + 1, FilesToCreate, ParentPid, [FileGuidAndPath | Files])
    end.

verify_file(Worker, SessionId, FileGuid) ->
    {ok, #file_attr{type = Type}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(Worker, SessionId, {guid, FileGuid}), 10 * ?ATTEMPTS),
    case Type of
        ?REGULAR_FILE_TYPE ->
            ?assertMatch({ok, #file_attr{size = ?TEST_DATA_SIZE}},
                lfm_proxy:stat(Worker, SessionId, {guid, FileGuid}), ?ATTEMPTS);
        _ ->
            ok
    end,
    ?SYNC_FILE_COUNTER ! verified.

verify_distribution(Worker, ExpectedDistribution, Config, FileGuid, FilePath, SessionId) ->
    case lfm_proxy:stat(Worker, SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            ?SYNC_FILE_COUNTER ! verified;
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} ->
            ?assertDistribution(Worker, ExpectedDistribution, Config, FilePath),
            ?SYNC_FILE_COUNTER ! verified
    end.

list_ended_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ended_transfers, [SpaceId]),
    Transfers.

list_waiting_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_waiting_transfers, [SpaceId]),
    Transfers.

list_ongoing_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ongoing_transfers, [SpaceId]),
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
            Ongoing = list_ongoing_transfers(Worker, SpaceId),
            Past = list_ended_transfers(Worker, SpaceId),
            Scheduled = list_waiting_transfers(Worker, SpaceId),
            lists:foreach(fun(Tid) ->
                rpc:call(Worker, transfer, delete, [Tid])
            end, lists:umerge([Ongoing, Past, Scheduled]))
        end, SpaceIds)
    end, Workers).

ensure_transfers_removed(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            ?assertMatch([], list_ended_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_ongoing_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_waiting_transfers(Worker, SpaceId), ?ATTEMPTS)
        end, SpaceIds)
    end, Workers).

get_finish_time(Worker, Tid, Config) ->
    Status = get_status(Worker, Tid, Config),
    maps:get(<<"finishTime">>, Status).

get_status(Worker, Tid, Config) ->
    {ok, 200, _, TransferStatus} = rest_test_utils:request(Worker, <<"transfers/", Tid/binary>>,
        get, ?USER_1_AUTH_HEADERS(Config), []),
    json_utils:decode(TransferStatus).

schedule_file_replication(Worker, ProviderId, File, Config) ->
    {ok, 201, _, Body} = ?assertMatch({ok, 201, _, _}, rest_test_utils:request(Worker,
        <<"replicas/", File/binary, "?provider_id=", ProviderId/binary>>,
        post, ?USER_1_AUTH_HEADERS(Config), []
    ), ?ATTEMPTS),
    DecodedBody = json_utils:decode(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

schedule_file_replication_by_id(Worker, ProviderId, FileId, Config) ->
    {ok, 201, _, Body} = ?assertMatch({ok, 201, _, _}, rest_test_utils:request(Worker,
        <<"replicas-id/", FileId/binary, "?provider_id=", ProviderId/binary>>,
        post, ?USER_1_AUTH_HEADERS(Config), []
    ), ?ATTEMPTS),
    DecodedBody = json_utils:decode(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

create_test_file_by_size(Worker, SessionId, File, Size) ->
    ChunkSize = 100 * 1024 * 1024,
    Chunks = Size div (ChunkSize + 1) + 1,
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    Data = crypto:strong_rand_bytes(ChunkSize),
    lists:foldl(fun(_ChunkNum, WrittenSum) ->
        {ok, Written} = lfm_proxy:write(Worker, Handle, WrittenSum, binary_part(Data, 0, min(Size - WrittenSum, ChunkSize))),
        Written + WrittenSum
    end, 0, lists:seq(0, Chunks - 1)),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

get_ongoing_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ongoing := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    Transfers.

get_ended_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ended := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    Transfers.

list_all_transfers_via_rest(Config, Worker, Space, State, ChunkSize) ->
    case list_all_transfers_via_rest(Config, Worker, Space, State, ChunkSize, <<"null">>, []) of
        Result when is_list(Result) ->
            % Make sure there are no duplicates
            ?assertEqual(lists:sort(Result), lists:usort(Result)),
            Result;
        Error ->
            Error
    end.

list_all_transfers_via_rest(Config, Worker, Space, State, ChunkSize, StartId, Acc) ->
    case list_transfers_via_rest(Config, Worker, Space, State, StartId, ChunkSize) of
        {ok, {Transfers, NextPageToken}} ->
            case NextPageToken of
                <<"null">> ->
                    Acc ++ Transfers;
                _ ->
                    list_all_transfers_via_rest(Config, Worker, Space, State, ChunkSize, NextPageToken, Acc ++ Transfers)
            end;
        Error ->
            Error
    end.

list_transfers_via_rest(Config, Worker, Space, State, StartId, LimitOrUndef) ->
    TokenParam = case StartId of
        <<"null">> -> <<"">>;
        Token -> <<"&page_token=", Token/binary>>
    end,
    LimitParam = case LimitOrUndef of
        undefined -> <<"">>;
        Int when is_integer(Int) ->
            <<"&limit=", (integer_to_binary(Int))/binary>>
    end,
    Url = str_utils:format_bin("spaces/~s/transfers?state=~s~s~s", [
        Space, State, TokenParam, LimitParam
    ]),
    case rest_test_utils:request(Worker, Url, get, ?USER_1_AUTH_HEADERS(Config), <<>>) of
        {ok, 200, _, Body} ->
            ParsedBody = json_utils:decode(Body),
            Transfers = maps:get(<<"transfers">>, ParsedBody),
            NextPageToken = maps:get(<<"nextPageToken">>, ParsedBody, <<"null">>),
            {ok, {Transfers, NextPageToken}};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

mock_get_share_on_other_node(OtherProviderNode, SupportingProviderNode, SessId, ShareId) ->
    case OtherProviderNode =/= SupportingProviderNode of
        true ->
            Res = rpc:call(SupportingProviderNode, share_logic, get, [SessId, ShareId]),
            test_utils:mock_expect(OtherProviderNode, share_logic, get,
                fun (_SessId, ShareId2) when ShareId2 == ShareId ->
                    Res
                end);
        false ->
            ok
    end.

get_op_nodes(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),
    Provider1Id = initializer:domain_to_provider_id(?GET_DOMAIN(Worker1)),
    {ok, Provider} = rpc:call(Worker1, provider_logic, get, [Provider1Id]),
    case rpc:call(Worker1, provider_logic, supports_space, [Provider]) of
        true ->
            {Worker1, Worker2};
        false ->
            {Worker2, Worker1}
    end.
