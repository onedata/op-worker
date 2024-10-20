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
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/http/headers.hrl").
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
    lookup_file_objectid/1,
    lookup_file_objectid_in_tmp_dir/1,
    lookup_file_objectid_duplicated_space_name/1,
    transfers_should_be_ordered_by_timestamps/1,
    metric_get/1,
    list_spaces/1,
    get_space/1,
    list_transfers/1,
    track_transferred_files/1
]).

%utils
-export([
    verify_file/3, create_file/3, create_dir/3,
    create_nested_directory_tree/4, sync_file_counter/3, create_file_counter/4
]).

all() ->
    ?ALL([
        lookup_file_objectid,
        lookup_file_objectid_in_tmp_dir,
        lookup_file_objectid_duplicated_space_name,
        transfers_should_be_ordered_by_timestamps,
        % @TODO VFS-8297 currently disabled
        % metric_get,
        list_spaces,
        get_space,
        list_transfers,
        track_transferred_files
    ]).

-define(ATTEMPTS, 100).

-define(SPACE1_ID, <<"space1">>).

-define(assertDistribution(Worker, ExpectedDistribution, Config, FileGuid),
    ?assertMatch(ExpectedDistribution, begin
        case rest_test_utils:request(
            Worker,
            <<
                "data/",
                (element(2, {ok, _} = file_id:guid_to_objectid(FileGuid)))/binary,
                "/distribution"
            >>,
            get,
            ?USER_1_AUTH_HEADERS(Config), []
        ) of
            {ok, 200, _, __Body} ->
                json_utils:decode(__Body);
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


lookup_file_objectid(Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FilePath = filename:join(["/", SpaceName, "get_file_objectid"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath),
    {ok, 200, _, Response} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
        WorkerP1, <<"lookup-file-id/", FilePath/binary>>, post,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), []
    )),
    #{<<"fileId">> := ObjectId} = json_utils:decode(Response),
    ?assertMatch({ok, ObjectId}, file_id:guid_to_objectid(FileGuid)).


lookup_file_objectid_in_tmp_dir(Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
    TmpDirPath = filename:join(["/", SpaceName, ".__onedata__tmp"]),
    {ok, 200, _, Response1} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
        WorkerP1, <<"lookup-file-id/", TmpDirPath/binary>>, post,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), []
    )),
    #{<<"fileId">> := TmpDirObjectId} = json_utils:decode(Response1),
    ?assertMatch({ok, TmpDirObjectId}, file_id:guid_to_objectid(TmpDirGuid)),

    FileName = <<"get_file_objectid">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, TmpDirGuid, FileName, undefined),
    FilePath = filename:join(["/", SpaceName, ".__onedata__tmp", FileName]),
    {ok, 200, _, Response2} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
        WorkerP1, <<"lookup-file-id/", FilePath/binary>>, post,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), []
    )),
    #{<<"fileId">> := ObjectId} = json_utils:decode(Response2),
    ?assertMatch({ok, ObjectId}, file_id:guid_to_objectid(FileGuid)).


lookup_file_objectid_duplicated_space_name(Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    Fun = fun(SpaceName) ->
        {ok, 200, _, Response} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
            WorkerP1, <<"lookup-file-id/", SpaceName/binary>>, post,
            ?USER_AUTH_HEADERS(Config, <<"user3">>, [{?HDR_CONTENT_TYPE, <<"application/json">>}]), []
        )),
        #{<<"fileId">> := ObjectId} = json_utils:decode(Response),
        ObjectId
    end,
    {ok, Space1ObjectId} = file_id:guid_to_objectid(fslogic_file_id:spaceid_to_space_dir_guid(<<"space_duplicated1">>)),
    {ok, Space2ObjectId} = file_id:guid_to_objectid(fslogic_file_id:spaceid_to_space_dir_guid(<<"space_duplicated2">>)),
    ?assertMatch(Space1ObjectId, Fun(<<"space_duplicated@space_duplicated1">>)),
    ?assertMatch(Space2ObjectId, Fun(<<"space_duplicated@space_duplicated2">>)).


transfers_should_be_ordered_by_timestamps(Config) ->
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    SpaceId = ?config(space_id, Config),
    DomainP1 = domain(WorkerP1),
    DomainP2 = domain(WorkerP2),
    StorageP1 = initializer:get_supporting_storage_id(WorkerP1, SpaceId),
    StorageP2 = initializer:get_supporting_storage_id(WorkerP2, SpaceId),

    FilePath = ?absPath(SpaceId, <<"file_sorted">>),
    Size = 1,
    FileGuid = create_test_file(WorkerP1, SessionId, FilePath, crypto:strong_rand_bytes(Size)),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    FilePath2 = ?absPath(SpaceId, <<"file_sorted2">>),
    Size2 = 3 * 1024 * 1024 * 1024,
    FileGuid2 = create_test_file_by_size(WorkerP1, SessionId, FilePath2, Size2),
    {ok, FileObjectId2} = file_id:guid_to_objectid(FileGuid2),

    % when
    ?assertMatch({ok, #file_attr{size = Size}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, FilePath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = Size2}}, lfm_proxy:stat(WorkerP2, SessionId2, {path, FilePath2}), ?ATTEMPTS),
    ExpectedDistributionFun = fun(S, StoragePath) -> #{
        <<"distributionPerProvider">> => #{
            DomainP1 => #{
                <<"distributionPerStorage">> => #{
                    StorageP1 =>
                        #{<<"blocks">> => [[0, S]], <<"physicalSize">> => S}
                },
                <<"locationsPerStorage">> => #{
                    StorageP1 => StoragePath
                },
                <<"virtualSize">> => S,
                <<"success">> => true
            },
            DomainP2 => #{
                <<"distributionPerStorage">> => #{
                    StorageP2 =>
                        #{<<"blocks">> => [],  <<"physicalSize">> => 0}
                },
                <<"locationsPerStorage">> => #{
                    StorageP2 => null
                },
                <<"virtualSize">> => S,
                <<"success">> => true
            }
        },
        <<"type">> => atom_to_binary(?REGULAR_FILE_TYPE)
    } end,
    ExpectedDistribution = ExpectedDistributionFun(Size, FilePath),
    ExpectedDistribution2 = ExpectedDistributionFun(Size2, FilePath2),
    ?assertDistribution(WorkerP2, ExpectedDistribution, Config, FileGuid),
    ?assertDistribution(WorkerP2, ExpectedDistribution2, Config, FileGuid2),

    Tid2 = schedule_file_replication(WorkerP2, DomainP2, FileGuid2, Config),
    ?assertEqual([Tid2], get_ongoing_transfers_for_file(WorkerP2, FileGuid2), ?ATTEMPTS),
    ?assertEqual([], get_ended_transfers_for_file(WorkerP2, FileGuid2), ?ATTEMPTS),
    % Wait 1 second to be sure that transfer Tid will have greater timestamp than transfer Tid2
    timer:sleep(timer:seconds(1)),
    Tid = schedule_file_replication(WorkerP2, DomainP2, FileGuid, Config),
    ?assertEqual([Tid], get_ongoing_transfers_for_file(WorkerP2, FileGuid), ?ATTEMPTS),
    ?assertEqual([], get_ended_transfers_for_file(WorkerP2, FileGuid), ?ATTEMPTS),

    % then
    ?assertTransferStatus(#{
        <<"replicationStatus">> := <<"completed">>,
        <<"replicatingProviderId">> := DomainP2,
        <<"filePath">> := FilePath,
        <<"evictionStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesFailed">> := 0,
        <<"filesReplicated">> := 1,
        <<"filesEvicted">> := 0,
        <<"bytesReplicated">> := Size,
        <<"mthHist">> := #{DomainP1 := [Size | _]}
    }, WorkerP1, Tid, Config),

    ?assertTransferStatus(#{
        <<"replicationStatus">> := <<"completed">>,
        <<"replicatingProviderId">> := DomainP2,
        <<"filePath">> := FilePath2,
        <<"evictionStatus">> := <<"skipped">>,
        <<"fileId">> := FileObjectId2,
        <<"callback">> := null,
        <<"filesToProcess">> := 1,
        <<"filesProcessed">> := 1,
        <<"filesReplicated">> := 1,
        <<"filesEvicted">> := 0,
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

metric_get(Config) ->
    Workers = [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
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

    ?assertMatch(ok, rpc:call(WorkerP1, monitoring_utils, create, [SpaceId, MonitoringId, global_clock:timestamp_seconds()])),
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


list_spaces(Config) ->
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    P1 = ?GET_DOMAIN_BIN(WorkerP1),
    P2 = ?GET_DOMAIN_BIN(WorkerP2),
    % when
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"spaces">>, get, ?USER_1_AUTH_HEADERS(Config), [])),

    % then
    ExpSpaces = lists:sort(lists:map(fun({SpaceId, Providers}) ->
        SpaceDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
        {ok, SpaceDirObjectId} = file_id:guid_to_objectid(SpaceDirGuid),
        {ok, TrashRootDirObjectId} = file_id:guid_to_objectid(file_id:pack_guid(?TRASH_DIR_UUID(SpaceId), SpaceId)),
        {ok, ArchivesRootDirObjectId} = file_id:guid_to_objectid(file_id:pack_guid(?ARCHIVES_ROOT_DIR_UUID(SpaceId), SpaceId)),

        #{
            <<"name">> => SpaceId,
            <<"spaceId">> => SpaceId,
            <<"fileId">> => SpaceDirObjectId,
            <<"dirId">> => SpaceDirObjectId,
            <<"trashDirId">> => TrashRootDirObjectId,
            <<"archivesDirId">> => ArchivesRootDirObjectId,
            <<"providers">> => lists:map(fun(P) ->
                #{
                    <<"providerId">> => P,
                    <<"providerName">> => P
                }
            end, Providers)
        }
    end, [{<<"space1">>, [P1]}, {<<"space2">>, [P1, P2]}, {<<"space3">>, [P1, P2]}, {<<"space4">>, [P1, P2]}])),

    ?assertEqual(ExpSpaces, lists:sort(json_utils:decode(Body))),

    % data access caveats can limit the pool of available spaces
    {_, _, _, Body2} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(WorkerP1, <<"spaces">>, get,
        [rest_test_utils:user_token_header(user1_token_with_access_limited_to_space3(Config))], [])),
    ?assertEqual(
        lists:filter(fun(SpaceData) -> maps:get(<<"spaceId">>, SpaceData) == <<"space3">> end, ExpSpaces),
        json_utils:decode(Body2)
    ).


get_space(Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),

    % the token used in both requests allows access only to space3
    AuthHeaders = [rest_test_utils:user_token_header(user1_token_with_access_limited_to_space3(Config))],
    SpaceId = <<"space3">>,

    {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(WorkerP1, <<"spaces/", SpaceId/binary>>, get, AuthHeaders, [])),

    SpaceDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, SpaceDirObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    % then
    DecodedBody = json_utils:decode(Body),
    ?assertMatch(
        #{
            <<"name">> := SpaceId,
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
            <<"spaceId">> := SpaceId,
            <<"fileId">> := SpaceDirObjectId
        },
        DecodedBody
    ),

    % space2 should not be available
    {_, _, _, Body2} = ?assertMatch({ok, 401, _, _}, rest_test_utils:request(
        WorkerP1, <<"spaces/space2">>, get, AuthHeaders, [])),
    #{<<"error">> := ErrorJson} = ?assertMatch(#{<<"error">> := _}, json_utils:decode(Body2)),
    ?assertMatch(?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(_)), errors:from_json(ErrorJson)).


list_transfers(Config) ->
    ct:timetrap({hours, 1}),
    Workers = [P1, P2] = ?config(op_worker_nodes, Config),
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
        schedule_file_replication(P1, DomainP2, FileGuid, Config)
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
        NewConfig1 = initializer:setup_storage(NewConfig),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(2)),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig1)),
        application:start(ssl),
        application:ensure_all_started(hackney),
        NewConfig1
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
    application:stop(hackney),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(transfers_should_be_ordered_by_timestamps, Config) ->
    init_per_testcase(all, [{space_id, <<"space2">>} | Config]);

init_per_testcase(metric_get, Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    start_monitoring_worker(WorkerP1),
    test_utils:mock_new(WorkerP1, rrd_utils),
    test_utils:mock_expect(WorkerP1, rrd_utils, export_rrd, fun(_, _, _) ->
        {ok, <<"{\"test\":\"rrd\"}">>}
    end),
    OldPrivs = rpc:call(WorkerP1, initializer, node_get_mocked_space_user_privileges, [<<"space2">>, <<"user1">>]),
    init_per_testcase(all, [{old_privs, OldPrivs} | Config]);

init_per_testcase(list_transfers, Config) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
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
    initializer:mock_auth_manager(Config2),
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config2).

end_per_testcase(metric_get = Case, Config) ->
    Workers = [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
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
        lfm_proxy:stat(Worker, SessionId, ?FILE_REF(FileGuid)), 10 * ?ATTEMPTS),
    case Type of
        ?REGULAR_FILE_TYPE ->
            ?assertMatch({ok, #file_attr{size = ?TEST_DATA_SIZE}},
                lfm_proxy:stat(Worker, SessionId, ?FILE_REF(FileGuid)), ?ATTEMPTS);
        _ ->
            ok
    end,
    ?SYNC_FILE_COUNTER ! verified.

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

schedule_file_replication(Worker, ProviderId, FileGuid, Config) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    {ok, 201, _, Body} = ?assertMatch({ok, 201, _, _}, rest_test_utils:request(
        Worker,
        <<"transfers">>,
        post,
        ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),
        json_utils:encode(#{
            <<"type">> => <<"replication">>,
            <<"replicatingProviderId">> => ProviderId,
            <<"dataSourceType">> => <<"file">>,
            <<"fileId">> => FileObjectId
        })
    ), ?ATTEMPTS),
    DecodedBody = json_utils:decode(Body),
    #{<<"transferId">> := Tid} = ?assertMatch(#{<<"transferId">> := _}, DecodedBody),
    Tid.

create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, ?FILE_REF(FileGuid), write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

create_test_file_by_size(Worker, SessionId, File, Size) ->
    ChunkSize = 100 * 1024 * 1024,
    Chunks = Size div (ChunkSize + 1) + 1,
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, ?FILE_REF(FileGuid), write),
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
                Null when Null =:= <<"null">> orelse Null =:= null ->
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
    Url = str_utils:format_bin("spaces/~ts/transfers?state=~ts~ts~ts", [
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


user1_token_with_access_limited_to_space3(Config) ->
    OriginalAccessTokenBin = ?config({access_token, <<"user1">>}, Config),
    LimitedAccessToken = tokens:confine(?check(tokens:deserialize(OriginalAccessTokenBin)), [
        #cv_data_readonly{},
        #cv_data_objectid{whitelist = [?RAND_OBJECTID(<<"space3">>), ?RAND_OBJECTID(<<"space2">>)]},
        #cv_data_path{whitelist = [?RAND_CANONICAL_PATH(<<"space1">>), ?RAND_CANONICAL_PATH(<<"space3">>), ?RAND_CANONICAL_PATH(<<"space4">>)]}
    ]),
    ?check(tokens:serialize(LimitedAccessToken)).