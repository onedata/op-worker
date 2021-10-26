%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils for harvesting tests.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_test_utils).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% init/teardown functions skeletons
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% helper functions
-export([set_mock_harvest_metadata_failure/2, subtract_batches/2]).

-define(SPACE_ID1, <<"space_id1">>).
-define(SPACE_ID2, <<"space_id2">>).
-define(SPACE_ID3, <<"space_id3">>).
-define(SPACE_ID4, <<"space_id4">>).
-define(SPACE_ID5, <<"space_id5">>).

-define(SPACE_NAME1, <<"space1">>).
-define(SPACE_NAME2, <<"space2">>).
-define(SPACE_NAME3, <<"space3">>).
-define(SPACE_NAME4, <<"space4">>).
-define(SPACE_NAME5, <<"space5">>).

-define(SPACE_NAMES, #{
    ?SPACE_ID1 => ?SPACE_NAME1,
    ?SPACE_ID2 => ?SPACE_NAME2,
    ?SPACE_ID3 => ?SPACE_NAME3,
    ?SPACE_ID4 => ?SPACE_NAME4,
    ?SPACE_ID5 => ?SPACE_NAME5
}).

-define(SPACE_NAME(__SpaceId), maps:get(__SpaceId, ?SPACE_NAMES)).
-define(SPACE_IDS, maps:keys(?SPACE_NAMES)).

-define(PATH(FileName, SpaceId), filename:join(["/", ?SPACE_NAME(SpaceId), FileName])).

-define(HARVESTER1, <<"harvester1">>).
-define(HARVESTER2, <<"harvester2">>).
-define(HARVESTER3, <<"harvester3">>).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(XATTR_NAME, <<"xattr_name_", (?RAND_NAME)/binary>>).
-define(XATTR_VAL, <<"xattr_val_", (?RAND_NAME)/binary>>).

-define(DIR_NAME, <<"dir", (?RAND_NAME)/binary>>).
-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(ATTEMPTS, 30).

-define(PROVIDER_ID(Node), rpc:call(Node, oneprovider, get_id, [])).

%% Test config:
%% space_id1:
%%  * supported by: p1
%%  * harvesters: harvester1
%%  * indices: index1
%% space_id2:
%%  * supported by: p1
%%  * harvesters: harvester1, harvester2
%% space_id3:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id4:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id5:
%%  * supported by: p1, p2
%%  * harvesters: harvester3

-define(HARVEST_METADATA, harvest_metadata).
-define(HARVEST_METADATA(SpaceId, Destination, Batch, ExpProviderId),
    {?HARVEST_METADATA, SpaceId, Destination, Batch, ExpProviderId}).

%% NOTE!!!
%% This assert assumes that list ExpBatch is sorted in the order of increasing sequences
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun
            AssertFun(__SpaceId, __Destination, [], __Unexpected, __ProviderId, __Timeout) ->
                % all expected changes has been received
                % resend these entries that were unexpected
                self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                ok;
            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
                __TimeoutInMillis = timer:seconds(__Timeout),
                receive
                    ?HARVEST_METADATA(
                        __SpaceId,
                        __Destination,
                        __ReceivedBatch,
                        __ProviderId
                    ) ->
                        {__ExpBatchLeft, __NewUnexpected} = harvesting_test_utils:subtract_batches(__Batch, __ReceivedBatch),
                        AssertFun(__SpaceId, __Destination, __ExpBatchLeft, __Unexpected ++ __NewUnexpected,
                            __ProviderId, __Timeout)
                after
                    __TimeoutInMillis ->
                        __Args = [{module, ?MODULE},
                            {line, ?LINE},
                            {expected, {__SpaceId, __Destination, __Batch, __ProviderId, __Timeout}},
                            {value, timeout}],
                        ct:print("assertReceivedHarvestMetadata_failed: ~p~n", [__Args]),
                        erlang:error({assertReceivedHarvestMetadata_failed, __Args})
                end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
            Stopwatch = stopwatch:start(),
            __TimeoutInMillis = timer:seconds(__Timeout),
            receive
                __HM = ?HARVEST_METADATA(
                    __SpaceId,
                    __Destination,
                    __ReceivedBatch,
                    __ProviderId
                ) ->
                    ElapsedTime = stopwatch:read_seconds(Stopwatch),
                    {__Batch2, __NewUnexpected} = harvesting_test_utils:subtract_batches(__Batch, __ReceivedBatch),
                    case length(__Batch2) < length(__Batch) of
                        false ->
                            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected ++ __NewUnexpected,
                                __ProviderId, max(__Timeout - ElapsedTime, 0));
                        true ->
                            % __Batch2 is smaller than __Batch which means that one of changes, which was
                            % expected not to occur, actually occurred
                            __Args = [
                                {module, ?MODULE},
                                {line, ?LINE}
                            ],
                            ct:print("assertNotReceivedHarvestMetadata_failed: ~lp~n"
                                "Unexpectedly received: ~p~n", [__Args, __HM]),
                            erlang:error({assertNotReceivedHarvestMetadata_failed, __Args})
                    end
            after
                __TimeoutInMillis ->
                    % resend these entries that were unexpected
                    self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                    ok
            end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(INDEX(N), <<"index", (integer_to_binary(N))/binary>>).
-define(INDEX11, <<"index11">>).
-define(INDEX21, <<"index21">>).
-define(INDEX22, <<"index22">>).
-define(INDEX23, <<"index23">>).
-define(INDEX31, <<"index31">>).

-define(DUMMY_RDF, <<"dummy rdf">>).
-define(DUMMY_RDF(Content),
    <<(?DUMMY_RDF)/binary, "_", (str_utils:to_binary(Content))/binary>>).

-define(MOCK_HARVEST_METADATA_FAILURE, mock_harvest_metadata_failure).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        application:ensure_all_started(hackney),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    application:stop(hackney),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"), Config2),

    lists:foreach(fun(W) ->
        lists:foreach(fun(SpaceId) ->
            {ok, SpaceDoc} = rpc:call(W, space_logic, get, [?ROOT_SESS_ID, SpaceId]),
            {ok, Harvesters} = space_logic:get_harvesters(SpaceDoc),
            lists:foreach(fun(HarvesterId) ->
                {ok, HarvesterDoc} = rpc:call(W, harvester_logic, get, [HarvesterId]),
                % trigger od_harvester posthooks
                rpc:call(W, initializer, put_into_cache, [HarvesterDoc])
            end, Harvesters),
            % trigger od_space posthooks
            rpc:call(W, initializer, put_into_cache, [SpaceDoc])
        end, ?SPACE_IDS)
    end, Workers),
    set_mock_harvest_metadata_failure(Workers, false),
    mock_space_logic_harvest_metadata(Workers),
    mock_space_quota_checks(Workers),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, space_logic),
    lists:foreach(fun(W) ->
        SupervisorPid = whereis(W, harvesting_stream_sup),
        exit(SupervisorPid, normal)
    end, Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Helper functions
%%%===================================================================

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

mock_space_logic_harvest_metadata(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, space_logic, harvest_metadata,
        fun(SpaceId, Destination, Batch, _MaxStreamSeq, _MaxSeq) ->
            case op_worker:get_env(?MOCK_HARVEST_METADATA_FAILURE, false) of
                true ->
                    {error, test_error};
                false ->
                    Self ! ?HARVEST_METADATA(SpaceId, Destination, Batch, oneprovider:get_id()),
                    {ok, #{}}
            end
        end
    ).

set_mock_harvest_metadata_failure(Nodes, Boolean) ->
    test_utils:set_env(Nodes, op_worker, ?MOCK_HARVEST_METADATA_FAILURE, Boolean).

mock_space_quota_checks(Node) ->
    % mock space_quota to mock error logs due to some test environment issues
    ok = test_utils:mock_new(Node, space_quota),
    ok = test_utils:mock_expect(Node, space_quota, get_disabled_spaces, fun() ->
        {ok, []} end).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).


subtract_batches(ExpectedBatch, ReceivedBatch) ->
    % Entries in ExpectedBatch and ReceivedBatch may not
    % be in the same order.
    % This may happen due to race when saving changes to DB.
    % Changes may be persisted in different order than they were performed on memory.
    subtract_sorted_batches(sort_batch(ExpectedBatch), sort_batch(ReceivedBatch)).

sort_batch(Batch) ->
    lists:sort(fun
        (#{<<"fileId">> := FileId1}, #{<<"fileId">> := FileId2}) -> FileId1 =< FileId2
    end, Batch).

%% @doc this function returns a 2-element tuple in which:
%%  * the 1st element is a list of expected batch entries that have not been received yet
%%  * the 2nd element is a list of unexpected batch entries that have been received
subtract_sorted_batches(ExpectedBatchSorted, ReceivedBatchSorted) ->
    subtract_sorted_batches(ExpectedBatchSorted, ReceivedBatchSorted, []).


subtract_sorted_batches(B1, [], UnexpectedReversed) ->
    {B1, lists:reverse(UnexpectedReversed)};
subtract_sorted_batches([], B2, RestReversed) ->
    {[], lists:reverse(RestReversed) ++ B2};
subtract_sorted_batches(
    B1 = [ExpectedEntry = #{<<"operation">> := <<"submit">>} | T],
    [ReceivedEntry = #{<<"operation">> := <<"submit">>} | T2],
    UnexpectedReversed
) ->
    case are_submit_entries_equal(ExpectedEntry, ReceivedEntry) of
        true ->
            subtract_sorted_batches(T, T2, UnexpectedReversed);
        false ->
            subtract_sorted_batches(B1, T2, [ReceivedEntry | UnexpectedReversed])
    end;
subtract_sorted_batches([#{
    <<"fileId">> := FileId,
    <<"operation">> := <<"delete">>
} | T], [#{
    <<"fileId">> := FileId,
    <<"operation">> := <<"delete">>
} | T2], UnexpectedReversed) ->
    subtract_sorted_batches(T, T2, UnexpectedReversed);
subtract_sorted_batches(B1, [H | T2], UnexpectedReversed) ->
    subtract_sorted_batches(B1, T2, [H | UnexpectedReversed]).


are_submit_entries_equal(ExpectedEntry = #{
    <<"fileId">> := FileId,
    <<"operation">> := <<"submit">>,
    <<"payload">> := Payload,
    <<"spaceId">> := SpaceId,
    <<"fileName">> := FileName,
    <<"fileType">> := FileType
}, ReceivedEntry) ->
    % mandatory fields
    (FileId == maps:get(<<"fileId">>, ReceivedEntry)) andalso
    (Payload == maps:get(<<"payload">>, ReceivedEntry)) andalso
    (SpaceId == maps:get(<<"spaceId">>, ReceivedEntry)) andalso
    (FileName == maps:get(<<"fileName">>, ReceivedEntry)) andalso
    (FileType == maps:get(<<"fileType">>, ReceivedEntry)) andalso
    % optional fields
    (maps:get(<<"datasetId">>, ExpectedEntry, undefined) == maps:get(<<"datasetId">>, ReceivedEntry, undefined)).
