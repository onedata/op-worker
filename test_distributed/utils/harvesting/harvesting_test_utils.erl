%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common setup and teardown for harvesting test suites. Mocks
%%% space_logic:harvest_metadata/5 so that every batch submitted by a
%%% harvesting stream is sent back to the test process as a
%%% ?HARVEST_METADATA message (see harvesting/harvesting_test.hrl).
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_test_utils).
-author("Jakub Kudzia").

-include("harvesting/harvesting_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% init/teardown functions skeletons
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%% helper functions
-export([set_mock_harvest_metadata_failure/2, subtract_batches/2]).

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
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].

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
        SupervisorPid = rpc:call(W, erlang, whereis, [harvesting_stream_sup]),
        exit(SupervisorPid, normal)
    end, Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Helper functions
%%%===================================================================

mock_space_logic_harvest_metadata(Nodes) ->
    Self = self(),
    ok = test_utils:mock_expect(Nodes, space_logic, harvest_metadata,
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

set_mock_harvest_metadata_failure(Nodes, ShouldFail) ->
    test_utils:set_env(Nodes, op_worker, ?MOCK_HARVEST_METADATA_FAILURE, ShouldFail).

mock_space_quota_checks(Nodes) ->
    % mock space_quota to mock error logs due to some test environment issues
    ok = test_utils:mock_new(Nodes, space_quota),
    ok = test_utils:mock_expect(Nodes, space_quota, get_disabled_spaces, fun() ->
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
