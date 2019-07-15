%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Definitions of records and macros used in transfers tests.
%%% @end
%%%-------------------------------------------------------------------

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-define(DEFAULT_USER, <<"user1">>).
-define(DEFAULT_MODE, 8#664).
-define(DEFAULT_CONTENT, <<"test_data">>).
-define(DEFAULT_SIZE, byte_size(?DEFAULT_CONTENT)).
-define(DEFAULT_SESSION(Node, Config),
    ?USER_SESSION(Node, ?DEFAULT_USER, Config)
).

-define(USER_SESSION(Node, User, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config)
).

-define(ATTEMPTS, 60).
-define(DEFAULT_TIMETRAP, timer:minutes(2)).

-define(TEST_TIMEOUT(Function), {test_timeout, Function}).

-define(FILE_PREFIX, <<"file_">>).
-define(DIR_PREFIX, <<"dir_">>).

-record(setup, {
    user = ?DEFAULT_USER,
    root_directory :: binary(),
    replicate_to_nodes = [],
    files_structure = [] :: [{non_neg_integer(), non_neg_integer()}],
    size = ?DEFAULT_SIZE :: non_neg_integer(),
    truncate = false :: boolean(), % if true truncate will be used instead of creating file content
    mode = ?DEFAULT_MODE :: non_neg_integer(),
    file_prefix = ?FILE_PREFIX :: binary(),
    dir_prefix = ?DIR_PREFIX :: binary(),
    setup_node :: node(),
    assertion_nodes :: [node()],
    distribution :: [#{}],
    attempts = ?ATTEMPTS,
    timeout = ?DEFAULT_TIMETRAP :: non_neg_integer()
}).

-record(scenario, {
    user = ?DEFAULT_USER,
    type = lfm :: lfm | rest,
    file_key_type = guid :: guid | path,
    schedule_node :: node(),
    replicating_nodes :: [node()],
    evicting_nodes :: [node()],
    function :: function(),
    index_name :: binary(),
    query_view_params :: list(),
    space_id :: od_space:id()
}).

-record(expected, {
    user = ?DEFAULT_USER,
    distribution :: [#{}],
    % distribution will be checked only for files from assert_files list
    % if it's undefined, assertion will be performed for all files
    assert_distribution_for_files = undefined :: undefined | list(),
    minHist :: #{},
    hrHist :: #{},
    dyHist :: #{},
    mthHist :: #{},
    expected_transfer :: #{},
    assertion_nodes :: [node()],
    assert_transferred_file_model = true :: boolean(),
    attempts = ?ATTEMPTS,
    timeout = ?DEFAULT_TIMETRAP :: non_neg_integer()
}).

-record(transfer_test_spec, {
    setup :: undefined | #setup{},
    expected :: undefined | #expected{},
    scenario :: undefined | #scenario{}
}).

-define(FILE_COUNTER, file_counter).
-define(WORKER_POOL, worker_pool).
-define(WORKER_POOL_SIZE, 8).

-define(MISSING_PROVIDER_NODE, missing_provider).
-define(MISSING_PROVIDER_ID, <<"missing_provider_id">>).

-define(HIST(Value, Length), begin
    __Hist = histogram:new(Length),
    histogram:increment(__Hist, Value)
end).

-define(HIST_ASSERT(DomainsAndBytes, Length),
    fun(HistMap) ->
        maps:fold(fun(Domain,  TransferredBytes, AccIn) ->
            case TransferredBytes > 0 of
                true ->
                    Hist = maps:get(Domain, HistMap),
                    AccIn and (lists:sum(Hist) =:= TransferredBytes) and (length(Hist) =:= Length);
                false ->
                    AccIn
            end
        end, true, DomainsAndBytes)
    end
).

-define(MIN_HIST(DomainsAndBytes), ?HIST_ASSERT(DomainsAndBytes, ?MIN_HIST_LENGTH)).
-define(HOUR_HIST(DomainsAndBytes), ?HIST_ASSERT(DomainsAndBytes, ?HOUR_HIST_LENGTH)).
-define(DAY_HIST(DomainsAndBytes), ?HIST_ASSERT(DomainsAndBytes, ?DAY_HIST_LENGTH)).
-define(MONTH_HIST(DomainsAndBytes), ?HIST_ASSERT(DomainsAndBytes, ?MONTH_HIST_LENGTH)).

% config keys
-define(ROOT_DIR_KEY, root_dir_guid_and_path).
-define(FILES_KEY, files_guids_and_paths).
-define(DIRS_KEY, dirs_guids_and_paths).
-define(TRANSFERS_KEY, transfer_ids).
-define(OLD_TRANSFERS_KEY, old_transfer_ids).
-define(SPACE_ID_KEY, space_id).

-define(assertIndexVisible(Worker, SpaceId, IndexName),
    ?assertIndexVisible(Worker, SpaceId, IndexName, ?ATTEMPTS)).

-define(assertIndexVisible(Worker, SpaceId, IndexName, Attempts),
    ?assertMatch(true, begin
        ListResult = rpc:call(Worker, index, list, [SpaceId]),
        GetResult = rpc:call(Worker, index, get, [IndexName, SpaceId]),
        case {ListResult, GetResult} of
            {{ok, Indices}, {ok, __Doc}} -> lists:member(IndexName, Indices);
            Other -> Other
        end
    end, Attempts)).

-define(assertIndexQuery(ExpectedValues, Worker, SpaceId, ViewName, Options),
    ?assertIndexQuery(ExpectedValues, Worker, SpaceId, ViewName, Options, ?ATTEMPTS)).

-define(assertIndexQuery(ExpectedValues, Worker, SpaceId, ViewName, Options, Attempts),
    ?assertEqual(lists:sort(ExpectedValues), begin
        try
            {ok, {Rows}} = rpc:call(Worker, index, query, [SpaceId, ViewName, Options]),
            lists:sort(lists:flatmap(fun(Row) ->
                {<<"value">>, Value} = lists:keyfind(<<"value">>, 1, Row),
                lists:flatten([Value])
            end, Rows))
        catch
            _:_ ->
                error
        end
    end, Attempts)
).

-define(assertVersion(ExpectedVersion, Worker, FileGuid, ProviderId, Attempts),
    ?assertEqual(ExpectedVersion, try
        __FileUuid = file_id:guid_to_uuid(FileGuid),
        __LocId = file_location:id(__FileUuid, ProviderId),
        {ok, __LocDoc} = rpc:call(Worker, fslogic_location_cache, get_location, [__LocId, __FileUuid]),
        version_vector:get_version(__LocId, ProviderId, file_location:get_version_vector(__LocDoc))
    catch
        _:_ -> error
    end, Attempts)
).