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
    ?config({session_id, {?DEFAULT_USER, ?GET_DOMAIN(Node)}}, Config)
).


-define(ATTEMPTS, 60).
-define(DEFAULT_TIMETRAP, timer:minutes(2)).

-define(TEST_TIMEOUT(Function), {test_timeout, Function}).

-define(FILE_PREFIX, <<"file_">>).
-define(DIR_PREFIX, <<"dir_">>).

-record(setup, {
    root_directory :: binary(),
    files_structure = [] :: [{non_neg_integer(), non_neg_integer()}],
    size = ?DEFAULT_SIZE :: non_neg_integer(),
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
    type = lfm :: lfm | rest,
    file_key_type = guid :: guid | path,
    schedule_node :: node(),
    target_nodes :: [node()],
    function :: function
}).

-record(expected, {
    distribution :: [#{}],
    minHist :: #{},
    hrHist :: #{},
    dyHist :: #{},
    mthHist :: #{},
    expected_transfer :: #{},
    assertion_nodes :: [node()],
    ended_transfers,
    attempts = ?ATTEMPTS,
    timeout = ?DEFAULT_TIMETRAP :: non_neg_integer()
}).

-record(transfer_test_spec, {
    setup :: undefined | #setup{},
    expected :: undefined | #expected{},
    scenario :: undefined | #scenario{}
}).

-record(expected_transfer, {
    assert_histograms = false :: boolean(),
    assert_counters = false :: boolean(),
    histograms_from :: binary(),
    user_id = ?DEFAULT_USER :: od_user:id(),
    enqueued = false :: boolean(),
    status = skipped :: transfer:status(),
    invalidation_status = skipped ::  transfer:status(),
    scheduling_provider_id :: od_provider:id(),
%%    source_provider_id :: undefined | od_provider:id(),
    invalidate_source_replica = false :: boolean(),
    files_to_process :: non_neg_integer(),
    files_processed :: non_neg_integer(),
    failed_files :: non_neg_integer(),
    files_transferred :: non_neg_integer(),
    bytes_transferred :: non_neg_integer(),
    files_invalidated :: non_neg_integer()
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

-define(HIST_MAP(DomainsAndBytes, Length),
    maps:fold(fun(Domain,  TransferredBytes, AccIn) ->
        case TransferredBytes > 0 of
            true ->
                AccIn#{Domain => ?HIST(TransferredBytes, Length)};
            false ->
                AccIn
        end
    end, #{}, DomainsAndBytes)
).

-define(MIN_HIST(DomainsAndBytes), ?HIST_MAP(DomainsAndBytes, ?MIN_HIST_LENGTH)).
-define(HOUR_HIST(DomainsAndBytes), ?HIST_MAP(DomainsAndBytes, ?HOUR_HIST_LENGTH)).
-define(DAY_HIST(DomainsAndBytes), ?HIST_MAP(DomainsAndBytes, ?DAY_HIST_LENGTH)).
-define(MONTH_HIST(DomainsAndBytes), ?HIST_MAP(DomainsAndBytes, ?MONTH_HIST_LENGTH)).

% config keys
-define(ROOT_DIR_KEY, root_dir_guid_and_path).
-define(FILES_KEY, files_guids_and_paths).
-define(DIRS_KEY, dirs_guids_and_paths).
-define(TRANSFERS_KEY, transfer_ids).
-define(OLD_TRANSFERS_KEY, old_transfer_ids).
-define(SPACE_ID_KEY, space_id).