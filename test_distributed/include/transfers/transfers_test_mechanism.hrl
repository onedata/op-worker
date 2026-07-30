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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

-define(DEFAULT_USER, <<"user1">>).
-define(DEFAULT_CONTENT, <<"test_data">>).
-define(DEFAULT_SIZE, byte_size(?DEFAULT_CONTENT)).
-define(DEFAULT_SESSION(Node, Config),
    ?USER_SESSION(Node, ?DEFAULT_USER, Config)
).

-define(USER_SESSION(Node, User, Config),
begin
    case ?config(use_initializer, Config, true) of
        true ->
            ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config);
        false ->
            ProviderId = rpc:call(Node, oneprovider, get_id, []),
            oct_background:get_user_session_id(User, ProviderId)
    end
end
).

-define(ATTEMPTS, 60).
-define(DEFAULT_TIMETRAP, timer:minutes(2)).

-define(FILE_PREFIX, <<"file_">>).
-define(DIR_PREFIX, <<"dir_">>).

-record(setup, {
    user = ?DEFAULT_USER,
    root_directory :: binary(),
    replicate_to_nodes = [],
    files_structure = [] :: [{non_neg_integer(), non_neg_integer()}],
    size = ?DEFAULT_SIZE :: non_neg_integer(),
    truncate = false :: boolean(), % if true truncate will be used instead of creating file content
    mode = ?DEFAULT_FILE_PERMS :: non_neg_integer(),
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
    file_key_type = guid :: guid,
    schedule_node :: node(),
    replicating_nodes :: [node()],
    evicting_nodes :: [node()],
    function :: function(),
    view_name :: binary(),
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
    expected :: undefined | #expected{} | [#expected{}],
    scenario :: undefined | #scenario{}
}).

-define(FILE_COUNTER, file_counter).
-define(WORKER_POOL, worker_pool).
-define(WORKER_POOL_SIZE, 8).

-define(HIST(Value, Length), begin
    __Hist = histogram:new(Length),
    histogram:increment(__Hist, Value)
end).

-define(HIST_ASSERT(DomainsAndBytes, Length),
    fun(HistMap) ->
        maps:fold(fun(Domain, TransferredBytes, AccIn) ->
            case is_integer(TransferredBytes) andalso TransferredBytes > 0 of
                true ->
                    Hist = maps:get(Domain, HistMap),
                    AccIn and (lists:sum(Hist) =:= TransferredBytes) and (length(Hist) =:= Length);
                false when is_function(TransferredBytes) ->
                    Hist = maps:get(Domain, HistMap),
                    AccIn and TransferredBytes(lists:sum(Hist));
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

