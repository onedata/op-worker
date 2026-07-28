%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains tests of file_popularity_view. The view is queried
%%% using the view_traverse mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_test_SUITE).
-author("Jakub Kudzia").

-behaviour(view_traverse).

-include("space_setup_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/file_popularity/file_popularity_view.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0, 
    init_per_suite/1, end_per_suite/1, 
    init_per_testcase/2, end_per_testcase/2
]).
-export([
    query_should_return_error_when_file_popularity_is_disabled/1,
    query_should_return_empty_list_when_file_popularity_is_enabled/1,
    query_should_return_empty_list_when_file_has_not_been_opened/1,
    query_should_return_file_when_file_has_been_opened/1,
    query_should_return_files_sorted_by_increasing_avg_open_count_per_day/1,
    query_should_return_files_sorted_by_increasing_last_open_timestamp/1,
    file_should_have_correct_popularity_value/1,
    file_should_have_correct_popularity_value2/1,
    file_should_have_correct_popularity_value3/1,
    avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default/1,
    avg_open_count_per_day_parameter_should_be_bounded_by_custom_value/1,
    changing_max_avg_open_count_per_day_limit_should_reindex_the_file/1,
    changing_last_open_weight_should_reindex_the_file/1,
    changing_avg_open_count_weight_should_reindex_the_file/1,
    time_warp_test/1
]).

%% view_traverse callbacks
-export([process_row/3, task_finished/1]).

%% view_processing_module API
-export([init/0, stop/0, run/2]).

%% exported for RPC
-export([start_collector/1, collector_loop/1]).

all() -> [
    query_should_return_error_when_file_popularity_is_disabled,
    query_should_return_empty_list_when_file_popularity_is_enabled,
    query_should_return_empty_list_when_file_has_not_been_opened,
    query_should_return_file_when_file_has_been_opened,
    file_should_have_correct_popularity_value,
    file_should_have_correct_popularity_value2,
    file_should_have_correct_popularity_value3,
    avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default,
    avg_open_count_per_day_parameter_should_be_bounded_by_custom_value,
    changing_max_avg_open_count_per_day_limit_should_reindex_the_file,
    changing_last_open_weight_should_reindex_the_file,
    changing_avg_open_count_weight_should_reindex_the_file,
    query_should_return_files_sorted_by_increasing_avg_open_count_per_day,
    query_should_return_files_sorted_by_increasing_last_open_timestamp,
    time_warp_test
].

-define(SPACE_ID(Config), ?config(space_id, Config)).

-define(VIEW_PROCESSING_MODULE, ?MODULE).

-define(ATTEMPTS, 10).

% name for process responsible for collecting traverse results
-define(COLLECTOR, collector).

% messages used to communicate with ?COLLECTOR process
-define(FINISHED, finished).
-define(COLLECTED_RESULTS(Rows), {collected_results, Rows}).
-define(ROW(FileId, Popularity, RowNum), {row, FileId, Popularity, RowNum}).

% testcases for which the cluster clock is frozen so that popularity timestamps
% change only via the explicit warps the case makes
-define(FROZEN_TIME_CASES, [
    avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default,
    query_should_return_files_sorted_by_increasing_avg_open_count_per_day,
    query_should_return_files_sorted_by_increasing_last_open_timestamp,
    file_should_have_correct_popularity_value,
    file_should_have_correct_popularity_value2,
    file_should_have_correct_popularity_value3,
    time_warp_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

query_should_return_error_when_file_popularity_is_disabled(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SpaceId = ?SPACE_ID(Config),
    ?assertMatch({error, not_found}, query(Node, SpaceId, #{})).

query_should_return_empty_list_when_file_popularity_is_enabled(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),
    ?assertMatch([], query(Node, SpaceId, #{})).

query_should_return_empty_list_when_file_has_not_been_opened(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),
    {ok, _} = create_file(Node, SessId, SpaceId, <<"file">>),
    ?assertMatch([], query(Node, SpaceId, #{})).

query_should_return_file_when_file_has_been_opened(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),
    {ok, G} = create_file(Node, SessId, SpaceId, <<"file">>),
    open_and_close_file(Node, SessId, G),
    {ok, FileId} = file_id:guid_to_objectid(G),
    ?assertMatch([{FileId, _}], query(Node, SpaceId, #{}), ?ATTEMPTS).

file_should_have_correct_popularity_value(Config) ->
    file_should_have_correct_popularity_value_base(Config, 1.123, 0).

file_should_have_correct_popularity_value2(Config) ->
    file_should_have_correct_popularity_value_base(Config, 0, 9.987).

file_should_have_correct_popularity_value3(Config) ->
    file_should_have_correct_popularity_value_base(Config, 1.123, 9.987).

avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),
    % 2 files should have the same probability value, despite having different avg_open_count
    DefaultMaxOpenCount = 100,
    OpenCountPerMonth1 = DefaultMaxOpenCount * 30, % avg_open_count = 100
    OpenCountPerMonth2 = (DefaultMaxOpenCount + 1) * 30, % avg_open_count = 101

    % all files will have the same timestamp as the time is frozen
    {ok, G1} = create_file(Node, SessId, SpaceId, <<"file1">>),
    {ok, G2} = create_file(Node, SessId, SpaceId, <<"file2">>),

    open_and_close_file(Node, SessId, G1, OpenCountPerMonth1),
    open_and_close_file(Node, SessId, G2, OpenCountPerMonth2),

    ?assertMatch([{_, Popularity}, {_, Popularity}], query(Node, SpaceId, #{}), ?ATTEMPTS).

avg_open_count_per_day_parameter_should_be_bounded_by_custom_value(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    % 2 files should have the same probability value, despite having different avg_open_count
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,

    OpenCountPerMonth1 = MaxOpenCount * 30, % avg_open_count = 100
    OpenCountPerMonth2 = (MaxOpenCount + 1) * 30, % avg_open_count = 200

    ok = configure_file_popularity(Node, SpaceId, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),

    {ok, G1} = create_file(Node, SessId, SpaceId, <<"file1">>),
    {ok, G2} = create_file(Node, SessId, SpaceId, <<"file2">>),

    open_and_close_file(Node, SessId, G1, OpenCountPerMonth1),
    open_and_close_file(Node, SessId, G2, OpenCountPerMonth2),

    ?assertMatch([{_, Popularity}, {_, Popularity}], query(Node, SpaceId, #{}), ?ATTEMPTS).

changing_max_avg_open_count_per_day_limit_should_reindex_the_file(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,
    MaxOpenCount2 = 20,
    OpenCountPerMonth = 15 * 30, % avg_open_count = 15
    ok = configure_file_popularity(Node, SpaceId, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = create_file(Node, SessId, SpaceId, <<"file">>),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(Node, SessId, G, OpenCountPerMonth),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query(Node, SpaceId, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(Node, SpaceId, undefined, undefined, undefined, MaxOpenCount2),

    ?assertNotMatch([{_, Popularity}], query(Node, SpaceId, #{})),
    ?assertMatch([{FileId, _}], query(Node, SpaceId, #{})).

changing_last_open_weight_should_reindex_the_file(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    LastOpenWeight = 1.0,
    LastOpenWeight2 = 2.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,

    ok = configure_file_popularity(Node, SpaceId, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = create_file(Node, SessId, SpaceId, <<"file">>),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(Node, SessId, G, 1),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query(Node, SpaceId, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(Node, SpaceId, undefined, LastOpenWeight2, undefined, undefined),

    ?assertNotMatch([{_, Popularity}], query(Node, SpaceId, #{})),
    ?assertMatch([{FileId, _}], query(Node, SpaceId, #{})).

changing_avg_open_count_weight_should_reindex_the_file(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    AvgOpenCountPerDayWeight2 = 40.0,
    MaxOpenCount = 10,

    ok = configure_file_popularity(Node, SpaceId, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = create_file(Node, SessId, SpaceId, <<"file">>),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(Node, SessId, G, 1),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query(Node, SpaceId, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(Node, SpaceId, undefined, undefined, AvgOpenCountPerDayWeight2, undefined),

    ?assertNotMatch([{_, Popularity}], query(Node, SpaceId, #{})),
    ?assertMatch([{FileId, _}], query(Node, SpaceId, #{})).

query_should_return_files_sorted_by_increasing_avg_open_count_per_day(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),

    % all files will have the same timestamp as the time is frozen
    {ok, G1} = create_file(Node, SessId, SpaceId, <<"file1">>),
    open_and_close_file(Node, SessId, G1, 1),
    {ok, G2} = create_file(Node, SessId, SpaceId, <<"file2">>),
    open_and_close_file(Node, SessId, G2, 2),
    {ok, G3} = create_file(Node, SessId, SpaceId, <<"file3">>),
    open_and_close_file(Node, SessId, G3, 3),

    {ok, FileId1} = file_id:guid_to_objectid(G1),
    {ok, FileId2} = file_id:guid_to_objectid(G2),
    {ok, FileId3} = file_id:guid_to_objectid(G3),

    ?assertMatch([{FileId1, _}, {FileId2, _}, {FileId3, _}], query(Node, SpaceId, #{}), ?ATTEMPTS).

query_should_return_files_sorted_by_increasing_last_open_timestamp(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),

    % simulate each next file being opened one hour after the previous one
    {ok, G1} = create_file(Node, SessId, SpaceId, <<"file1">>),
    open_and_close_file(Node, SessId, G1), % 3 hours before query
    time_test_utils:simulate_seconds_passing(3600),

    {ok, G2} = create_file(Node, SessId, SpaceId, <<"file2">>),
    open_and_close_file(Node, SessId, G2), % 2 hours before query
    time_test_utils:simulate_seconds_passing(3600),

    {ok, G3} = create_file(Node, SessId, SpaceId, <<"file3">>),
    open_and_close_file(Node, SessId, G3), % 1 hour before query
    time_test_utils:simulate_seconds_passing(3600),

    {ok, FileId1} = file_id:guid_to_objectid(G1),
    {ok, FileId2} = file_id:guid_to_objectid(G2),
    {ok, FileId3} = file_id:guid_to_objectid(G3),

    ?assertMatch([{FileId1, _}, {FileId2, _}, {FileId3, _}], query(Node, SpaceId, #{}), ?ATTEMPTS).

time_warp_test(Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = enable_file_popularity(Node, SpaceId),

    % simulate each next file being opened one hour BEFORE the previous one (due to a time warp)
    {ok, G1} = create_file(Node, SessId, SpaceId, <<"file1">>),
    open_and_close_file(Node, SessId, G1), % 1 hour before query
    time_test_utils:simulate_seconds_passing(-3600),

    {ok, G2} = create_file(Node, SessId, SpaceId, <<"file2">>),
    open_and_close_file(Node, SessId, G2), % 2 hours before query
    time_test_utils:simulate_seconds_passing(-3600),

    {ok, G3} = create_file(Node, SessId, SpaceId, <<"file3">>),
    open_and_close_file(Node, SessId, G3), % 3 hours before query
    time_test_utils:simulate_seconds_passing(-3600),

    {ok, FileId1} = file_id:guid_to_objectid(G1),
    {ok, FileId2} = file_id:guid_to_objectid(G2),
    {ok, FileId3} = file_id:guid_to_objectid(G3),

    % Files should be sorted by increasing timestamp of last open.
    % Due to time warps, the file created as last one has the smallest timestamp, so the files
    % should be sorted in reverse order than they were created.
    ?assertMatch([{FileId3, _}, {FileId2, _}, {FileId1, _}], query(Node, SpaceId, #{}), ?ATTEMPTS).

%%%===================================================================
%%% Test base functions
%%%===================================================================

%% @private
-spec file_should_have_correct_popularity_value_base(
    test_config:config(), LastOpenW :: number(), AvgOpenW :: number()
) ->
    ok | no_return().
file_should_have_correct_popularity_value_base(Config, LastOpenW, AvgOpenW) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = ?SPACE_ID(Config),
    ok = configure_file_popularity(Node, SpaceId, true, LastOpenW, AvgOpenW),
    FrozenTimestamp = time_test_utils:get_frozen_time_hours(),
    AvgOpen = 1 / 30,
    Popularity = popularity(FrozenTimestamp, LastOpenW, AvgOpen, AvgOpenW),
    {ok, G} = create_file(Node, SessId, SpaceId, <<"file">>),
    open_and_close_file(Node, SessId, G),
    {ok, FileId} = file_id:guid_to_objectid(G),
    ?assertMatch([{FileId, Popularity}], query(Node, SpaceId, #{}), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run(all(), [krakow]),
            init_pool(oct_background:get_random_provider_node(krakow)),
            NewConfig
        end
    }).

end_per_suite(_Config) ->
    stop_pool(oct_background:get_random_provider_node(krakow)),
    oct_background:end_per_suite().

init_per_testcase(Case, Config) ->
    case lists:member(Case, ?FROZEN_TIME_CASES) of
        true -> time_test_utils:freeze_time(Config);
        false -> ok
    end,
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = user1,
        supports = [#support_spec{
            provider = krakow,
            storage_spec = create_posix_storage(),
            size = 1073741824
        }]
    }),
    lfm_proxy:init([{space_id, SpaceId} | Config]).

end_per_testcase(Case, Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    ensure_collector_stopped(Node),
    lfm_proxy:teardown(Config),
    case lists:member(Case, ?FROZEN_TIME_CASES) of
        true -> ok = time_test_utils:unfreeze_time(Config);
        false -> ok
    end.

%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

-spec process_row(json_utils:json_map(), view_traverse:info(), RowNum :: non_neg_integer()) -> ok.
process_row(Row, _Info, RowNum) ->
    Popularity = maps:get(<<"key">>, Row),
    FileId = maps:get(<<"value">>, Row),
    ?COLLECTOR ! ?ROW(FileId, Popularity, RowNum),
    ok.

-spec task_finished(view_traverse:task_id()) -> term().
task_finished(_TaskId) ->
    whereis(?COLLECTOR) ! ?FINISHED.

%%%===================================================================
%%% view processing module API function
%%%===================================================================

%% @doc Runs on the op_worker node.
-spec init() -> ok.
init() ->
    view_traverse:init(?VIEW_PROCESSING_MODULE).

%% @doc Runs on the op_worker node.
-spec stop() -> ok.
stop() ->
    view_traverse:stop(?VIEW_PROCESSING_MODULE).

%% @doc Runs on the op_worker node.
-spec run(od_space:id(), ViewTraverseOpts :: map()) -> {ok, view_traverse:task_id()} | {error, term()}.
run(SpaceId, Opts) ->
    view_traverse:run(?VIEW_PROCESSING_MODULE, ?FILE_POPULARITY_VIEW(SpaceId), Opts).

%%%===================================================================
%%% Functions exported for RPC
%%%===================================================================

%% @doc Runs on the op_worker node.
-spec start_collector(TestMasterPid :: pid()) -> true.
start_collector(TestMasterPid) ->
    register(?COLLECTOR, spawn(?MODULE, collector_loop, [TestMasterPid])).

%% @doc Runs on the op_worker node.
-spec collector_loop(TestMaster :: pid()) -> ok.
collector_loop(TestMaster) ->
    collector_loop(TestMaster, #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_posix_storage() -> storage:id().
create_posix_storage() ->
    space_setup_utils:create_storage(krakow, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>
    }).

%% @private
-spec create_file(node(), session:id(), od_space:id(), file_meta:name()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(Node, SessId, SpaceId, Name) ->
    lfm_proxy:create(Node, SessId, space_dir:guid(SpaceId), Name, ?DEFAULT_FILE_PERMS).

%% @private
-spec init_pool(node()) -> ok.
init_pool(Worker) ->
    rpc:call(Worker, ?MODULE, init, []).

%% @private
-spec stop_pool(node()) -> ok.
stop_pool(Worker) ->
    rpc:call(Worker, ?MODULE, stop, []).

%% @private
-spec run(node(), od_space:id(), ViewTraverseOpts :: map()) ->
    {ok, view_traverse:task_id()} | {error, term()}.
run(Worker, SpaceId, Opts) ->
    rpc:call(Worker, ?MODULE, run, [SpaceId, Opts]).

%% @private
-spec start_collector_remote(node()) -> true.
start_collector_remote(Worker) ->
    true = rpc:call(Worker, ?MODULE, start_collector, [self()]).

%% @private
%% @doc Runs on the op_worker node.
-spec collector_loop(TestMaster :: pid(), RowsMap :: #{non_neg_integer() => {binary(), number()}}) ->
    ok.
collector_loop(TestMaster, RowsMap) ->
    receive
        ?FINISHED ->
            TestMaster ! ?COLLECTED_RESULTS([{FileId, PopValue} || {_RN, {FileId, PopValue}} <- lists:sort(maps:to_list(RowsMap))]);
        ?ROW(FileId, Popularity, RowNum) ->
            collector_loop(TestMaster, RowsMap#{RowNum => {FileId, Popularity}})
    end.

%% @private
-spec query(node(), od_space:id(), ViewTraverseOpts :: map()) ->
    [{file_id:objectid(), Popularity :: number()}] | {error, term()}.
query(Worker, SpaceId, Opts) ->
    start_collector_remote(Worker),
    case run(Worker, SpaceId, Opts) of
        {ok, _} ->
            receive ?COLLECTED_RESULTS(Rows) -> Rows end;
        Error = {error, _} ->
            Error
    end.

%% @private
-spec ensure_collector_stopped(node()) -> ok | true.
ensure_collector_stopped(Worker) ->
    case whereis(Worker, ?COLLECTOR) of
        undefined -> ok;
        CollectorPid -> exit(CollectorPid, kill)
    end.

%% @private
-spec enable_file_popularity(node(), od_space:id()) -> ok | {error, term()}.
enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

%% @private
-spec configure_file_popularity(
    node(), od_space:id(), Enabled :: undefined | boolean(),
    LastOpenWeight :: undefined | number(), AvgOpenCountPerDayWeight :: undefined | number()
) ->
    ok | {error, term()}.
configure_file_popularity(Worker, SpaceId, Enabled, LastOpenWeight, AvgOpenCountPerDayWeight) ->
    rpc:call(Worker, file_popularity_api, configure, [SpaceId, filter_undefined_values(#{
        enabled => Enabled,
        last_open_hour_weight => LastOpenWeight,
        avg_open_count_per_day_weight => AvgOpenCountPerDayWeight
    })]).

%% @private
-spec configure_file_popularity(
    node(), od_space:id(), Enabled :: undefined | boolean(),
    LastOpenWeight :: undefined | number(), AvgOpenCountPerDayWeight :: undefined | number(),
    MaxAvgOpenCountPerDay :: undefined | number()
) ->
    ok | {error, term()}.
configure_file_popularity(Worker, SpaceId, Enabled, LastOpenWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay) ->
    rpc:call(Worker, file_popularity_api, configure, [SpaceId, filter_undefined_values(#{
        enabled => Enabled,
        last_open_hour_weight => LastOpenWeight,
        avg_open_count_per_day_weight => AvgOpenCountPerDayWeight,
        max_avg_open_count_per_day => MaxAvgOpenCountPerDay
    })]).

%% @private
-spec open_and_close_file(node(), session:id(), file_id:file_guid(), Times :: non_neg_integer()) -> ok.
open_and_close_file(Worker, SessId, Guid, Times) ->
    lists:foreach(fun(_) ->
        open_and_close_file(Worker, SessId, Guid)
    end, lists:seq(1, Times)).

%% @private
-spec open_and_close_file(node(), session:id(), file_id:file_guid()) -> ok.
open_and_close_file(Worker, SessId, Guid) ->
    {ok, H} = lfm_proxy:open(Worker, SessId, ?FILE_REF(Guid), read),
    ok = lfm_proxy:close(Worker, H).

%% @private
-spec popularity(
    LastOpen :: number(), LastOpenW :: number(), AvgOpen :: number(), AvgOpenW :: number()
) ->
    number().
popularity(LastOpen, LastOpenW, AvgOpen, AvgOpenW) ->
    LastOpen * LastOpenW + AvgOpen * AvgOpenW.

%% @private
-spec filter_undefined_values(map()) -> map().
filter_undefined_values(Map) ->
    maps:filter(fun
        (_, undefined) -> false;
        (_, _) -> true
    end, Map).

%% @private
-spec whereis(node(), Name :: atom()) -> undefined | pid().
whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).
