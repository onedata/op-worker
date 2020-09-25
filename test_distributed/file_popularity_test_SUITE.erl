%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of file_popularity_view.
%%% The view is queried using view_traverse mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_test_SUITE).
-author("Jakub Kudzia").

-behaviour(view_traverse).

-include("global_definitions.hrl").
-include("modules/fslogic/file_popularity_view.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([
    query_should_return_error_when_file_popularity_is_disabled/1,
    query_should_return_empty_list_when_file_popularity_is_enabled/1,
    query_should_return_empty_list_when_file_has_not_been_opened/1,
    query_should_return_file_when_file_has_been_opened/1,
    query_should_return_files_sorted_by_increasing_last_open_timestamp/1,
    query_should_return_files_sorted_by_increasing_avg_open_count_per_day/1,
    file_should_have_correct_popularity_value/1,
    file_should_have_correct_popularity_value2/1,
    file_should_have_correct_popularity_value3/1,
    avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default/1,
    avg_open_count_per_day_parameter_should_be_bounded_by_custom_value/1,
    changing_max_avg_open_count_per_day_limit_should_reindex_the_file/1,
    changing_last_open_weight_should_reindex_the_file/1,
    changing_avg_open_count_weight_should_reindex_the_file/1
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
    query_should_return_files_sorted_by_increasing_last_open_timestamp
].

-define(SPACE_ID, <<"space1">>).
-define(VIEW_PROCESSING_MODULE, ?MODULE).

-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

-define(USER, <<"user1">>).
-define(SESSION(Worker, Config), ?SESSION(?USER, Worker, Config)).
-define(SESSION(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

-define(ATTEMPTS, 10).

% name for process responsible for collecting traverse results
-define(COLLECTOR, collector).

% messages used to communicate with ?COLLECTOR process
-define(FINISHED, finished).
-define(COLLECTED_RESULTS(Rows), {collected_results, Rows}).
-define(ROW(FileId, Popularity, RowNum), {row, FileId, Popularity, RowNum}).

%%%===================================================================
%%% API
%%%===================================================================

query_should_return_error_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ?assertMatch({error, not_found}, query2(W, ?SPACE_ID, #{})).

query_should_return_empty_list_when_file_popularity_is_enabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    ?assertMatch([], query2(W, ?SPACE_ID, #{})).

query_should_return_empty_list_when_file_has_not_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, _} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    ?assertMatch([], query2(W, ?SPACE_ID, #{})).

query_should_return_file_when_file_has_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),
    {ok, FileId} = file_id:guid_to_objectid(G),
    ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

file_should_have_correct_popularity_value(Config) ->
    file_should_have_correct_popularity_value_base(Config, 1.123, 0).

file_should_have_correct_popularity_value2(Config) ->
    file_should_have_correct_popularity_value_base(Config, 0, 9.987).

file_should_have_correct_popularity_value3(Config) ->
    file_should_have_correct_popularity_value_base(Config, 1.123, 9.987).

avg_open_count_per_day_parameter_should_be_bounded_by_100_by_default(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    % 2 files should have the same probability value, despite having different avg_open_count
    FileName1 = <<"file1">>,
    FileName2 = <<"file2">>,
    FilePath1 = ?FILE_PATH(FileName1),
    FilePath2 = ?FILE_PATH(FileName2),
    DefaultMaxOpenCount = 100,
    OpenCountPerMonth1 = DefaultMaxOpenCount * 30, % avg_open_count = 100
    OpenCountPerMonth2 = (DefaultMaxOpenCount + 1) * 30, % avg_open_count = 101

    {ok, G1} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath1, 8#664),
    {ok, G2} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath2, 8#664),

    % ensure that all files will have the same timestamp
    mock_cluster_time_hours(W, current_timestamp_hours(W)),

    open_and_close_file(W, ?SESSION(W, Config), G1, OpenCountPerMonth1),
    open_and_close_file(W, ?SESSION(W, Config), G2, OpenCountPerMonth2),

    ?assertMatch([{_, _Popularity}, {_, _Popularity}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

avg_open_count_per_day_parameter_should_be_bounded_by_custom_value(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    % 2 files should have the same probability value, despite having different avg_open_count
    FileName1 = <<"file1">>,
    FileName2 = <<"file2">>,
    FilePath1 = ?FILE_PATH(FileName1),
    FilePath2 = ?FILE_PATH(FileName2),
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,

    OpenCountPerMonth1 = MaxOpenCount * 30, % avg_open_count = 100
    OpenCountPerMonth2 = (MaxOpenCount + 1) * 30, % avg_open_count = 200

    ok = configure_file_popularity(W, ?SPACE_ID, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),

    {ok, G1} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath1, 8#664),
    {ok, G2} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath2, 8#664),

    open_and_close_file(W, ?SESSION(W, Config), G1, OpenCountPerMonth1),
    open_and_close_file(W, ?SESSION(W, Config), G2, OpenCountPerMonth2),

    ?assertMatch([{_, _Popularity}, {_, _Popularity}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

changing_max_avg_open_count_per_day_limit_should_reindex_the_file(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,
    MaxOpenCount2 = 20,
    OpenCountPerMonth = 15 * 30, % avg_open_count = 15
    ok = configure_file_popularity(W, ?SPACE_ID, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(W, ?SESSION(W, Config), G, OpenCountPerMonth),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(W, ?SPACE_ID, undefined, undefined, undefined, MaxOpenCount2),

    ?assertNotMatch([{_, Popularity}], query2(W, ?SPACE_ID, #{})),
    ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{})).

changing_last_open_weight_should_reindex_the_file(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    LastOpenWeight = 1.0,
    LastOpenWeight2 = 2.0,
    AvgOpenCountPerDayWeight = 20.0,
    MaxOpenCount = 10,

    ok = configure_file_popularity(W, ?SPACE_ID, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(W, ?SESSION(W, Config), G, 1),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(W, ?SPACE_ID, undefined, LastOpenWeight2, undefined, undefined),

    ?assertNotMatch([{_, Popularity}], query2(W, ?SPACE_ID, #{})),
    ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{})).

changing_avg_open_count_weight_should_reindex_the_file(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    LastOpenWeight = 1.0,
    AvgOpenCountPerDayWeight = 20.0,
    AvgOpenCountPerDayWeight2 = 40.0,
    MaxOpenCount = 10,

    ok = configure_file_popularity(W, ?SPACE_ID, true, LastOpenWeight, AvgOpenCountPerDayWeight, MaxOpenCount),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, FileId} = file_id:guid_to_objectid(G),
    open_and_close_file(W, ?SESSION(W, Config), G, 1),

    [{_, Popularity}] = ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS),
    ok = configure_file_popularity(W, ?SPACE_ID, undefined, undefined, AvgOpenCountPerDayWeight2, undefined),

    ?assertNotMatch([{_, Popularity}], query2(W, ?SPACE_ID, #{})),
    ?assertMatch([{FileId, _}], query2(W, ?SPACE_ID, #{})).

query_should_return_files_sorted_by_increasing_avg_open_count_per_day(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName1 = <<"file1">>,
    FileName2 = <<"file2">>,
    FileName3 = <<"file3">>,
    FilePath1 = ?FILE_PATH(FileName1),
    FilePath2 = ?FILE_PATH(FileName2),
    FilePath3 = ?FILE_PATH(FileName3),

    % ensure that all files will have the same timestamp
    mock_cluster_time_hours(W, current_timestamp_hours(W)),

    {ok, G1} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath1, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G1}, read),
    ok = lfm_proxy:close(W, H),

    {ok, G2} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath2, 8#664),
    {ok, H2} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G2}, read),
    ok = lfm_proxy:close(W, H2),
    {ok, H22} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G2}, read),
    ok = lfm_proxy:close(W, H22),

    {ok, G3} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath3, 8#664),
    {ok, H3} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G3}, read),
    ok = lfm_proxy:close(W, H3),
    {ok, H32} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G3}, read),
    ok = lfm_proxy:close(W, H32),
    {ok, H33} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G3}, read),
    ok = lfm_proxy:close(W, H33),

    {ok, FileId1} = file_id:guid_to_objectid(G1),
    {ok, FileId2} = file_id:guid_to_objectid(G2),
    {ok, FileId3} = file_id:guid_to_objectid(G3),

    ?assertMatch([{FileId1, _}, {FileId2, _}, {FileId3, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

query_should_return_files_sorted_by_increasing_last_open_timestamp(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName1 = <<"file1">>,
    FileName2 = <<"file2">>,
    FileName3 = <<"file3">>,
    FilePath1 = ?FILE_PATH(FileName1),
    FilePath2 = ?FILE_PATH(FileName2),
    FilePath3 = ?FILE_PATH(FileName3),

    Timestamp = current_timestamp_hours(W),

    {ok, G1} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath1, 8#664),
    % pretend that G1 was opened for the last time 3 hours ago
    mock_cluster_time_hours(W, Timestamp - 3),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G1}, read),
    ok = lfm_proxy:close(W, H),

    {ok, G2} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath2, 8#664),
    % pretend that G2 was opened for the last time 2 hours ago
    mock_cluster_time_hours(W, Timestamp - 2),
    {ok, H2} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G2}, read),
    ok = lfm_proxy:close(W, H2),

    % pretend that G3 was opened for the last time 1 hours ago
    {ok, G3} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath3, 8#664),
    mock_cluster_time_hours(W, Timestamp - 1),
    {ok, H3} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G3}, read),
    ok = lfm_proxy:close(W, H3),

    {ok, FileId1} = file_id:guid_to_objectid(G1),
    {ok, FileId2} = file_id:guid_to_objectid(G2),
    {ok, FileId3} = file_id:guid_to_objectid(G3),

    ?assertMatch([{FileId1, _}, {FileId2, _}, {FileId3, _}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

%%%===================================================================
%%% Test base functions
%%%===================================================================

file_should_have_correct_popularity_value_base(Config, LastOpenW, AvgOpenW) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = configure_file_popularity(W, ?SPACE_ID, true, LastOpenW, AvgOpenW),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    Timestamp = current_timestamp_hours(W),
    mock_cluster_time_hours(W, Timestamp),
    AvgOpen = 1 / 30,
    Popularity = popularity(Timestamp, LastOpenW, AvgOpen, AvgOpenW),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    open_and_close_file(W, ?SESSION(W, Config), G),
    {ok, FileId} = file_id:guid_to_objectid(G),
    ?assertMatch([{FileId, Popularity}], query2(W, ?SPACE_ID, #{}), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].

init_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    init_pool(W),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    disable_file_popularity(W, ?SPACE_ID),
    ensure_collector_stopped(W),
    test_utils:mock_unload(W, file_popularity),
    clean_space(?SPACE_ID, Config),
    lfm_proxy:teardown(Config).

end_per_suite(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    stop_pool(W),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl).

%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

process_row(Row, _Info, RowNum) ->
    Popularity = maps:get(<<"key">>, Row),
    FileId = maps:get(<<"value">>, Row),
    ?COLLECTOR ! ?ROW(FileId, Popularity, RowNum),
    ok.

task_finished(_TaskId) ->
    whereis(?COLLECTOR) ! ?FINISHED.

%%%===================================================================
%%% view processing module API function
%%%===================================================================

init() ->
    view_traverse:init(?VIEW_PROCESSING_MODULE).

stop() ->
    view_traverse:stop(?VIEW_PROCESSING_MODULE).

run(SpaceId, Opts) ->
    view_traverse:run(?VIEW_PROCESSING_MODULE, ?FILE_POPULARITY_VIEW(SpaceId), Opts).

%%%===================================================================
%%% Functions exported for RPC
%%%===================================================================

start_collector(TestMasterPid) ->
    register(?COLLECTOR, spawn_link(?MODULE, collector_loop, [TestMasterPid])).

collector_loop(TestMaster) ->
    collector_loop(TestMaster, #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_pool(Worker) ->
    rpc:call(Worker, ?MODULE, init, []).

stop_pool(Worker) ->
    rpc:call(Worker, ?MODULE, stop, []).

run(Worker, SpaceId, Opts) ->
    rpc:call(Worker, ?MODULE, run, [SpaceId, Opts]).

start_collector_remote(Worker) ->
    true = rpc:call(Worker, ?MODULE, start_collector, [self()]).

collector_loop(TestMaster, RowsMap) ->
    receive
        ?FINISHED ->
            TestMaster ! ?COLLECTED_RESULTS([ {FileId, PopValue} || {_RN, {FileId, PopValue}} <- lists:sort(maps:to_list(RowsMap))]);
        ?ROW(FileId, Popularity, RowNum) ->
            collector_loop(TestMaster, RowsMap#{RowNum => {FileId, Popularity}})
    end.

query2(Worker, SpaceId, Opts) ->
    start_collector_remote(Worker),
    case run(Worker, SpaceId, Opts) of
        {ok, _} ->
            receive ?COLLECTED_RESULTS(Rows) -> Rows end;
        Error  = {error, _} ->
            Error
    end.

ensure_collector_stopped(Worker) ->
    case whereis(Worker, ?COLLECTOR) of
        undefined -> ok;
        CollectorPid -> exit(CollectorPid, kill)
    end.

enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

configure_file_popularity(Worker, SpaceId, Enabled, LastOpenWeight, AvgOpenCountPerDayWeight) ->
    rpc:call(Worker, file_popularity_api, configure, [SpaceId, filter_undefined_values(#{
        enabled => Enabled,
        last_open_hour_weight => LastOpenWeight,
        avg_open_count_per_day_weight => AvgOpenCountPerDayWeight
    })]).

configure_file_popularity(Worker, SpaceId, Enabled, LastOpenWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay) ->
    rpc:call(Worker, file_popularity_api, configure, [SpaceId, filter_undefined_values(#{
        enabled => Enabled,
        last_open_hour_weight => LastOpenWeight,
        avg_open_count_per_day_weight => AvgOpenCountPerDayWeight,
        max_avg_open_count_per_day => MaxAvgOpenCountPerDay
    })]).

disable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, disable, [SpaceId]).

clean_space(SpaceId, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(Worker, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    clean_space(Worker, SessId, SpaceGuid, 0, BatchSize).

clean_space(Worker, SessId, SpaceGuid, Offset, BatchSize) ->
    {ok, GuidsAndPaths} = lfm_proxy:get_children(Worker, SessId, {guid, SpaceGuid}, Offset, BatchSize),
    FilesNum = length(GuidsAndPaths),
    delete_files(Worker, SessId, GuidsAndPaths),
    case FilesNum < BatchSize of
        true ->
            ok;
        false ->
            clean_space(Worker, SessId, SpaceGuid, Offset + BatchSize, BatchSize)
    end.

delete_files(Worker, SessId, GuidsAndPaths) ->
    lists:foreach(fun({G, _}) ->
        ok = lfm_proxy:rm_recursive(Worker, SessId, {guid, G})
    end, GuidsAndPaths).

current_timestamp_hours(Worker) ->
    rpc:call(Worker, time_utils, timestamp_seconds, []) div 3600.

open_and_close_file(Worker, SessId, Guid, Times) ->
    lists:foreach(fun(_) ->
        open_and_close_file(Worker, SessId, Guid)
    end, lists:seq(1, Times)).

open_and_close_file(Worker, SessId, Guid) ->
    {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, read),
    ok = lfm_proxy:close(Worker, H).

popularity(LastOpen, LastOpenW, AvgOpen, AvgOpenW) ->
    LastOpen * LastOpenW + AvgOpen * AvgOpenW.

filter_undefined_values(Map) ->
    maps:filter(fun
        (_, undefined) -> false;
        (_, _) -> true
    end, Map).

mock_cluster_time_hours(Worker, Hours) ->
    test_utils:mock_new(Worker, file_popularity),
    ok = test_utils:mock_expect(Worker, file_popularity, cluster_time_hours, fun() -> Hours end).

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).