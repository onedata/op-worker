%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of file_popularity_view.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("modules/fslogic/file_popularity_view.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([
    query_should_return_error_when_file_popularity_is_disabled/1,
    query_should_return_empty_list_when_file_popularity_is_enabled/1,
    query_should_return_empty_list_when_file_has_not_been_opened/1,
    query_should_return_file_when_file_has_been_opened/1,
    query_should_return_empty_list_when_number_of_opens_is_greater_than_startkey/1,
    query_should_return_empty_list_when_number_of_opens_is_less_than_endkey/1,
    query_should_return_file_when_number_of_opens_is_between_startkey_and_endkey/1,
    query_should_return_empty_list_when_last_open_timestamp_is_newer_than_startkey/1,
    query_should_return_empty_list_when_last_open_timestamp_is_older_than_endkey/1,
    query_should_return_file_when_last_open_timestamp_is_between_startkey_and_endkey/1,
    query_should_return_empty_list_when_size_is_greater_than_startkey/1,
    query_should_return_empty_list_when_size_is_less_than_endkey/1,
    query_should_return_file_when_size_is_between_startkey_and_endkey/1,
    query_should_return_empty_list_when_hourly_avg_is_greater_than_startkey/1,
    query_should_return_empty_list_when_hourly_avg_is_less_than_endkey/1,
    query_should_return_file_when_hourly_avg_is_between_startkey_and_endkey/1,
    query_should_return_empty_list_when_daily_avg_is_greater_than_startkey/1,
    query_should_return_empty_list_when_daily_avg_is_less_than_endkey/1,
    query_should_return_file_when_daily_avg_is_between_startkey_and_endkey/1,
    query_should_return_empty_list_when_monthly_avg_is_greater_than_startkey/1,
    query_should_return_empty_list_when_monthly_avg_is_less_than_endkey/1,
    query_should_return_file_when_monthly_avg_is_between_startkey_and_endkey/1,
    query_should_return_files_sorted_by_number_of_opens/1,
    query_with_option_limit_should_return_limited_number_of_files/1,
    iterate_over_100_results_using_limit_1_and_startkey_docid/1,
    iterate_over_100_results_using_limit_10_and_startkey_docid/1,
    iterate_over_100_results_using_limit_100_and_startkey_docid/1,
    iterate_over_100_results_using_limit_1000_and_startkey_docid/1
]).


all() -> [
    query_should_return_error_when_file_popularity_is_disabled,
    query_should_return_empty_list_when_file_popularity_is_enabled,
    query_should_return_empty_list_when_file_has_not_been_opened,
    query_should_return_file_when_file_has_been_opened,
    query_should_return_empty_list_when_number_of_opens_is_greater_than_startkey,
    query_should_return_empty_list_when_number_of_opens_is_less_than_endkey,
    query_should_return_file_when_number_of_opens_is_between_startkey_and_endkey,
    query_should_return_empty_list_when_last_open_timestamp_is_newer_than_startkey,
    query_should_return_empty_list_when_last_open_timestamp_is_older_than_endkey,
    query_should_return_file_when_last_open_timestamp_is_between_startkey_and_endkey,
    query_should_return_empty_list_when_size_is_greater_than_startkey,
    query_should_return_empty_list_when_size_is_less_than_endkey,
    query_should_return_file_when_size_is_between_startkey_and_endkey,
    query_should_return_empty_list_when_hourly_avg_is_greater_than_startkey,
    query_should_return_empty_list_when_hourly_avg_is_less_than_endkey,
    query_should_return_file_when_hourly_avg_is_between_startkey_and_endkey,
    query_should_return_empty_list_when_daily_avg_is_greater_than_startkey,
    query_should_return_empty_list_when_daily_avg_is_less_than_endkey,
    query_should_return_file_when_daily_avg_is_between_startkey_and_endkey,
    query_should_return_empty_list_when_monthly_avg_is_greater_than_startkey,
    query_should_return_empty_list_when_monthly_avg_is_less_than_endkey,
    query_should_return_file_when_monthly_avg_is_between_startkey_and_endkey,
    query_should_return_files_sorted_by_number_of_opens,
    query_with_option_limit_should_return_limited_number_of_files,
    iterate_over_100_results_using_limit_1_and_startkey_docid,
    iterate_over_100_results_using_limit_10_and_startkey_docid,
    iterate_over_100_results_using_limit_100_and_startkey_docid,
    iterate_over_100_results_using_limit_1000_and_startkey_docid
].

-define(SPACE_ID, <<"space1">>).

-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

-define(USER, <<"user1">>).
-define(SESSION(Worker, Config), ?SESSION(?USER, Worker, Config)).
-define(SESSION(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

-define(ATTEMPTS, 10).
-define(LIMIT, 10).

%%%===================================================================
%%% API
%%%===================================================================

query_should_return_error_when_file_popularity_is_disabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ?assertMatch({error, {<<"not_found">>, _}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_file_popularity_is_enabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_file_has_not_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, _} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_file_has_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),
    Ctx = file_ctx:new_by_guid(G),
    ?assertMatch({[Ctx], #token{}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_number_of_opens_is_greater_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    NumberOfOpens = 10,
    StartKey = [NumberOfOpens - 1, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    lists:foreach(fun(_) ->
        {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
        ok = lfm_proxy:close(W, H)
    end, lists:seq(1, NumberOfOpens)),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_number_of_opens_is_less_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    NumberOfOpens = 10,
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [NumberOfOpens + 1, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    lists:foreach(fun(_) ->
        {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
        ok = lfm_proxy:close(W, H)
    end, lists:seq(1, NumberOfOpens)),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_number_of_opens_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    NumberOfOpens = 10,
    StartKey = [NumberOfOpens + 1, 100, 100, 100, 100, 100],
    EndKey = [NumberOfOpens - 1, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    lists:foreach(fun(_) ->
        {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
        ok = lfm_proxy:close(W, H)
    end, lists:seq(1, NumberOfOpens)),

    Ctx = file_ctx:new_by_guid(G),
    ?assertMatch({[Ctx], #token{}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_last_open_timestamp_is_newer_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    LastOpen = 10,
    StartKey = [1, 0, LastOpen - 1, 100, 100, 100],
    EndKey = [1, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend the file was opened at 10 (hours since epoch)
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{last_open = LastOpen}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_last_open_timestamp_is_older_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    LastOpen = 10,
    StartKey = [1, 0, 100, 100, 100, 100],
    EndKey = [1, 0, LastOpen + 1, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend the file was opened at 10 (hours since epoch)
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{last_open = LastOpen}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_last_open_timestamp_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, CurrentTimestampHours + 1, 100, 100, 100, 100],
    EndKey = [1, CurrentTimestampHours - 1, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),
    Ctx = file_ctx:new_by_guid(G),

    ?assertMatch({[Ctx], #token{}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_size_is_greater_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Size = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, CurrentTimestampHours, Size - 1, 100, 100, 100],
    EndKey = [1, CurrentTimestampHours, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(W, H),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_size_is_less_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Size = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, CurrentTimestampHours, 100, 100, 100, 100],
    EndKey = [1, CurrentTimestampHours, Size + 1, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(W, H),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_size_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Size = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, CurrentTimestampHours, Size + 1, 100, 100, 100],
    EndKey = [1, CurrentTimestampHours, Size - 1, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, write),
    {ok, _} = lfm_proxy:write(W, H, 0, crypto:strong_rand_bytes(Size)),
    ok = lfm_proxy:close(W, H),
    Ctx = file_ctx:new_by_guid(G),

    ?assertMatch({[Ctx], #token{}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_should_return_empty_list_when_hourly_avg_is_greater_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    HrMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, HrMovingAvg - 1, 100, 100],
    EndKey = [1, 0, CurrentTimestampHours, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's hr_mov_avg equals HrMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{hr_mov_avg = HrMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_hourly_avg_is_less_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    HrMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 100, 100, 100],
    EndKey = [1, 0, CurrentTimestampHours, HrMovingAvg + 1, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's hr_mov_avg equals HrMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{hr_mov_avg = HrMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_hourly_avg_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 2, 100, 100],
    EndKey = [1, 0, CurrentTimestampHours, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_daily_avg_is_greater_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    DyMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, DyMovingAvg - 1, 100],
    EndKey = [1, 0, CurrentTimestampHours, 1, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's dy_mov_avg equals DyMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{dy_mov_avg = DyMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_daily_avg_is_less_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    DyMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, 100, 100],
    EndKey = [1, 0, CurrentTimestampHours, 1, DyMovingAvg + 1, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's dy_mov_avg equals DyMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{dy_mov_avg = DyMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_daily_avg_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, 2, 100],
    EndKey = [1, 0, CurrentTimestampHours, 1, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_monthly_avg_is_greater_than_startkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    MthMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, 1, MthMovingAvg - 1],
    EndKey = [1, 0, CurrentTimestampHours, 1, 1, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's mth_mov_avg equals MthMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{mth_mov_avg = MthMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_empty_list_when_monthly_avg_is_less_than_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    MthMovingAvg = 10,
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, 1, 100],
    EndKey = [1, 0, CurrentTimestampHours, 1, 1, MthMovingAvg + 1],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    % pretend that file's mth_mov_avg equals MthMovingAvg
    U = fslogic_uuid:guid_to_uuid(G),
    {ok, _} = rpc:call(W, file_popularity, update, [U, fun(FP) ->
        {ok, FP#file_popularity{mth_mov_avg = MthMovingAvg}}
    end]),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_file_when_monthly_avg_is_between_startkey_and_endkey(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    CurrentTimestampHours = current_timestamp_hours(W),
    StartKey = [1, 0, CurrentTimestampHours, 1, 1, 2],
    EndKey = [1, 0, CurrentTimestampHours, 1, 1, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),

    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),

    ?assertMatch({[], #token{last_doc_id = undefined}},
        query(W, ?SPACE_ID, Token, ?LIMIT)).

query_should_return_files_sorted_by_number_of_opens(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName1 = <<"file1">>,
    FileName2 = <<"file2">>,
    FileName3 = <<"file3">>,
    FilePath1 = ?FILE_PATH(FileName1),
    FilePath2 = ?FILE_PATH(FileName2),
    FilePath3 = ?FILE_PATH(FileName3),
    
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
    
    
    Ctx1 = file_ctx:new_by_guid(G1),
    Ctx2 = file_ctx:new_by_guid(G2),
    Ctx3 = file_ctx:new_by_guid(G3),

    ?assertMatch({[Ctx3, Ctx2, Ctx1], #token{}},
        query(W, ?SPACE_ID, Token, ?LIMIT), ?ATTEMPTS).

query_with_option_limit_should_return_limited_number_of_files(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [100, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 3,
    NumberOfFiles = 10,
    SessId = ?SESSION(W, Config),

    CtxsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {file_ctx:new_by_guid(G), N}
    end, lists:seq(NumberOfFiles, 1, -1)),  % the resulting list will bo sorted descending by number of opens
    Ctxs = [C || {C, _} <- CtxsAndOpensNum],
    ExpectedResult = lists:sublist(Ctxs, Limit),

    ?assertMatch({ExpectedResult, #token{}}, query(W, ?SPACE_ID, Token, Limit), ?ATTEMPTS).

iterate_over_100_results_using_limit_1_and_startkey_docid(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [101, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 1,
    NumberOfFiles = 100,
    SessId = ?SESSION(W, Config),

    CtxsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {file_ctx:new_by_guid(G), N}
    end, lists:seq(NumberOfFiles, 1, -1)),
    Ctxs = [C || {C, _} <- CtxsAndOpensNum],

    ?assertMatch(Ctxs, iterate(W, ?SPACE_ID, Token, Limit), ?ATTEMPTS).

iterate_over_100_results_using_limit_10_and_startkey_docid(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [101, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 10,
    NumberOfFiles = 100,
    SessId = ?SESSION(W, Config),

    CtxsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {file_ctx:new_by_guid(G), N}
    end, lists:seq(NumberOfFiles, 1, -1)),
    Ctxs = [C || {C, _} <- CtxsAndOpensNum],

    ?assertMatch(Ctxs, iterate(W, ?SPACE_ID, Token, Limit), ?ATTEMPTS).

iterate_over_100_results_using_limit_100_and_startkey_docid(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [101, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 100,
    NumberOfFiles = 100,
    SessId = ?SESSION(W, Config),

    CtxsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {file_ctx:new_by_guid(G), N}
    end, lists:seq(NumberOfFiles, 1, -1)),
    Ctxs = [C || {C, _} <- CtxsAndOpensNum],

    ?assertMatch(Ctxs, iterate(W, ?SPACE_ID, Token, Limit), ?ATTEMPTS).


iterate_over_100_results_using_limit_1000_and_startkey_docid(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StartKey = [101, 100, 100, 100, 100, 100],
    EndKey = [0, 0, 0, 0, 0, 0],
    Token = initial_token(W, StartKey, EndKey),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 1000,
    NumberOfFiles = 100,
    SessId = ?SESSION(W, Config),

    CtxsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {file_ctx:new_by_guid(G), N}
    end, lists:seq(NumberOfFiles, 1, -1)),
    Ctxs = [C || {C, _} <- CtxsAndOpensNum],

    ?assertMatch(Ctxs, iterate(W, ?SPACE_ID, Token, Limit), ?ATTEMPTS).

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
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    delete_view(Config, ?SPACE_ID),
    clean_space(?SPACE_ID, Config),
    lfm_proxy:teardown(Config).

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl).

%%%===================================================================
%%% Internal functions
%%%===================================================================

enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

disable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, disable, [SpaceId]).

initial_token(Worker, StartKey, EndKey) ->
    rpc:call(Worker, file_popularity_api, initial_token, [StartKey, EndKey]).

query(Worker, SpaceId, Token, Limit) ->
    rpc:call(Worker, file_popularity_api, query, [SpaceId, Token, Limit]).

delete_view(Config, SpaceId) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = disable_file_popularity(W, SpaceId),
    rpc:call(W, file_popularity_view, delete, [SpaceId]).

clean_space(SpaceId, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(Worker, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    clean_space(Worker, SessId, SpaceGuid, 0, BatchSize).

clean_space(Worker, SessId, SpaceGuid, Offset, BatchSize) ->
    {ok, GuidsAndPaths} = lfm_proxy:ls(Worker, SessId, {guid, SpaceGuid}, Offset, BatchSize),
    FilesNum = length(GuidsAndPaths),
    case FilesNum < BatchSize of
        true ->
            remove_files(Worker, SessId, GuidsAndPaths);
        false ->
            remove_files(Worker, SessId, GuidsAndPaths),
            clean_space(Worker, SessId, SpaceGuid, Offset + BatchSize, BatchSize)
    end.

remove_files(Worker, SessId, GuidsAndPaths) ->
    lists:foreach(fun({G, _}) ->
        ok = lfm_proxy:rm_recursive(Worker, SessId, {guid, G})
    end, GuidsAndPaths).

current_timestamp_hours(Worker) ->
    rpc:call(Worker, time_utils, cluster_time_seconds, []) div 3600.

open_and_close_file(Worker, SessId, Guid, Times) ->
    lists:foreach(fun(_) ->
        open_and_close_file(Worker, SessId, Guid)
    end, lists:seq(1, Times)).

open_and_close_file(Worker, SessId, Guid) ->
    {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, read),
    ok = lfm_proxy:close(Worker, H).

iterate(Worker, SpaceId, Token, Limit) ->
    iterate(Worker, SpaceId, Token, Limit, []).

iterate(Worker, SpaceId, Token, Limit, Result) ->
    case query(Worker, SpaceId, Token, Limit) of
        {Ctxs, _NewToken} when length(Ctxs) < Limit ->
            Result ++ Ctxs;
        {Ctxs, NewToken} ->
            iterate(Worker, SpaceId, NewToken, Limit, Result ++ Ctxs)
    end.
