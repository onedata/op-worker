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

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([
    query_should_return_error_when_file_popularity_is_disabled/1,
    query_should_return_empty_list_when_file_popularity_is_enabled/1,
    query_should_return_empty_list_when_file_has_not_been_opened/1,
    query_should_return_file_when_file_has_been_opened/1,
    query_should_return_files_sorted_by_increasing_popularity_function_value/1,
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
    query_should_return_files_sorted_by_increasing_popularity_function_value,
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
    ?assertMatch({error, {<<"not_found">>, _}},
        query(W, ?SPACE_ID, ?LIMIT)).

query_should_return_empty_list_when_file_popularity_is_enabled(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    ?assertMatch({[], undefined},
        query(W, ?SPACE_ID, ?LIMIT)).

query_should_return_empty_list_when_file_has_not_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, _} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    ?assertMatch({[], undefined},
        query(W, ?SPACE_ID, ?LIMIT), ?ATTEMPTS).

query_should_return_file_when_file_has_been_opened(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FileName = <<"file">>,
    FilePath = ?FILE_PATH(FileName),
    {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
    {ok, H} = lfm_proxy:open(W, ?SESSION(W, Config), {guid, G}, read),
    ok = lfm_proxy:close(W, H),
    {ok, FileId} = cdmi_id:guid_to_objectid(G),
    ?assertMatch({[FileId], #index_token{}},
        query(W, ?SPACE_ID, ?LIMIT), ?ATTEMPTS).


query_should_return_files_sorted_by_increasing_popularity_function_value(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
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
    
    
    {ok, FileId1} = cdmi_id:guid_to_objectid(G1),
    {ok, FileId2} = cdmi_id:guid_to_objectid(G2),
    {ok, FileId3} = cdmi_id:guid_to_objectid(G3),

    ?assertMatch({[FileId1, FileId2, FileId3], #index_token{}},
        query(W, ?SPACE_ID, ?LIMIT), ?ATTEMPTS).

query_with_option_limit_should_return_limited_number_of_files(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    Limit = 3,
    NumberOfFiles = 10,
    SessId = ?SESSION(W, Config),

    IdsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {ok, FileId} = cdmi_id:guid_to_objectid(G),
        {FileId, N}
    end, lists:seq(1, NumberOfFiles)),  % the resulting list will bo sorted ascending by number of opens
    Ids = [C || {C, _} <- IdsAndOpensNum],
    ExpectedResult = lists:sublist(Ids, Limit),

    ?assertMatch({ExpectedResult, #index_token{}}, query(W, ?SPACE_ID, Limit), ?ATTEMPTS).

iterate_over_100_results_using_limit_1_and_startkey_docid(Config) ->
    iterate_over_100_results_using_given_limit_and_startkey_docid(Config, 1).

iterate_over_100_results_using_limit_10_and_startkey_docid(Config) ->
    iterate_over_100_results_using_given_limit_and_startkey_docid(Config, 10).

iterate_over_100_results_using_limit_100_and_startkey_docid(Config) ->
    iterate_over_100_results_using_given_limit_and_startkey_docid(Config, 100).

iterate_over_100_results_using_limit_1000_and_startkey_docid(Config) ->
   iterate_over_100_results_using_given_limit_and_startkey_docid(Config, 1000).

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
    [W | _] = ?config(op_worker_nodes, Config),
    disable_file_popularity(W, ?SPACE_ID),
    clean_space(?SPACE_ID, Config),
    lfm_proxy:teardown(Config).

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl).

%%%===================================================================
%%% Internal functions
%%%===================================================================

iterate_over_100_results_using_given_limit_and_startkey_docid(Config, Limit) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = enable_file_popularity(W, ?SPACE_ID),
    FilePrefix = <<"file_">>,
    NumberOfFiles = 100,
    SessId = ?SESSION(W, Config),

    IdsAndOpensNum = lists:map(fun(N) ->
        % each file will be opened N times
        FilePath = ?FILE_PATH(<<FilePrefix/binary, (integer_to_binary(N))/binary>>),
        {ok, G} = lfm_proxy:create(W, ?SESSION(W, Config), FilePath, 8#664),
        open_and_close_file(W, SessId, G, N),
        {ok, FileId} = cdmi_id:guid_to_objectid(G),
        {FileId, N}
    end, lists:seq(1, NumberOfFiles)),
    Ids = [C || {C, _} <- IdsAndOpensNum],

    ?assertMatch(Ids, iterate(W, ?SPACE_ID, Limit), ?ATTEMPTS).

enable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, enable, [SpaceId]).

disable_file_popularity(Worker, SpaceId) ->
    rpc:call(Worker, file_popularity_api, disable, [SpaceId]).

query(Worker, SpaceId, Limit) ->
    rpc:call(Worker, file_popularity_api, query, [SpaceId, Limit]).

query(Worker, SpaceId, IndexToken, Limit) ->
    rpc:call(Worker, file_popularity_api, query, [SpaceId, IndexToken, Limit]).

clean_space(SpaceId, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESSION(Worker, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    clean_space(Worker, SessId, SpaceGuid, 0, BatchSize).

clean_space(Worker, SessId, SpaceGuid, Offset, BatchSize) ->
    {ok, GuidsAndPaths} = lfm_proxy:ls(Worker, SessId, {guid, SpaceGuid}, Offset, BatchSize),
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
    rpc:call(Worker, time_utils, cluster_time_seconds, []) div 3600.

open_and_close_file(Worker, SessId, Guid, Times) ->
    lists:foreach(fun(_) ->
        open_and_close_file(Worker, SessId, Guid)
    end, lists:seq(1, Times)).

open_and_close_file(Worker, SessId, Guid) ->
    {ok, H} = lfm_proxy:open(Worker, SessId, {guid, Guid}, read),
    ok = lfm_proxy:close(Worker, H).

iterate(Worker, SpaceId, Limit) ->
    iterate(Worker, SpaceId, undefined, Limit, []).

iterate(Worker, SpaceId, IndexToken, Limit, Result) ->
    case query(Worker, SpaceId, IndexToken, Limit) of
        {Ids, _NewIndexToken} when length(Ids) < Limit ->
            Result ++ Ids;
        {Ids, NewIndexToken} ->
            iterate(Worker, SpaceId, NewIndexToken, Limit, Result ++ Ids)
    end.