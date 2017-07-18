%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests reverse LUMA
%%% @end
%%%--------------------------------------------------------------------
-module(reverse_luma_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/logging.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    get_user_id_on_posix_storage/1,
    get_user_id_on_posix_storage_should_return_error_when_reverse_luma_is_disabled/1,
    get_user_id_on_posix_storage_should_fail_with_404_error/1,
    get_user_id_should_fail_with_not_supported_storage_error/1,
    get_user_id_on_posix_storage_should_query_reverse_luma_once/1,
    get_user_id_on_posix_storage_should_query_reverse_luma_twice/1]).

all() ->
    ?ALL([
        get_user_id_on_posix_storage,
        get_user_id_on_posix_storage_should_return_error_when_reverse_luma_is_disabled,
        get_user_id_on_posix_storage_should_fail_with_404_error,
        get_user_id_should_fail_with_not_supported_storage_error,
        get_user_id_on_posix_storage_should_query_reverse_luma_once,
        get_user_id_on_posix_storage_should_query_reverse_luma_twice
    ]).

-define(TEST_URL, <<"http://127.0.0.1:5000">>).

-define(DEFAULT_TIMEOUT, timer:minutes(5)).

-define(LUMA_CONFIG, ?LUMA_CONFIG(?DEFAULT_TIMEOUT)).
-define(LUMA_CONFIG(CacheTimeout), #luma_config{
    url = ?TEST_URL,
    cache_timeout = CacheTimeout,
    api_key = <<"test_api_key">>
}).

-define(STORAGE_DISABLED_LUMA, ?STORAGE(?POSIX_HELPER_NAME, undefined)).
-define(STORAGE_ID, <<"test_storage_id">>).
-define(STORAGE, ?STORAGE(?POSIX_HELPER_NAME, ?LUMA_CONFIG)).
-define(STORAGE(LumaConfig), ?STORAGE(?POSIX_HELPER_NAME, LumaConfig)).
-define(STORAGE(HelperName, LumaConfig), #storage{
    name = <<"test_storage">>,
    helpers = [#helper{name=HelperName}],
    luma_config = LumaConfig
}).

%%%===================================================================
%%% Test functions
%%%===================================================================
get_user_id_on_posix_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE]),
    ?assertEqual({ok, <<"test_user_id">>}, Result).

get_user_id_on_posix_storage_should_return_error_when_reverse_luma_is_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE_DISABLED_LUMA]),
    ?assertEqual({ok, <<"0">>}, Result).


get_user_id_on_posix_storage_should_fail_with_404_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE]),
    ?assertMatch({badrpc, {'EXIT', _}}, Result).

get_user_id_should_fail_with_not_supported_storage_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    LumaConfig = ?LUMA_CONFIG,
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE(<<"NOT SUPPORTED HELPER NAME">>, LumaConfig)]),
    ?assertEqual({error, not_supported_storage_type}, Result).

get_user_id_on_posix_storage_should_query_reverse_luma_once(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE]),
    ?assertEqual({ok, <<"test_user_id">>}, Result),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE]),
    ?assertEqual({ok, <<"test_user_id">>}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_', '_'], 1).

get_user_id_on_posix_storage_should_query_reverse_luma_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, <<"test_user_id">>}, Result),

    timer:sleep(timer:seconds(CacheTimeout + 1)),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, <<"0">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, <<"test_user_id">>}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_', '_'], 2).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= get_user_id_on_posix_storage;
    Case =:= get_user_id_on_posix_storage_should_query_reverse_luma_once ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, storage]),
    mock_resolve_user_identity_post(Worker, {ok, 200, [], <<"{\"id\": \"test_user_id\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_user_id_on_posix_storage_should_query_reverse_luma_twice ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, reverse_luma_proxy]),
    mock_resolve_user_identity_post(Worker, {ok, 200, [], <<"{\"id\": \"test_user_id\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_user_id_on_posix_storage_should_fail_with_404_error ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client]),
    mock_resolve_user_identity_post(Worker, {ok, 404, [], <<"{\"error\": \"reason\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Docs} = rpc:call(Worker, reverse_luma, list, []),
    lists:foreach(fun(#document{key = Key}) ->
        rpc:call(Worker, reverse_luma, delete, [Key])
    end, Docs),
    test_utils:mock_unload(Workers, [http_client, reverse_luma_proxy]).

mock_resolve_user_identity_post(Worker, Expected) ->
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_binary(Url) ->
            case binary:split(Url, <<"/">>, [global]) of
                <<"resolve_user_identity">> ->
                    Expected;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) when is_list(Url) ->
            case lists:last(string:tokens(Url, "/")) of
                "resolve_user_identity" ->
                    Expected;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).