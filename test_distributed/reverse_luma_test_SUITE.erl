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
-include_lib("cluster_worker/include/api_errors.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    get_user_id_on_posix_storage/1,
    get_user_id_on_posix_storage_by_acl_username/1,
    get_user_id_on_posix_storage_should_return_root_user_id_when_reverse_luma_is_disabled/1,
    get_user_id_on_posix_storage_by_acl_username_should_return_error_when_reverse_luma_is_disabled/1,
    get_user_id_on_posix_storage_should_fail_with_404_error/1,
    get_user_id_on_posix_storage_by_acl_username_should_fail_with_404_error/1,
    get_user_id_should_fail_with_not_supported_storage_error/1,
    get_user_id_by_acl_username_should_fail_with_not_supported_storage_error/1,
    get_user_id_on_posix_storage_should_query_reverse_luma_once/1,
    get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_once/1,
    get_user_id_on_posix_storage_should_query_reverse_luma_twice/1,
    get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_twice/1,
    get_group_id_on_posix_storage/1,
    get_group_id_on_posix_storage_by_acl_groupname/1,
    get_group_id_on_posix_storage_should_return_undefined_when_reverse_luma_is_disabled/1,
    get_group_id_on_posix_storage_by_acl_groupname_should_return_error_when_reverse_luma_is_disabled/1,
    get_group_id_on_posix_storage_should_fail_with_404_error/1,
    get_group_id_on_posix_storage_by_acl_groupname_should_fail_with_404_error/1,
    get_group_id_should_fail_with_not_supported_storage_error/1,
    get_group_id_by_acl_groupname_should_fail_with_not_supported_storage_error/1,
    get_group_id_on_posix_storage_should_query_reverse_luma_once/1,
    get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_once/1,
    get_group_id_on_posix_storage_should_query_reverse_luma_twice/1,
    get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_twice/1
]).

all() ->
    ?ALL([
        get_user_id_on_posix_storage,
        get_user_id_on_posix_storage_by_acl_username,
        get_user_id_on_posix_storage_should_return_root_user_id_when_reverse_luma_is_disabled,
        get_user_id_on_posix_storage_by_acl_username_should_return_error_when_reverse_luma_is_disabled,
        get_user_id_on_posix_storage_should_fail_with_404_error,
        get_user_id_on_posix_storage_by_acl_username_should_fail_with_404_error,
        get_user_id_should_fail_with_not_supported_storage_error,
        get_user_id_by_acl_username_should_fail_with_not_supported_storage_error,
        get_user_id_on_posix_storage_should_query_reverse_luma_once,
        get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_once,
        get_user_id_on_posix_storage_should_query_reverse_luma_twice,
        get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_twice,
        get_group_id_on_posix_storage,
        get_group_id_on_posix_storage_by_acl_groupname,
        get_group_id_on_posix_storage_should_return_undefined_when_reverse_luma_is_disabled,
        get_group_id_on_posix_storage_by_acl_groupname_should_return_error_when_reverse_luma_is_disabled,
        get_group_id_on_posix_storage_should_fail_with_404_error,
        get_group_id_on_posix_storage_by_acl_groupname_should_fail_with_404_error,
        get_group_id_should_fail_with_not_supported_storage_error,
        get_group_id_by_acl_groupname_should_fail_with_not_supported_storage_error,
        get_group_id_on_posix_storage_should_query_reverse_luma_once,
        get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_once,
        get_group_id_on_posix_storage_should_query_reverse_luma_twice,
        get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_twice
    ]).

-define(TEST_URL, <<"http://127.0.0.1:5000">>).

-define(DEFAULT_TIMEOUT, timer:minutes(5)).

-define(TEST_PROVIDER_ID, <<"test_provider_id">>).
-define(TEST_USER_ID, <<"test_user_id">>).
-define(TEST_GROUP_ID, <<"test_group_id">>).
-define(TEST_MAPPED_GROUP_ID, <<"test_mapped_group_id">>).

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
    helpers = [#helper{name = HelperName}],
    luma_config = LumaConfig
}).

-define(STORAGE_DOC(Key, Value), #document{
    key = Key,
    value = Value
}).

-define(SPACE_ID, <<"test_space_id">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_user_id_on_posix_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ExpectedUserId = datastore_utils2:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),
    ?assertEqual({ok, ExpectedUserId}, Result).

get_user_id_on_posix_storage_by_acl_username(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ExpectedUserId = datastore_utils2:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),
    ?assertEqual({ok, ExpectedUserId}, Result).

get_user_id_on_posix_storage_should_return_root_user_id_when_reverse_luma_is_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE_DISABLED_LUMA)]),
    ?assertEqual({ok, ?ROOT_USER_ID}, Result).

get_user_id_on_posix_storage_by_acl_username_should_return_error_when_reverse_luma_is_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE_DISABLED_LUMA)]),
    ?assertEqual({error, luma_disabled}, Result).

get_user_id_on_posix_storage_should_fail_with_404_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertMatch({error, {ok, 404, _, _}}, Result).

get_user_id_on_posix_storage_by_acl_username_should_fail_with_404_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertMatch({error,{ok, 404, _, _}}, Result).

get_user_id_should_fail_with_not_supported_storage_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    LumaConfig = ?LUMA_CONFIG,
    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_ID, ?STORAGE(<<"NOT SUPPORTED HELPER NAME">>, LumaConfig)]),
    ?assertEqual({error, not_supported_storage_type}, Result).

get_user_id_by_acl_username_should_fail_with_not_supported_storage_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    LumaConfig = ?LUMA_CONFIG,
    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_ID, ?STORAGE(<<"NOT SUPPORTED HELPER NAME">>, LumaConfig)]),
    ?assertEqual({error, not_supported_storage_type}, Result).

get_user_id_on_posix_storage_should_query_reverse_luma_once(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),
    ExpectedUserId = datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),

    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ExpectedUserId}, Result),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ExpectedUserId}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_'], 1).

get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_once(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),
    ExpectedUserId = datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),

    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ExpectedUserId}, Result),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ExpectedUserId}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id_by_name,
        ['_', '_', '_', '_'], 1).

get_user_id_on_posix_storage_should_query_reverse_luma_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),
    ExpectedUserId = datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),

    Result = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, ExpectedUserId}, Result),

    timer:sleep(timer:seconds(CacheTimeout + 1)),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id,
        [<<"0">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),

    ?assertEqual({ok, ExpectedUserId}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_'], 2).

get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),
    ExpectedUserId = datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [?TEST_PROVIDER_ID, ?TEST_USER_ID])),

    Result = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, ExpectedUserId}, Result),

    timer:sleep(timer:seconds(CacheTimeout + 1)),

    Result2 = rpc:call(Worker, reverse_luma, get_user_id_by_name,
        [<<"user@nfsdomain.org">>, ?STORAGE_ID, ?STORAGE(LumaConfig)]),

    ?assertEqual({ok, ExpectedUserId}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id_by_name,
        ['_', '_', '_', '_'], 2).

get_group_id_on_posix_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result).

get_group_id_on_posix_storage_by_acl_groupname(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result).

get_group_id_on_posix_storage_should_return_undefined_when_reverse_luma_is_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE_DISABLED_LUMA]),
    ?assertEqual({ok, undefined}, Result).

get_group_id_on_posix_storage_by_acl_groupname_should_return_error_when_reverse_luma_is_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE_DISABLED_LUMA)]),
    ?assertEqual({error, luma_disabled}, Result).

get_group_id_on_posix_storage_should_fail_with_404_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE]),
    ?assertMatch({error, {ok, 404, _, _}}, Result).

get_group_id_on_posix_storage_by_acl_groupname_should_fail_with_404_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertMatch({error, {ok, 404, _, _}}, Result).

get_group_id_should_fail_with_not_supported_storage_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    LumaConfig = ?LUMA_CONFIG,
    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE(<<"NOT SUPPORTED HELPER NAME">>, LumaConfig)]),
    ?assertEqual({error, not_supported_storage_type}, Result).

get_group_id_by_acl_groupname_should_fail_with_not_supported_storage_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    LumaConfig = ?LUMA_CONFIG,
    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_ID, ?STORAGE(<<"NOT SUPPORTED HELPER NAME">>, LumaConfig)]),
    ?assertEqual({error, not_supported_storage_type}, Result).

get_group_id_on_posix_storage_should_query_reverse_luma_once(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result),

    Result2 = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 1).

get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_once(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result),

    Result2 = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_DOC(?STORAGE_ID, ?STORAGE)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id_by_name,
        ['_', '_', '_', '_', '_'], 1).

get_group_id_on_posix_storage_should_query_reverse_luma_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result),

    timer:sleep(timer:seconds(CacheTimeout + 1)),

    Result2 = rpc:call(Worker, reverse_luma, get_group_id,
        [<<"0">>, ?STORAGE_ID, ?SPACE_ID, ?STORAGE(LumaConfig)]),

    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 2).

get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_new(Worker, reverse_luma_proxy, [passthrough]),

    Result = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_ID, ?STORAGE(LumaConfig)]),
    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result),

    timer:sleep(timer:seconds(CacheTimeout + 1)),

    Result2 = rpc:call(Worker, reverse_luma, get_group_id_by_name,
        [<<"group@nfsdomain.org">>, ?SPACE_ID, ?STORAGE_ID, ?STORAGE(LumaConfig)]),

    ?assertEqual({ok, ?TEST_MAPPED_GROUP_ID}, Result2),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id_by_name,
        ['_', '_', '_', '_', '_'], 2).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= get_user_id_on_posix_storage;
    Case =:= get_user_id_on_posix_storage_should_query_reverse_luma_once ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, storage]),
    mock_resolve_user_post(Worker,
        {ok, 200, [], str_utils:format_bin("{
        \"idp\": \"~s\",
        \"userId\": \"~s\"
        \}", [?TEST_PROVIDER_ID, ?TEST_USER_ID])}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_user_id_on_posix_storage_by_acl_username;
    Case =:= get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_once ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, storage]),
    mock_resolve_acl_user_post(Worker,
        {ok, 200, [], str_utils:format_bin("{
        \"idp\": \"~s\",
        \"userId\": \"~s\"
        \}", [?TEST_PROVIDER_ID, ?TEST_USER_ID])}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_user_id_on_posix_storage_should_query_reverse_luma_twice, Config)->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, reverse_luma_proxy]),
    mock_resolve_user_post(Worker,
        {ok, 200, [], str_utils:format_bin("{
        \"idp\": \"~s\",
        \"userId\": \"~s\"
        \}", [?TEST_PROVIDER_ID, ?TEST_USER_ID])}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_user_id_on_posix_storage_by_acl_username_should_query_reverse_luma_twice, Config)->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, reverse_luma_proxy]),
    mock_resolve_acl_user_post(Worker,
        {ok, 200, [], str_utils:format_bin("{
        \"idp\": \"~s\",
        \"userId\": \"~s\"
        \}", [?TEST_PROVIDER_ID, ?TEST_USER_ID])}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_user_id_on_posix_storage_should_fail_with_404_error, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client]),
    mock_resolve_user_post(Worker, {ok, 404, [], <<"{\"error\": \"reason\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_user_id_on_posix_storage_by_acl_username_should_fail_with_404_error, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client]),
    mock_resolve_acl_user_post(Worker, {ok, 404, [], <<"{\"error\": \"reason\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_group_id_on_posix_storage;
    Case =:= get_group_id_on_posix_storage_should_query_reverse_luma_once;
    Case =:= get_group_id_on_posix_storage_should_query_reverse_luma_twice
    ->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, storage, provider_logic]),
    mock_resolve_group_post(Worker,
        {
            ok, 200, [], str_utils:format_bin("{
                \"idp\": \"~s\",
                \"groupId\": \"~s\"\}", [?TEST_PROVIDER_ID, ?TEST_GROUP_ID])
        }
    ),
    mock_idp_group_mapping(Worker,
        ?TEST_PROVIDER_ID, ?TEST_GROUP_ID,
        ?TEST_MAPPED_GROUP_ID
    ),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_group_id_on_posix_storage_by_acl_groupname;
    Case =:= get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_twice;
    Case =:= get_group_id_on_posix_storage_by_acl_groupname_should_query_reverse_luma_once->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client, storage]),
    mock_resolve_acl_group_post(Worker,
        {
            ok, 200, [], str_utils:format_bin("{
                \"idp\": \"~s\",
                \"groupId\": \"~s\"\}", [?TEST_PROVIDER_ID, ?TEST_GROUP_ID])
        }
    ),
    mock_idp_group_mapping(Worker,
        ?TEST_PROVIDER_ID, ?TEST_GROUP_ID,
        ?TEST_MAPPED_GROUP_ID
    ),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_group_id_on_posix_storage_should_fail_with_404_error, Config)  ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client]),
    mock_resolve_group_post(Worker, {ok, 404, [], <<"{\"error\": \"reason\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = get_group_id_on_posix_storage_by_acl_groupname_should_fail_with_404_error, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [http_client]),
    mock_resolve_acl_group_post(Worker, {ok, 404, [], <<"{\"error\": \"reason\"\}">>}),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    ok = rpc:call(Worker, luma_cache, invalidate, []),
    test_utils:mock_unload(Workers, [http_client, reverse_luma_proxy, provider_logic]).

mock_resolve_acl_user_post(Worker, Expected) ->
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_list(Url) ->
            case lists:last(string:tokens(Url, "/")) of
                "resolve_acl_user" ->
                    Expected;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).

mock_resolve_user_post(Worker, Expected) ->
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_list(Url) ->
            case lists:last(string:tokens(Url, "/")) of
                "resolve_user" ->
                    Expected;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).

mock_resolve_group_post(Worker, ExpectedLuma) ->
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_list(Url) ->
            case lists:last(string:tokens(Url, "/")) of
                "resolve_group" ->
                    ExpectedLuma;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).

mock_resolve_acl_group_post(Worker, ExpectedLuma) ->
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_list(Url) ->
            case lists:last(string:tokens(Url, "/")) of
                "resolve_acl_group" ->
                    ExpectedLuma;
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).

mock_idp_group_mapping(Worker, IdP, IdpGroupId, GroupId) ->
    test_utils:mock_expect(Worker, provider_logic, map_idp_group_to_onedata,
        fun(IdPArg, IdpGroupIdArg) ->
            case {IdPArg, IdpGroupIdArg} of
                {IdP, IdpGroupId} ->
                    {ok, GroupId};
                _ ->
                    ?ERROR_BAD_VALUE_ID_NOT_FOUND(IdP)
            end
        end).
