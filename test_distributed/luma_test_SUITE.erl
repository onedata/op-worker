%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests LUMA
%%% @end
%%%--------------------------------------------------------------------
-module(luma_test_SUITE).
-author("Michal Wrona").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    get_server_user_ctx_should_fail_with_missing_helper_error/1,
    get_server_user_ctx_should_return_admin_ctx/1,
    get_server_user_ctx_should_skip_fetch_when_luma_server_disabled/1,
    get_server_user_ctx_should_fetch_user_ctx_from_luma_server/1,
    get_server_user_ctx_should_fail_with_invalid_fetch_user_ctx/1,
    get_server_user_ctx_should_fail_when_luma_server_enabled_and_ctx_not_fetched/1,
    get_server_user_ctx_should_generate_user_ctx/1,
    get_server_user_ctx_should_fallback_to_admin_ctx/1,
    get_client_user_ctx_should_fetch_user_ctx_from_luma_server/1,
    get_client_user_ctx_should_return_insecure_user_ctx/1,
    get_client_user_ctx_should_fail_with_undefined_user_ctx/1,
    get_posix_user_ctx_should_return_server_user_ctx/1,
    get_posix_user_ctx_should_generate_user_ctx/1,
    get_posix_user_ctx_should_fetch_user_ctx/1,
    get_posix_user_ctx_should_fetch_user_ctx_twice/1,
    get_posix_user_ctx_should_fetch_user_ctx_by_group_id/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_when_luma_returns_null_and_group_is_undefined/1,
    get_posix_user_ctx_by_group_id_should_return_0_for_root/1]).

all() ->
    ?ALL([
        get_server_user_ctx_should_fail_with_missing_helper_error,
        get_server_user_ctx_should_return_admin_ctx,
        get_server_user_ctx_should_skip_fetch_when_luma_server_disabled,
        get_server_user_ctx_should_fetch_user_ctx_from_luma_server,
        get_server_user_ctx_should_fail_with_invalid_fetch_user_ctx,
        get_server_user_ctx_should_fail_when_luma_server_enabled_and_ctx_not_fetched,
        get_server_user_ctx_should_generate_user_ctx,
        get_server_user_ctx_should_fallback_to_admin_ctx,
        get_client_user_ctx_should_fetch_user_ctx_from_luma_server,
        get_client_user_ctx_should_return_insecure_user_ctx,
        get_client_user_ctx_should_fail_with_undefined_user_ctx,
        get_posix_user_ctx_should_return_server_user_ctx,
        get_posix_user_ctx_should_generate_user_ctx,
        get_posix_user_ctx_should_fetch_user_ctx,
        get_posix_user_ctx_should_fetch_user_ctx_twice,
        get_posix_user_ctx_should_fetch_user_ctx_by_group_id,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_when_luma_returns_null_and_group_is_undefined
    ]).

-define(TEST_URL, <<"http://127.0.0.1:5000">>).
-define(DEFAULT_TIMEOUT, timer:minutes(5)).


-define(MOCK_SESS_ID, <<"sessionId">>).
-define(MOCK_USER_ID, <<"userId">>).


-define(LUMA_CONFIG, ?LUMA_CONFIG(?DEFAULT_TIMEOUT)).
-define(LUMA_CONFIG(CacheTimeout), #luma_config{
    url = ?TEST_URL,
    cache_timeout = CacheTimeout,
    api_key = <<"test_api_key">>
}).


-define(POSIX_STORAGE_DOC_DISABLED_LUMA, #document{
    key = <<"posixStorageId">>,
    value = #storage{
        name = <<"POSIX">>,
        helpers = [helper:new_posix_helper(
            <<"mountPoint">>,
            #{},
            helper:new_posix_user_ctx(0, 0),
            ?CANONICAL_STORAGE_PATH
        )],
        luma_config = undefined
    }
}).

-define(POSIX_STORAGE_DOC, #document{
    key = <<"posixStorageId">>,
    value = #storage{
        name = <<"POSIX">>,
        helpers = [helper:new_posix_helper(
            <<"mountPoint">>,
            #{},
            helper:new_posix_user_ctx(0, 0),
            ?CANONICAL_STORAGE_PATH
        )],
        luma_config = ?LUMA_CONFIG
    }
}).

-define(POSIX_STORAGE_DOC(LumaConfig), #document{
    key = <<"posixStorageId">>,
    value = #storage{
        name = <<"POSIX">>,
        helpers = [helper:new_posix_helper(
            <<"mountPoint">>,
            #{},
            helper:new_posix_user_ctx(0, 0),
            ?CANONICAL_STORAGE_PATH
        )],
        luma_config = LumaConfig
    }
}).

-define(CEPH_STORAGE_DOC(Insecure), #document{
    key = <<"cephStorageId">>,
    value = #storage{
        name = <<"CEPH">>,
        helpers = [helper:new_ceph_helper(
            <<"monitorHostname">>,
            <<"clusterName">>,
            <<"poolName">>,
            #{},
            helper:new_ceph_user_ctx(<<"username">>, <<"key">>),
            Insecure,
            ?FLAT_STORAGE_PATH
        )],
        luma_config = ?LUMA_CONFIG
    }
}).


-define(CEPH_STORAGE_DOC(Insecure, LumaConfig), #document{
    key = <<"cephStorageId">>,
    value = #storage{
        name = <<"CEPH">>,
        helpers = [helper:new_ceph_helper(
            <<"monitorHostname">>,
            <<"clusterName">>,
            <<"poolName">>,
            #{},
            helper:new_ceph_user_ctx(<<"username">>, <<"key">>),
            Insecure,
            ?FLAT_STORAGE_PATH
        )],
        luma_config = LumaConfig
    }
}).

-define(CEPH_STORAGE_DOC_LUMA_DISABLED(Insecure), #document{
    key = <<"cephStorageId">>,
    value = #storage{
        name = <<"CEPH">>,
        helpers = [helper:new_ceph_helper(
            <<"monitorHostname">>,
            <<"clusterName">>,
            <<"poolName">>,
            #{},
            helper:new_ceph_user_ctx(<<"username">>, <<"key">>),
            Insecure,
            ?FLAT_STORAGE_PATH
        )],
        luma_config = undefined
    }
}).

-define(SPACE_STORAGE_DOC(StorageIds), #space_storage{storage_ids = [StorageIds]}).

-define(UID1, 1).
-define(GID1, 1).
-define(GID2, 2).
-define(GID_NULL, <<"null">>).

-define(SPACE_ID, <<"spaceId">>).
-define(GROUP_ID, <<"groupId">>).
-define(GID_RANGE, {100000, 2000000}).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_server_user_ctx_should_fail_with_missing_helper_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID,
        ?ROOT_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC,
        <<"helperName">>
    ]),
    ?assertEqual({error, not_found}, Result).

get_server_user_ctx_should_return_admin_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID,
        ?ROOT_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC,
        ?POSIX_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"uid">> := <<"0">>}}, Result),
    ?assertMatch({ok, #{<<"gid">> := <<"0">>}}, Result).

get_server_user_ctx_should_skip_fetch_when_luma_server_disabled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, http_client, [passthrough]),
    rpc:call(Worker, luma, get_server_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC_DISABLED_LUMA,
        ?POSIX_HELPER_NAME
    ]),
    test_utils:mock_assert_num_calls(Worker, http_client, post, ['_', '_', '_'], 0).

get_server_user_ctx_should_fetch_user_ctx_from_luma_server(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], <<"{\"uid\": \"1\",\"gid\": \"2\"}">>}
    end),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC,
        ?POSIX_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"uid">> := <<"1">>}}, Result),
    ?assertMatch({ok, #{<<"gid">> := <<"2">>}}, Result).

get_server_user_ctx_should_fail_with_invalid_fetch_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, http_client),
    lists:foreach(fun({ResponseBody, Reason}) ->
        test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
            {ok, 200, [], ResponseBody}
        end),
        Result = rpc:call(Worker, luma, get_server_user_ctx, [
            ?MOCK_SESS_ID,
            ?MOCK_USER_ID,
            <<"spaceId">>,
            ?POSIX_STORAGE_DOC,
            ?POSIX_HELPER_NAME
        ]),
        ?assertEqual({error, {luma_server, Reason}}, Result)
    end, [
        {<<"{\"gid\": 2}">>, {missing_field, <<"uid">>}},
        {<<"{\"uid\": \[1,2,3\],\"gid\": 2}">>, {invalid_field_value, <<"uid">>, [1,2,3]}},
        {<<"{\"uid\": \"null\",\"gid\": 2}">>, {invalid_field_value, <<"uid">>, <<"null">>}},
        {<<"{\"uid\": null,\"gid\": 2}">>, {invalid_field_value, <<"uid">>, null}},
        {<<"{\"uid\": 1,\"gid\": 2,\"other\": \"value\"}">>,
            {invalid_additional_fields, #{<<"other">> => <<"value">>}}}
    ]).

get_server_user_ctx_should_fail_when_luma_server_enabled_and_ctx_not_fetched(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC,
        ?POSIX_HELPER_NAME
    ]),
    ?assertEqual({error, {luma_server, econnrefused}}, Result).

get_server_user_ctx_should_generate_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?POSIX_STORAGE_DOC_DISABLED_LUMA,
        ?POSIX_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"uid">> := _, <<"gid">> := _}}, Result).

get_server_user_ctx_should_fallback_to_admin_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?CEPH_STORAGE_DOC_LUMA_DISABLED(true),
        ?CEPH_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"username">> := <<"username">>}}, Result),
    ?assertMatch({ok, #{<<"key">> := <<"key">>}}, Result).

get_client_user_ctx_should_fetch_user_ctx_from_luma_server(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], <<"{\"username\": \"user1\",\"key\": \"key1\"}">>}
    end),
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?CEPH_STORAGE_DOC(true),
        ?CEPH_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"username">> := <<"user1">>}}, Result),
    ?assertMatch({ok, #{<<"key">> := <<"key1">>}}, Result).

get_client_user_ctx_should_return_insecure_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?CEPH_STORAGE_DOC_LUMA_DISABLED(true),
        ?CEPH_HELPER_NAME
    ]),
    ?assertMatch({ok, #{<<"username">> := <<"username">>}}, Result),
    ?assertMatch({ok, #{<<"key">> := <<"key">>}}, Result).

get_client_user_ctx_should_fail_with_undefined_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>,
        ?CEPH_STORAGE_DOC_LUMA_DISABLED(false),
        ?CEPH_HELPER_NAME
    ]),
    ?assertEqual({error, undefined_user_context}, Result).

get_posix_user_ctx_should_return_server_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC_DISABLED_LUMA}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),
    test_utils:mock_assert_num_calls(Worker, luma, get_server_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assert(is_integer(Uid)),
    ?assert(is_integer(Gid)).

get_posix_user_ctx_should_generate_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?CEPH_STORAGE_DOC(true)}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assert(is_integer(Uid)),
    ?assert(is_integer(Gid)).

get_posix_user_ctx_should_fetch_user_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assert(is_integer(Uid)),
    ?assert(is_integer(Gid)).

get_posix_user_ctx_should_fetch_user_ctx_twice(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    CacheTimeout = 5,
    LumaConfig = ?LUMA_CONFIG(CacheTimeout),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC(LumaConfig)}
    end),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),
    timer:sleep(CacheTimeout + 1),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        <<"spaceId">>
    ]),

    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 2),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(?GID1, Gid).

get_posix_user_ctx_should_fetch_user_ctx_by_group_id(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        ?GROUP_ID,
        <<"spaceId">>
    ]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(?GID2, Gid).


get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        ?GROUP_ID,
        ?SPACE_ID
    ]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(generate_posix_identifier(?GROUP_ID, ?GID_RANGE), Gid).

get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_when_luma_returns_null_and_group_is_undefined(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?MOCK_SESS_ID,
        ?MOCK_USER_ID,
        undefined,
        ?SPACE_ID
    ]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(generate_posix_identifier(?SPACE_ID, ?GID_RANGE), Gid).

get_posix_user_ctx_by_group_id_should_return_0_for_root(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, ?POSIX_STORAGE_DOC}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        <<"0">>,
        undefined,
        ?SPACE_ID
    ]),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(0, Uid),
    ?assertEqual(0, Gid).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Case, Config) when
    Case =:= get_posix_user_ctx_should_return_server_user_ctx;
    Case =:= get_posix_user_ctx_should_generate_user_ctx ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [space_storage, storage, user_logic]),
    test_utils:mock_expect(Worker, space_storage, get, fun(_) ->
        {ok, #document{value = #space_storage{storage_ids = [<<"storageId">>]}}}
    end),
    test_utils:mock_expect(Worker, user_logic, get_protected_data,
        fun(?MOCK_SESS_ID, ?MOCK_USER_ID) ->
            {ok, #document{value = #od_user{
                name = <<"whatever">>,
                login = <<"whatever">>,
                email_list = [<<"whatever">>],
                linked_accounts = []
            }}}
        end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= get_posix_user_ctx_should_fetch_user_ctx;
    Case =:= get_posix_user_ctx_should_fetch_user_ctx_twice ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [space_storage, storage, http_client]),
    test_utils:mock_expect(Worker, space_storage, get, fun(_) ->
        {ok, ?SPACE_STORAGE_DOC([<<"storage_id">>])}
    end),
    Expected = json_utils:encode_map(#{<<"uid">> => ?UID1, <<"gid">> => ?GID1}),
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_binary(Url) ->
            case lists:last(binary:split(Url, <<"/">>, [global])) of
                <<"map_user_credentials">> ->
                    {ok, 200, [], Expected};
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);


init_per_testcase(Case, Config) when
    Case =:= get_posix_user_ctx_should_fetch_user_ctx_by_group_id->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [space_storage, storage, http_client]),
    test_utils:mock_expect(Worker, space_storage, get, fun(_) ->
        {ok, ?SPACE_STORAGE_DOC([<<"storage_id">>])}
    end),
    Expected = json_utils:encode_map(#{<<"uid">> => ?UID1, <<"gid">> => ?GID1}),
    Expected2 = json_utils:encode_map(#{<<"gid">> => ?GID2}),
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_binary(Url) ->
            case lists:last(binary:split(Url, <<"/">>, [global])) of
                <<"map_user_credentials">> ->
                    {ok, 200, [], Expected};
                <<"map_group">> ->
                    {ok, 200, [], Expected2};
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);


init_per_testcase(Case, Config) when
    Case =:= get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_when_luma_returns_null_and_group_is_undefined;
    Case =:= get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found->

    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [space_storage, storage, http_client]),
    test_utils:mock_expect(Worker, space_storage, get, fun(_) ->
        {ok, ?SPACE_STORAGE_DOC([<<"storage_id">>])}
    end),
    Expected = json_utils:encode_map(#{<<"uid">> => ?UID1, <<"gid">> => ?GID1}),
    Expected2 = json_utils:encode_map(#{<<"error">> => <<"mapping not found">>}),
    test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_binary(Url) ->
            case lists:last(binary:split(Url, <<"/">>, [global])) of
                <<"map_user_credentials">> ->
                    {ok, 200, [], Expected};
                <<"map_group">> ->
                    {ok, 404, [], Expected2};
                _ ->
                    meck:passthrough([Url, Headers, Body])
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Case, Config) when
    Case =:= get_posix_user_ctx_should_return_server_user_ctx;
    Case =:= get_posix_user_ctx_should_return_server_user_ctx;
    Case =:= get_posix_user_ctx_should_fetch_user_ctx;
    Case =:= get_posix_user_ctx_should_fetch_user_ctx_twice ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [space_storage, storage, user_logic]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    rpc:call(Worker, luma_cache, invalidate, []),
    test_utils:mock_unload(Workers, [http_client, luma_proxy]).

generate_posix_identifier(Id, {Low, High}) ->
    PosixId = crypto:bytes_to_integer(Id),
    Low + (PosixId rem (High - Low)).