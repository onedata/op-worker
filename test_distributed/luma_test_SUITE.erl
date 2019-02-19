%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @author Jakub Kudzia
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
-author("Jakub Kudzia").

-include("luma_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2, init_per_suite/1, end_per_suite/1]).

-export([
    get_server_user_ctx_fails_due_to_not_existing_helper/1,
    insecure_get_server_user_ctx_for_root_returns_admin_ctx/1,
    insecure_get_server_user_ctx_for_root_returns_admin_ctx_invalidate_cache/1,
    insecure_get_server_user_ctx_should_not_query_luma/1,
    secure_get_server_user_ctx_fetches_user_ctx_from_luma/1,
    get_server_user_ctx_fails_on_invalid_response_from_luma/1,
    secure_get_server_user_ctx_fails_when_luma_server_is_unavailable/1,
    get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping/1,
    get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache/1,
    insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages/1,
    insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache/1,
    secure_get_client_user_ctx_fetches_user_ctx_from_luma/1,
    secure_get_client_user_ctx_fetches_user_ctx_from_luma_invalidate_cache/1,
    insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages/1,
    insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache/1,
    get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping/1,
    get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache/1,
    insecure_get_posix_user_ctx_should_generate_user_ctx/1,
    secure_get_posix_user_ctx_should_return_server_user_ctx_on_posix_compatible_storages/1,
    secure_get_posix_user_ctx_should_generate_user_ctx_on_posix_incompatible_storages/1,
    get_posix_user_ctx_should_fetch_user_ctx_by_group_id_once/1,
    get_posix_user_ctx_should_fetch_user_ctx_by_group_id_twice/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping_invalidate_cache/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined/1,
    get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_invalidate_cache/1,
    get_posix_user_ctx_by_group_id_should_return_0_for_root/1]).

% utils
-export([generate_posix_identifier/2]).

all() ->
    ?ALL([
        get_server_user_ctx_fails_due_to_not_existing_helper,
        insecure_get_server_user_ctx_for_root_returns_admin_ctx,
        insecure_get_server_user_ctx_for_root_returns_admin_ctx_invalidate_cache,
        insecure_get_server_user_ctx_should_not_query_luma,
        secure_get_server_user_ctx_fetches_user_ctx_from_luma,
        get_server_user_ctx_fails_on_invalid_response_from_luma,
        secure_get_server_user_ctx_fails_when_luma_server_is_unavailable,
        get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping,
        get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache,
        insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages,
        insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache,
        secure_get_client_user_ctx_fetches_user_ctx_from_luma,
        secure_get_client_user_ctx_fetches_user_ctx_from_luma_invalidate_cache,
        insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages,
        insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache,
        get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping,
        get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache,
        insecure_get_posix_user_ctx_should_generate_user_ctx,
        secure_get_posix_user_ctx_should_return_server_user_ctx_on_posix_compatible_storages,
        secure_get_posix_user_ctx_should_generate_user_ctx_on_posix_incompatible_storages,
        get_posix_user_ctx_should_fetch_user_ctx_by_group_id_once,
        get_posix_user_ctx_should_fetch_user_ctx_by_group_id_twice,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined,
        get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_invalidate_cache,
        get_posix_user_ctx_by_group_id_should_return_0_for_root
    ]).

-define(SPACE_STORAGE_DOC(StorageIds), #space_storage{storage_ids = [StorageIds]}).

-define(TEST_BASE,
    binary_to_atom(<<(atom_to_binary(?FUNCTION_NAME, latin1))/binary, "_base">>, latin1)).

-define(RUN(Config, StorageConfigs, TestFun),
    run_test_for_all_storage_configs(?TEST_BASE, TestFun, Config, StorageConfigs)).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_server_user_ctx_fails_due_to_not_existing_helper(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun get_server_user_ctx_fails_due_to_not_existing_helper_base/2
    ).

insecure_get_server_user_ctx_for_root_returns_admin_ctx(Config) ->
    ?RUN(Config, ?INSECURE_STORAGE_CONFIGS,
        fun insecure_get_server_user_ctx_for_root_returns_admin_ctx_base/2
    ).

insecure_get_server_user_ctx_for_root_returns_admin_ctx_invalidate_cache(Config) ->
    ?RUN(Config, ?INSECURE_STORAGE_CONFIGS,
        fun insecure_get_server_user_ctx_for_root_returns_admin_ctx_invalidate_cache_base/2
    ).

insecure_get_server_user_ctx_should_not_query_luma(Config) ->
    ?RUN(Config, ?INSECURE_STORAGE_CONFIGS,
        fun insecure_get_server_user_ctx_should_not_query_luma_base/2
    ).

secure_get_server_user_ctx_fetches_user_ctx_from_luma(Config) ->
    ?RUN(Config, ?SECURE_STORAGE_CONFIGS,
        fun secure_get_server_user_ctx_fetches_user_ctx_from_luma_base/2
    ).

get_server_user_ctx_fails_on_invalid_response_from_luma(Config) ->
    ?RUN(Config, ?SECURE_STORAGE_CONFIGS,
        fun get_server_user_ctx_fails_on_invalid_response_from_luma_base/2
    ).

secure_get_server_user_ctx_fails_when_luma_server_is_unavailable(Config) ->
    ?RUN(Config, ?SECURE_STORAGE_CONFIGS,
        fun secure_get_server_user_ctx_fails_when_luma_server_is_unavailable_base/2
    ).

get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping_base/2
    ).

get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache_base/2
    ).

insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_base/2
    ).

insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache_base/2
    ).

secure_get_client_user_ctx_fetches_user_ctx_from_luma(Config) ->
    ?RUN(Config, ?SECURE_STORAGE_CONFIGS,
        fun secure_get_client_user_ctx_fetches_user_ctx_from_luma_base/2
    ).

secure_get_client_user_ctx_fetches_user_ctx_from_luma_invalidate_cache(Config) ->
    ?RUN(Config, ?SECURE_STORAGE_CONFIGS,
        fun secure_get_client_user_ctx_fetches_user_ctx_from_luma_invalidate_cache_base/2
    ).

insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_base/2
    ).

insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache_base/2
    ).

get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping_base/2
    ).

get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache_base/2
    ).

insecure_get_posix_user_ctx_should_generate_user_ctx(Config) ->
    ?RUN(Config, ?INSECURE_STORAGE_CONFIGS,
        fun insecure_get_posix_user_ctx_should_generate_user_ctx_base/2
    ).

secure_get_posix_user_ctx_should_return_server_user_ctx_on_posix_compatible_storages(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun secure_secure_get_posix_user_ctx_should_return_server_user_ctx_on_posix_compatible_storages_base/2
    ).

secure_get_posix_user_ctx_should_generate_user_ctx_on_posix_incompatible_storages(Config) ->
    ?RUN(Config, ?INSECURE_POSIX_INCOMPATIBLE_STORAGE_CONFIGS,
        fun secure_get_posix_user_ctx_should_generate_user_ctx_on_posix_incompatible_storages_base/2
    ).

get_posix_user_ctx_should_fetch_user_ctx_by_group_id_once(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_should_fetch_user_ctx_by_group_id_once_base/2
    ).

get_posix_user_ctx_should_fetch_user_ctx_by_group_id_twice(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_should_fetch_user_ctx_by_group_id_twice_base/2
    ).

get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping_base/2
    ).

get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping_invalidate_cache(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping__invalidate_cache_base/2
    ).

get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_base/2
    ).

get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_invalidate_cache(Config) ->
    ?RUN(Config, ?SECURE_POSIX_COMPATIBLE_STORAGE_CONFIGS,
        fun get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_invalidate_cache_base/2
    ).

get_posix_user_ctx_by_group_id_should_return_0_for_root(Config) ->
    ?RUN(Config, ?ALL_STORAGE_CONFIGS,
        fun get_posix_user_ctx_by_group_id_should_return_0_for_root_base/2
    ).

%%%===================================================================
%%% Test bases
%%%===================================================================

get_server_user_ctx_fails_due_to_not_existing_helper_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, StorageDoc, <<"DUMMY_HELPER">>]),
    ?assertEqual({error, not_found}, Result).

insecure_get_server_user_ctx_for_root_returns_admin_ctx_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    ExpectedCtx = maps:get(expected_admin_ctx, StorageConfig, AdminCtx),
    ok = test_utils:mock_new(Worker, luma, [passthrough]),

    % get ctx for the 1st time
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    % get ctx for the 2nd time, now it should be served from cache
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    % AdminCtx should be served from cache
    ?assertEqual({ok, ExpectedCtx}, Result),
    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx, ['_', '_'], 1).

insecure_get_server_user_ctx_for_root_returns_admin_ctx_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    ExpectedCtx = maps:get(expected_admin_ctx, StorageConfig, AdminCtx),
    StorageId = storage:get_id(StorageDoc),
    test_utils:mock_new(Worker, luma, [passthrough]),

    % get ctx for the 1st time
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    invalidate_luma_cache(Worker, StorageId),

    % get ctx for the 2nd time, it shouldn't be served from cache as it was invalidated
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx, ['_', '_'], 2),
    ?assertEqual({ok, ExpectedCtx}, Result).

insecure_get_server_user_ctx_should_not_query_luma_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    test_utils:mock_new(Worker, http_client, [passthrough]),
    rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, http_client, post, ['_', '_', '_'], 0).

secure_get_server_user_ctx_fetches_user_ctx_from_luma_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    UserCtx = maps:get(user_ctx, StorageConfig),
    UserCtxLumaMock = maps:get(user_ctx_luma_mock, StorageConfig, UserCtx),
    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], json_utils:encode(UserCtxLumaMock)}
    end),

    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    ?assertEqual({ok, UserCtx}, Result).

get_server_user_ctx_fails_on_invalid_response_from_luma_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    test_utils:mock_new(Worker, http_client),
    lists:foreach(fun({ResponseBody, Reason}) ->
        test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
            {ok, 200, [], ResponseBody}
        end),
        Result = rpc:call(Worker, luma, get_server_user_ctx,
            [?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
        ?assertEqual({error, {luma_server, Reason}}, Result)
    end, maps:get(HelperName, ?HELPERS_TO_ERRONEOUS_LUMA_RESPONSES_MAP)).

secure_get_server_user_ctx_fails_when_luma_server_is_unavailable_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    ?assertEqual({error, {luma_server, econnrefused}}, Result).

get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [reverse_luma_proxy, luma], [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),

    % 1st call should call generate_user_ctx
    Result = rpc:call(Worker, luma, get_server_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    % 2nd call should serve ctx from cache
    Result = rpc:call(Worker, luma, get_server_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    ExpectedUid = integer_to_binary(generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE)),

    % reverse mapping should be served from cache
    Result2 = rpc:call(Worker, reverse_luma, get_user_id, [ExpectedUid, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_', '_'], 0),

    ?assertEqual(Result2, {ok, ?USER_ID}),
    ?assertEqual({ok, ?GENERATED_POSIX_USER_CTX}, Result).

get_server_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma, [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    StorageId = storage:get_id(StorageDoc),
    HelperName = maps:get(helper_name, StorageConfig),
    % 1st call should call generate_user_ctx
    Result = rpc:call(Worker, luma, get_server_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    invalidate_luma_cache(Worker, StorageId),

    % 2nd call should call generate_user_ctx too
    Result = rpc:call(Worker, luma, get_server_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 2),

    ?assertEqual({ok, ?GENERATED_POSIX_USER_CTX}, Result).

insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma, [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    % 1st call should call get_admin_ctx
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx,
        [?ROOT_SESS_ID, '_'], 1),
    
    % 2nd call should be served from cache
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx,
        [?ROOT_SESS_ID, '_'], 1),
    
    ?assertEqual({ok, AdminCtx}, Result).

insecure_get_server_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma, [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    StorageId = storage:get_id(StorageDoc),

    % 1st call should call get_admin_ctx
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx,
        [?ROOT_SESS_ID, '_'], 1),

    invalidate_luma_cache(Worker, StorageId),

    % 2nd call should call get_admin_ctx too
    Result = rpc:call(Worker, luma, get_server_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_admin_ctx,
        [?ROOT_SESS_ID, '_'], 2),

    ?assertEqual({ok, AdminCtx}, Result).

secure_get_client_user_ctx_fetches_user_ctx_from_luma_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    UserCtx = maps:get(user_ctx, StorageConfig),
    UserCtxLumaMock = maps:get(user_ctx_luma_mock, StorageConfig, UserCtx),
    test_utils:mock_new(Worker, [luma_proxy, http_client], [passthrough]),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], json_utils:encode(UserCtxLumaMock)}
    end),

    % 1st call should query luma
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),

    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),

    % 2nd call should be served from cache
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),

    ?assertMatch({ok, UserCtx}, Result).

secure_get_client_user_ctx_fetches_user_ctx_from_luma_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    UserCtx = maps:get(user_ctx, StorageConfig),
    UserCtxLumaMock = maps:get(user_ctx_luma_mock, StorageConfig, UserCtx),
    StorageId = storage:get_id(StorageDoc),
    test_utils:mock_new(Worker, [luma_proxy, http_client], [passthrough]),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], json_utils:encode(UserCtxLumaMock)}
    end),

    % 1st call should query luma
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),

    invalidate_luma_cache(Worker, StorageId),

    % 2nd call should query luma too
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 2),

    ?assertMatch({ok, UserCtx}, Result).

insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    test_utils:mock_new(Worker, [luma], [passthrough]),
    
    % 1st call should call get_insecure_user_ctx
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_insecure_user_ctx,
        ['_'], 1),
    
    % 2nd call should be served from cache
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_insecure_user_ctx,
        ['_'], 1),
    
    ?assertEqual({ok, AdminCtx}, Result).

insecure_get_client_user_ctx_returns_admin_ctx_on_posix_incompatible_storages_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    AdminCtx = maps:get(admin_ctx, StorageConfig),
    StorageId = storage:get_id(StorageDoc),
    test_utils:mock_new(Worker, [luma], [passthrough]),

    % 1st call should call get_insecure_user_ctx
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_insecure_user_ctx,
        ['_'], 1),

    invalidate_luma_cache(Worker, StorageId),

    % 2nd call should call get_insecure_user_ctx too
    Result = rpc:call(Worker, luma, get_client_user_ctx, [
        ?SESS_ID, ?USER_ID, ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, get_insecure_user_ctx,
        ['_'], 2),
    
    ?assertEqual({ok, AdminCtx}, Result).

get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_and_caches_reverse_mapping_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [reverse_luma_proxy, luma], [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),

    % 1st call should call generate_user_ctx
    Result = rpc:call(Worker, luma, get_client_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    % 2nd call should serve ctx from cache
    Result = rpc:call(Worker, luma, get_client_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    ExpectedUid = integer_to_binary(generate_posix_identifier(?USER_ID, ?POSIX_ID_RANGE)),

    % reverse mapping should be served from cache
    Result2 = rpc:call(Worker, reverse_luma, get_user_id, [ExpectedUid, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_user_id,
        ['_', '_', '_', '_', '_'], 0),

    ?assertEqual(Result2, {ok, ?USER_ID}),
    ?assertEqual({ok, ?GENERATED_POSIX_USER_CTX}, Result).

get_client_user_ctx_generates_user_ctx_on_posix_compatible_storages_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, luma, [passthrough]),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    HelperName = maps:get(helper_name, StorageConfig),
    StorageId = storage:get_id(StorageDoc),

    % 1st call should call generate_user_ctx
    Result = rpc:call(Worker, luma, get_client_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 1),

    invalidate_luma_cache(Worker, StorageId),

    % 2nd call should call generate_user_ctx too
    Result = rpc:call(Worker, luma, get_client_user_ctx, [?SESS_ID, ?USER_ID,
        ?SPACE_ID, StorageDoc, HelperName]),
    test_utils:mock_assert_num_calls(Worker, luma, generate_user_ctx,
        ['_', '_'], 2),

    ?assertEqual({ok, ?GENERATED_POSIX_USER_CTX}, Result).

insecure_get_posix_user_ctx_should_generate_user_ctx_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    ok = test_utils:mock_new(Worker, [storage]),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID, ?SPACE_ID]),
    ?assertEqual(?POSIX_CTX_TU_TUPLE(?GENERATED_POSIX_USER_CTX), Result).

secure_secure_get_posix_user_ctx_should_return_server_user_ctx_on_posix_compatible_storages_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    UserCtx = maps:get(user_ctx, StorageConfig),
    ok = test_utils:mock_new(Worker, [luma, http_client], [passthrough]),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], json_utils:encode(UserCtx)}
    end),
    ok = test_utils:mock_new(Worker, storage),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID, ?SPACE_ID]),
    ?assertEqual(?POSIX_CTX_TU_TUPLE(UserCtx), Result).

secure_get_posix_user_ctx_should_generate_user_ctx_on_posix_incompatible_storages_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    UserCtx = maps:get(user_ctx, StorageConfig),
    ok = test_utils:mock_new(Worker, [luma, http_client], [passthrough]),
    test_utils:mock_expect(Worker, http_client, post, fun(_, _, _) ->
        {ok, 200, [], json_utils:encode(UserCtx)}
    end),
    ok = test_utils:mock_new(Worker, storage),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID, ?SPACE_ID]),
    ?assertEqual(?POSIX_CTX_TU_TUPLE(?GENERATED_POSIX_USER_CTX), Result).

get_posix_user_ctx_should_fetch_user_ctx_by_group_id_once_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    test_utils:mock_new(Worker, [luma_proxy, luma], [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(#{
            <<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 200, [], json_utils:encode(
            #{<<"gid">> => ?GID2})
        }
    }),

    % 1st call should fetch ctx from luma
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID,
        ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),

    % 2nd call should be serverd from cache
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID,
        ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(?GID2, Gid).

get_posix_user_ctx_should_fetch_user_ctx_by_group_id_twice_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    StorageId = storage:get_id(StorageDoc),
    test_utils:mock_new(Worker, [luma_proxy, luma], [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, StorageDoc}
    end),

    ok = mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(#{
            <<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 200, [], json_utils:encode(
            #{<<"gid">> => ?GID2})
        }
    }),
    % 1st call should fetch ctx from luma
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID,
        ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),

    ok = invalidate_luma_cache(Worker, StorageId),

    % 2nd call should fetch ctx from luma too
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [?SESS_ID, ?USER_ID,
        ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 2),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 2),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(?GID2, Gid).

get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    ok = test_utils:mock_new(Worker, [luma, luma_proxy, reverse_luma_proxy, storage], [passthrough]),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(
            #{<<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 404, [], json_utils:encode(
            #{<<"error">> => <<"mapping not found">>})
        }
    }),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 1),

    % reverse mapping should be served from cache
    ExpectedGid = generate_posix_identifier(?GROUP_ID, ?POSIX_ID_RANGE),
    Result2 = rpc:call(Worker, reverse_luma, get_group_id, [
        ExpectedGid, ?SPACE_ID, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 0),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(ExpectedGid, Gid),
    ?assertEqual(Result2, {ok, ?GROUP_ID}).

get_posix_user_ctx_by_group_id_should_generate_gid_by_group_id_when_mapping_is_not_found_and_cache_reverse_mapping__invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    StorageId = storage:get_id(StorageDoc),
    ok = test_utils:mock_new(Worker, [luma, luma_proxy, reverse_luma_proxy, storage], [passthrough]),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(
            #{<<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 404, [], json_utils:encode(
            #{<<"error">> => <<"mapping not found">>})
        }
    }),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 1),

    ExpectedGid = generate_posix_identifier(?GROUP_ID, ?POSIX_ID_RANGE),
    % reverse mapping should be served from cache
    Result2 = rpc:call(Worker, reverse_luma, get_group_id, [
        ExpectedGid, ?SPACE_ID, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 0),

    invalidate_luma_cache(Worker, StorageId),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, ?GROUP_ID, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 2),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 2),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 2),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(ExpectedGid, Gid),
    ?assertEqual(Result2, {ok, ?GROUP_ID}).

get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    ok = test_utils:mock_new(Worker, [luma, luma_proxy, reverse_luma_proxy, storage], [passthrough]),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(
            #{<<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 404, [], json_utils:encode(
            #{<<"error">> => <<"mapping not found">>})
        }
    }),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, undefined, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 1),

    % reverse mapping should be served from cache
    ExpectedGid = generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE),
    Result2 = rpc:call(Worker, reverse_luma, get_group_id, [
        ExpectedGid, ?SPACE_ID, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 0),

    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 0),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(ExpectedGid, Gid),
    ?assertEqual(Result2, {ok, ?SPACE_ID}).

get_posix_user_ctx_by_group_id_should_generate_gid_by_space_id_once_when_mapping_is_not_found_and_group_is_undefined_invalidate_cache_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    StorageId = storage:get_id(StorageDoc),
    ok = test_utils:mock_new(Worker, [luma, luma_proxy, reverse_luma_proxy, storage], [passthrough]),
    ok = test_utils:mock_expect(Worker, storage, get, fun(_) -> {ok, StorageDoc} end),

    mock_luma_response(Worker, #{
        <<"map_user_credentials">> => {ok, 200, [], json_utils:encode(
            #{<<"uid">> => ?UID1, <<"gid">> => ?GID1})
        },
        <<"map_group">> => {ok, 404, [], json_utils:encode(
            #{<<"error">> => <<"mapping not found">>})
        }
    }),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, undefined, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 1),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 1),

    ExpectedGid = generate_posix_identifier(?SPACE_ID, ?POSIX_ID_RANGE),
    % reverse mapping should be served from cache
    Result2 = rpc:call(Worker, reverse_luma, get_group_id, [
        ExpectedGid, ?SPACE_ID, StorageDoc]),
    test_utils:mock_assert_num_calls(Worker, reverse_luma_proxy, get_group_id,
        ['_', '_', '_', '_', '_'], 0),

    invalidate_luma_cache(Worker, StorageId),

    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?SESS_ID, ?USER_ID, undefined, ?SPACE_ID]),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_user_ctx,
        ['_', '_', '_', '_', '_'], 2),
    test_utils:mock_assert_num_calls(Worker, luma_proxy, get_group_ctx,
        ['_', '_', '_', '_'], 2),
    test_utils:mock_assert_num_calls(Worker, luma, generate_group_ctx,
        ['_', '_', '_'], 2),

    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(?UID1, Uid),
    ?assertEqual(ExpectedGid, Gid),
    ?assertEqual(Result2, {ok, ?SPACE_ID}).

get_posix_user_ctx_by_group_id_should_return_0_for_root_base(Config, StorageConfig) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    StorageDoc = maps:get(storage_doc, StorageConfig),
    test_utils:mock_new(Worker, luma_proxy, [passthrough]),
    test_utils:mock_expect(Worker, storage, get, fun(_) ->
        {ok, StorageDoc}
    end),
    Result = rpc:call(Worker, luma, get_posix_user_ctx, [
        ?ROOT_SESS_ID, ?ROOT_USER_ID, undefined, ?SPACE_ID]),
    {Uid, Gid} = ?assertMatch({_, _}, Result),
    ?assertEqual(0, Uid),
    ?assertEqual(0, Gid).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config).

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [idp_access_token, space_storage]),
    test_utils:mock_expect(Worker,idp_access_token, get, fun(?OD_ACCESS_TOKEN, ?OAUTH2_IDP) ->
        {ok, {?IDP_ADMIN_TOKEN, ?TTL}}
    end),
    test_utils:mock_expect(Worker,idp_access_token, get, fun(?USER_ID, ?SESS_ID, ?OAUTH2_IDP) ->
        {ok, {?IDP_USER_TOKEN, ?TTL}}
    end),
    ok = test_utils:mock_expect(Worker, space_storage, get, fun(_) ->
        {ok, #document{value = #space_storage{storage_ids = [<<"whatever">>]}}}
    end),
    Config.


end_per_testcase(_Case, Config) ->
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    invalidate_cache_for_all_storages(Worker),
    ok = test_utils:mock_unload(Workers, [http_client, luma_proxy, luma, space_storage, storage]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_test_for_all_storage_configs(TestCase, TestFun, Config, StorageConfigs) when is_list(StorageConfigs) ->
    lists:foreach(fun(StorageConfig) ->
        Name = maps:get(name, StorageConfig),
        try
            run_test(TestCase, TestFun, Config, StorageConfig)
        catch
            Error:Reason ->
                ct:pal("Testcase \"~p\" failed for config ~p due to ~p:~p~nStacktrace: ~p",
                    [TestCase, Name, Error, Reason, erlang:get_stacktrace()]
                ),
                ct:fail("Failed testcase")
        end
    end, StorageConfigs);
run_test_for_all_storage_configs(TestCase, TestFun, Config, StorageConfig) ->
    run_test_for_all_storage_configs(TestCase, TestFun, Config, [StorageConfig]).

run_test(TestCase, TestFun, Config, StorageConfig) ->
    Config2 = init_per_testcase(TestCase, Config),
    TestFun(Config, StorageConfig),
    end_per_testcase(TestCase, Config2).

generate_posix_identifier(Id, {Low, High}) ->
    PosixId = crypto:bytes_to_integer(Id),
    Low + (PosixId rem (High - Low)).

invalidate_luma_cache(Worker, StorageId) ->
    ok = rpc:call(Worker, luma_cache, invalidate, [StorageId]).

invalidate_cache_for_all_storages(Worker) ->
    StorageIds = lists:usort(lists:map(fun(StorageConfig) ->
        StorageDoc = maps:get(storage_doc, StorageConfig),
        storage:get_id(StorageDoc)
    end, ?ALL_STORAGE_CONFIGS)),

    lists:foreach(fun(StorageId) ->
        invalidate_luma_cache(Worker, StorageId)
    end, StorageIds).

mock_luma_response(Worker, Expectations) ->
    ok = test_utils:mock_new(Worker, http_client),
    ok = test_utils:mock_expect(Worker, http_client, post, fun
        (Url, Headers, Body) when is_binary(Url) ->
            Resource = lists:last(binary:split(Url, <<"/">>, [global])),
            case maps:get(Resource, Expectations, undefined) of
                undefined ->
                    meck:passthrough([Url, Headers, Body]);
                Expectation ->
                    Expectation
            end;
        (Url, Headers, Body) ->
            meck:passthrough([Url, Headers, Body])
    end).