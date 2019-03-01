%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of webdav helper.
%%% It focuses on tests of refreshing webdav token.
%%% Tests use webdav helper in a "test mode" which is enabled by
%%% setting <<"testTokenRefreshMode">> => true.
%%% In this mode, setxattr and removexattr operations does not validate
%%% token, they only check whether token TTL has been expired.
%%% @end
%%%-------------------------------------------------------------------
-module(webdav_token_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([
    operation_performed_by_user_with_expired_token_should_fail_insecure_storage_test/1,
    operation_performed_by_root_with_expired_token_should_fail_insecure_storage_test/1,
    operation_performed_by_user_with_refreshed_token_should_succeed_insecure_storage_test/1,
    operation_performed_by_root_with_refreshed_token_should_succeed_insecure_storage_test/1,
    operation_performed_by_user_with_expired_token_should_fail_secure_storage_test/1,
    operation_performed_by_root_with_expired_token_should_fail_secure_storage_test/1,
    operation_performed_by_user_with_refreshed_token_should_succeed_secure_storage_test/1,
    operation_performed_by_root_with_refreshed_token_should_succeed_secure_storage_test/1
]).


all() -> [
    operation_performed_by_user_with_expired_token_should_fail_insecure_storage_test,
    operation_performed_by_root_with_expired_token_should_fail_insecure_storage_test,
    operation_performed_by_user_with_refreshed_token_should_succeed_insecure_storage_test,
    operation_performed_by_root_with_refreshed_token_should_succeed_insecure_storage_test,
    operation_performed_by_user_with_expired_token_should_fail_secure_storage_test,
    operation_performed_by_root_with_expired_token_should_fail_secure_storage_test,
    operation_performed_by_user_with_refreshed_token_should_succeed_secure_storage_test,
    operation_performed_by_root_with_refreshed_token_should_succeed_secure_storage_test
].

-define(SPACE_ID, <<"space1">>).
-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).
-define(USER, <<"user1">>).
-define(SESSION(Worker, Config), ?SESSION(?USER, Worker, Config)).
-define(SESSION(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

-define(USER_CREDENTIALS, <<"USER">>).
-define(IDP_ACCESS_TOKEN, <<"IDP_ACCESS_TOKEN">>).
-define(IDP, <<"IDP">>).
-define(ONEDATA_ACCESS_TOKEN, <<"ONEDATA_ACCESS_TOKEN">>).

-define(assertAcquireTokenCalls(Worker, Args, ExpNumCalls),
    test_utils:mock_assert_num_calls(Worker, idp_access_token, acquire, Args, ExpNumCalls)).

%%%===================================================================
%%% Tests
%%%===================================================================

operation_performed_by_user_with_expired_token_should_fail_insecure_storage_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    operation_with_expired_token_in_admin_ctx_should_fail_base(SessionId, Config).

operation_performed_by_root_with_expired_token_should_fail_insecure_storage_test(Config) ->
    operation_with_expired_token_in_admin_ctx_should_fail_base(?ROOT_SESS_ID, Config).

operation_performed_by_user_with_refreshed_token_should_succeed_insecure_storage_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(SessionId, Config).

operation_performed_by_root_with_refreshed_token_should_succeed_insecure_storage_test(Config) ->
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(?ROOT_SESS_ID, Config).

operation_performed_by_user_with_expired_token_should_fail_secure_storage_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    Uuid = <<"dummyUuid">>,
    FileName = <<"dummyFile">>,
    FilePath = ?FILE_PATH(FileName),
    StorageId = storage_id(W),
    TTL = 0,
    SFMHandle = get_sfm_handle(W, ?SPACE_ID, SessionId, Uuid, StorageId, FilePath),
    ok = test_utils:mock_new(W, helpers_fallback, [passthrough]),
    ok = test_utils:mock_new(W, helpers, [passthrough]),
    ok = test_utils:mock_new(W, idp_access_token, [passthrough]),

    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),
    mock_luma(W),

    % setxattr should return EKEYEXPIRED due to TTL=0
    ?assertEqual({error, ?EKEYEXPIRED}, setxattr(W, SFMHandle, <<"K">>, <<"V">>)),

    % ensure that helper params were refreshed
    test_utils:mock_assert_num_calls(W, helpers_fallback, refresh_params, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    test_utils:mock_assert_num_calls(W, helpers, setxattr, ['_', '_', '_', '_', '_', '_'], 2),
    % ensure that token was acquired in admin_ctx
    ?assertAcquireTokenCalls(W, [?USER, SessionId, ?IDP], 2).

operation_performed_by_root_with_expired_token_should_fail_secure_storage_test(Config) ->
    operation_with_expired_token_in_admin_ctx_should_fail_base(?ROOT_SESS_ID, Config).

operation_performed_by_user_with_refreshed_token_should_succeed_secure_storage_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    Uuid = <<"dummyUuid">>,
    FileName = <<"dummyFile">>,
    FilePath = filename:join(["/", ?SPACE_ID, FileName]),
    StorageId = storage_id(W),
    TTL = 5,
    SFMHandle = get_sfm_handle(W, ?SPACE_ID, SessionId, Uuid, StorageId, FilePath),
    test_utils:mock_new(W, helpers_fallback, [passthrough]),
    test_utils:mock_new(W, helpers, [passthrough]),
    ok = test_utils:mock_new(W, idp_access_token, [passthrough]),

    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),
    mock_luma(W),

    ?assertEqual(ok, setxattr(W, SFMHandle, <<"K">>, <<"V">>)),

    %sleep longer than TTL to ensure that new token will be fetched
    timer:sleep(timer:seconds(TTL + 1)),

    % setxattr should succeed after retry with refreshed token
    ?assertEqual(ok, setxattr(W, SFMHandle, <<"K2">>, <<"V2">>)),

    % ensure that helper params were refreshed
    test_utils:mock_assert_num_calls(W, helpers_fallback, refresh_params, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    test_utils:mock_assert_num_calls(W, helpers, setxattr, ['_', '_', '_', '_', '_', '_'], 3),
    % ensure that token was acquired in admin_ctx
    ?assertAcquireTokenCalls(W, [?USER, SessionId, ?IDP], 2).

operation_performed_by_root_with_refreshed_token_should_succeed_secure_storage_test(Config) ->
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(?ROOT_SESS_ID, Config).

%%%===================================================================
%%% Test bases
%%%===================================================================

operation_with_expired_token_in_admin_ctx_should_fail_base(SessionId, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Uuid = <<"dummyUuid">>,
    FileName = <<"dummyFile">>,
    FilePath = ?FILE_PATH(FileName),
    StorageId = storage_id(W),
    TTL = 0,
    SFMHandle = get_sfm_handle(W, ?SPACE_ID, SessionId, Uuid, StorageId, FilePath),
    ok = test_utils:mock_new(W, helpers_fallback, [passthrough]),
    ok = test_utils:mock_new(W, helpers, [passthrough]),
    ok = test_utils:mock_new(W, idp_access_token, [passthrough]),

    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),
    % setxattr should return EKEYEXPIRED due to TTL=0

    ?assertEqual({error, ?EKEYEXPIRED}, setxattr(W, SFMHandle, <<"K">>, <<"V">>)),

    % ensure that helper params were refreshed
    test_utils:mock_assert_num_calls(W, helpers_fallback, refresh_params, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    test_utils:mock_assert_num_calls(W, helpers, setxattr, ['_', '_', '_', '_', '_', '_'], 2),
    % ensure that token was acquired in admin_ctx
    ?assertAcquireTokenCalls(W, ['_', ?ONEDATA_ACCESS_TOKEN, ?IDP], 2).

operation_with_refreshed_token_in_admin_ctx_should_succeed_base(SessionId, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Uuid = <<"dummyUuid">>,
    FileName = <<"dummyFile">>,
    FilePath = filename:join(["/", ?SPACE_ID, FileName]),
    StorageId = storage_id(W),
    TTL = 5,
    SFMHandle = get_sfm_handle(W, ?SPACE_ID, SessionId, Uuid, StorageId, FilePath),
    test_utils:mock_new(W, helpers_fallback, [passthrough]),
    test_utils:mock_new(W, helpers, [passthrough]),
    ok = test_utils:mock_new(W, idp_access_token, [passthrough]),

    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),
    ?assertEqual(ok, setxattr(W, SFMHandle, <<"K">>, <<"V">>)),

    %sleep longer than TTL to ensure that new token will be fetched
    timer:sleep(timer:seconds(TTL + 1)),

    % setxattr should succeed after retry with refreshed token
    ?assertEqual(ok, setxattr(W, SFMHandle, <<"K2">>, <<"V2">>)),

    % ensure that helper params were refreshed
    test_utils:mock_assert_num_calls(W, helpers_fallback, refresh_params, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    test_utils:mock_assert_num_calls(W, helpers, setxattr, ['_', '_', '_', '_', '_', '_'], 3),
    % ensure that token was acquired in admin_ctx
    ?assertAcquireTokenCalls(W, ['_', ?ONEDATA_ACCESS_TOKEN, ?IDP], 2).

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

init_per_testcase(Case, Config) when
    Case =:= operation_performed_by_user_with_expired_token_should_fail_insecure_storage_test;
    Case =:= operation_performed_by_root_with_expired_token_should_fail_insecure_storage_test;
    Case =:= operation_performed_by_user_with_refreshed_token_should_succeed_insecure_storage_test;
    Case =:= operation_performed_by_root_with_refreshed_token_should_succeed_insecure_storage_test 
    ->
    [W | _] = ?config(op_worker_nodes, Config),
    enable_webdav_test_mode_insecure_storage(W, storage_id(W)),
    Config;

init_per_testcase(Case, Config) when
    Case =:= operation_performed_by_user_with_expired_token_should_fail_secure_storage_test;
    Case =:= operation_performed_by_root_with_expired_token_should_fail_secure_storage_test;
    Case =:= operation_performed_by_user_with_refreshed_token_should_succeed_secure_storage_test;
    Case =:= operation_performed_by_root_with_refreshed_token_should_succeed_secure_storage_test
    ->
    [W | _] = ?config(op_worker_nodes, Config),
    enable_webdav_test_mode_secure_storage(W, storage_id(W)),
    Config.

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(W, [helpers_fallback, helpers, idp_access_token, luma_proxy]),
    ok = rpc:call(W, session_helpers, delete_helpers, [?SESSION(W, Config)]),
    ok = rpc:call(W, session_helpers, delete_helpers, [?ROOT_SESS_ID]),
    Config.

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_luma(Worker) ->
    ok = test_utils:mock_new(Worker, luma_proxy),
    ok = test_utils:mock_expect(Worker, luma_proxy, get_user_ctx, fun(_, _, _, _, _) ->
        {ok, #{
            <<"credentialsType">> => <<"oauth2">>,
            <<"credentials">> => ?USER_CREDENTIALS
        }}
    end).


mock_fetch_token(Worker, Token, TTL) ->
    ok = test_utils:mock_expect(Worker, user_logic, acquire_idp_access_token,
        fun (_, _) -> {ok, {Token, TTL}} end),

    ok = test_utils:mock_expect(Worker, user_logic, acquire_idp_access_token,
        fun (_, _, _) -> {ok, {Token, TTL}} end).

storage_id(Worker) ->
    {ok, [Storage]} = rpc:call(Worker, storage, list, []),
    storage:get_id(Storage).

storage_update(Worker, StorageId, UpdateFun) ->
    rpc:call(Worker, storage, update, [StorageId, UpdateFun]).

enable_webdav_test_mode_secure_storage(Worker, StorageId) ->
    enable_webdav_test_mode(Worker, StorageId, false).

enable_webdav_test_mode_insecure_storage(Worker, StorageId) ->
    enable_webdav_test_mode(Worker, StorageId, true).

enable_webdav_test_mode(Worker, StorageId, Insecure) ->
    LumaConfig = case Insecure of
        true -> undefined;
        false -> #luma_config{}
    end,

    {ok, _} = storage_update(Worker, StorageId, fun(Storage = #storage{helpers = [Helper]}) ->
        #helper{args = Args, admin_ctx = AdminCtx}  = Helper,
        Args2 = Args#{
            <<"testTokenRefreshMode">> => <<"true">>,
            <<"oauth2IdP">> => ?IDP
        },
        {ok, Storage#storage{helpers = [Helper#helper{
            args = Args2,
            admin_ctx = AdminCtx#{
                <<"credentialsType">> => <<"oauth2">>,
                <<"onedataAccessToken">> => ?ONEDATA_ACCESS_TOKEN,
                <<"credentials">> => <<"ADMIN">>
            },
            insecure = Insecure
        }],
            luma_config = LumaConfig
        }}
    end).

get_sfm_handle(Worker, SpaceId, SessionId, Uuid, StorageId, FilePath) ->
    {ok, StorageDoc} = rpc:call(Worker, storage, get, [StorageId]),
    rpc:call(Worker, storage_file_manager, new_handle,
        [SessionId, SpaceId, Uuid, StorageDoc, FilePath, undefined]).

setxattr(Worker, SFMHandle, Key, Value) ->
    rpc:call(Worker, storage_file_manager, setxattr, [SFMHandle, Key, Value, true, true]).