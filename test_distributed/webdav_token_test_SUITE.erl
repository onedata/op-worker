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
-include("proto/common/credentials.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([
    user_operation_fails_with_expired_token_on_storage_with_auto_feed_luma/1,
    root_operation_fails_with_expired_token_on_storage_with_auto_feed_luma/1,
    user_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma/1,
    root_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma/1,
    user_operation_fails_with_expired_token_on_storage_with_local_feed_luma/1,
    root_operation_fails_with_expired_token_on_storage_with_local_feed_luma/1,
    user_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma/1,
    root_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma/1,
    user_operation_fails_with_expired_token_on_storage_with_external_feed_luma/1,
    root_operation_fails_with_expired_token_on_storage_with_external_feed_luma/1,
    user_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma/1,
    root_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma/1
]).


all() -> [
    user_operation_fails_with_expired_token_on_storage_with_auto_feed_luma,
    root_operation_fails_with_expired_token_on_storage_with_auto_feed_luma,
    user_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma,
    root_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma,
    user_operation_fails_with_expired_token_on_storage_with_local_feed_luma,
    root_operation_fails_with_expired_token_on_storage_with_local_feed_luma,
    user_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma,
    root_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma,
    user_operation_fails_with_expired_token_on_storage_with_external_feed_luma,
    root_operation_fails_with_expired_token_on_storage_with_external_feed_luma,
    user_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma,
    root_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma
].

-define(SPACE_ID1, <<"space1">>). % space supported by webdav with auto LUMA feed
-define(SPACE_ID2, <<"space2">>). % space supported by webdav with local LUMA feed
-define(SPACE_ID3, <<"space3">>). % space supported by webdav with external LUMA feed

-define(STORAGE_IDS, [<<"webdav_auto_luma_feed">>, <<"webdav_local_luma_feed">>, <<"webdav_external_luma_feed">>]).

-define(STORAGE_FILE_ID(SpaceId), filename:join(["/", SpaceId, <<"dummyFile">>])).
-define(USER1, <<"user1">>).
-define(SESSION(Worker, Config), ?SESSION(?USER1, Worker, Config)).
-define(SESSION(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).
-define(UUID, <<"dummyUuid">>).


-define(USER_CREDENTIALS, <<"USER">>).
-define(IDP_ACCESS_TOKEN, <<"IDP_ACCESS_TOKEN">>).
-define(IDP, <<"IDP">>).
-define(ONEDATA_ACCESS_TOKEN, <<"ONEDATA_ACCESS_TOKEN">>).
-define(ADMIN_ID, <<"ADMIN_ID">>).
-define(ADMIN_CREDENTIALS,
    auth_manager:build_token_credentials(
        ?ONEDATA_ACCESS_TOKEN, undefined,
        undefined, undefined, disallow_data_access_caveats
    )).

-define(getFetchTokenCalls(Worker, Args),
    rpc:call(Worker, meck, num_calls, [user_logic, fetch_idp_access_token, Args])
).

-define(assertRefreshParamsCalls(W, Args, ExpNumCalls),
    test_utils:mock_assert_num_calls(W, helpers_reload, refresh_handle_params, Args, ExpNumCalls)).

-define(assertSetxattrCalls(W, Args, ExpNumCalls),
    test_utils:mock_assert_num_calls(W, helpers, setxattr, Args, ExpNumCalls)).

%%%===================================================================
%%% Tests
%%%===================================================================

user_operation_fails_with_expired_token_on_storage_with_auto_feed_luma(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    operation_with_expired_token_in_admin_ctx_should_fail_base(SessionId, ?SPACE_ID1, Config).

root_operation_fails_with_expired_token_on_storage_with_auto_feed_luma(Config) ->
    operation_with_expired_token_in_admin_ctx_should_fail_base(?ROOT_SESS_ID, ?SPACE_ID1, Config).

user_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(SessionId, ?SPACE_ID1, Config).

root_operation_succeeds_with_refreshed_token_on_storage_with_auto_feed_luma(Config) ->
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(?ROOT_SESS_ID, ?SPACE_ID1, Config).

user_operation_fails_with_expired_token_on_storage_with_local_feed_luma(Config) ->
    operation_with_expired_token_in_user_ctx_should_fail_base(Config, ?SPACE_ID2).

root_operation_fails_with_expired_token_on_storage_with_local_feed_luma(Config) ->
    operation_with_expired_token_in_admin_ctx_should_fail_base(?ROOT_SESS_ID, ?SPACE_ID2, Config).

user_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma(Config) ->
    operation_with_refreshed_token_in_user_ctx_should_succeed_base(Config, ?SPACE_ID2).

root_operation_succeeds_with_refreshed_token_on_storage_with_local_feed_luma(Config) ->
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(?ROOT_SESS_ID, ?SPACE_ID2, Config).

user_operation_fails_with_expired_token_on_storage_with_external_feed_luma(Config) ->
    operation_with_expired_token_in_user_ctx_should_fail_base(Config, ?SPACE_ID3).

root_operation_fails_with_expired_token_on_storage_with_external_feed_luma(Config) ->
    operation_with_expired_token_in_admin_ctx_should_fail_base(?ROOT_SESS_ID, ?SPACE_ID3, Config).

user_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma(Config) ->
    operation_with_refreshed_token_in_user_ctx_should_succeed_base(Config, ?SPACE_ID3).

root_operation_succeeds_with_refreshed_token_on_storage_with_external_feed_luma(Config) ->
    operation_with_refreshed_token_in_admin_ctx_should_succeed_base(?ROOT_SESS_ID, ?SPACE_ID3, Config).

%%%===================================================================
%%% Test bases
%%%===================================================================

operation_with_expired_token_in_admin_ctx_should_fail_base(SessionId, SpaceId, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    TTL = 0,
    SDHandle = get_sd_handle(W, SpaceId, SessionId, ?STORAGE_FILE_ID(SpaceId)),
    FetchTokenCallsNum0 = ?getFetchTokenCalls(W, [?ADMIN_CREDENTIALS, ?ADMIN_ID, ?IDP]),
    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),

    % setxattr should return EKEYEXPIRED due to TTL=0
    ?assertEqual({error, ?EKEYEXPIRED}, setxattr(W, SDHandle, <<"K">>, <<"V">>)),
    % ensure that helper params were refreshed
    ?assertRefreshParamsCalls(W, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    ?assertSetxattrCalls(W, ['_', '_', '_', '_', '_', '_'], 2),
    % ensure that token was acquired in admin_ctx
    ?assertEqual(FetchTokenCallsNum0 + 2, ?getFetchTokenCalls(W, [?ADMIN_CREDENTIALS, ?ADMIN_ID, ?IDP])).


operation_with_refreshed_token_in_admin_ctx_should_succeed_base(SessionId, SpaceId, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    TTL = 5,
    SDHandle = get_sd_handle(W, SpaceId, SessionId, ?STORAGE_FILE_ID(SpaceId)),
    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),
    FetchTokenCallsNum0 = ?getFetchTokenCalls(W, [?ADMIN_CREDENTIALS, ?ADMIN_ID, ?IDP]),

    ?assertEqual(ok, setxattr(W, SDHandle, <<"K">>, <<"V">>)),

    %sleep longer than TTL to ensure that new token will be fetched
    timer:sleep(timer:seconds(TTL + 1)),

    % setxattr should succeed after retry with refreshed token
    ?assertEqual(ok, setxattr(W, SDHandle, <<"K2">>, <<"V2">>)),
    % ensure that helper params were refreshed
    ?assertRefreshParamsCalls(W, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    ?assertSetxattrCalls(W, ['_', '_', '_', '_', '_', '_'], 3),
    % ensure that token was acquired in admin_ctx
    ?assertEqual(FetchTokenCallsNum0 + 2, ?getFetchTokenCalls(W, [?ADMIN_CREDENTIALS, ?ADMIN_ID, ?IDP])).


operation_with_expired_token_in_user_ctx_should_fail_base(Config, SpaceId) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    TTL = 0,
    SDHandle = get_sd_handle(W, SpaceId, SessionId, ?STORAGE_FILE_ID(SpaceId)),
    FetchTokenCallsNum0 = ?getFetchTokenCalls(W, [SessionId, ?USER1, ?IDP]),
    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),

    % setxattr should return EKEYEXPIRED due to TTL=0
    ?assertEqual({error, ?EKEYEXPIRED}, setxattr(W, SDHandle, <<"K">>, <<"V">>)),
    % ensure that helper params were refreshed
    ?assertRefreshParamsCalls(W, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    ?assertSetxattrCalls(W, ['_', '_', '_', '_', '_', '_'], 2),
    % ensure that token was acquired in user_ctx
    ?assertEqual(FetchTokenCallsNum0 + 2, ?getFetchTokenCalls(W, [SessionId, ?USER1, ?IDP])).


operation_with_refreshed_token_in_user_ctx_should_succeed_base(Config, SpaceId) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessionId = ?SESSION(W, Config),
    TTL = 5,
    SDHandle = get_sd_handle(W, SpaceId, SessionId, ?STORAGE_FILE_ID(SpaceId)),
    FetchTokenCallsNum0 = ?getFetchTokenCalls(W, [SessionId, ?USER1, ?IDP]),
    mock_fetch_token(W, ?IDP_ACCESS_TOKEN,  TTL),

    ?assertEqual(ok, setxattr(W, SDHandle, <<"K">>, <<"V">>)),

    %sleep longer than TTL to ensure that new token will be fetched
    timer:sleep(timer:seconds(TTL + 1)),

    % setxattr should succeed after retry with refreshed token
    ?assertEqual(ok, setxattr(W, SDHandle, <<"K2">>, <<"V2">>)),
    % ensure that helper params were refreshed
    ?assertRefreshParamsCalls(W, ['_', '_', '_', '_'], 1),
    % ensure that setxattr was repeated
    ?assertSetxattrCalls(W, ['_', '_', '_', '_', '_', '_'], 3),
    % ensure that token was acquired in user_ctx
    ?assertEqual(FetchTokenCallsNum0 + 2, ?getFetchTokenCalls(W, [SessionId, ?USER1, ?IDP])).

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
    enable_webdav_test_mode_on_all_storages(W),
    test_utils:mock_new(W, [helpers, helpers_reload], [passthrough]),
    Config.

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(W, [helpers_reload, helpers]),
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

mock_fetch_token(Worker, Token, TTL) ->
    ok = test_utils:mock_expect(Worker, user_logic, fetch_idp_access_token,
        fun (_, _, _) -> {ok, {Token, TTL}} end).

enable_webdav_test_mode_on_all_storages(Worker) ->
    lists:foreach(fun(StorageId) ->
        enable_webdav_test_mode(Worker, StorageId)
    end, ?STORAGE_IDS).

enable_webdav_test_mode(Worker, StorageId) ->
    UpdateHelperFun = fun(#helper{args = Args, admin_ctx = AdminCtx}  = Helper) ->
        Args2 = Args#{
            <<"testTokenRefreshMode">> => <<"true">>,
            <<"oauth2IdP">> => ?IDP
        },
        {ok, Helper#helper{
            args = Args2,
            admin_ctx = AdminCtx#{
                <<"credentialsType">> => <<"oauth2">>,
                <<"onedataAccessToken">> => ?ONEDATA_ACCESS_TOKEN,
                <<"adminId">> => ?ADMIN_ID,
                <<"credentials">> => <<"ADMIN">>
            }
        }}
    end,
    ok = rpc:call(Worker, storage, update_helper, [StorageId, UpdateHelperFun]).

get_sd_handle(Worker, SpaceId, SessionId, FilePath) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    rpc:call(Worker, storage_driver, new_handle, [SessionId, SpaceId, undefined, StorageId, FilePath]).

setxattr(Worker, SDHandle, Key, Value) ->
    rpc:call(Worker, storage_driver, setxattr, [SDHandle, Key, Value, true, true]).