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

-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([posix_user_provider_test/1, posix_user_proxy_test/1, ceph_user_provider_test/1,
    ceph_user_proxy_test/1, s3_user_provider_test/1, s3_user_proxy_test/1]).

all() -> ?ALL([posix_user_provider_test, posix_user_proxy_test, ceph_user_provider_test,
    ceph_user_proxy_test, s3_user_provider_test, s3_user_proxy_test]).

-define(SESSION_ID, <<"SessId">>).
-define(POSIX_SPACE_NAME, <<"s1">>).
-define(CEPH_SPACE_NAME, <<"s2">>).
-define(S3_SPACE_NAME, <<"s3">>).
-define(USER_ID, <<"u1">>).
-define(POSIX_STORAGE_NAME, <<"/mnt/st1">>).
-define(CEPH_STORAGE_NAME, <<"ceph">>).
-define(S3_STORAGE_NAME, <<"s3">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

posix_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?POSIX_SPACE_NAME]),

    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?POSIX_SPACE_NAME}}} end),

    PosixCtx = ?assertMatch(#posix_user_ctx{}, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, SpaceUUID])).


posix_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    PosixSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?POSIX_SPACE_NAME]),
    CephSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?CEPH_SPACE_NAME]),
    S3SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?S3_SPACE_NAME]),
    UID = 1,

    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, get, fun(_, _, _, _) -> {ok, 200, [],
        json_utils:encode([{<<"status">>, <<"success">>}, {<<"data">>, [{<<"uid">>, UID}]}])} end),
    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get,
        fun({uuid, SpaceUUID}) when SpaceUUID == PosixSpaceUUID ->
            {ok, #document{value = #file_meta{name = ?POSIX_SPACE_NAME}}};
            ({uuid, SpaceUUID}) when SpaceUUID == CephSpaceUUID ->
                {ok, #document{value = #file_meta{name = ?CEPH_SPACE_NAME}}};
            ({uuid, SpaceUUID}) when SpaceUUID == S3SpaceUUID ->
                {ok, #document{value = #file_meta{name = ?S3_SPACE_NAME}}}
        end),

    PosixCtx = ?assertMatch(#posix_user_ctx{uid = UID}, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, PosixSpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, PosixSpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, PosixSpaceUUID])),
    test_utils:mock_num_calls(Worker, http_client, get, 4, 1),

    PosixCephCtx = ?assertMatch(#posix_user_ctx{uid = UID}, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, CephSpaceUUID])),
    ?assertEqual(PosixCephCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, CephSpaceUUID])),

    PosixS3Ctx = ?assertMatch(#posix_user_ctx{uid = UID}, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, S3SpaceUUID])),
    ?assertEqual(PosixS3Ctx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, S3SpaceUUID])),

    ?assertNotEqual(PosixCtx#posix_user_ctx.gid, PosixCephCtx#posix_user_ctx.gid),
    ?assertNotEqual(PosixCtx#posix_user_ctx.gid, PosixS3Ctx#posix_user_ctx.gid),
    ?assertNotEqual(PosixCephCtx#posix_user_ctx.gid, PosixS3Ctx#posix_user_ctx.gid),

    test_utils:mock_num_calls(Worker, http_client, get, 4, 2).

ceph_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?CEPH_SPACE_NAME]),
    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?CEPH_SPACE_NAME}}} end),

    CephCtx = ?assertMatch(#ceph_user_ctx{}, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(CephCtx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])).

ceph_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?CEPH_SPACE_NAME]),
    UserName = <<"username">>,
    UserKey = <<"key">>,

    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, get, fun(_, _, _, _) -> {ok, 200, [],
        json_utils:encode([{<<"status">>, <<"success">>}, {<<"data">>,
            [{<<"user_name">>, UserName}, {<<"user_key">>, UserKey}]}])} end),
    CephCtx = ?assertMatch(#ceph_user_ctx{user_name = UserName, user_key = UserKey},
        rpc:call(Worker, luma_proxy, new_user_ctx,
            [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(CephCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    test_utils:mock_num_calls(Worker, http_client, get, 4, 1).

s3_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?S3_SPACE_NAME]),
    AccessKey = <<"AccessKey">>,
    SecretKey = <<"SecretKey">>,
    test_utils:mock_new(Worker, amazonaws_iam),
    test_utils:mock_expect(Worker, amazonaws_iam, create_user, fun(_, _, _, _, _) -> ok end),
    test_utils:mock_expect(Worker, amazonaws_iam, create_access_key, fun(_, _, _, _, _) ->
        {ok, {AccessKey, SecretKey}} end),
    test_utils:mock_expect(Worker, amazonaws_iam, allow_access_to_bucket, fun(_, _, _, _, _, _) -> ok end),
    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?S3_SPACE_NAME}}} end),

    S3Ctx = ?assertMatch(#s3_user_ctx{access_key = AccessKey, secret_key = SecretKey},
        rpc:call(Worker, luma_provider, new_user_ctx,
            [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(S3Ctx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    test_utils:mock_num_calls(Worker, amazonaws_iam, create_user, 5, 1),
    test_utils:mock_num_calls(Worker, amazonaws_iam, create_access_key, 5, 1),
    test_utils:mock_num_calls(Worker, amazonaws_iam, allow_access_to_bucket, 6, 1).

s3_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?S3_SPACE_NAME]),
    AccessKey = <<"AccessKey">>,
    SecretKey = <<"SecretKey">>,

    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, get, fun(_, _, _, _) -> {ok, 200, [],
        json_utils:encode([{<<"status">>, <<"success">>}, {<<"data">>,
            [{<<"access_key">>, AccessKey}, {<<"secret_key">>, SecretKey}]}])} end),
    S3Ctx = ?assertMatch(#s3_user_ctx{access_key = AccessKey, secret_key = SecretKey},
        rpc:call(Worker, luma_proxy, new_user_ctx,
            [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(S3Ctx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    test_utils:mock_num_calls(Worker, http_client, get, 4, 1).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    EnvUpResult = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, EnvUpResult),
    Self = self(),
    Iden = #identity{user_id = ?USER_ID},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager, reuse_or_create_fuse_session, [?SESSION_ID, Iden, Self])),
    create_space(Worker, ?POSIX_STORAGE_NAME, ?POSIX_SPACE_NAME),
    create_space(Worker, ?CEPH_STORAGE_NAME, ?CEPH_SPACE_NAME),
    create_space(Worker, ?S3_STORAGE_NAME, ?S3_SPACE_NAME),
    EnvUpResult.


end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertMatch(ok, rpc:call(Worker, luma_response, delete, [{?USER_ID, ?DIRECTIO_HELPER_NAME}])),
    ?assertMatch(ok, rpc:call(Worker, ceph_user, delete, [?USER_ID])),
    ?assertMatch(ok, rpc:call(Worker, s3_user, delete, [?USER_ID])),
    clear_luma_response(Worker, ?POSIX_STORAGE_NAME),
    clear_luma_response(Worker, ?CEPH_STORAGE_NAME),
    clear_luma_response(Worker, ?S3_STORAGE_NAME).

create_space(Worker, StorageName, SpaceName) ->
    {ok, Storage} = ?assertMatch({ok, _}, rpc:call(Worker, storage, get_by_name, [StorageName])),
    StorageId = rpc:call(Worker, storage, id, [Storage]),
    {ok, SpaceName} = ?assertMatch({ok, _}, rpc:call(Worker, space_storage, add, [SpaceName, StorageId])),
    ?assertMatch({ok, _}, rpc:call(Worker, space_storage, get, [SpaceName])).

clear_luma_response(Worker, StorageName) ->
    {ok, Storage} = ?assertMatch({ok, _}, rpc:call(Worker, storage, get_by_name, [StorageName])),
    StorageId = rpc:call(Worker, storage, id, [Storage]),
    ?assertMatch(ok, rpc:call(Worker, luma_response, delete, [{?USER_ID, StorageId}])).
