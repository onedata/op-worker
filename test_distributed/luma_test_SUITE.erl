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

-export([posix_user_provider_test/1, posix_user_proxy_test/1,
    ceph_user_provider_test/1, ceph_user_proxy_test/1,
    s3_user_provider_test/1, s3_user_proxy_test/1,
    swift_user_provider_test/1, swift_user_proxy_test/1]).

all() ->
    ?ALL([posix_user_provider_test, posix_user_proxy_test, ceph_user_provider_test,
        ceph_user_proxy_test, s3_user_provider_test, s3_user_proxy_test,
        swift_user_provider_test, swift_user_proxy_test]).

-define(SESSION_ID, <<"SessId">>).
-define(POSIX_SPACE_NAME, <<"s1">>).
-define(CEPH_SPACE_NAME, <<"s2">>).
-define(S3_SPACE_NAME, <<"s3">>).
-define(SWIFT_SPACE_NAME, <<"s4">>).
-define(USER_ID, <<"u1">>).
-define(POSIX_STORAGE_NAME, <<"/mnt/st1">>).
-define(CEPH_STORAGE_NAME, <<"ceph">>).
-define(S3_STORAGE_NAME, <<"s3">>).
-define(SWIFT_STORAGE_NAME, <<"swift">>).
-define(USER_NAME, <<"username">>).
-define(USER_KEY, <<"key">>).
-define(ACCESS_KEY, <<"AccessKey">>).
-define(SECRET_KEY, <<"SecretKey">>).
-define(PASSWORD, <<"Password">>).
-define(UID, 1).

%%%===================================================================
%%% Test functions
%%%===================================================================

posix_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?POSIX_SPACE_NAME]),

    %% each new_user_ctx invocation should return same posix ctx
    %% for posix storage type
    PosixCtx = ?assertMatch(#posix_user_ctx{}, rpc:call(Worker, luma_provider,
        new_user_ctx, [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID,
            SpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    %% get_posix_user_ctx should return same ctx as new_user_ctx
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, SpaceUUID])),

    %% get_posix_user_ctx should return posix context for storages
    %% different than posix
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?SWIFT_HELPER_NAME, ?SESSION_ID, SpaceUUID])).


posix_user_proxy_test(Config) ->
    timer:sleep(3000), % tmp solution until mocking is fixed (VFS-1851)
    [Worker | _] = ?config(op_worker_nodes, Config),
    PosixSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?POSIX_SPACE_NAME]),
    CephSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?CEPH_SPACE_NAME]),
    S3SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?S3_SPACE_NAME]),
    SwiftSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?SWIFT_SPACE_NAME]),

    %% each invocation of new_user_ctx posix should return same posix ctx
    %% for posix storage type
    PosixCtx = ?assertMatch(#posix_user_ctx{uid = ?UID}, rpc:call(Worker,
        luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, PosixSpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, PosixSpaceUUID])),

    %% get_posix_user_ctx should return same ctx as new_user_ctx
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, PosixSpaceUUID])),
    %% user ctx from LUMA server should be requested only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 1),

    %% for non posix storages get_posix_user_ctx should return posix ctx

    %% get_posix_user_ctx should return same ctx on every invocation
    PosixCephCtx = ?assertMatch(#posix_user_ctx{uid = ?UID}, rpc:call(Worker,
        luma_proxy, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, CephSpaceUUID])),
    ?assertEqual(PosixCephCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?CEPH_HELPER_NAME, ?SESSION_ID, CephSpaceUUID])),
    %% user ctx from LUMA server should be requested only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 2),

    %% get_posix_user_ctx should return same ctx on every invocation
    PosixS3Ctx = ?assertMatch(#posix_user_ctx{uid = ?UID}, rpc:call(Worker,
        luma_proxy, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, S3SpaceUUID])),
    ?assertEqual(PosixS3Ctx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?S3_HELPER_NAME, ?SESSION_ID, S3SpaceUUID])),

    %% user ctx from LUMA server should be requested only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 3),

    %% get_posix_user_ctx should return same ctx on every invocation
    PosixSwiftCtx = ?assertMatch(#posix_user_ctx{uid = ?UID}, rpc:call(Worker,
        luma_proxy, get_posix_user_ctx,
        [?SWIFT_HELPER_NAME, ?SESSION_ID, SwiftSpaceUUID])),
    ?assertEqual(PosixSwiftCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?SWIFT_HELPER_NAME, ?SESSION_ID, SwiftSpaceUUID])),

    %% user ctx from LUMA server should be requested only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 4),

    %% posix ctx for each storage type should differ on gid due to
    %% its generation from space name
    ?assertNotEqual(PosixCtx#posix_user_ctx.gid, PosixCephCtx#posix_user_ctx.gid),
    ?assertNotEqual(PosixCtx#posix_user_ctx.gid, PosixS3Ctx#posix_user_ctx.gid),
    ?assertNotEqual(PosixCephCtx#posix_user_ctx.gid, PosixS3Ctx#posix_user_ctx.gid),
    ?assertNotEqual(PosixSwiftCtx#posix_user_ctx.gid, PosixCtx#posix_user_ctx.gid),
    ?assertNotEqual(PosixSwiftCtx#posix_user_ctx.gid, PosixCephCtx#posix_user_ctx.gid),
    ?assertNotEqual(PosixSwiftCtx#posix_user_ctx.gid, PosixS3Ctx#posix_user_ctx.gid).


ceph_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?CEPH_SPACE_NAME]),

    %% each new_user_ctx invocation should return same ceph ctx for ceph storage type
    CephCtx = ?assertMatch(#ceph_user_ctx{}, rpc:call(Worker, luma_provider,
        new_user_ctx, [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID,
            SpaceUUID])),
    ?assertEqual(CephCtx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])).

ceph_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?CEPH_SPACE_NAME]),

    %% each new_user_ctx invocation should return same ceph ctx for ceph storage type
    CephCtx = ?assertMatch(#ceph_user_ctx{user_name = ?USER_NAME, user_key = ?USER_KEY},
        rpc:call(Worker, luma_proxy, new_user_ctx,
            [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(CephCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?CEPH_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    %% LUMA server should be requested for ctx only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 1).

s3_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?S3_SPACE_NAME]),

    %% each new_user_ctx invocation should return same s3 ctx for s3 storage type
    S3Ctx = ?assertMatch(#s3_user_ctx{access_key = ?ACCESS_KEY, secret_key = ?SECRET_KEY},
        rpc:call(Worker, luma_provider, new_user_ctx,
            [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(S3Ctx, rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    %% amazonaws_iam API should be requested only once for new user
    test_utils:mock_assert_num_calls(Worker, amazonaws_iam, create_user, 6, 1),
    test_utils:mock_assert_num_calls(Worker, amazonaws_iam, create_access_key, 6, 1),
    test_utils:mock_assert_num_calls(Worker, amazonaws_iam, allow_access_to_bucket, 7, 1).

s3_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?S3_SPACE_NAME]),

    %% each new_user_ctx invocation should return same s3 ctx for s3 storage type
    S3Ctx = ?assertMatch(#s3_user_ctx{access_key = ?ACCESS_KEY, secret_key = ?SECRET_KEY},
        rpc:call(Worker, luma_proxy, new_user_ctx,
            [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(S3Ctx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?S3_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    %% LUMA server should be requested for ctx only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 1).

swift_user_provider_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?SWIFT_SPACE_NAME]),

    %% each new_user_ctx invocation should return same swift ctx for swift storage type
    SwiftCtx = ?assertMatch(#swift_user_ctx{}, rpc:call(Worker, luma_provider,
        new_user_ctx, [#helper_init{name = ?SWIFT_HELPER_NAME}, ?SESSION_ID,
            SpaceUUID])),
    ?assertEqual(SwiftCtx , rpc:call(Worker, luma_provider, new_user_ctx,
        [#helper_init{name = ?SWIFT_HELPER_NAME}, ?SESSION_ID, SpaceUUID])).

swift_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?SWIFT_SPACE_NAME]),

    %% each new_user_ctx invocation should return same swift ctx for swift storage type
    SwiftCtx = ?assertMatch(#swift_user_ctx{user_name = ?USER_NAME, password = ?PASSWORD},
        rpc:call(Worker, luma_proxy, new_user_ctx,
            [#helper_init{name = ?SWIFT_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(SwiftCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?SWIFT_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),

    %% LUMA server should be requested for ctx only once
    test_utils:mock_assert_num_calls(Worker, http_client, post, 3, 1).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    EnvUpResult = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, EnvUpResult),
    Self = self(),
    Iden = #user_identity{user_id = ?USER_ID},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [?SESSION_ID, Iden, Self])),

    create_space(Worker, ?POSIX_STORAGE_NAME, ?POSIX_SPACE_NAME),
    create_space(Worker, ?CEPH_STORAGE_NAME, ?CEPH_SPACE_NAME),
    create_space(Worker, ?S3_STORAGE_NAME, ?S3_SPACE_NAME),
    create_space(Worker, ?SWIFT_STORAGE_NAME, ?SWIFT_SPACE_NAME),

    EnvUpResult.

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(posix_user_provider_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?POSIX_SPACE_NAME}}} end),
    Config;

init_per_testcase(posix_user_proxy_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    PosixSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?POSIX_SPACE_NAME]),
    CephSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?CEPH_SPACE_NAME]),
    S3SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?S3_SPACE_NAME]),
    SwiftSpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid,
        [?SWIFT_SPACE_NAME]),

    %% mock LUMA server response for posix ctx
    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, post, fun( _, _, _) ->
        {ok, 200, [], json_utils:encode([{<<"uid">>, ?UID}])} end),
    test_utils:mock_new(Worker, file_meta),
    %% return different space name for each space uuid
    test_utils:mock_expect(Worker, file_meta, get,
        fun({uuid, SpaceUUID}) when SpaceUUID == PosixSpaceUUID ->
            {ok, #document{value = #file_meta{name = ?POSIX_SPACE_NAME}}};
            ({uuid, SpaceUUID}) when SpaceUUID == CephSpaceUUID ->
                {ok, #document{value = #file_meta{name = ?CEPH_SPACE_NAME}}};
            ({uuid, SpaceUUID}) when SpaceUUID == S3SpaceUUID ->
                {ok, #document{value = #file_meta{name = ?S3_SPACE_NAME}}};
            ({uuid, SpaceUUID}) when SpaceUUID == SwiftSpaceUUID ->
                {ok, #document{value = #file_meta{name = ?SWIFT_SPACE_NAME}}}
        end),
    Config;

init_per_testcase(ceph_user_provider_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?CEPH_SPACE_NAME}}} end),
    Config;


init_per_testcase(ceph_user_proxy_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    %% mock LUMA server response for ceph ctx
    test_utils:mock_new(Worker, [file_meta, http_client]),
    test_utils:mock_expect(Worker, http_client, post, fun( _, _, _) ->
        {ok, 200, [], json_utils:encode([{<<"userName">>, ?USER_NAME},
            {<<"userKey">>, ?USER_KEY}])} end),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?CEPH_SPACE_NAME}}} end),
    Config;

init_per_testcase(s3_user_provider_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    %% mock invocation of amazonaws_iam API calls
    test_utils:mock_new(Worker, amazonaws_iam),
    test_utils:mock_expect(Worker, amazonaws_iam, create_user,
        fun(_, _, _, _, _, _) -> ok end),
    test_utils:mock_expect(Worker, amazonaws_iam, create_access_key,
        fun(_, _, _, _, _, _) -> {ok, {?ACCESS_KEY, ?SECRET_KEY}} end),
    test_utils:mock_expect(Worker, amazonaws_iam, allow_access_to_bucket,
        fun(_, _, _, _, _, _, _) -> ok end),

    test_utils:mock_new(Worker, file_meta),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?S3_SPACE_NAME}}} end),
    Config;

init_per_testcase(s3_user_proxy_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [file_meta, http_client]),
    test_utils:mock_expect(Worker, http_client, post, fun( _, _, _) ->
        {ok, 200, [], json_utils:encode([{<<"accessKey">>, ?ACCESS_KEY},
                    {<<"secretKey">>, ?SECRET_KEY}])} end),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?S3_SPACE_NAME}}} end),
    Config;

init_per_testcase(swift_user_proxy_test = Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [file_meta, http_client]),
    test_utils:mock_expect(Worker, http_client, post, fun( _, _, _) ->
        {ok, 200, [], json_utils:encode([{<<"userName">>, ?USER_NAME},
                    {<<"password">>, ?PASSWORD}])} end),
    test_utils:mock_expect(Worker, file_meta, get, fun(_) ->
        {ok, #document{value = #file_meta{name = ?SWIFT_SPACE_NAME}}} end),
    Config;

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    Config.

end_per_testcase(Case, Config) when
    Case =:= posix_user_provider_test;
    Case =:= ceph_user_provider_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, file_meta),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(posix_user_proxy_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, [file_meta, http_client]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= ceph_user_proxy_test;
    Case =:= s3_user_proxy_test;
    Case =:= swift_user_proxy_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, [file_meta, http_client]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(s3_user_provider_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, [amazonaws_iam, file_meta]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertMatch(ok, rpc:call(Worker, ceph_user, delete, [?USER_ID])),
    ?assertMatch(ok, rpc:call(Worker, s3_user, delete, [?USER_ID])),
    ?assertMatch(ok, rpc:call(Worker, swift_user, delete, [?USER_ID])),
    ?assertMatch(ok, rpc:call(Worker, posix_user, delete, [?USER_ID])).

create_space(Worker, StorageName, SpaceName) ->
    {ok, Storage} = ?assertMatch({ok, _}, rpc:call(Worker,
        storage, get_by_name, [StorageName])),
    StorageId = rpc:call(Worker, storage, id, [Storage]),
    {ok, SpaceName} = ?assertMatch({ok, _}, rpc:call(Worker,
        space_storage, add, [SpaceName, StorageId])),
    ?assertMatch({ok, _}, rpc:call(Worker, space_storage, get, [SpaceName])).

