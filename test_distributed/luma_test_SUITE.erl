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

-export([posix_user_provider_test/1, posix_user_proxy_test/1]).

all() -> ?ALL([posix_user_provider_test, posix_user_proxy_test]).

-define(SESSION_ID, <<"SessId">>).
-define(POSIX_SPACE_NAME, <<"s1">>).

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
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_provider, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, SpaceUUID])).


posix_user_proxy_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceUUID = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [?POSIX_SPACE_NAME]),
    UID = 1,
    GID = 1,

    test_utils:mock_new(Worker, http_client),
    test_utils:mock_expect(Worker, http_client, get, fun(_, _, _, _) -> {ok, 200, [],
        json_utils:encode([{<<"status">>, <<"success">>}, {<<"data">>, [{<<"uid">>, UID}, {<<"gid">>, GID}]}])} end),

    PosixCtx = ?assertMatch(#posix_user_ctx{uid = UID, gid = GID}, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertEqual(PosixCtx, rpc:call(Worker, luma_proxy, new_user_ctx,
        [#helper_init{name = ?DIRECTIO_HELPER_NAME}, ?SESSION_ID, SpaceUUID])),
    ?assertMatch(PosixCtx, rpc:call(Worker, luma_proxy, get_posix_user_ctx,
        [?DIRECTIO_HELPER_NAME, ?SESSION_ID, SpaceUUID])),

    test_utils:mock_num_calls(Worker, http_client, get, 4, 1).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    EnvUpResult = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, EnvUpResult),

    Self = self(),
    UserId = <<"u1">>,
    StorageName = <<"/mnt/st1">>,

    {ok, Storage} = rpc:call(Worker, storage, get_by_name, [StorageName]),
    StorageId = rpc:call(Worker, storage, id, [Storage]),
    {ok, ?POSIX_SPACE_NAME} = rpc:call(Worker, space_storage, add, [?POSIX_SPACE_NAME, StorageId]),
    {ok, _} = rpc:call(Worker, space_storage, get, [?POSIX_SPACE_NAME]),
    Iden = #identity{user_id = UserId},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager, reuse_or_create_fuse_session, [?SESSION_ID, Iden, Self])),
    {ok, #document{}} = rpc:call(Worker, session, get, [?SESSION_ID]),

    EnvUpResult.


end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.
