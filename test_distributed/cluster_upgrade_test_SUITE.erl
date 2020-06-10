%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test verifies if cluster upgrade procedures (employed during software
%%% upgrades) work as expected.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_upgrade_test_SUITE).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").


%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).
-export([
    upgrade_from_19_02_x_storages/1,
    upgrade_from_20_02_0_beta3_storages/1
]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([
    upgrade_from_19_02_x_storages,
    upgrade_from_20_02_0_beta3_storages
]).

%%%===================================================================
%%% Tests
%%%===================================================================

upgrade_from_19_02_x_storages(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,

    St = <<"storage1">>,
    Helper = #helper{
        name = <<"HelperName">>,
        args = #{},
        admin_ctx = #{},
        insecure = false,
        extended_direct_io = true,
        storage_path_type = <<"storage_path_type">>
    },
    LumaConfig = #luma_config{
        url = <<"https://example.com">>,
        api_key = <<"api_key">>
    },
    Storage = #storage{
        name = St,
        helpers = [Helper],
        readonly = false,
        luma_config = LumaConfig
    },
    ExpectedStorageConfig = #storage_config{
        helper = Helper,
        readonly = false,
        luma_config = LumaConfig,
        imported_storage = false
    },
    rpc:call(Worker, datastore_model, create, [storage:get_ctx(), #document{key = St, value = Storage}]),
    rpc:call(Worker, datastore_model, create, [space_storage:get_ctx(), #document{
        key = SpaceId,
        value = #space_storage{
            storage_ids = [St],
            mounted_in_root = [St]
        }
    }]),

    ?assertEqual({ok, 2}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [1])),

    ?assertMatch({error, not_found}, rpc:call(Worker, datastore_model, get, [storage:get_ctx(), St])),
    ?assertMatch({error, not_found}, rpc:call(Worker, datastore_model, get, [space_storage:get_ctx(), SpaceId])),

    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, upgrade_legacy_support, 2, 1),
    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, create_in_zone, 3, 1),
    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, set_imported, ['_', true], 1),
    % Virtual storage should be removed in onezone
    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, delete_in_zone, 1, 1),
    ?assertMatch({ok, #document{value = ExpectedStorageConfig}}, rpc:call(Worker, storage_config, get, [St])).


upgrade_from_20_02_0_beta3_storages(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    St = <<"storage2">>,
    rpc:call(Worker, storage_config, create, [St, #helper{}, false, undefined]),
    rpc:call(Worker, datastore_model, update, [storage_config:get_ctx(), St, 
        fun(StorageConfig) -> {ok, StorageConfig#storage_config{imported_storage = true}} end]),
    test_utils:mock_expect(Worker, provider_logic, get_storage_ids, fun() -> {ok, [St]} end),
    
    ?assertEqual({ok, 3}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [2])),
    
    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, set_imported, ['_', true], 1).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(upgrade_from_19_02_x_storages, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, storage_logic, [passthrough]),
    test_utils:mock_expect(Worker, storage_logic, create_in_zone, fun(_, _, StorageId) -> {ok, StorageId} end),
    test_utils:mock_expect(Worker, storage_logic, delete_in_zone, fun(_) -> ok end),
    test_utils:mock_expect(Worker, storage_logic, upgrade_legacy_support, fun(_,_) -> ok end),
    test_utils:mock_expect(Worker, storage_logic, set_imported, fun(_,_) -> ok end),
    test_utils:mock_new(Worker, oneprovider),
    test_utils:mock_expect(Worker, oneprovider, is_connected_to_oz, fun() -> true end),
    Config;

init_per_testcase(upgrade_from_20_02_0_beta3_storages, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    test_utils:mock_new(Worker, storage_logic, [passthrough]),
    test_utils:mock_expect(Worker, storage_logic, set_imported, fun(_,_) -> ok end),
    test_utils:mock_new(Worker, oneprovider),
    test_utils:mock_expect(Worker, oneprovider, is_connected_to_oz, fun() -> true end),
    Config.

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker),
    ok.


end_per_suite(_Config) ->
    ok.
