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
-include("modules/storage_file_manager/helpers/helpers.hrl").
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
    upgrade_from_19_02_x_storages/1
]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([
    upgrade_from_19_02_x_storages
]).

%%%===================================================================
%%% Tests
%%%===================================================================


upgrade_from_19_02_x_storages(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

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
        name = St,
        helpers = [Helper],
        readonly = false,
        luma_config = LumaConfig,
        imported_storage = false
    },
    rpc:call(Worker, datastore_model, create, [storage:get_ctx(), #document{key = St, value = Storage}]),

    ?assertEqual({ok, 2}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [1])),

    ?assertMatch({error, not_found}, rpc:call(Worker, datastore_model, get, [storage:get_ctx(), St])),
    ?assertMatch({ok, #document{value = ExpectedStorageConfig}}, rpc:call(Worker, storage_config, get, [St])).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:setup_storage(NewConfig),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(upgrade_from_19_02_x_storages, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, storage_logic),
    test_utils:mock_expect(Worker, storage_logic, create_in_zone, fun(_,StorageId) -> {ok, StorageId} end),
    test_utils:mock_expect(Worker, storage_logic, delete_in_zone, fun(_) -> ok end),
    test_utils:mock_new(Worker, oneprovider),
    test_utils:mock_expect(Worker, oneprovider, is_connected_to_oz, fun() -> true end),
    Config.

end_per_testcase(upgrade_from_19_02_x_storages, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker),
    ok.


end_per_suite(_Config) ->
    ok.
