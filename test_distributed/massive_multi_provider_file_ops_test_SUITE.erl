%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync and proxy
%%% @end
%%%-------------------------------------------------------------------
-module(massive_multi_provider_file_ops_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    db_sync_many_ops_test_base/1, multi_space_test/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test, db_sync_many_ops_test, db_sync_distributed_modification_test,
    multi_space_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

-define(performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, dirs_num}, {value, 2}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 25}],
                [{name, files_num}, {value, 75}]
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {3,0,0}, 60).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {3,0,0}, 60, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {3,0,0}, 60).

multi_space_test(Config) ->
    User = <<"user1">>,
    Spaces = ?config({spaces, User}, Config),
    Attempts = 120,

    SpaceConfigs = lists:foldl(fun({_, SN}, Acc) ->
        {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider} = case SN of
            <<"space1">> ->
                {3,0,0,1};
            _ ->
                {0,3,1,1}
        end,
        EC = multi_provider_file_ops_test_base:extend_config(Config, User,
            {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
        [{SN, EC} | Acc]
    end, [], Spaces),

    multi_provider_file_ops_test_base:multi_space_test_base(Config, SpaceConfigs, User).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].

init_per_testcase(_Case, Config) ->
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, dbsync_flush_queue_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(2))
    end, ?config(op_worker_nodes, Config)),

    ct:timetrap({minutes, 60}),
    application:start(etls),
    hackney:start(),
    initializer:enable_grpca_based_communication(Config),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(etls).
