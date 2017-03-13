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
-module(massive_multi_provider_file_ops2_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    file_consistency_test/1, db_sync_many_ops_test_base/1, file_consistency_test_base/1,
    permission_cache_invalidate_test/1, multi_space_test/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test, db_sync_many_ops_test, db_sync_distributed_modification_test, file_consistency_test,
    permission_cache_invalidate_test, multi_space_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test, file_consistency_test
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
                [{name, dirs_num}, {value, 10}],
                [{name, files_num}, {value, 20}]
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,2,0}, 120).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,2,0}, 120, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {4,2,0}, 120).

file_consistency_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_cases}, {value, [1,14]}, {description, "Number of test cases to be executed"}]
        ]},
        {description, "Tests file consistency"},
        {config, [{name, all_cases},
            {parameters, [
                [{name, test_cases}, {value, [1,2,3,4,5,6,7,8,9,10,11,12,13,14]}]
            ]},
            {description, ""}
        ]}
    ]).
file_consistency_test_base(Config) ->
    ConfigsNum = ?config(test_cases, Config),

    Workers = ?config(op_worker_nodes, Config),
    {Worker1, Worker2, Worker3} = lists:foldl(fun(W, {Acc1, Acc2, Acc3}) ->
        Check = fun(Acc, Prov) ->
            case is_atom(Acc) of
                true ->
                    Acc;
                _ ->
                    case string:str(atom_to_list(W), Prov) of
                        0 -> Acc;
                        _ -> W
                    end
            end
        end,
        {Check(Acc1, "p1"), Check(Acc2, "p2"), Check(Acc3, "p3")}
    end, {[], [], []}, Workers),

    multi_provider_file_ops_test_base:file_consistency_test_skeleton(Config, Worker1, Worker2, Worker3, ConfigsNum).

permission_cache_invalidate_test(Config) ->
    multi_provider_file_ops_test_base:permission_cache_invalidate_test_base(Config, 120).

multi_space_test(Config) ->
    User = <<"user1">>,
    Spaces = ?config({spaces, User}, Config),
    Attempts = 120,

    SpaceConfigs = lists:foldl(fun({_, SN}, Acc) ->
        {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider} = case SN of
            <<"space1">> ->
                {4,2,0,1};
            <<"space2">> ->
                {2,4,0,1};
            _ ->
                {0,6,1,1}
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
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).