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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    db_sync_many_ops_test_base/1, multi_space_test/1, rtransfer_test/1, rtransfer_test_base/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test, db_sync_many_ops_test, db_sync_distributed_modification_test,
    multi_space_test, rtransfer_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test, rtransfer_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

-define(db_sync_performance_description(Desc),
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

-define(rtransfer_performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, small_files_num}, {value, 3},
                {description, "Numbers of small files used during test."}],
            [{name, medium_files_num}, {value, 3},
                {description, "Numbers of medium files used during test."}],
            [{name, big_files_num}, {value, 1},
                {description, "Numbers of big files used during test."}],
            [{name, big_file_parts}, {value, 5},
                {description, "Numbers of parts of big file."}],
            [{name, transfers_num}, {value, 1},
                {description, "Numbers of transfers used during test."}],
            [{name, transfer_file_parts}, {value, 20},
                {description, "Numbers of parts of transfered file."}]
        ]},
        {description, Desc},
        % TODO - uncomment when rtransfer will be faster
%%        {config, [{name, large_config},
%%            {parameters, [
%%                [{name, small_files_num}, {value, 30}],
%%                [{name, medium_files_num}, {value, 20}],
%%                [{name, big_files_num}, {value, 3}],
%%                [{name, big_file_parts}, {value, 10}],
%%                [{name, transfers_num}, {value, 2}],
%%                [{name, transfer_file_parts}, {value, 20}]
%%            ]},
%%            {description, ""}
%%        ]},
%%        {config, [{name, small_config},
%%            {parameters, [
%%                [{name, small_files_num}, {value, 50}],
%%                [{name, medium_files_num}, {value, 0}],
%%                [{name, big_files_num}, {value, 0}],
%%                [{name, transfers_num}, {value, 0}]
%%            ]},
%%            {description, ""}
%%        ]},
%%        {config, [{name, very_small_config},
%%            {parameters, [
%%                [{name, small_files_num}, {value, 3}],
%%                [{name, medium_files_num}, {value, 0}],
%%                [{name, big_files_num}, {value, 0}],
%%                [{name, transfers_num}, {value, 0}]
%%            ]},
%%            {description, ""}
%%        ]},
        {config, [{name, default_config},
            {parameters, [
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {3,0,0}, 60).

rtransfer_test(Config) ->
    ?PERFORMANCE(Config, ?rtransfer_performance_description("Tests rtransfer")).
rtransfer_test_base(Config) ->
    SMN = ?config(small_files_num, Config),
    MFN = ?config(medium_files_num, Config),
    BFN = ?config(big_files_num, Config),
    BFP = ?config(big_file_parts, Config),
    TN = ?config(transfers_num, Config),
    TFP = ?config(transfer_file_parts, Config),
    multi_provider_file_ops_test_base:rtransfer_test_base(Config, <<"user1">>,
        {3,0,0}, 180, timer:minutes(5), SMN, MFN, BFN, BFP, TN, TFP).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?db_sync_performance_description("Tests working on dirs and files with db_sync")).
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
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
