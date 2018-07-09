%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains performance test for 2 provider environment.
%%% @end
%%%--------------------------------------------------------------------
-module(performance_2_provider_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    synchronizer_test/1, synchronizer_test_base/1
]).

-define(TEST_CASES, [
    synchronizer_test
]).

all() ->
    ?ALL(?TEST_CASES, ?TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

synchronizer_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, file_size_mb}, {value, 10}, {description, "File size in MB"}],
            [{name, block_size}, {value, 1}, {description, "Block size in bytes"}],
            [{name, block_count}, {value, 20000},
                {description, "Number of blocks read from each file"}],
            [{name, random_read}, {value, false}, {description, "Random read"}],
            [{name, separate_blocks}, {value, true},
                {description, "Separate blocks in non-random read"}],
            [{name, threads_num}, {value, 1}, {description, "Number of threads/files"}]
        ]},
        {description, "Tests performance of synchronizer"},
        {config, [{name, basic},
            {parameters, [
            ]},
            {description, ""}
        ]},
        {config, [{name, random},
            {parameters, [
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, simple},
            {parameters, [
                [{name, separate_blocks}, {value, false}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many1},
            {parameters, [
                [{name, threads_num}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many2},
            {parameters, [
                [{name, threads_num}, {value, 10}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many3},
            {parameters, [
                [{name, threads_num}, {value, 20}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many4},
            {parameters, [
                [{name, threads_num}, {value, 50}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many1_random},
            {parameters, [
                [{name, threads_num}, {value, 5}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many2_random},
            {parameters, [
                [{name, threads_num}, {value, 10}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many3_random},
            {parameters, [
                [{name, threads_num}, {value, 20}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many4_random},
            {parameters, [
                [{name, threads_num}, {value, 50}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]}
    ]).
synchronizer_test_base(Config) ->
    multi_provider_file_ops_test_base:synchronizer_test_base(Config).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].

init_per_testcase(_Case, Config) ->
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, minimal_sync_request, 1)
    end, ?config(op_worker_nodes, Config)),

    ssl:start(),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    hackney:stop(),
    ssl:stop().