%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of proxy in multi provider environment.
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_proxy_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    proxy_basic_opts_test1/1,
    proxy_many_ops_test1/1,
    proxy_distributed_modification_test1/1,
    proxy_basic_opts_test2/1,
    proxy_many_ops_test2/1,
    proxy_distributed_modification_test2/1,
    proxy_many_ops_test1_base/1,
    proxy_many_ops_test2_base/1
]).

-define(TEST_CASES, [
    proxy_basic_opts_test1,
    proxy_many_ops_test1,
    proxy_distributed_modification_test1,
    proxy_basic_opts_test2,
    proxy_many_ops_test2,
    proxy_distributed_modification_test2
]).

-define(PERFORMANCE_TEST_CASES, [
    proxy_many_ops_test1,
    proxy_many_ops_test2
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
            [{name, dirs_num}, {value, 5}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 200}],
                [{name, files_num}, {value, 300}]
            ]},
            {description, ""}
        ]}
    ]).

proxy_basic_opts_test1(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user2">>, {0,4,1,2}, 0).

proxy_many_ops_test1(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
proxy_many_ops_test1_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user2">>, {0,4,1,2}, 0, DirsNum, FilesNum).

proxy_distributed_modification_test1(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user2">>, {0,4,1,2}, 0).

proxy_basic_opts_test2(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user3">>, {0,4,1,2}, 0).

proxy_many_ops_test2(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
proxy_many_ops_test2_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user3">>, {0,4,1,2}, 0, DirsNum, FilesNum).

proxy_distributed_modification_test2(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user3">>, {0,4,1,2}, 0).

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