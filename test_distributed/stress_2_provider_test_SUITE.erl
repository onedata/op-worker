%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for 2 provider environment.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_2_provider_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1]).

-export([
    db_sync_test/1, proxy_test1/1, proxy_test2/1,
    db_sync_test_base/1, proxy_test1_base/1, proxy_test2_base/1
]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
    proxy_test1, proxy_test2, db_sync_test
]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 75}, % TODO - check error i proxy test
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]
    ).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

db_sync_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, dirs_num}, {value, 25}, {description, "Number of directorines with single parent."}],
            [{name, files_num}, {value, 50}, {description, "Number of files with single parent."}],
            [{name, attempts}, {value, 60}, {description, "Attempts param for assertion macros"}]
        ]},
        {description, "Performs multiple file operations on space 1."}
    ]).
db_sync_test_base(Config) ->
    Dirs = ?config(dirs_num, Config),
    Files = ?config(files_num, Config),
    Attempts = ?config(attempts, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {2,0,0}, Attempts, Dirs, Files),
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {2,0,0}, Attempts).

%%%===================================================================

proxy_test1(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, dirs_num}, {value, 15}, {description, "Number of directorines with single parent."}],
            [{name, files_num}, {value, 25}, {description, "Number of files with single parent."}]
        ]},
        {description, "Performs multiple file operations on space 2."}
    ]).
proxy_test1_base(Config) ->
    Dirs = ?config(dirs_num, Config),
    Files = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user2">>, {0,2,1}, 0, Dirs, Files),
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user2">>, {0,2,1}, 0).

%%%===================================================================

proxy_test2(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, dirs_num}, {value, 15}, {description, "Number of directorines with single parent."}],
            [{name, files_num}, {value, 25}, {description, "Number of files with single parent."}]
        ]},
        {description, "Performs multiple file operations on space 3."}
    ]).
proxy_test2_base(Config) ->
    Dirs = ?config(dirs_num, Config),
    Files = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user3">>, {0,2,1}, 0, Dirs, Files),
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user3">>, {0,2,1}, 0).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].


init_per_testcase(stress_test, Config) ->
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    initializer:enable_grpca_based_communication(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    initializer:unload_quota_mocks(Config),
    hackney:stop(),
    application:stop(etls);

end_per_testcase(_Case, Config) ->
    Config.