%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for 2 provider environment with null storage.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_2_provider_null_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1]).

-export([
  random_read_test/1, random_read_test_base/1
]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
  random_read_test
]).

all() ->
  ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
  ?STRESS(Config,[
    {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
    {success_rate, 90},
    {config, [{name, stress}, {description, "Basic config for stress test"}]}
  ]
  ).
stress_test_base(Config) ->
  ?STRESS_TEST_BASE(Config).

%%%===================================================================

random_read_test(Config) ->
  ?PERFORMANCE(Config, [
    {parameters, [
      [{name, file_size_gb}, {value, 1}, {description, "File size in GB"}],
      [{name, block_size}, {value, 1}, {description, "Block size in bytes"}],
      [{name, block_per_repeat}, {value, 1000},
        {description, "Number of blocks read in each cycle"}]
    ]},
    {description, "Performs multiple file operations on space 1."}
  ]).
random_read_test_base(Config) ->
  multi_provider_file_ops_test_base:random_read_test_base(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
  [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].


init_per_testcase(stress_test, Config) ->
  lists:foreach(fun(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, minimal_sync_request, 1)
  end, ?config(op_worker_nodes, Config)),

  ssl:start(),
  hackney:start(),
  initializer:disable_quota_limit(Config),
  ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
  lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(stress_test, Config) ->
  lfm_proxy:teardown(Config),
  %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
  initializer:clean_test_users_and_spaces_no_validate(Config),
  initializer:unload_quota_mocks(Config),
  hackney:stop(),
  ssl:stop();

end_per_testcase(_Case, Config) ->
  Config.