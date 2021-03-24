%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of hardlinks in multi provider environment
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_hardlinks_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    foo_test/1
]).

all() -> [
    foo_test
].

%%%===================================================================
%%% Test functions
%%%===================================================================

foo_test(_) -> ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).