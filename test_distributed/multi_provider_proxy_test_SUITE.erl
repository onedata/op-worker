%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
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

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    proxy_session_token_update_test/1
]).


all() -> [
    proxy_session_token_update_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================


proxy_session_token_update_test(Config) ->
    multi_provider_file_ops_test_base:proxy_session_token_update_test_base(
        Config, {0,4,1,2}, 10
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].


end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).


init_per_testcase(proxy_session_token_update_test = Case, Config) ->
    initializer:mock_auth_manager(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).


end_per_testcase(proxy_session_token_update_test = Case, Config) ->
    initializer:unmock_auth_manager(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
