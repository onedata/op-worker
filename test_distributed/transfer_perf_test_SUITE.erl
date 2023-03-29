%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains performance tests featuring transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_perf_test_SUITE).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([rtransfer_stats_updates_test/1]).

all() -> ?ALL([
    rtransfer_stats_updates_test
]).


%%%===================================================================
%%% Test functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Check transfer stats and file location updates per second during
%% large transfers.
%% @end
%%-------------------------------------------------------------------
rtransfer_stats_updates_test(Config) ->
    ct:timetrap({hours, 4}),
    multi_provider_file_ops_test_base:rtransfer_test_base2(
        Config, <<"user1">>, {2,0,0,2}, 60, 14400, 1048576000000
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].


end_per_suite(_Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    ssl:start(),
    application:ensure_all_started(hackney),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    application:stop(hackney),
    ssl:stop().
