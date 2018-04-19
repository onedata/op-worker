%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains tests checking transfer stats and file location updates
%%% per second during large transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_2_provider_1_node_test_SUITE).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    rtransfer_test/1, rtransfer_test_base/1
]).

all() ->
    ?ALL([rtransfer_test]).

%%%===================================================================
%%% Test functions
%%%===================================================================

rtransfer_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, transfer_file_parts}, {value, 20000},
                {description, "Numbers of parts of transfered file."}]
        ]},
        {description, "Check transfer stats updates and file location updates
                        per second rates."}
    ]).
rtransfer_test_base(Config) ->
    TFP = ?config(transfer_file_parts, Config),
    multi_provider_file_ops_test_base:rtransfer_test_base2(Config, <<"user1">>,
        {2,0,0,2}, 500, TFP).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
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
