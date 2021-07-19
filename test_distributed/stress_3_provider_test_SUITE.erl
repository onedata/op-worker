%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for 3 provider environment.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_3_provider_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).
-export([stress_test/1, stress_test_base/1]).

-export([
    rtransfer_test/1, rtransfer_test_base/1
]).

-define(STRESS_CASES, [
    rtransfer_test
]).
-define(STRESS_NO_CLEARING_CASES, []).

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

rtransfer_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, small_files_num}, {value, 30},
                {description, "Numbers of small files used during test."}],
            [{name, medium_files_num}, {value, 20},
                {description, "Numbers of medium files used during test."}],
            [{name, big_files_num}, {value, 3},
                {description, "Numbers of big files used during test."}],
            [{name, big_file_parts}, {value, 10},
                {description, "Numbers of parts of big file."}],
            [{name, transfers_num}, {value, 2},
                {description, "Numbers of transfers used during test."}],
            [{name, transfer_file_parts}, {value, 20},
                {description, "Numbers of parts of transfered file."}]
        ]},
        {description, "Performs multiple file operations on space 1."}
    ]).
rtransfer_test_base(Config) ->
    SMN = ?config(small_files_num, Config),
    MFN = ?config(medium_files_num, Config),
    BFN = ?config(big_files_num, Config),
    BFP = ?config(big_file_parts, Config),
    TN = ?config(transfers_num, Config),
    TFP = ?config(transfer_file_parts, Config),
    multi_provider_file_ops_test_base:rtransfer_test_base(Config, <<"user1">>,
        {6,0,0,2}, 180, timer:minutes(5), SMN, MFN, BFN, BFP, TN, TFP).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(stress_test, Config) ->
    ssl:start(),
    application:ensure_all_started(hackney),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config, true),
    lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    application:stop(hackney),
    ssl:stop();

end_per_testcase(_Case, Config) ->
    Config.
