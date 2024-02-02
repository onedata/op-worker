%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_SUITE).
-author("Jakub Kudzia").

-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("storage_import_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    check_storage_creation/1,
    check_space_creation/1
]).

-define(TEST_CASES, [
    check_storage_creation,
    check_space_creation
]).

-define(RANDOM_PROVIDER(), case ?RAND_BOOL() of
    true -> krakow;
    false -> paris
end).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

check_storage_creation(_Config) ->
    Data = #storage_spec{name = posix_test,
        params = #posix_storage_params{
            type = <<"posix">>,
            mountPoint = <<"/tmp">>
        }
    },
    storage_import_utils:create_storage(?RANDOM_PROVIDER(), Data).


check_space_creation(_Config) ->
    Data = #space_spec{name = space_test, owner = user1, users = [user2],
        supports = [
            #support_spec{
                provider = ?RANDOM_PROVIDER(),
                storage = #storage_spec{
                    name = posix_test,
                    params = #posix_storage_params{
                        type = <<"posix">>,
                        mountPoint = <<"/tmp">>
                }},
                size = 1000000
             }
    ]},
    storage_import_utils:create_space(Data).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
