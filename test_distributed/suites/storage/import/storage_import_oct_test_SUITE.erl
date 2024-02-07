%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_SUITE).
-author("Katarzyna Such").

-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("space_setup_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_storage_test/1,
    set_up_space_test/1
]).

-define(TEST_CASES, [
    create_storage_test,
    set_up_space_test
]).

-define(RANDOM_PROVIDER(), ?RAND_ELEMENT([krakow, paris])).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_storage_test(_Config) ->
    Data =  #posix_storage_params{mount_point = <<"/tmp">>},
    space_setup_utils:create_storage(?RANDOM_PROVIDER(), Data).


set_up_space_test(_Config) ->
    Data = #space_spec{name = space_test, owners = [user1], users = [user2],
        supports = [
            #support_spec{
                provider = ?RANDOM_PROVIDER(),
                storage_params = #posix_storage_params{mount_point = <<"/tmp">>},
                size = 1000000
             }
    ]},
    space_setup_utils:set_up_space(Data).

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
