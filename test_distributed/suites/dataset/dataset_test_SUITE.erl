%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of datasets mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_test_SUITE).
-author("Jakub Kudzia").

-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    foo_test/1
]).

all() -> ?ALL([
    foo_test
]).

%%%===================================================================
%%% API functions
%%%===================================================================

foo_test(_Config) ->
    ok.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "2op"}).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.