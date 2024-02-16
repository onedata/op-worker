%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl permissions
%%% with corresponding lfm (logical_file_manager) functions and posix storage.
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_posix_test_SUITE).
-author("Bartosz Walkowicz").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    multi_provider_permission_cache_test/1,
    expired_session_test/1
]).

all() -> [
    multi_provider_permission_cache_test,
    expired_session_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================


multi_provider_permission_cache_test(Config) ->
    permissions_test_base:multi_provider_permission_cache_test(Config).


expired_session_test(Config) ->
    permissions_test_base:expired_session_test(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    permissions_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    permissions_test_base:end_per_suite(Config).


init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 5}),
    permissions_test_base:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) ->
    permissions_test_base:end_per_testcase(Case, Config).
