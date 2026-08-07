%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% This file contains tests of lfm API on s3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_s3_test_SUITE).
-author("Michal Cwiertnia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).


%% tests
-export([
    fslogic_new_file_test/1,
    lfm_acl_test/1
]).

-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_acl_test
]).

-define(SPACE_ID, <<"space1">>).

all() ->
    ?ALL(?TEST_CASES).


%%%====================================================================
%%% Test function
%%%====================================================================

fslogic_new_file_test(Config) ->
    lfm_files_test_base:fslogic_new_file(Config).

lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    lfm_files_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).
