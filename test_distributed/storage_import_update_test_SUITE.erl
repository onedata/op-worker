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
-module(storage_import_update_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    create_delete_import2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1
]).

-define(TEST_CASES, [
    create_delete_import2_test,
    create_subfiles_and_delete_before_import_is_finished_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_delete_import2_test(Config) ->
    storage_import_test_base:create_delete_import2_test(Config).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_import_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_import_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_import_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_import_test_base:init_per_testcase(Case, Config).

end_per_testcase(_Case, Config) ->
    storage_import_test_base:end_per_testcase(_Case, Config).