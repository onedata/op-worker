%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests replica migration transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_migration_test_SUITE).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- basic shapes ---
    empty_dir_test/1,
    tree_of_empty_dirs_test/1,
    regular_file_test/1,
    file_in_directory_test/1,
    big_file_test/1,

    %% --- scale ---
    hundred_files_in_one_transfer_test/1,
    hundred_files_in_separate_transfers_test/1,

    %% --- protection flags ---
    transfer_despite_protection_flags_test/1
]).

all() -> [
    %% --- basic shapes ---
    empty_dir_test,
    tree_of_empty_dirs_test,
    regular_file_test,
    file_in_directory_test,
    big_file_test,

    %% --- scale ---
    hundred_files_in_one_transfer_test,
    hundred_files_in_separate_transfers_test,

    %% --- protection flags ---
    transfer_despite_protection_flags_test
].

-define(SUITE_CTX, #transfer_test_suite_ctx{
    transfer_type = migration,
    space_selector = space_krk_par_p,
    user_selector = user1,
    creation_provider_selector = krakow,
    other_provider_selector = paris
}).
-define(run_test(), transfer_common_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- basic shapes ---


empty_dir_test(_Config) -> ?run_test().
tree_of_empty_dirs_test(_Config) -> ?run_test().
regular_file_test(_Config) -> ?run_test().
file_in_directory_test(_Config) -> ?run_test().
big_file_test(_Config) -> ?run_test().


%% --- scale ---


hundred_files_in_one_transfer_test(_Config) -> ?run_test().
hundred_files_in_separate_transfers_test(_Config) -> ?run_test().


%% --- protection flags ---


transfer_despite_protection_flags_test(_Config) -> ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, transfer_test_utils, transfer_common_test_base],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    transfer_common_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    transfer_common_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
