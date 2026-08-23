%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage import on POSIX storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_initial_posix_test_SUITE).
-author("Katarzyna Such").

-include("storage_import_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- structure ---
    import_empty_storage_test/1,
    import_empty_directory_test/1,
    import_empty_file_test/1,
    import_file_with_content_test/1,
    import_file_in_directory_test/1,
    import_many_subfiles_test/1,
    import_many_directories_test/1,
    import_nested_directory_tree_test/1,

    %% --- ownership (LUMA uid/gid) ---
    import_directory_check_user_id_test/1,
    import_file_check_user_id_test/1,
    import_directory_check_user_id_error_test/1,
    import_file_check_user_id_error_test/1,

    %% --- permissions ---
    import_directory_without_read_permission_test/1,

    %% --- acl ---
    import_nfs_acl_test/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1,

    %% --- ignored entries ---
    import_ignores_fifo_test/1,

    %% --- failure ---
    import_directory_error_test/1
]).

all() -> [
    %% --- structure ---
    import_empty_storage_test,
    import_empty_directory_test,
    import_empty_file_test,
    import_file_with_content_test,
    import_file_in_directory_test,
    import_many_subfiles_test,
    import_many_directories_test,
    import_nested_directory_tree_test,

    %% --- ownership (LUMA uid/gid) ---
    import_directory_check_user_id_test,
    import_file_check_user_id_test,
    import_directory_check_user_id_error_test,
    import_file_check_user_id_error_test,

    %% --- permissions ---
    import_directory_without_read_permission_test,

    %% --- acl ---
    import_nfs_acl_test,
    import_nfs_acl_with_disabled_luma_should_fail_test,

    %% --- ignored entries ---
    import_ignores_fifo_test,

    %% --- failure ---
    import_directory_error_test
].

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = posix,
    importing_provider_selector = krakow,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_initial_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- structure ---


import_empty_storage_test(_Config) -> ?run_test().
import_empty_directory_test(_Config) -> ?run_test().
import_empty_file_test(_Config) -> ?run_test().
import_file_with_content_test(_Config) -> ?run_test().
import_file_in_directory_test(_Config) -> ?run_test().
import_many_subfiles_test(_Config) -> ?run_test().
import_many_directories_test(_Config) -> ?run_test().
import_nested_directory_tree_test(_Config) -> ?run_test().


%% --- ownership (LUMA uid/gid) ---


import_directory_check_user_id_test(_Config) -> ?run_test().
import_file_check_user_id_test(_Config) -> ?run_test().
import_directory_check_user_id_error_test(_Config) -> ?run_test().
import_file_check_user_id_error_test(_Config) -> ?run_test().


%% --- permissions ---


import_directory_without_read_permission_test(_Config) -> ?run_test().


%% --- acl ---


import_nfs_acl_test(_Config) -> ?run_test().
import_nfs_acl_with_disabled_luma_should_fail_test(_Config) -> ?run_test().


%% --- ignored entries ---


import_ignores_fifo_test(_Config) -> ?run_test().


%% --- failure ---


import_directory_error_test(_Config) -> ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE, sd_test_utils, storage_file_setup_utils,
        storage_import_test_utils, storage_import_initial_test_base
    ],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {dbsync_changes_broadcast_interval, timer:seconds(1)},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            storage_import_test_utils:clean_up_after_previous_run(all(), ?SUITE_CTX),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    storage_import_initial_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    storage_import_initial_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
