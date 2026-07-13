%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests how storage import treats logical links (hardlinks and
%%% symlinks) in the space on POSIX storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_fs_links_posix_test_SUITE).
-author("Bartosz Walkowicz").

-include("storage_import_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- hardlinks (file x hardlink matrix) ---
    hardlink_file_kept_link_kept_test/1,
    hardlink_file_kept_link_deleted_while_open_test/1,
    hardlink_file_kept_link_deleted_test/1,
    hardlink_file_deleted_while_open_link_kept_test/1,
    hardlink_file_deleted_while_open_link_deleted_while_open_test/1,
    hardlink_file_deleted_while_open_link_deleted_test/1,
    hardlink_file_deleted_link_kept_test/1,
    hardlink_file_deleted_link_deleted_while_open_test/1,
    hardlink_file_deleted_link_deleted_test/1,

    %% --- symlinks ---
    symlink_is_ignored_by_continuous_scan_test/1
]).

all() -> [
    %% --- hardlinks (file x hardlink matrix) ---
    hardlink_file_kept_link_kept_test,
    hardlink_file_kept_link_deleted_while_open_test,
    hardlink_file_kept_link_deleted_test,
    hardlink_file_deleted_while_open_link_kept_test,
    hardlink_file_deleted_while_open_link_deleted_while_open_test,
    hardlink_file_deleted_while_open_link_deleted_test,
    hardlink_file_deleted_link_kept_test,
    hardlink_file_deleted_link_deleted_while_open_test,
    hardlink_file_deleted_link_deleted_test,

    %% --- symlinks ---
    symlink_is_ignored_by_continuous_scan_test
].

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = posix,
    importing_provider_selector = krakow,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_fs_links_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- hardlinks (file x hardlink matrix) ---


hardlink_file_kept_link_kept_test(_Config) -> ?run_test().
hardlink_file_kept_link_deleted_while_open_test(_Config) -> ?run_test().
hardlink_file_kept_link_deleted_test(_Config) -> ?run_test().
hardlink_file_deleted_while_open_link_kept_test(_Config) -> ?run_test().
hardlink_file_deleted_while_open_link_deleted_while_open_test(_Config) -> ?run_test().
hardlink_file_deleted_while_open_link_deleted_test(_Config) -> ?run_test().
hardlink_file_deleted_link_kept_test(_Config) -> ?run_test().
hardlink_file_deleted_link_deleted_while_open_test(_Config) -> ?run_test().
hardlink_file_deleted_link_deleted_test(_Config) -> ?run_test().


%% --- symlinks ---


symlink_is_ignored_by_continuous_scan_test(_Config) -> ?run_test().


%%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE, sd_test_utils, storage_file_setup_utils,
        storage_import_test_utils, storage_import_fs_links_test_base
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
    storage_import_fs_links_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    storage_import_fs_links_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
