%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage import continuous (update) scans on POSIX storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_posix_oct_test_SUITE).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- modifications ---
    append_file_update_test/1,
    append_file_not_changing_mtime_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    chmod_file_update_test/1,
    chmod_file_update_in_batched_dir_test/1,
    move_file_update_test/1,
    copy_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/1
]).

all() -> [
    %% --- modifications ---
    append_file_update_test,
    append_file_not_changing_mtime_update_test,
    append_empty_file_update_test,
    truncate_file_update_test,
    chmod_file_update_test,
    chmod_file_update_in_batched_dir_test,
    move_file_update_test,
    copy_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test
].

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = posix,
    importing_provider_selector = krakow,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_update_oct_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- modifications ---


append_file_update_test(_Config) -> ?run_test().
append_file_not_changing_mtime_update_test(_Config) -> ?run_test().
append_empty_file_update_test(_Config) -> ?run_test().
truncate_file_update_test(_Config) -> ?run_test().
chmod_file_update_test(_Config) -> ?run_test().
chmod_file_update_in_batched_dir_test(_Config) -> ?run_test().
move_file_update_test(_Config) -> ?run_test().
copy_file_update_test(_Config) -> ?run_test().
change_file_content_constant_size_test(_Config) -> ?run_test().
change_file_content_update_test(_Config) -> ?run_test().
change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(_Config) -> ?run_test().


%%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE, sd_test_utils, storage_file_setup_utils,
        storage_import_test_utils, storage_import_update_oct_test_base
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
            storage_import_update_oct_test_base:clean_up_after_previous_run(all(), ?SUITE_CTX),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    storage_import_update_oct_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    storage_import_update_oct_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
