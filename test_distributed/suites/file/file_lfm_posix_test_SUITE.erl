%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the lfm API on a posix storage. This module holds only the wiring -
%%% every test body lives in the per-topic file_lfm_*_tests module the group
%%% belongs to, and is shared with file_lfm_s3_test_SUITE.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_posix_test_SUITE).
-author("Bartosz Walkowicz").

-include("file/file_lfm_test.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_and_unlink_test/1,
    create_under_regular_file_fails_test/1,
    basic_rdwr_test/1,
    rdwr_opens_storage_file_once_test/1,
    rdwr_after_storage_file_delete_test/1,
    write_and_read_all_subranges_test/1,
    write_and_check_test/1,
    file_gap_test/1,
    writes_are_visible_to_subsequent_opens_test/1,
    get_attrs_test/1,
    truncate_test/1,
    truncate_and_write_test/1,
    mkdir_and_rmdir_test/1,
    rmdir_of_space_dir_fails_test/1,
    rm_recursive_test/1,
    rm_recursive_of_space_dir_fails_test/1,
    close_deleted_open_files_test/1,
    ensure_dir_test/1,
    create_dir_at_path_test/1,

    recreate_handle_test/1,
    write_with_creation_handle_without_write_perms_test/1,
    recreate_handle_after_delete_test/1,
    direct_io_open_registers_no_storage_handle_test/1,
    open_failure_test/1,
    create_and_open_failure_test/1,
    open_failure_does_not_affect_other_session_test/1,
    mv_between_spaces_failure_test/1,
    monitored_open_releases_handle_on_process_death_test/1,
    list_process_handles_test/1,

    storage_file_is_created_on_open_test/1,
    mv_before_storage_file_creation_test/1,
    truncate_before_storage_file_creation_test/1,
    recreate_missing_storage_file_on_open_test/1,
    sparse_files_test/1,

    cp_file_test/1,
    cp_empty_dir_test/1,
    cp_dir_with_children_test/1,
    cp_dir_into_itself_fails_test/1,
    mv_dir_into_symlink_to_itself_fails_test/1,

    create_share_test/1,
    remove_share_test/1,
    share_root_getattr_test/1,
    share_child_getattr_test/1,
    share_get_parent_test/1,
    share_list_test/1,
    share_read_test/1,
    guest_cannot_access_unshared_file_test/1,

    get_children_attrs_of_empty_dir_test/1,
    get_children_attrs_with_zero_limit_test/1,
    get_children_attrs_with_zero_offset_test/1,
    get_children_attrs_with_non_zero_offset_test/1,
    get_children_attrs_with_limit_greater_than_dir_size_test/1,
    get_children_attrs_with_offset_beyond_dir_size_test/1,
    get_children_with_start_index_test/1,
    get_children_attrs_with_start_index_test/1,
    get_children_attrs_with_xattrs_test/1,

    get_children_with_pagination_token_test/1,
    get_children_with_pagination_token_and_not_full_last_batch_test/1,
    get_children_attrs_with_pagination_token_test/1,
    get_children_attrs_with_pagination_token_and_not_full_last_batch_test/1,

    get_recursive_file_list_test/1,
    get_recursive_file_list_with_prefix_test/1,
    get_recursive_file_list_with_inaccessible_paths_test/1,
    get_recursive_file_list_with_xattrs_test/1,
    get_recursive_file_list_spanning_multiple_internal_batches_test/1,

    ls_test/1, ls_test_base/1,
    ls_with_stats_test/1, ls_with_stats_test_base/1,
    echo_loop_test/1, echo_loop_test_base/1
]).

groups() -> [
    {crud_tests, [], [
        create_and_unlink_test,
        create_under_regular_file_fails_test,
        basic_rdwr_test,
        rdwr_opens_storage_file_once_test,
        rdwr_after_storage_file_delete_test,
        write_and_read_all_subranges_test,
        write_and_check_test,
        file_gap_test,
        writes_are_visible_to_subsequent_opens_test,
        get_attrs_test,
        truncate_test,
        truncate_and_write_test,
        mkdir_and_rmdir_test,
        rmdir_of_space_dir_fails_test,
        rm_recursive_test,
        rm_recursive_of_space_dir_fails_test,
        close_deleted_open_files_test,
        ensure_dir_test,
        create_dir_at_path_test
    ]},

    {handles_tests, [], [
        recreate_handle_test,
        write_with_creation_handle_without_write_perms_test,
        recreate_handle_after_delete_test,
        direct_io_open_registers_no_storage_handle_test,
        open_failure_test,
        create_and_open_failure_test,
        open_failure_does_not_affect_other_session_test,
        mv_between_spaces_failure_test,
        monitored_open_releases_handle_on_process_death_test,
        list_process_handles_test
    ]},

    {storage_tests, [], [
        storage_file_is_created_on_open_test,
        mv_before_storage_file_creation_test,
        truncate_before_storage_file_creation_test,
        recreate_missing_storage_file_on_open_test,
        sparse_files_test
    ]},

    {copy_tests, [], [
        cp_file_test,
        cp_empty_dir_test,
        cp_dir_with_children_test,
        cp_dir_into_itself_fails_test,
        mv_dir_into_symlink_to_itself_fails_test
    ]},

    {shares_tests, [], [
        create_share_test,
        remove_share_test,
        share_root_getattr_test,
        share_child_getattr_test,
        share_get_parent_test,
        share_list_test,
        share_read_test,
        guest_cannot_access_unshared_file_test
    ]},

    {children_listing_tests, [], [
        get_children_attrs_of_empty_dir_test,
        get_children_attrs_with_zero_limit_test,
        get_children_attrs_with_zero_offset_test,
        get_children_attrs_with_non_zero_offset_test,
        get_children_attrs_with_limit_greater_than_dir_size_test,
        get_children_attrs_with_offset_beyond_dir_size_test,
        get_children_with_start_index_test,
        get_children_attrs_with_start_index_test,
        get_children_attrs_with_xattrs_test
    ]},

    % NOTE: these tests tamper with the fold cache timeout on the provider,
    % so they must not run alongside any other listing
    {children_listing_with_pagination_token_tests, [], [
        get_children_with_pagination_token_test,
        get_children_with_pagination_token_and_not_full_last_batch_test,
        get_children_attrs_with_pagination_token_test,
        get_children_attrs_with_pagination_token_and_not_full_last_batch_test
    ]},

    {recursive_listing_tests, [], [
        get_recursive_file_list_test,
        get_recursive_file_list_with_prefix_test,
        get_recursive_file_list_with_inaccessible_paths_test,
        get_recursive_file_list_with_xattrs_test,
        get_recursive_file_list_spanning_multiple_internal_batches_test
    ]},

    {performance_tests, [], [
        ls_test,
        ls_with_stats_test,
        echo_loop_test
    ]}
].

-define(STANDARD_CASES, [
    {group, crud_tests},
    {group, handles_tests},
    {group, storage_tests},
    {group, copy_tests},
    {group, shares_tests},
    {group, children_listing_tests},
    {group, children_listing_with_pagination_token_tests},
    {group, recursive_listing_tests},
    {group, performance_tests}
]).

-define(PERFORMANCE_CASES, [
    ls_test,
    ls_with_stats_test,
    echo_loop_test
]).

all() -> ?ALL(?STANDARD_CASES, ?PERFORMANCE_CASES).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% CRUD tests
%%%===================================================================


create_and_unlink_test(_Config) ->
    ?RUN_CRUD_TEST().


create_under_regular_file_fails_test(_Config) ->
    ?RUN_CRUD_TEST().


basic_rdwr_test(_Config) ->
    ?RUN_CRUD_TEST().


rdwr_opens_storage_file_once_test(_Config) ->
    ?RUN_CRUD_TEST().


rdwr_after_storage_file_delete_test(_Config) ->
    ?RUN_CRUD_TEST().


write_and_read_all_subranges_test(_Config) ->
    ?RUN_CRUD_TEST().


write_and_check_test(_Config) ->
    ?RUN_CRUD_TEST().


file_gap_test(_Config) ->
    ?RUN_CRUD_TEST().


writes_are_visible_to_subsequent_opens_test(_Config) ->
    ?RUN_CRUD_TEST().


get_attrs_test(_Config) ->
    ?RUN_CRUD_TEST().


truncate_test(_Config) ->
    ?RUN_CRUD_TEST().


truncate_and_write_test(_Config) ->
    ?RUN_CRUD_TEST().


mkdir_and_rmdir_test(_Config) ->
    ?RUN_CRUD_TEST().


rmdir_of_space_dir_fails_test(_Config) ->
    ?RUN_CRUD_TEST().


rm_recursive_test(_Config) ->
    ?RUN_CRUD_TEST().


rm_recursive_of_space_dir_fails_test(_Config) ->
    ?RUN_CRUD_TEST().


close_deleted_open_files_test(_Config) ->
    ?RUN_CRUD_TEST().


ensure_dir_test(_Config) ->
    ?RUN_CRUD_TEST().


create_dir_at_path_test(_Config) ->
    ?RUN_CRUD_TEST().


%%%===================================================================
%%% Handle tests
%%%===================================================================


recreate_handle_test(_Config) ->
    ?RUN_HANDLES_TEST().


write_with_creation_handle_without_write_perms_test(_Config) ->
    ?RUN_HANDLES_TEST().


recreate_handle_after_delete_test(_Config) ->
    ?RUN_HANDLES_TEST().


direct_io_open_registers_no_storage_handle_test(_Config) ->
    ?RUN_HANDLES_TEST().


open_failure_test(_Config) ->
    ?RUN_HANDLES_TEST().


create_and_open_failure_test(_Config) ->
    ?RUN_HANDLES_TEST().


open_failure_does_not_affect_other_session_test(_Config) ->
    ?RUN_HANDLES_TEST().


mv_between_spaces_failure_test(_Config) ->
    ?RUN_HANDLES_TEST().


monitored_open_releases_handle_on_process_death_test(_Config) ->
    ?RUN_HANDLES_TEST().


list_process_handles_test(_Config) ->
    ?RUN_HANDLES_TEST().


%%%===================================================================
%%% Storage tests
%%%===================================================================


storage_file_is_created_on_open_test(_Config) ->
    ?RUN_STORAGE_TEST().


mv_before_storage_file_creation_test(_Config) ->
    ?RUN_STORAGE_TEST().


truncate_before_storage_file_creation_test(_Config) ->
    ?RUN_STORAGE_TEST().


recreate_missing_storage_file_on_open_test(_Config) ->
    ?RUN_STORAGE_TEST().


sparse_files_test(_Config) ->
    ?RUN_STORAGE_TEST([read]).


%%%===================================================================
%%% Copy tests
%%%===================================================================


cp_file_test(_Config) ->
    ?RUN_COPY_TEST().


cp_empty_dir_test(_Config) ->
    ?RUN_COPY_TEST().


cp_dir_with_children_test(_Config) ->
    ?RUN_COPY_TEST().


cp_dir_into_itself_fails_test(_Config) ->
    ?RUN_COPY_TEST().


mv_dir_into_symlink_to_itself_fails_test(_Config) ->
    ?RUN_COPY_TEST().


%%%===================================================================
%%% Share tests
%%%===================================================================


create_share_test(_Config) ->
    ?RUN_SHARES_TEST().


remove_share_test(_Config) ->
    ?RUN_SHARES_TEST().


share_root_getattr_test(_Config) ->
    ?RUN_SHARES_TEST().


share_child_getattr_test(_Config) ->
    ?RUN_SHARES_TEST().


share_get_parent_test(_Config) ->
    ?RUN_SHARES_TEST().


share_list_test(_Config) ->
    ?RUN_SHARES_TEST().


share_read_test(_Config) ->
    ?RUN_SHARES_TEST().


guest_cannot_access_unshared_file_test(_Config) ->
    ?RUN_SHARES_TEST().


%%%===================================================================
%%% Listing tests
%%%===================================================================


get_children_attrs_of_empty_dir_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_zero_limit_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_zero_offset_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_non_zero_offset_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_limit_greater_than_dir_size_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_offset_beyond_dir_size_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_with_start_index_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_start_index_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_xattrs_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_with_pagination_token_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_with_pagination_token_and_not_full_last_batch_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_pagination_token_test(_Config) ->
    ?RUN_LISTING_TEST().


get_children_attrs_with_pagination_token_and_not_full_last_batch_test(_Config) ->
    ?RUN_LISTING_TEST().


get_recursive_file_list_test(_Config) ->
    ?RUN_LISTING_TEST().


get_recursive_file_list_with_prefix_test(_Config) ->
    ?RUN_LISTING_TEST().


get_recursive_file_list_with_inaccessible_paths_test(_Config) ->
    ?RUN_LISTING_TEST().


get_recursive_file_list_with_xattrs_test(_Config) ->
    ?RUN_LISTING_TEST().


get_recursive_file_list_spanning_multiple_internal_batches_test(_Config) ->
    ?RUN_LISTING_TEST().


ls_test(Config) ->
    ?PERFORMANCE(Config, file_lfm_listing_tests:ls_test_performance_spec()).

ls_test_base(Config) ->
    file_lfm_listing_tests:ls_test_base(Config).


ls_with_stats_test(Config) ->
    ?PERFORMANCE(Config, file_lfm_listing_tests:ls_with_stats_test_performance_spec()).

ls_with_stats_test_base(Config) ->
    file_lfm_listing_tests:ls_with_stats_test_base(Config).


echo_loop_test(Config) ->
    ?PERFORMANCE(Config, file_lfm_crud_tests:echo_loop_test_performance_spec()).

echo_loop_test_base(Config) ->
    file_lfm_crud_tests:echo_loop_test_base(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [file_lfm_storage_tests, file_lfm_handles_tests]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            % undo what a previously interrupted run may have left behind on a reused deployment
            file_lfm_listing_tests:ensure_default_fold_cache_timeout(),
            file_lfm_test_utils:ensure_storage_driver_unmocked(),
            file_lfm_copy_tests:ensure_default_ls_batch_limit(),
            file_lfm_shares_tests:ensure_share_management_privileges(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    Config.


end_per_testcase(_Case, _Config) ->
    % NOTE: the tests that tamper with any of the below restore it themselves,
    % but a test case killed by its timetrap never gets to do so - and unlike the
    % case body, end_per_testcase is run by CT even then (see
    % test_server:handle_tc_exit/2). Hence the unconditional restore here, ahead
    % of the space cleanup, which is the part that may itself fail.
    file_lfm_listing_tests:ensure_default_fold_cache_timeout(),
    file_lfm_copy_tests:ensure_default_ls_batch_limit(),
    file_lfm_test_utils:ensure_storage_driver_unmocked(),
    file_lfm_test_utils:ensure_direct_io(),

    Node = file_lfm_test_utils:get_node(),
    lfm_test_utils:clean_space(Node, [Node], file_lfm_test_utils:get_space_id(), ?ATTEMPTS),
    lfm_ct:clear_context().
