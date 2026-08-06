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
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            % undo what a previously interrupted run may have left behind on a reused deployment
            file_lfm_listing_tests:ensure_default_fold_cache_timeout(),
            file_lfm_crud_tests:ensure_storage_driver_unmocked(),
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
    Node = oct_background:get_random_provider_node(krakow),
    lfm_test_utils:clean_space(Node, [Node], oct_background:get_space_id(space_krk), ?ATTEMPTS),
    lfm_ct:clear_context().
