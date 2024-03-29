%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% This file contains tests of lfm API on posix storage.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_posix_test_SUITE).
-author("Michal Cwiertnia").

-include("lfm_files_test_base.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_create_and_unlink_test/1,
    lfm_create_and_access_test/1,
    lfm_create_failure/1,
    lfm_basic_rename_test/1,
    lfm_basic_rdwr_test/1,
    lfm_basic_rdwr_opens_file_once_test/1,
    lfm_basic_rdwr_after_file_delete_test/1,
    lfm_write_test/1,
    lfm_stat_test/1,
    lfm_get_attrs_test/1,
    lfm_synch_stat_test/1,
    lfm_cp_file/1,
    lfm_cp_empty_dir/1,
    lfm_cp_dir_to_itself_should_fail/1,
    lfm_cp_dir_to_symlink_to_this_dir_should_fail/1,
    lfm_cp_dir_to_its_child_should_fail/1,
    lfm_cp_dir/1,
    lfm_truncate_test/1,
    lfm_truncate_and_write/1,
    lfm_acl_test/1,
    lfm_rmdir_test/1,
    lfm_rmdir_fails_with_eperm_on_space_directory_test/1,
    rm_recursive_test/1,
    rm_recursive_fails_with_eperm_on_space_directory_test/1,
    file_gap_test/1,
    ls_test/1,
    ls_with_stats_test/1,
    create_share_dir_test/1,
    create_share_file_test/1,
    remove_share_test/1,
    share_getattr_test/1,
    share_get_parent_test/1,
    share_list_test/1,
    share_read_test/1,
    share_child_getattr_test/1,
    share_child_list_test/1,
    share_child_read_test/1,
    share_permission_denied_test/1,
    echo_loop_test/1,
    storage_file_creation_should_be_deferred_until_open/1,
    deferred_creation_should_not_prevent_mv/1,
    deferred_creation_should_not_prevent_truncate/1,
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
    lfm_ensure_dir/1, 
    readdir_plus_should_return_empty_result_for_empty_dir/1,
    readdir_plus_should_return_empty_result_zero_size/1,
    readdir_plus_should_work_with_zero_offset/1,
    readdir_plus_should_work_with_non_zero_offset/1,
    readdir_plus_should_work_with_size_greater_than_dir_size/1,
    readdir_plus_should_work_with_api_token/1,
    readdir_plus_should_work_with_api_token_not_full_batch/1,
    readdir_should_work_with_api_token/1,
    readdir_should_work_with_api_token_not_full_batch/1,
    readdir_should_work_with_startid/1,
    readdir_plus_should_read_xattrs/1,
    get_children_attrs_should_return_empty_result_for_empty_dir/1,
    get_children_attrs_should_return_empty_result_zero_size/1,
    get_children_attrs_should_work_with_zero_offset/1,
    get_children_attrs_should_work_with_non_zero_offset/1,
    get_children_attrs_should_work_with_size_greater_than_dir_size/1,
    get_children_attrs_should_work_with_startid/1,
    get_recursive_file_list/1,
    get_recursive_file_list_prefix_test/1,
    get_recursive_file_list_inaccessible_paths_test/1,
    get_recursive_file_list_should_read_xattrs/1,
    get_recursive_file_list_internal_multibatch/1,
    lfm_recreate_handle_test/1,
    lfm_write_after_create_no_perms_test/1,
    lfm_recreate_handle_after_delete_test/1,
    lfm_open_failure_test/1,
    lfm_create_and_open_failure_test/1,
    lfm_open_in_direct_mode_test/1,
    lfm_mv_failure_test/1,
    lfm_mv_dir_to_symlink_to_this_dir_should_fail/1,
    lfm_open_multiple_times_failure_test/1,
    lfm_open_failure_multiple_users_test/1,
    lfm_open_and_create_open_failure_test/1,
    lfm_mv_failure_multiple_users_test/1,
    sparse_files_should_be_created/1,
    rename_removed_opened_file_test/1,
    mkdir_removed_opened_file_test/1,
    rename_removed_opened_file_races_test/1,
    rename_removed_opened_file_races_test2/1,
    lfm_monitored_open/1,
    lfm_create_and_read_symlink/1,
    lfm_create_hardlink_to_symlink/1,
    recreate_file_on_storage/1,
    lfm_close_deleted_open_files/1,
    lfm_create_dir_at_path/1
]).


-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_create_and_unlink_test,
    lfm_create_and_access_test,
    lfm_create_failure,
    lfm_basic_rename_test,
    lfm_basic_rdwr_test,
    lfm_basic_rdwr_opens_file_once_test,
    lfm_basic_rdwr_after_file_delete_test,
    lfm_write_test,
    lfm_stat_test,
    lfm_get_attrs_test,
    lfm_synch_stat_test,
    lfm_cp_file,
    lfm_cp_empty_dir,
    lfm_cp_dir_to_itself_should_fail,
    lfm_cp_dir_to_symlink_to_this_dir_should_fail,
    lfm_cp_dir_to_its_child_should_fail,
    lfm_cp_dir,
    lfm_truncate_test,
    lfm_truncate_and_write,
    lfm_acl_test,
    lfm_rmdir_test,
    lfm_rmdir_fails_with_eperm_on_space_directory_test,
    rm_recursive_test,
    rm_recursive_fails_with_eperm_on_space_directory_test,
    file_gap_test,
    ls_test,
    ls_with_stats_test,
    create_share_dir_test,
    create_share_file_test,
    remove_share_test,
    share_getattr_test,
    share_get_parent_test,
    share_list_test,
    share_read_test,
    share_child_getattr_test,
    share_child_list_test,
    share_child_read_test,
    share_permission_denied_test,
    echo_loop_test,
    storage_file_creation_should_be_deferred_until_open,
    deferred_creation_should_not_prevent_mv,
    deferred_creation_should_not_prevent_truncate,
    new_file_should_not_have_popularity_doc,
    new_file_should_have_zero_popularity,
    opening_file_should_increase_file_popularity,
    file_popularity_should_have_correct_file_size,
    lfm_ensure_dir, 
    readdir_plus_should_return_empty_result_for_empty_dir,
    readdir_plus_should_return_empty_result_zero_size,
    readdir_plus_should_work_with_zero_offset,
    readdir_plus_should_work_with_non_zero_offset,
    readdir_plus_should_work_with_size_greater_than_dir_size,
    readdir_plus_should_work_with_api_token_not_full_batch,
    readdir_plus_should_work_with_api_token,
    readdir_should_work_with_api_token,
    readdir_should_work_with_api_token_not_full_batch,
    readdir_should_work_with_startid,
    readdir_plus_should_read_xattrs,
    get_children_attrs_should_return_empty_result_for_empty_dir,
    get_children_attrs_should_return_empty_result_zero_size,
    get_children_attrs_should_work_with_zero_offset,
    get_children_attrs_should_work_with_non_zero_offset,
    get_children_attrs_should_work_with_size_greater_than_dir_size,
    get_children_attrs_should_work_with_startid,
    get_recursive_file_list,
    get_recursive_file_list_prefix_test,
    get_recursive_file_list_inaccessible_paths_test,
    get_recursive_file_list_should_read_xattrs,
    get_recursive_file_list_internal_multibatch,
    lfm_recreate_handle_test,
    lfm_write_after_create_no_perms_test,
    lfm_recreate_handle_after_delete_test,
    lfm_open_failure_test,
    lfm_create_and_open_failure_test,
    lfm_open_in_direct_mode_test,
    lfm_mv_failure_test,
    lfm_mv_dir_to_symlink_to_this_dir_should_fail,
    lfm_open_multiple_times_failure_test,
    lfm_open_failure_multiple_users_test,
    lfm_open_and_create_open_failure_test,
    lfm_mv_failure_multiple_users_test,
    sparse_files_should_be_created,
    rename_removed_opened_file_test,
    mkdir_removed_opened_file_test,
    rename_removed_opened_file_races_test,
    rename_removed_opened_file_races_test2,
    lfm_monitored_open,
    lfm_create_and_read_symlink,
    lfm_create_hardlink_to_symlink,
    recreate_file_on_storage,
    lfm_close_deleted_open_files,
    lfm_create_dir_at_path
]).


-define(PERFORMANCE_TEST_CASES, [
    ls_test, ls_with_stats_test, echo_loop_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).


%%%====================================================================
%%% Test function
%%%====================================================================


fslogic_new_file_test(Config) ->
    lfm_files_test_base:fslogic_new_file(Config).


lfm_create_and_unlink_test(Config) ->
    lfm_files_test_base:lfm_create_and_unlink(Config).


lfm_create_and_access_test(Config) ->
    lfm_files_test_base:lfm_create_and_access(Config).


lfm_create_failure(Config) ->
    lfm_files_test_base:lfm_create_failure(Config).


lfm_basic_rename_test(Config) ->
    lfm_files_test_base:lfm_basic_rename(Config).


lfm_basic_rdwr_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr(Config).


lfm_basic_rdwr_opens_file_once_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr_opens_file_once(Config).


lfm_basic_rdwr_after_file_delete_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr_after_file_delete(Config).


lfm_write_test(Config) ->
    lfm_files_test_base:lfm_write(Config).


lfm_stat_test(Config) ->
    lfm_files_test_base:lfm_stat(Config).


lfm_get_attrs_test(Config) ->
    lfm_files_test_base:lfm_stat2(Config).


lfm_synch_stat_test(Config) ->
    lfm_files_test_base:lfm_synch_stat(Config).

lfm_cp_file(Config) ->
    lfm_files_test_base:lfm_cp_file(Config).

lfm_cp_empty_dir(Config) ->
    lfm_files_test_base:lfm_cp_empty_dir(Config).

lfm_cp_dir_to_itself_should_fail(Config) ->
    lfm_files_test_base:lfm_cp_dir_to_itself_should_fail(Config).

lfm_cp_dir_to_symlink_to_this_dir_should_fail(Config) ->
    lfm_files_test_base:lfm_cp_dir_to_symlink_to_this_dir_should_fail(Config).

lfm_cp_dir_to_its_child_should_fail(Config) ->
    lfm_files_test_base:lfm_cp_dir_to_its_child_should_fail(Config).

lfm_cp_dir(Config) ->
    lfm_files_test_base:lfm_cp_dir(Config).

lfm_truncate_test(Config) ->
    lfm_files_test_base:lfm_truncate(Config).


lfm_truncate_and_write(Config) ->
    lfm_files_test_base:lfm_truncate_and_write(Config).


lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).


lfm_rmdir_test(Config) ->
    lfm_files_test_base:lfm_rmdir(Config).


lfm_rmdir_fails_with_eperm_on_space_directory_test(Config) ->
    lfm_files_test_base:lfm_rmdir_fails_with_eperm_on_space_directory(Config).


rm_recursive_test(Config) ->
    lfm_files_test_base:rm_recursive(Config).


rm_recursive_fails_with_eperm_on_space_directory_test(Config) ->
    lfm_files_test_base:rm_recursive_fails_with_eperm_on_space_directory(Config).


file_gap_test(Config) ->
    lfm_files_test_base:file_gap(Config).


ls_test(Config) ->
    lfm_files_test_base:ls(Config).


ls_with_stats_test(Config) ->
    lfm_files_test_base:ls_with_stats(Config).


create_share_dir_test(Config) ->
    lfm_files_test_base:create_share_dir(Config).


create_share_file_test(Config) ->
    lfm_files_test_base:create_share_file(Config).


remove_share_test(Config) ->
    lfm_files_test_base:remove_share(Config).


share_getattr_test(Config) ->
    lfm_files_test_base:share_getattr(Config).


share_get_parent_test(Config) ->
    lfm_files_test_base:share_get_parent(Config).


share_list_test(Config) ->
    lfm_files_test_base:share_list(Config).


share_read_test(Config) ->
    lfm_files_test_base:share_read(Config).


share_child_getattr_test(Config) ->
    lfm_files_test_base:share_child_getattr(Config).


share_child_list_test(Config) ->
    lfm_files_test_base:share_child_list(Config).


share_child_read_test(Config) ->
    lfm_files_test_base:share_child_read(Config).


share_permission_denied_test(Config) ->
    lfm_files_test_base:share_permission_denied(Config).


echo_loop_test(Config) ->
    lfm_files_test_base:echo_loop(Config).


storage_file_creation_should_be_deferred_until_open(Config) ->
    lfm_files_test_base:storage_file_creation_should_be_deferred_until_open(Config).


deferred_creation_should_not_prevent_mv(Config) ->
    lfm_files_test_base:deferred_creation_should_not_prevent_mv(Config).


deferred_creation_should_not_prevent_truncate(Config) ->
    lfm_files_test_base:deferred_creation_should_not_prevent_truncate(Config).


new_file_should_not_have_popularity_doc(Config) ->
    lfm_files_test_base:new_file_should_not_have_popularity_doc(Config).


new_file_should_have_zero_popularity(Config) ->
    lfm_files_test_base:new_file_should_have_zero_popularity(Config).


opening_file_should_increase_file_popularity(Config) ->
    lfm_files_test_base:opening_file_should_increase_file_popularity(Config).


file_popularity_should_have_correct_file_size(Config) ->
    lfm_files_test_base:file_popularity_should_have_correct_file_size(Config).


lfm_ensure_dir(Config) ->
    lfm_files_test_base:lfm_ensure_dir(Config).


readdir_plus_should_return_empty_result_for_empty_dir(Config) ->
    lfm_files_test_base:readdir_plus_should_return_empty_result_for_empty_dir(Config).


readdir_plus_should_return_empty_result_zero_size(Config) ->
    lfm_files_test_base:readdir_plus_should_return_empty_result_zero_size(Config).


readdir_plus_should_work_with_zero_offset(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_zero_offset(Config).


readdir_plus_should_work_with_non_zero_offset(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_non_zero_offset(Config).


readdir_plus_should_work_with_size_greater_than_dir_size(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_size_greater_than_dir_size(Config).


readdir_plus_should_work_with_api_token(Config) ->
    lfm_files_test_base:readdir_should_work_with_token(Config, 12, readdir_plus).


readdir_plus_should_work_with_api_token_not_full_batch(Config) ->
    lfm_files_test_base:readdir_should_work_with_token(Config, 10, readdir_plus).


readdir_should_work_with_api_token(Config) ->
    lfm_files_test_base:readdir_should_work_with_token(Config, 12, readdir).


readdir_should_work_with_api_token_not_full_batch(Config) ->
    lfm_files_test_base:readdir_should_work_with_token(Config, 10, readdir).


readdir_should_work_with_startid(Config) ->
    lfm_files_test_base:readdir_should_work_with_startid(Config).


readdir_plus_should_read_xattrs(Config) ->
    lfm_files_test_base:readdir_plus_should_read_xattrs(Config).


get_recursive_file_list_should_read_xattrs(Config) ->
    lfm_files_test_base:get_recursive_file_list_should_read_xattrs(Config).

get_recursive_file_list_internal_multibatch(Config) ->
    lfm_files_test_base:get_recursive_file_list_internal_multibatch(Config).


get_children_attrs_should_return_empty_result_for_empty_dir(Config) ->
    lfm_files_test_base:get_children_attrs_should_return_empty_result_for_empty_dir(Config).


get_children_attrs_should_return_empty_result_zero_size(Config) ->
    lfm_files_test_base:get_children_attrs_should_return_empty_result_zero_size(Config).


get_children_attrs_should_work_with_zero_offset(Config) ->
    lfm_files_test_base:get_children_attrs_should_work_with_zero_offset(Config).


get_children_attrs_should_work_with_non_zero_offset(Config) ->
    lfm_files_test_base:get_children_attrs_should_work_with_non_zero_offset(Config).


get_children_attrs_should_work_with_size_greater_than_dir_size(Config) ->
    lfm_files_test_base:get_children_attrs_should_work_with_size_greater_than_dir_size(Config).


get_children_attrs_should_work_with_startid(Config) ->
    lfm_files_test_base:get_children_attrs_should_work_with_startid(Config).


get_recursive_file_list(Config) ->
    lfm_files_test_base:get_recursive_file_list(Config).


get_recursive_file_list_prefix_test(Config) ->
    lfm_files_test_base:get_recursive_file_list_prefix_test_base(Config).


get_recursive_file_list_inaccessible_paths_test(Config) ->
    lfm_files_test_base:get_recursive_file_list_inaccessible_paths_test_base(Config).


lfm_recreate_handle_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, ?DEFAULT_FILE_PERMS, dont_delete_file).


lfm_write_after_create_no_perms_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, 8#444, dont_delete_file).


lfm_recreate_handle_after_delete_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, ?DEFAULT_FILE_PERMS, delete_after_open).


lfm_open_failure_test(Config) ->
    lfm_files_test_base:lfm_open_failure(Config).


lfm_create_and_open_failure_test(Config) ->
    lfm_files_test_base:lfm_create_and_open_failure(Config).


lfm_open_in_direct_mode_test(Config) ->
    lfm_files_test_base:lfm_open_in_direct_mode(Config).


lfm_mv_failure_test(Config) ->
    lfm_files_test_base:lfm_mv_failure(Config).


lfm_mv_dir_to_symlink_to_this_dir_should_fail(Config) ->
    lfm_files_test_base:lfm_mv_dir_to_symlink_to_this_dir_should_fail(Config).


lfm_open_multiple_times_failure_test(Config) ->
    lfm_files_test_base:lfm_open_multiple_times_failure(Config).


lfm_open_failure_multiple_users_test(Config) ->
    lfm_files_test_base:lfm_open_failure_multiple_users(Config).


lfm_open_and_create_open_failure_test(Config) ->
    lfm_files_test_base:lfm_open_and_create_open_failure(Config).


lfm_mv_failure_multiple_users_test(Config) ->
    lfm_files_test_base:lfm_mv_failure_multiple_users(Config).

sparse_files_should_be_created(Config) ->
    lfm_files_test_base:sparse_files_should_be_created(Config, read).

rename_removed_opened_file_test(Config) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    User = <<"user1">>,
    User2 = <<"user2">>,

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),
    Guid1String = binary_to_list(Guid1),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([Guid1String], ListAns3 -- InitialDeletedDir),
    RenamedStorageId = filename:join([?DELETED_OPENED_FILES_DIR, Guid1]),
    ?assertMatch({ok, #file_location{file_id = RenamedStorageId}},
        lfm_proxy:get_file_location(Worker, SessId(User), ?FILE_REF(Guid1))),
    ?assertMatch({error, ?EACCES}, lfm_proxy:get_file_location(Worker, SessId(User2), ?FILE_REF(Guid1))),

    lfm_proxy:close_all(Worker),
    {ok, ListAns4} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns4 -- InitialDeletedDir),

    ok.


mkdir_removed_opened_file_test(Config) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileName2 = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    FilePath2 = <<FilePath/binary, "/", FileName2/binary>>,
    User = <<"user1">>,

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),
    Guid1String = binary_to_list(Guid1),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([Guid1String], ListAns3 -- InitialDeletedDir),

    {ok, _} = lfm_proxy:mkdir(Worker, SessId(User), FilePath),
    {ok, _} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath2),
    {ok, ListAns4} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns4 -- InitialSpaceFiles),

    lfm_proxy:close_all(Worker),
    {ok, ListAns5} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns5 -- InitialDeletedDir),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath2})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId(User), {path, FilePath})),
    {ok, ListAns6} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns6 -- InitialSpaceFiles),
    ok.


rename_removed_opened_file_races_test(Config) ->
    rename_removed_opened_file_races_test_base(Config, before_mv).


rename_removed_opened_file_races_test2(Config) ->
    rename_removed_opened_file_races_test_base(Config, after_mv).


rename_removed_opened_file_races_test_base(Config, MockOpts) ->
    SpaceId = ?SPACE_ID1,
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = fun(User) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end,
    FileName = generator:gen_name(),
    FileNameString = binary_to_list(FileName),
    FilePath = <<"/space_name1/",  FileName/binary>>,
    User = <<"user1">>,
    Master = self(),

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, InitialSpaceFiles} = case rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])]) of
        {error, ?ENOENT} -> {ok, []};
        Other -> Other
    end,
    {ok, InitialDeletedDir} = case rpc:call(Worker, file, list_dir,
        [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])]) of
        {error, ?ENOENT} -> {ok, []};
        Other2 -> Other2
    end,

    case MockOpts of
        before_mv ->
            test_utils:mock_expect(Workers, storage_driver, mv,
                fun(Handle, TargetFileId) ->
                    case get(mv_test) of
                        undefined ->
                            put(mv_test, ok),
                            Master ! {mv_beg, self()},
                            receive
                                mv_start -> ok
                            end;
                        _ ->
                            ok
                    end,
                    meck:passthrough([Handle, TargetFileId])
                end);
        after_mv ->
            test_utils:mock_expect(Workers, storage_driver, mv,
                fun(Handle, TargetFileId) ->
                    case meck:passthrough([Handle, TargetFileId]) of
                        ok ->
                            Master ! {mv_beg, self()},
                            receive
                                mv_start -> ok
                            end;
                        Other3 ->
                            Other3
                    end
                end)
    end,

    {ok, {Guid1, _}} = lfm_proxy:create_and_open(Worker, SessId(User), FilePath),

    StorageDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    {ok, ListAns} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([FileNameString], ListAns -- InitialSpaceFiles),

    spawn(fun() ->
        Ans = lfm_proxy:unlink(Worker, SessId(User), {path, FilePath}),
        Master ! {unlink_ans, Ans}
    end),

    ?assertEqual(ok, receive
        {mv_beg, MvPid} ->
            lfm_proxy:close_all(Worker),
            MvPid ! mv_start,
            ok
    after
        5000 -> timeout
    end),

    ?assertEqual(ok, receive
        {unlink_ans, UnlinkAns} -> UnlinkAns
    after
        5000 -> timeout
    end),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId(User), ?FILE_REF(Guid1))),
    {ok, StorageDirList} = ?assertMatch({ok, _}, rpc:call(Worker, file, list_dir, [StorageDir])),
    ?assert(lists:member(?DELETED_OPENED_FILES_DIR_STRING, StorageDirList)),
    {ok, ListAns2} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, SpaceId])])),
    ?assertEqual([], ListAns2 -- InitialSpaceFiles),
    {ok, ListAns3} = ?assertMatch({ok, _},
        rpc:call(Worker, file, list_dir, [filename:join([StorageDir, ?DELETED_OPENED_FILES_DIR])])),
    ?assertEqual([], ListAns3 -- InitialDeletedDir),

    ok.


lfm_monitored_open(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    File1Path = <<"/space_name1/lfm_monitored_open1">>,
    {ok, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, File1Path)),
    File1Uuid = file_id:guid_to_uuid(File1Guid),

    File2Path = <<"/space_name1/lfm_monitored_open2">>,
    {ok, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, File2Path)),
    File2Uuid = file_id:guid_to_uuid(File2Guid),

    Self = self(),
    Attempts = 10,

    OpenAndHungFun = fun() ->
        Self !  lfm:open(SessId1, ?FILE_REF(File1Guid), read),
        receive _ -> ok end
    end,
    MonitoredOpenAndHungFun = fun() ->
        Self !  lfm:monitored_open(SessId1, ?FILE_REF(File2Guid), read),
        receive _ -> ok end
    end,
    GetAllProcessHandles = fun(Pid) ->
        rpc:call(W, process_handles, get_all_process_handles, [Pid])
    end,

    % Assert that handle remains open if it was created using 'open' and wasn't closed before
    % process died.
    ProcOpeningFile = spawn(W, OpenAndHungFun),

    receive
        {ok, OpenedFileHandle} ->
            HandleId1 = lfm_context:get_handle_id(OpenedFileHandle),
            ?assertMatch({ok, _}, rpc:call(W, session_handles, get, [SessId1, HandleId1]), Attempts),
            ?assertMatch(true, rpc:call(W, file_handles, is_file_opened, [File1Uuid]), Attempts),
            ?assertMatch(?ERROR_NOT_FOUND, GetAllProcessHandles(ProcOpeningFile), Attempts),

            exit(ProcOpeningFile, kill),
            timer:sleep(1000),

            ?assertMatch({ok, _}, rpc:call(W, session_handles, get, [SessId1, HandleId1]), Attempts),
            ?assertMatch(true, rpc:call(W, file_handles, is_file_opened, [File1Uuid]), Attempts),
            ?assertMatch(?ERROR_NOT_FOUND, GetAllProcessHandles(ProcOpeningFile), Attempts);
        Error1 ->
            ct:fail(Error1)
    end,

    % Assert that handle is released even if file wasn't closed before process died
    % when it was created using 'monitored_open'.
    ProcMonitorOpeningFile = spawn(W, MonitoredOpenAndHungFun),

    receive
        {ok, MonitorOpenedFileHandle} ->
            HandleId2 = lfm_context:get_handle_id(MonitorOpenedFileHandle),
            ?assertMatch({ok, _}, rpc:call(W, session_handles, get, [SessId1, HandleId2]), Attempts),
            ?assertMatch(true, rpc:call(W, file_handles, is_file_opened, [File2Uuid]), Attempts),
            ?assertMatch({ok, [MonitorOpenedFileHandle]}, GetAllProcessHandles(ProcMonitorOpeningFile), Attempts),

            exit(ProcMonitorOpeningFile, kill),
            timer:sleep(1000),

            ?assertMatch(?ERROR_NOT_FOUND, rpc:call(W, session_handles, get, [SessId1, HandleId2]), Attempts),
            ?assertMatch(false, rpc:call(W, file_handles, is_file_opened, [File2Uuid]), Attempts),
            ?assertMatch(?ERROR_NOT_FOUND, GetAllProcessHandles(ProcMonitorOpeningFile), Attempts);
        Error2 ->
            ct:fail(Error2)
    end,

    % TODO VFS-6833 move this to its own testcase.
    FilesNum = 230,
    BatchSize = 50,

    ExpFileIds = lists:sort(lists:map(fun(Num) ->
        FileIdx = integer_to_binary(Num),
        FilePath = <<"/space_name1/file_", FileIdx/binary>>,
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath)),

        spawn(W, fun() ->
            Self !  lfm:monitored_open(SessId1, ?FILE_REF(FileGuid), read),
            receive _ -> ok end
        end),
        filename:join([<<"/">>, ?SPACE_ID1, <<"file_", FileIdx/binary>>])
    end, lists:seq(1, FilesNum))),

    GetFileIdsFun = fun(ProcessHandlesDocs) ->
        lists:map(fun(#document{value = #process_handles{handles = Handles}}) ->
            ?assertEqual(1, map_size(Handles)),
            [FileHandle] = maps:values(Handles),
            lfm_context:get_file_id(FileHandle)
        end, ProcessHandlesDocs)
    end,

    GetAllDocsFun = fun F(StartFromId) ->
        {ok, FetchedDocs} = rpc:call(W, process_handles, list_docs, [StartFromId, BatchSize]),
        case length(FetchedDocs) < BatchSize of
            true ->
                GetFileIdsFun(FetchedDocs);
            false ->
                [LastDoc | _] = FetchedDocs,
                GetFileIdsFun(FetchedDocs) ++ F(LastDoc#document.key)
        end
    end,

    ?assertEqual(ExpFileIds, lists:usort(GetAllDocsFun(undefined)), Attempts).


lfm_create_and_read_symlink(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    % Prepare test dir and link data
    TestDir = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, TestDir)),
    Path = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    LinkTarget = <<"test_link">>,
    LinkSize = byte_size(LinkTarget),

    % Create symlink and check its times
    {ok, LinkAttrs} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE, size = LinkSize, is_fully_replicated = undefined, parent_guid = DirGuid}},
        lfm_proxy:make_symlink(W, SessId, Path, LinkTarget)),
    ?assert(LinkAttrs#file_attr.atime > 0),
    ?assert(LinkAttrs#file_attr.mtime > 0),
    ?assert(LinkAttrs#file_attr.ctime > 0),
    ?assert(fslogic_file_id:is_symlink_uuid(file_id:guid_to_uuid(LinkAttrs#file_attr.guid))),

    % Read link and check it
    time_test_utils:simulate_seconds_passing(2), % ensure time change
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, Path})),
    {ok, LinkAttrs2} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE, size = LinkSize, is_fully_replicated = undefined, parent_guid = DirGuid}},
        lfm_proxy:stat(W, SessId, {path, Path})),
    ?assert(LinkAttrs2#file_attr.atime > LinkAttrs#file_attr.atime),
    ?assertMatch({ok, [LinkAttrs2], _}, lfm_proxy:get_children_attrs(W, SessId, ?FILE_REF(DirGuid), #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),

    % Unlink and check if symlink is deleted
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, Path})),
    ?assertEqual({error, enoent}, lfm_proxy:read_symlink(W, SessId, {path, Path})),
    ?assertMatch({ok, [], _}, lfm_proxy:get_children_attrs(W, SessId, ?FILE_REF(DirGuid), #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),

    % Delete test dir
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId, ?FILE_REF(DirGuid))),
    ok.


lfm_create_hardlink_to_symlink(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    % Prepare test dir and link data
    TestDir = <<"/space_name1/", (generator:gen_name())/binary>>,
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, TestDir)),
    SymlinkPath = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    HardlinkPath = <<TestDir/binary, "/", (generator:gen_name())/binary>>,
    LinkTarget = <<"test_link">>,

    % Create symlink and hardlink to this symlink
    {ok, #file_attr{guid = SymlinkGuid}} = ?assertMatch({ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_symlink(W, SessId, SymlinkPath, LinkTarget)),
    {ok, #file_attr{guid = HardlinkGuid}} = ?assertMatch({ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_link(W, SessId, HardlinkPath, SymlinkGuid)),

    % Verify links
    ?assertNotEqual(SymlinkGuid, HardlinkGuid),
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, SymlinkPath})),
    ?assertEqual({ok, LinkTarget}, lfm_proxy:read_symlink(W, SessId, {path, HardlinkPath})),

    % Clean
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, SymlinkPath})),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId, {path, HardlinkPath})),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId, ?FILE_REF(DirGuid))),
    ok.


recreate_file_on_storage(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    % Mock to prevent storage file creation (only metadata will be set)
    ?assertEqual(ok, test_utils:mock_new(Workers, storage_driver)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, create, fun(_SDHandle, _Mode) -> ok end)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, open, fun(SDHandle, _Flag) -> {ok, SDHandle} end)),
    ?assertEqual(ok, test_utils:mock_expect(Workers, storage_driver, release, fun(_SDHandle) -> ok end)),

    % Create file on worker1
    {ok, {Guid, Handle0}} = ?assertMatch({ok, _},
        lfm_proxy:create_and_open(Worker, SessId, <<"/space_name1/recreate_file_on_storage">>, undefined)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle0)),

    % Unload mock - file is created according to metadata but it has not been created on storage
    ?assertEqual(ok, test_utils:mock_unload(Workers, storage_driver)),

    % File should be created on disk and read should succeed
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Guid), read)),
    ?assertEqual({ok, <<>>}, lfm_proxy:read(Worker, Handle2, 0, 10)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle2)).


lfm_close_deleted_open_files(Config) ->
    lfm_files_test_base:lfm_close_deleted_open_files(Config).


lfm_create_dir_at_path(Config) ->
    lfm_files_test_base:lfm_create_dir_at_path(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    lfm_files_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) when
    Case =:= rename_removed_opened_file_races_test;
    Case =:= rename_removed_opened_file_races_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, storage_driver, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(lfm_create_and_read_symlink = Case, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= readdir_plus_should_work_with_token;
    Case =:= readdir_plus_should_work_with_token_not_full_batch;
    Case =:= readdir_plus_should_work_with_api_token_not_full_batch;
    Case =:= readdir_plus_should_work_with_api_token;
    Case =:= readdir_should_work_with_token;
    Case =:= readdir_should_work_with_token_not_full_batch;
    Case =:= readdir_should_work_with_api_token;
    Case =:= readdir_should_work_with_api_token_not_full_batch
    ->
    lfm_files_test_base:init_per_testcase(readdir_should_work_with_token, Config);

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) when
    Case =:= rename_removed_opened_file_races_test;
    Case =:= rename_removed_opened_file_races_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(lfm_create_and_read_symlink = Case, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= readdir_plus_should_work_with_token;
    Case =:= readdir_plus_should_work_with_token_not_full_batch;
    Case =:= readdir_plus_should_work_with_api_token_not_full_batch;
    Case =:= readdir_plus_should_work_with_api_token;
    Case =:= readdir_should_work_with_token;
    Case =:= readdir_should_work_with_token_not_full_batch;
    Case =:= readdir_should_work_with_api_token;
    Case =:= readdir_should_work_with_api_token_not_full_batch
    ->
    lfm_files_test_base:end_per_testcase(readdir_should_work_with_token, Config);

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).