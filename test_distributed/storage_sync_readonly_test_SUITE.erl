%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync with readonly storage.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_readonly_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_sync_test.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_directory_import_test/1,
    create_file_import_test/1,
    create_file_in_dir_update_test/1,
    delete_file_update_test/1,
    append_file_update_test/1,
    copy_file_update_test/1,
    move_file_update_test/1,
    truncate_file_update_test/1,
    chmod_file_update_test/1,
    chmod_file_update2_test/1,
    update_timestamps_file_import_test/1,
    create_file_in_dir_import_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    create_directory_import_many_test/1,
    create_subfiles_import_many_test/1,
    delete_non_empty_directory_update_test/1,
    delete_empty_directory_update_test/1,
    should_not_detect_timestamp_update_test/1,
    create_directory_import_without_read_permission_test/1,
    create_subfiles_import_many2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_directory_import_check_user_id_test/1,
    create_directory_import_check_user_id_error_test/1,
    create_file_import_check_user_id_test/1,
    create_file_import_check_user_id_error_test/1,
    import_nfs_acl_test/1,
    update_nfs_acl_test/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1]).

-define(TEST_CASES, [
    create_directory_import_test,
    create_directory_import_check_user_id_test,
    create_directory_import_check_user_id_error_test,
    create_directory_import_without_read_permission_test,
    create_directory_import_many_test,
    create_file_import_test,
    create_file_import_check_user_id_test,
    create_file_import_check_user_id_error_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,
    delete_empty_directory_update_test,
    delete_non_empty_directory_update_test,
    delete_file_update_test,
    append_file_update_test,
    copy_file_update_test,
    move_file_update_test,
    truncate_file_update_test,
    chmod_file_update_test,
    chmod_file_update2_test,
    update_timestamps_file_import_test,
    should_not_detect_timestamp_update_test,
    import_nfs_acl_test,
    update_nfs_acl_test,
    import_nfs_acl_with_disabled_luma_should_fail_test
%%    import_file_by_path_test, %todo uncomment after resolving and merging with VFS-3052
%%    get_child_attr_by_path_test,
%%    import_remote_file_by_path_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_directory_import_test(Config) ->
    storage_sync_test_base:create_directory_import_test(Config, true).

create_directory_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_test(Config, true).

create_directory_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_error_test(Config, true).

create_directory_import_without_read_permission_test(Config) ->
    storage_sync_test_base:create_directory_import_without_read_permission_test(Config, true).

create_directory_import_many_test(Config) ->
    storage_sync_test_base:create_directory_import_many_test(Config, true).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, true).

create_file_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_test(Config, true).

create_file_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_error_test(Config, true).

create_file_in_dir_import_test(Config) ->
    storage_sync_test_base:create_file_in_dir_import_test(Config, true).

create_subfiles_import_many_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many_test(Config, true).

create_subfiles_import_many2_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many2_test(Config, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, true).

create_file_in_dir_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_update_test(Config, true).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_exceed_batch_update_test(Config, true).

delete_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_empty_directory_update_test(Config, true).

delete_non_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_non_empty_directory_update_test(Config, true).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, true).

append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, true).

copy_file_update_test(Config) ->
    storage_sync_test_base:copy_file_update_test(Config, true).

move_file_update_test(Config) ->
    storage_sync_test_base:move_file_update_test(Config, true).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, true).

chmod_file_update_test(Config) ->
    storage_sync_test_base:chmod_file_update_test(Config, true).

chmod_file_update2_test(Config) ->
    storage_sync_test_base:chmod_file_update2_test(Config, true).

update_timestamps_file_import_test(Config) ->
    storage_sync_test_base:update_timestamps_file_import_test(Config, true).

should_not_detect_timestamp_update_test(Config) ->
    storage_sync_test_base:should_not_detect_timestamp_update_test(Config, true).

import_nfs_acl_test(Config) ->
    storage_sync_test_base:import_nfs_acl_test(Config, true).

update_nfs_acl_test(Config) ->
    storage_sync_test_base:update_nfs_acl_test(Config, true).

import_file_by_path_test(Config) ->
    storage_sync_test_base:import_file_by_path_test(Config, true).

get_child_attr_by_path_test(Config) ->
    storage_sync_test_base:get_child_attr_by_path_test(Config, true).

import_remote_file_by_path_test(Config) ->
    storage_sync_test_base:import_remote_file_by_path_test(Config, true).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    storage_sync_test_base:import_nfs_acl_with_disabled_luma_should_fail_test(Config, true).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:enable_grpca_based_communication(NewConfig),
        NewConfig
    end,
    [{?LOAD_MODULES, [initializer, storage_sync_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    ssl:stop().

init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _, _) ->
        {ok, ?USER}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _, _) ->
        error(test_reason)
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_update_test;
    Case =:= should_not_detect_timestamp_update_test
->
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= delete_empty_directory_update_test;
    Case =:= delete_non_empty_directory_update_test;
    Case =:= delete_file_update_test;
    Case =:= move_file_update_test;
    Case =:= create_subfiles_and_delete_before_import_is_finished_test
->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_exceed_batch_update_test
->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test
->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [{old_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager, reverse_luma_proxy, storage_file_ctx]),

    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx2} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx2}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _, _) ->
        {ok, ?USER}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(Args, _, _, _, _) ->
        case maps:keys(Args) of
            [<<"gid">>] ->
                {ok, ?GROUP};
            [<<"name">>] ->
                {ok, ?GROUP2}
        end
    end),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_with_disabled_luma_should_fail_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager]),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);


init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    ConfigWithProxy = lfm_proxy:init(ConfigWithSessionInfo),
    storage_sync_test_base:add_workers_storage_mount_points(ConfigWithProxy).

end_per_testcase(Case, Config) when
    Case =:= import_file_by_path_test;
    Case =:= get_child_attr_by_path_test
->
    Workers = ?config(op_worker_nodes, Config),
    storage_sync_test_base:reset_enoent_strategies(Workers, ?SPACE_ID),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test;
    Case =:= create_file_in_dir_exceed_batch_update_test
->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    ok = storage_sync_test_base:clean_reverse_luma_cache(W1),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    ok = storage_sync_test_base:clean_reverse_luma_cache(W1),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx, storage_file_manager]),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    storage_sync_test_base:disable_storage_sync(Config),
    storage_sync_test_base:clean_storage(Config, true),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================