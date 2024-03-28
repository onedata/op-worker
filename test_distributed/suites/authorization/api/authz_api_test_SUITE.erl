%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of authorization mechanism
%%% with corresponding operations.
%%% TODO VFS-7563 add tests concerning datasets
%%% @end
%%%-------------------------------------------------------------------
-module(authz_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    test_all_operations_are_checked/1,

    test_mkdir/1,
    test_get_children_attrs/1,
    test_get_child_attr/1,
    test_mv_dir/1,
    test_rm_dir/1,

    test_create_file/1,
    test_open_for_read/1,
    test_open_for_write/1,
    test_open_for_rdwr/1,
    test_create_and_open/1,
    test_truncate/1,
    test_mv_file/1,
    test_rm_file/1,

    test_get_parent/1,
    test_get_file_path/1,
    test_resolve_guid/1,
    test_stat/1,

    test_set_perms/1,
    test_check_read_perms/1,
    test_check_write_perms/1,
    test_check_rdwr_perms/1,

    test_create_share/1,
    test_remove_share/1,
    test_share_perms_are_checked_only_up_to_share_root/1,

    test_get_acl/1,
    test_set_acl/1,
    test_remove_acl/1,

    test_get_transfer_encoding/1,
    test_set_transfer_encoding/1,
    test_get_cdmi_completion_status/1,
    test_set_cdmi_completion_status/1,
    test_get_mimetype/1,
    test_set_mimetype/1,

    test_get_custom_metadata/1,
    test_set_custom_metadata/1,
    test_remove_custom_metadata/1,
    test_get_xattr/1,
    test_list_xattr/1,
    test_set_xattr/1,
    test_remove_xattr/1,
    test_get_file_distribution/1,
    test_get_historical_dir_size_stats/1,
    test_get_file_storage_locations/1,

    test_add_qos_entry/1,
    test_get_qos_entry/1,
    test_remove_qos_entry/1,
    test_get_effective_file_qos/1,
    test_check_qos_status/1
]).

groups() -> [
    {authz_dir_api_tests, [], [
        test_mkdir,
        test_get_children_attrs,
        test_get_child_attr,
        test_mv_dir,
        test_rm_dir
    ]},

    {authz_reg_file_api_tests, [], [
        test_create_file,
        test_open_for_read,
        test_open_for_write,
        test_open_for_rdwr,
        test_create_and_open,
        test_truncate,
        test_mv_file,
        test_rm_file
    ]},

    {authz_file_common_api_tests, [], [
        test_get_parent,
        test_get_file_path,
        test_resolve_guid,
        test_stat
    ]},

    {authz_perms_api_tests, [], [
        test_set_perms,
        test_check_read_perms,
        test_check_write_perms,
        test_check_rdwr_perms
    ]},

    {authz_share_api_tests, [], [
        test_create_share,
        test_remove_share,
        test_share_perms_are_checked_only_up_to_share_root
    ]},

    {authz_acl_api_tests, [], [
        test_get_acl,
        test_set_acl,
        test_remove_acl
    ]},

    {authz_cdmi_api_tests, [], [
        test_get_transfer_encoding,
        test_set_transfer_encoding,
        test_get_cdmi_completion_status,
        test_set_cdmi_completion_status,
        test_get_mimetype,
        test_set_mimetype
    ]},

    {authz_file_metadata_api_tests, [], [
        test_get_custom_metadata,
        test_set_custom_metadata,
        test_remove_custom_metadata,
        test_get_xattr,
        test_list_xattr,
        test_set_xattr,
        test_remove_xattr,
        test_get_file_distribution,
        test_get_historical_dir_size_stats,
        test_get_file_storage_locations
    ]},

    {authz_qos_api_tests, [], [
        test_add_qos_entry,
        test_get_qos_entry,
        test_remove_qos_entry,
        test_get_effective_file_qos,
        test_check_qos_status
    ]}
].

all() -> [
    test_all_operations_are_checked,
    {group, authz_dir_api_tests},
    {group, authz_reg_file_api_tests},
    {group, authz_file_common_api_tests},
    {group, authz_perms_api_tests},
    {group, authz_share_api_tests},
    {group, authz_acl_api_tests},
    {group, authz_cdmi_api_tests},
    {group, authz_file_metadata_api_tests},
    {group, authz_qos_api_tests}
].


-define(NOT_YET_TESTED_OPERATIONS, [
    {lfm, abort_multipart_upload, 2},
    {lfm, check_result, 1},
    {lfm, check_size_and_read, 3},
    {lfm, complete_multipart_upload, 2},
    {lfm, cp, 4},
    {lfm, create_dir_at_path, 3},
    {lfm, create_multipart_upload, 3},
    {lfm, ensure_dir, 4},
    {lfm, fsync, 1},
    {lfm, fsync, 3},
    {lfm, get_children_count, 2},
    {lfm, get_file_location, 2},
    {lfm, get_file_references, 2},
    {lfm, get_files_recursively, 4},
    {lfm, get_fs_stats, 2},
    {lfm, has_custom_metadata, 2},
    {lfm, is_dir, 2},
    {lfm, list_multipart_parts, 4},
    {lfm, list_multipart_uploads, 4},
    {lfm, make_link, 4},
    {lfm, make_symlink, 4},
    {lfm, monitored_open, 3},
    {lfm, monitored_release, 1},
    {lfm, read, 3},
    {lfm, read_symlink, 2},
    {lfm, release, 1},
    {lfm, resolve_guid_by_relative_path, 3},
    {lfm, resolve_symlink, 2},
    {lfm, rm_recursive, 2},
    {lfm, silent_read, 3},
    {lfm, sync_block, 4},
    {lfm, update_times, 5},
    {lfm, upload_multipart_part, 3},
    {lfm, write, 3},
    {mi_archives, archive_dataset, 6},
    {mi_archives, browse_recall_log, 3},
    {mi_archives, cancel_archivisation, 3},
    {mi_archives, cancel_recall, 2},
    {mi_archives, delete, 3},
    {mi_archives, get_info, 2},
    {mi_archives, get_recall_details, 2},
    {mi_archives, get_recall_progress, 2},
    {mi_archives, list, 4},
    {mi_archives, recall, 4},
    {mi_archives, update, 3},
    {mi_atm, discard_workflow_execution, 3},
    {mi_atm, force_continue_workflow_execution, 2},
    {mi_atm, init_cancel_workflow_execution, 2},
    {mi_atm, init_pause_workflow_execution, 2},
    {mi_atm, repeat_workflow_execution, 4},
    {mi_atm, resume_workflow_execution, 2},
    {mi_atm, schedule_workflow_execution, 7},
    {mi_datasets, establish, 3},
    {mi_datasets, get_file_eff_summary, 2},
    {mi_datasets, get_info, 2},
    {mi_datasets, list_children_datasets, 4},
    {mi_datasets, list_top_datasets, 5},
    {mi_datasets, remove, 2},
    {mi_datasets, update, 5},
    {mi_transfers, schedule_file_transfer, 5},
    {mi_transfers, schedule_view_transfer, 7}
]).

-define(RUN_AUTHZ_DIR_API_TEST(__CONFIG),
    authz_dir_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_REG_FILE_API_TEST(__CONFIG),
    authz_reg_file_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_FILE_COMMON_API_TEST(__CONFIG),
    authz_file_common_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_PERMS_API_TEST(__CONFIG),
    authz_perms_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_SHARE_API_TEST(__CONFIG),
    authz_share_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_ACL_API_TEST(__CONFIG),
    authz_acl_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_CDMI_API_TEST(__CONFIG),
    authz_cdmi_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_FILE_METADATA_API_TEST(__CONFIG),
    authz_file_metadata_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_QOS_API_TEST(__CONFIG),
    authz_qos_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).

-define(ATTEMPTS, 10).


%%%===================================================================
%%% Test functions
%%%===================================================================


test_all_operations_are_checked(_Config) ->
    AllAvailableOperations = lists:usort(lists:flatmap(fun(Module) ->
        lists:filtermap(fun
            ({module_info, _}) -> false;
            ({FunName, Arity}) -> {true, {Module, FunName, Arity}}
        end, Module:module_info(exports))
    end, [
        lfm,
        mi_archives, mi_atm, mi_cdmi, mi_datasets, mi_file_metadata, mi_qos, mi_shares, mi_transfers
    ])),

    TestedOperations = lists:flatten(lists:filtermap(fun({FunName, _Arity}) ->
        case str_utils:to_binary(FunName) of
            <<"test_", _/binary>> when FunName =/= ?FUNCTION_NAME ->
                {true, erlang:apply(?MODULE, FunName, [tested_operations])};
            _ ->
                false
        end
    end, module_info(exports))),

    ?assertEqual([], (AllAvailableOperations -- ?NOT_YET_TESTED_OPERATIONS) -- TestedOperations).


test_mkdir(tested_operations) ->
    [{lfm, mkdir, 3}, {lfm, mkdir, 4}];
test_mkdir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


test_get_children_attrs(tested_operations) ->
    [{lfm, get_children_attrs, 3}, {lfm, get_children_attrs, 4}];
test_get_children_attrs(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


test_get_child_attr(tested_operations) ->
    [{lfm, get_child_attr, 3}];
test_get_child_attr(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


test_mv_dir(tested_operations) ->
    [{lfm, mv, 4}];
test_mv_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


test_rm_dir(tested_operations) ->
    [{lfm, unlink, 3}];
test_rm_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


test_create_file(tested_operations) ->
    [{lfm, create, 2}, {lfm, create, 3}, {lfm, create, 4}];
test_create_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_open_for_read(tested_operations) ->
    [{lfm, open, 3}];
test_open_for_read(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_open_for_write(tested_operations) ->
    [{lfm, open, 3}];
test_open_for_write(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_open_for_rdwr(tested_operations) ->
    [{lfm, open, 3}];
test_open_for_rdwr(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_create_and_open(tested_operations) ->
    [{lfm, create_and_open, 4}, {lfm, create_and_open, 5}];
test_create_and_open(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_truncate(tested_operations) ->
    [{lfm, truncate, 3}];
test_truncate(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_mv_file(tested_operations) ->
    [{lfm, mv, 4}];
test_mv_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_rm_file(tested_operations) ->
    [{lfm, unlink, 3}];
test_rm_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


test_get_parent(tested_operations) ->
    [{lfm, get_parent, 2}];
test_get_parent(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


test_get_file_path(tested_operations) ->
    [{lfm, get_file_path, 2}];
test_get_file_path(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


test_resolve_guid(tested_operations) ->
    [{lfm, get_file_guid, 2}];
test_resolve_guid(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


test_stat(tested_operations) ->
    [{lfm, stat, 2}, {lfm, stat, 3}];
test_stat(Config) ->
    ?RUN_AUTHZ_FILE_COMMON_API_TEST(Config).


test_set_perms(tested_operations) ->
    [{lfm, set_perms, 3}];
test_set_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


test_check_read_perms(tested_operations) ->
    [{lfm, check_perms, 3}];
test_check_read_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


test_check_write_perms(tested_operations) ->
    [{lfm, check_perms, 3}];
test_check_write_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


test_check_rdwr_perms(tested_operations) ->
    [{lfm, check_perms, 3}];
test_check_rdwr_perms(Config) ->
    ?RUN_AUTHZ_PERMS_API_TEST(Config).


test_create_share(tested_operations) ->
    [{mi_shares, create, 4}];
test_create_share(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


test_remove_share(tested_operations) ->
    [{mi_shares, remove, 2}];
test_remove_share(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


test_share_perms_are_checked_only_up_to_share_root(tested_operations) ->
    [];
test_share_perms_are_checked_only_up_to_share_root(Config) ->
    ?RUN_AUTHZ_SHARE_API_TEST(Config).


test_get_acl(tested_operations) ->
    [{lfm, get_acl, 2}];
test_get_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


test_set_acl(tested_operations) ->
    [{lfm, set_acl, 3}];
test_set_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


test_remove_acl(tested_operations) ->
    [{lfm, remove_acl, 2}];
test_remove_acl(Config) ->
    ?RUN_AUTHZ_ACL_API_TEST(Config).


test_get_transfer_encoding(tested_operations) ->
    [{mi_cdmi, get_transfer_encoding, 2}];
test_get_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_set_transfer_encoding(tested_operations) ->
    [{mi_cdmi, set_transfer_encoding, 3}];
test_set_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_get_cdmi_completion_status(tested_operations) ->
    [{mi_cdmi, get_cdmi_completion_status, 2}];
test_get_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_set_cdmi_completion_status(tested_operations) ->
    [{mi_cdmi, set_cdmi_completion_status, 3}];
test_set_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_get_mimetype(tested_operations) ->
    [{mi_cdmi, get_mimetype, 2}];
test_get_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_set_mimetype(tested_operations) ->
    [{mi_cdmi, set_mimetype, 3}];
test_set_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


test_get_custom_metadata(tested_operations) ->
    [{mi_file_metadata, get_custom_metadata, 5}];
test_get_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_set_custom_metadata(tested_operations) ->
    [{mi_file_metadata, set_custom_metadata, 5}];
test_set_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_remove_custom_metadata(tested_operations) ->
    [{mi_file_metadata, remove_custom_metadata, 3}];
test_remove_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_get_xattr(tested_operations) ->
    [{lfm, get_xattr, 4}];
test_get_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_list_xattr(tested_operations) ->
    [{lfm, list_xattr, 4}];
test_list_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_set_xattr(tested_operations) ->
    [{lfm, set_xattr, 3}, {lfm, set_xattr, 5}];
test_set_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_remove_xattr(tested_operations) ->
    [{lfm, remove_xattr, 3}];
test_remove_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_get_file_distribution(tested_operations) ->
    [{mi_file_metadata, gather_distribution, 2}];
test_get_file_distribution(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_get_historical_dir_size_stats(tested_operations) ->
    [{mi_file_metadata, get_historical_dir_size_stats, 4}];
test_get_historical_dir_size_stats(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_get_file_storage_locations(tested_operations) ->
    [{mi_file_metadata, get_storage_locations, 2}];
test_get_file_storage_locations(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


test_add_qos_entry(tested_operations) ->
    [{mi_qos, add_qos_entry, 4}, {mi_qos, add_qos_entry, 5}];
test_add_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


test_get_qos_entry(tested_operations) ->
    [{mi_qos, get_qos_entry, 2}];
test_get_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


test_remove_qos_entry(tested_operations) ->
    [{mi_qos, remove_qos_entry, 2}];
test_remove_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


test_get_effective_file_qos(tested_operations) ->
    [{mi_qos, get_effective_file_qos, 2}];
test_get_effective_file_qos(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


test_check_qos_status(tested_operations) ->
    [{mi_qos, check_qos_status, 2}, {mi_qos, check_qos_status, 3}];
test_check_qos_status(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    StorageType = ?RAND_ELEMENT([posix, s3]),

    ModulesToLoad = [?MODULE, authz_api_test_runner],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            delete_spaces_from_previous_run(),
            [{storage_type, StorageType}, {storage_id, find_storage_id(StorageType)} | NewConfig]
        end
    }).


%% @private
-spec delete_spaces_from_previous_run() -> ok.
delete_spaces_from_previous_run() ->
    AllTestCases = lists:filtermap(fun({FunName, _Arity}) ->
        case str_utils:to_binary(FunName) of
            <<"test_", _/binary>> = FunNameBin -> {true, FunNameBin};
            _ -> false
        end
    end, module_info(exports)),

    RemovedSpaces = lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),

        Exists = lists:member(SpaceName, AllTestCases),
        Exists andalso ozw_test_rpc:delete_space(SpaceId),

        Exists
    end, ozw_test_rpc:list_spaces()),

    ?assertEqual([], lists_utils:intersect(opw_test_rpc:get_spaces(krakow), RemovedSpaces), ?ATTEMPTS),

    ok.


%% @private
-spec find_storage_id(posix | s3) -> storage:id().
find_storage_id(StorageType) ->
    StorageTypeBin = atom_to_binary(StorageType),

    [StorageId] = lists:filter(fun(StorageId) ->
        StorageDetails = opw_test_rpc:storage_describe(krakow, StorageId),
        StorageTypeBin == maps:get(<<"type">>, StorageDetails)
    end, opw_test_rpc:get_storages(krakow)),

    StorageId.


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_GroupName, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_GroupName, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(Case, Config) ->
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = space_owner,
        users = [user1, user2],
        supports = [#support_spec{
            provider = krakow,
            size = 10000000,
            storage_spec = ?config(storage_id, Config)
        }]
    }),
    [{space_id, SpaceId} | Config].


end_per_testcase(_Case, _Config) ->
    ok.
