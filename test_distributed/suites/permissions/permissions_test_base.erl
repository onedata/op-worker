%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl
%%% permissions with corresponding lfm (logical_file_manager) functions
%%% TODO VFS-11695 rewrite permissions test to onenv
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_base).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include("proto/common/handshake_messages.hrl").
-include("storage_files_test_SUITE.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    mkdir_test/1,
    get_children_test/1,
    get_children_attrs_test/1,
    get_children_details_test/1,
    get_child_attr_test/1,
    mv_dir_test/1,
    rm_dir_test/1,

    create_file_test/1,
    open_for_read_test/1,
    open_for_write_test/1,
    open_for_rdwr_test/1,
    create_and_open_test/1,
    truncate_test/1,
    mv_file_test/1,
    rm_file_test/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,
    get_file_attr_test/1,
    get_file_details_test/1,
    get_file_distribution_test/1,
    get_historical_dir_size_stats_test/1,
    get_file_storage_locations_test/1,

    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
    remove_share_test/1,
    share_perms_test/1,

    get_acl_test/1,
    set_acl_test/1,
    remove_acl_test/1,

    get_transfer_encoding_test/1,
    set_transfer_encoding_test/1,
    get_cdmi_completion_status_test/1,
    set_cdmi_completion_status_test/1,
    get_mimetype_test/1,
    set_mimetype_test/1,

    get_metadata_test/1,
    set_metadata_test/1,
    remove_metadata_test/1,
    get_xattr_test/1,
    list_xattr_test/1,
    set_xattr_test/1,
    remove_xattr_test/1,

    add_qos_entry_test/1,
    get_qos_entry_test/1,
    remove_qos_entry_test/1,
    get_effective_file_qos_test/1,
    check_qos_fulfillment_test/1,

    multi_provider_permission_cache_test/1,
    expired_session_test/1
]).
% Export for use in rpc
-export([check_perms/3]).


-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).

-define(ATTEMPTS, 35).

% TODO VFS-7563 add tests concerning datasets

%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:mkdir(W, SessId, ParentDirGuid, <<"dir2">>, 8#777) of
                {ok, DirGuid} ->
                    permissions_test_utils:ensure_dir_created_on_storage(W, DirGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/dir2">>}
        end
    }, Config).


get_children_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children(W, SessId, DirKey, 0, 100))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_children_attrs_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children_attrs(W, SessId, DirKey, #{offset => 0, limit => 100, tune_for_large_continuous_listing => false}))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_children_details_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children_details(W, SessId, DirKey, #{offset => 0, limit => 100, tune_for_large_continuous_listing => false}))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_child_attr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container],
            children = [#file{name = <<"file1">>}]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:get_child_attr(W, SessId, ParentDirGuid, <<"file1">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


mv_dir_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_subcontainer]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            SrcDirPath = <<TestCaseRootDirPath/binary, "/dir1/dir11">>,
            SrcDirKey = maps:get(SrcDirPath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(W, SessId, SrcDirKey, DstDirKey, <<"dir21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/dir21">>}
        end
    }, Config).


rm_dir_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir2">>,
                        perms = [?delete, ?list_container]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1/dir2">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:unlink(W, SessId, DirKey))
        end
    }, Config).


create_file_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:create(W, SessId, ParentDirGuid, <<"file1">>, 8#777) of
                {ok, FileGuid} ->
                    permissions_test_utils:ensure_file_created_on_storage(W, FileGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


open_for_read_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


open_for_write_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


open_for_rdwr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


create_and_open_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % Create dummy file to ensure that directory is created on storage.
                % Otherwise it may be not possible during tests without necessary perms.
                create_dummy_file(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:create_and_open(W, SessId, ParentDirGuid, <<"file1">>, 8#777))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


truncate_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:truncate(W, SessId, FileKey, 0))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


mv_file_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_object]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            SrcFilePath = <<TestCaseRootDirPath/binary, "/dir1/file11">>,
            SrcFileKey = maps:get(SrcFilePath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(W, SessId, SrcFileKey, DstDirKey, <<"file21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/file21">>}
        end
    }, Config).


rm_file_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file1">>,
                        perms = [?delete]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:unlink(W, SessId, FileKey))
        end
    }, Config).


get_parent_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_parent(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_path_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = false, % TODO VFS-6057
        available_in_open_handle_mode = false, % TODO VFS-6057
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            ?FILE_REF(FileGuid) = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_file_path(W, SessId, FileGuid))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_guid_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            extract_ok(lfm_proxy:resolve_guid(W, SessId, FilePath))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_attr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:stat(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_details_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_details(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_distribution_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_distribution_deprecated(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_historical_dir_size_stats_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),
    
    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?read_metadata],
            children = [
                #file{
                    name = <<"file1">>,
                    perms = []
                }
            ]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_historical_dir_size_stats(
                W, SessId, FileKey, ?GET_DOMAIN_BIN(W), #time_series_layout_get_request{}))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_file_storage_locations_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),
    
    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_storage_locations(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_perms_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),
    FileOwner = <<"user1">>,

    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
    OtherUserSessId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),
    SpaceOwnerSessId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = <<"/space1/dir1">>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, FileOwnerUserSessId, DirPath)
    ),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, FileOwnerUserSessId, FilePath, 8#777)
    ),
    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Open file to ensure it's creation on storage
    {ok, Handle} = lfm_proxy:open(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(W, Handle),

    AssertProperStorageAttrsFun = fun(ExpMode) ->
        permissions_test_utils:assert_user_is_file_owner_on_storage(
            W, ?SPACE_ID, FilePath, FileOwnerUserSessId, #{mode => ?FILE_MODE(ExpMode)}
        )
    end,

    AssertProperStorageAttrsFun(8#777),

    %% POSIX

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_modes(W, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    permissions_test_utils:set_modes(W, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)),
    AssertProperStorageAttrsFun(8#000),

    % but not if that access is via shared guid
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(ShareFileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % other users from space can't change perms no matter what
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % with exception being space owner who can always change perms no matter what
    permissions_test_utils:set_modes(W, #{DirGuid => 8#000, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(FileGuid), 8#555)),
    AssertProperStorageAttrsFun(8#555),

    % but even space owner cannot perform write operation on space dir
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(<<"space1">>),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#000)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#555)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#777)),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    %% ACL

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),

    permissions_test_utils:set_acls(W, #{
        DirGuid => [?traverse_container],
        FileGuid => []
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)),

    % but not if that access is via shared guid
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(ShareFileGuid), 8#000)
    ),

    % file owner cannot change acl after his access was denied by said acl
    permissions_test_utils:set_acls(W, #{}, #{
        FileGuid => ?ALL_FILE_PERMS
    }, ?everyone, ?no_flags_mask),

    PermsBitmask = permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS),

    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_acl(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), [
            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
        ])
    ),

    % but space owner always can change acl for any file
    ?assertMatch(
        ok,
        lfm_proxy:set_acl(W, SpaceOwnerSessId, ?FILE_REF(FileGuid), [
            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
        ])
    ),

    % other users from space can't change perms no matter what
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, ?FILE_REF(FileGuid), 8#000)
    ).


check_read_perms_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_write_perms_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_rdwr_perms_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


create_share_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(opt_shares:create(W, SessId, DirKey, <<"create_share">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


remove_share_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    SpaceOwnerSessId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),
    {ok, SpaceName} = rpc:call(W, space_logic, get_name, [?ROOT_SESS_ID, ?SPACE_ID]),
    ScenariosRootDirPath = filename:join(["/", SpaceName, ?SCENARIO_NAME]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SpaceOwnerSessId, ScenariosRootDirPath, 8#700)),

    FilePath = filename:join([ScenariosRootDirPath, ?RAND_STR()]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SpaceOwnerSessId, FilePath, 8#700)),
    {ok, ShareId} = opt_shares:create(W, SpaceOwnerSessId, ?FILE_REF(FileGuid), <<"share">>),

    RemoveShareFun = fun(SessId) ->
        % Remove share via gs call to test token data caveats
        % (middleware should reject any api call with data caveats)
        rpc:call(W, middleware, handle, [#op_req{
            auth = permissions_test_utils:get_auth(W, SessId),
            gri = #gri{type = op_share, id = ShareId, aspect = instance},
            operation = delete
        }])
    end,

    UserId = <<"user2">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),

    % Assert share removal requires only ?SPACE_MANAGE_SHARES space priv
    % and no file permissions
    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, []),
    ?assertEqual(?ERROR_POSIX(?EPERM), RemoveShareFun(UserSessId)),

    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, privileges:space_admin()),
    MainToken = initializer:create_access_token(UserId),

    % Assert api operations are unauthorized in case of data caveats
    Token1 = tokens:confine(MainToken, #cv_data_readonly{}),
    CaveatSessId1 = permissions_test_utils:create_session(W, UserId, Token1),
    ?assertEqual(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_readonly})),
        RemoveShareFun(CaveatSessId1)
    ),
    Token2 = tokens:confine(MainToken, #cv_data_path{whitelist = [ScenariosRootDirPath]}),
    CaveatSessId2 = permissions_test_utils:create_session(W, UserId, Token2),
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_path, [ScenariosRootDirPath]})),
        RemoveShareFun(CaveatSessId2)
    ),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    Token3 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [ObjectId]}),
    CaveatSessId3 = permissions_test_utils:create_session(W, UserId, Token3),
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_objectid, [ObjectId]})),
        RemoveShareFun(CaveatSessId3)
    ),

    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, [?SPACE_MANAGE_SHARES]),
    ?assertEqual(ok, RemoveShareFun(UserSessId)).


share_perms_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),
    FileOwner = <<"user1">>,

    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),

    ScenarioDirName = ?SCENARIO_NAME,
    ScenarioDirPath = <<"/space1/", ScenarioDirName/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, ScenarioDirPath, 8#700)),

    MiddleDirPath = <<ScenarioDirPath/binary, "/dir2">>,
    {ok, MiddleDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, MiddleDirPath, 8#777)),

    BottomDirPath = <<MiddleDirPath/binary, "/dir3">>,
    {ok, BottomDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, BottomDirPath), 8#777),

    FilePath = <<BottomDirPath/binary, "/file1">>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, FileOwnerUserSessId, FilePath, 8#777)),

    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(W, FileOwnerUserSessId, ?FILE_REF(MiddleDirGuid), <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Accessing file in normal mode by space user should result in eacces (dir1 perms -> 8#700)
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(FileGuid))
    ),
    % But accessing it in share mode should succeed as perms should be checked only up to
    % share root (dir1/dir2 -> 8#777) and not space root
    ?assertMatch(
        {ok, #file_attr{guid = ShareFileGuid}},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(ShareFileGuid))
    ),

    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
    ?assertEqual(ok, lfm_proxy:set_perms(W, ?ROOT_SESS_ID, ?FILE_REF(BottomDirGuid), 8#770)),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(ShareFileGuid))
    ).


get_acl_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_acl(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_acl_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_acl(W, SessId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ]))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_acl_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_acl(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_transfer_encoding_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                opt_cdmi:set_transfer_encoding(W, FileOwnerSessId, ?FILE_REF(Guid), <<"base64">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_transfer_encoding(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_transfer_encoding_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_transfer_encoding(W, SessId, FileKey, <<"base64">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_cdmi_completion_status_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                opt_cdmi:set_cdmi_completion_status(W, FileOwnerSessId, ?FILE_REF(Guid), <<"Completed">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_cdmi_completion_status(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_cdmi_completion_status_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_cdmi_completion_status(W, SessId, FileKey, <<"Completed">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_mimetype_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                opt_cdmi:set_mimetype(W, FileOwnerSessId, ?FILE_REF(Guid), <<"mimetype">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_mimetype(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_mimetype_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_mimetype(W, SessId, FileKey, <<"mimetype">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_metadata_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                opt_file_metadata:set_custom_metadata(W, FileOwnerSessId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_custom_metadata(W, SessId, FileKey, json, [], false))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_metadata_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:set_custom_metadata(W, SessId, FileKey, json, <<"VAL">>, []))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_metadata_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                opt_file_metadata:set_custom_metadata(W, FileOwnerSessId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:remove_custom_metadata(W, SessId, FileKey, json))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_xattr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_xattr(W, SessId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


list_xattr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:list_xattr(W, SessId, FileKey, false, false))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_xattr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_xattr(W, SessId, FileKey, #xattr{
                name = <<"myxattr">>, value = <<"VAL">>
            }))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_xattr_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_xattr(W, SessId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


add_qos_entry_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:add_qos_entry(W, SessId, FileKey, <<"country=FR">>, 1))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_qos_entry_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end 
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:get_qos_entry(W, SessId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_qos_entry_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:remove_qos_entry(W, SessId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_effective_file_qos_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, _QosEntryId} = opt_qos:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:get_effective_file_qos(W, SessId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_qos_fulfillment_test(Config) ->
    [_, _, W] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:check_qos_status(W, SessId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


multi_provider_permission_cache_test(Config) ->
    [P1W1, P1W2, P2] = ?config(op_worker_nodes, Config),
    Nodes = [P1W2, P1W1, P2],

    User = <<"user1">>,

    Path = <<"/space1/multi_provider_permission_cache_test">>,
    P1W2SessId = ?config({session_id, {User, ?GET_DOMAIN(P1W2)}}, Config),

    {Guid, AllPerms} = case rand:uniform(2) of
        1 ->
            {_, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(P1W2, P1W2SessId, Path, 8#777)),
            {FileGuid, ?ALL_FILE_PERMS};
        2 ->
            {_, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(P1W2, P1W2SessId, Path, 8#777)),
            {DirGuid, ?ALL_DIR_PERMS}
    end,

    % Set random posix permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        PosixPerms = lists_utils:random_sublist(?ALL_POSIX_PERMS),
        Mode = lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, owner)
        end, 0, PosixPerms),
        permissions_test_utils:set_modes(P1W2, #{Guid => Mode}),

        {AllowedPerms, DeniedPerms} = lists:foldl(fun(Perm, {AllowedPermsAcc, DeniedPermsAcc}) ->
            case permissions_test_utils:perm_to_posix_perms(Perm) -- [owner, owner_if_parent_sticky | PosixPerms] of
                [] -> {[Perm | AllowedPermsAcc], DeniedPermsAcc};
                _ -> {AllowedPermsAcc, [Perm | DeniedPermsAcc]}
            end
        end, {[], []}, AllPerms),

        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, DeniedPerms,
            {error, ?EACCES}, <<"denied posix perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, AllowedPerms,
            ok, <<"allowed posix perm">>, Config
        )
    end, lists:seq(1, 5)),

    % Set random acl permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        SetPerms = lists_utils:random_sublist(AllPerms),
        permissions_test_utils:set_acls(P1W2, #{Guid => SetPerms}, #{}, ?everyone, ?no_flags_mask),

        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, permissions_test_utils:complementary_perms(P1W2, Guid, SetPerms),
            {error, ?EACCES}, <<"denied acl perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, SetPerms,
            ok, <<"allowed acl perm">>, Config
        )
    end, lists:seq(1, 10)).


run_multi_provider_perm_test(Nodes, User, Guid, PermsSet, TestedPerms, ExpResult, Scenario, Config) ->
    lists:foreach(fun(TestedPerm) ->
        lists:foreach(fun(Node) ->
            try
                ?assertMatch(
                    ExpResult,
                    check_perms(Node, User, Guid, [TestedPerm], Config),
                    ?ATTEMPTS
                )
            catch _:Reason ->
                ct:pal(
                    "PERMISSIONS TESTS FAILURE~n"
                    "   Scenario: multi_provider_permission_cache_test ~p~n"
                    "   Node: ~p~n"
                    "   Perms set: ~p~n"
                    "   Tested perm: ~p~n"
                    "   Reason: ~p~n",
                    [
                        Scenario, Node, PermsSet, TestedPerm, Reason
                    ]
                ),
                erlang:error(perms_test_failed)
            end
        end, Nodes)
    end, TestedPerms).


expired_session_test(Config) ->
    % Setup
    [_, _, W] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {_, GUID} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, SessId1, <<"/space1/es_file">>, 8#770)
    ),

    ok = rpc:call(W, session, delete, [SessId1]),

    % Verification
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:open(W, SessId1, ?FILE_REF(GUID), write)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        NewConfig2 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig1, "env_desc.json"),
            [{spaces_owners, [<<"owner">>]} | NewConfig1]
        ),
        initializer:mock_auth_manager(NewConfig2),

        % Increase permissions_cache size during cache check procedure to prevent cache cleaning during tests
        % (cache is cleaned only when it exceeds size)
        Workers = ?config(op_worker_nodes, NewConfig),
        test_utils:mock_new(Workers, bounded_cache, [passthrough]),
        test_utils:mock_expect(Workers, bounded_cache, check_cache_size, fun
            (#{name := permissions_cache} = Options) -> meck:passthrough([Options#{size := 1000000000}]);
            (Options) -> meck:passthrough([Options])
        end),
        lists:foreach(fun({SpaceId, _}) ->
            lists:foreach(fun(W) ->
                ?assertEqual(ok, rpc:call(W, dir_stats_service_state, enable, [SpaceId])),
                ?assertEqual(enabled, rpc:call(W, dir_stats_service_state, get_extended_status, [SpaceId]), ?ATTEMPTS)
            end, initializer:get_different_domain_workers(NewConfig2))
        end, ?config(spaces, NewConfig2)),
        NewConfig2
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].


end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, bounded_cache),

    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(multi_provider_permission_cache_test, Config) ->
    ct:timetrap({minutes, 15}),
    init_per_testcase(default, Config);

init_per_testcase(mv_dir_test, Config) ->
    ct:timetrap({minutes, 5}),
    init_per_testcase(default, Config);

init_per_testcase(mv_file_test, Config) ->
    ct:timetrap({minutes, 5}),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
check_perms(Node, User, Guid, Perms, Config) ->
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),

    rpc:call(Node, ?MODULE, check_perms, [
        UserCtx, file_ctx:new_by_guid(Guid),
        [?OPERATIONS(permissions_test_utils:perms_to_bitmask(Perms))]
    ]).


%% @private
check_perms(UserCtx, FileCtx, Perms) ->
    try
        fslogic_authz:ensure_authorized(UserCtx, FileCtx, Perms),
        ok
    catch _Type:Reason ->
        {error, Reason}
    end.


%% @private
-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid()) -> ok.
fill_file_with_dummy_data(Node, SessId, Guid) ->
    lfm_test_utils:write_file(Node, SessId, Guid, <<"DATA">>).


%% @private
-spec create_dummy_file(node(), session:id(), file_id:file_guid()) -> ok.
create_dummy_file(Node, SessId, DirGuid) ->
    RandomFileName = <<"DUMMY_FILE_", (integer_to_binary(rand:uniform(1024)))/binary>>,
    {ok, {_Guid, FileHandle}} =
        lfm_proxy:create_and_open(Node, SessId, DirGuid, RandomFileName, ?DEFAULT_FILE_PERMS),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).


%% @private
-spec extract_ok
    (ok | {ok, term()} | {ok, term(), term()} | {ok, term(), term(), term()}) -> ok;
    ({error, term()}) -> {error, term()}.
extract_ok(ok) -> ok;
extract_ok({ok, _}) -> ok;
extract_ok({ok, _, _}) -> ok;
extract_ok({ok, _, _, _}) -> ok;
extract_ok({error, _} = Error) -> Error.
