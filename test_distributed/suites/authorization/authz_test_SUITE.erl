%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-11734 Port permission tests to authz_test_runner
%%% This test suite verifies correct behaviour of authorization mechanism
%%% with corresponding lfm (logical_file_manager) functions.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_test_SUITE).
-author("Bartosz Walkowicz").

-include("authz_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    mkdir_test/1,
    get_children_test/1,
    get_children_attrs_test/1,
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
    get_file_distribution_test/1,
    get_historical_dir_size_stats_test/1,
    get_file_storage_locations_test/1,

%%    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
%%    remove_share_test/1,
%%    share_perms_test/1,

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
    check_qos_fulfillment_test/1
]).

all() -> [
    mkdir_test,
    get_children_test,
    get_children_attrs_test,
    get_child_attr_test,
    mv_dir_test,
    rm_dir_test,

    create_file_test,
    open_for_read_test,
    open_for_write_test,
    open_for_rdwr_test,
    create_and_open_test,
    truncate_test,
    mv_file_test,
    rm_file_test,

    get_parent_test,
    get_file_path_test,
    get_file_guid_test,
    get_file_attr_test,
    get_file_distribution_test,
    get_historical_dir_size_stats_test,
    get_file_storage_locations_test,

%%    set_perms_test,
    check_read_perms_test,
    check_write_perms_test,
    check_rdwr_perms_test,

    create_share_test,
%%    remove_share_test,
%%    share_perms_test,

    get_acl_test,
    set_acl_test,
    remove_acl_test,

    get_transfer_encoding_test,
    set_transfer_encoding_test,
    get_cdmi_completion_status_test,
    set_cdmi_completion_status_test,
    get_mimetype_test,
    set_mimetype_test,

    get_metadata_test,
    set_metadata_test,
    remove_metadata_test,
    get_xattr_test,
    list_xattr_test,
    set_xattr_test,
    remove_xattr_test,

    add_qos_entry_test,
    get_qos_entry_test,
    remove_qos_entry_test,
    get_effective_file_qos_test,
    check_qos_fulfillment_test
].


-define(ATTEMPTS, 10).


%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:mkdir(Node, SessionId, ParentDirGuid, <<"dir2">>, 8#777) of
                {ok, DirGuid} ->
                    permissions_test_utils:ensure_dir_created_on_storage(Node, DirGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/dir2">>}
        end
    }).


get_children_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children(Node, SessionId, DirKey, 0, 100))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


get_children_attrs_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children_attrs(Node, SessionId, DirKey, #{
                offset => 0, limit => 100, tune_for_large_continuous_listing => false
            }))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


get_child_attr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?traverse_container],
            children = [#ct_authz_file_spec{name = <<"file1">>}]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:get_child_attr(Node, SessionId, ParentDirGuid, <<"file1">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }).


mv_dir_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #ct_authz_dir_spec{
                        name = <<"dir11">>,
                        perms = [?delete]
                    }
                ]
            },
            #ct_authz_dir_spec{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_subcontainer]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            SrcDirPath = <<TestCaseRootDirPath/binary, "/dir1/dir11">>,
            SrcDirKey = maps:get(SrcDirPath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(Node, SessionId, SrcDirKey, DstDirKey, <<"dir21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/dir21">>}
        end
    }).


rm_dir_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #ct_authz_dir_spec{
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
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1/dir2">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:unlink(Node, SessionId, DirKey))
        end
    }).


create_file_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:create(Node, SessionId, ParentDirGuid, <<"file1">>, 8#777) of
                {ok, FileGuid} ->
                    permissions_test_utils:ensure_file_created_on_storage(Node, FileGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }).


open_for_read_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(Node, FileOwnerSessionId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(Node, SessionId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


open_for_write_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_object],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(Node, FileOwnerSessionId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(Node, SessionId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


open_for_rdwr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object, ?write_object],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(Node, FileOwnerSessionId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(Node, SessionId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


create_and_open_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                % Create dummy file to ensure that directory is created on storage.
                % Otherwise it may be not possible during tests without necessary perms.
                create_dummy_file(Node, FileOwnerSessionId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:create_and_open(Node, SessionId, ParentDirGuid, <<"file1">>, 8#777))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }).


truncate_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:truncate(Node, SessionId, FileKey, 0))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


mv_file_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #ct_authz_file_spec{
                        name = <<"file11">>,
                        perms = [?delete]
                    }
                ]
            },
            #ct_authz_dir_spec{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_object]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            SrcFilePath = <<TestCaseRootDirPath/binary, "/dir1/file11">>,
            SrcFileKey = maps:get(SrcFilePath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(Node, SessionId, SrcFileKey, DstDirKey, <<"file21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/file21">>}
        end
    }).


rm_file_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #ct_authz_file_spec{
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
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:unlink(Node, SessionId, FileKey))
        end
    }).


get_parent_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_parent(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_path_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = false, % TODO VFS-6057
        available_in_open_handle_mode = false, % TODO VFS-6057
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            ?FILE_REF(FileGuid) = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_file_path(Node, SessionId, FileGuid))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_guid_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            extract_ok(lfm_proxy:resolve_guid(Node, SessionId, FilePath))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_attr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:stat(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_distribution_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_distribution_deprecated(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_historical_dir_size_stats_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            perms = [?read_metadata],
            children = [
                #ct_authz_file_spec{
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
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1">>,
            FileKey = maps:get(FilePath, ExtraData),
            ProviderId = opw_test_rpc:get_provider_id(Node),

            extract_ok(opt_file_metadata:get_historical_dir_size_stats(
                Node, SessionId, FileKey, ProviderId, #time_series_layout_get_request{})
            )
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


get_file_storage_locations_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_storage_locations(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


%%%% TODO
%%set_perms_test(Config) ->
%%    [_, _, Node] = ?config(op_worker_nodes, Config),
%%    FileOwner = <<"user1">>,
%%
%%    FileOwnerUserSessionId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
%%    GroupUserSessionId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
%%    OtherUserSessionId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),
%%    SpaceOwnerSessionId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),
%%
%%    DirPath = <<"/space1/dir1">>,
%%    {ok, DirGuid} = ?assertMatch(
%%        {ok, _},
%%        lfm_proxy:mkdir(Node, FileOwnerUserSessionId, DirPath)
%%    ),
%%
%%    FilePath = <<"/space1/dir1/file1">>,
%%    {ok, FileGuid} = ?assertMatch(
%%        {ok, _},
%%        lfm_proxy:create(Node, FileOwnerUserSessionId, FilePath, 8#777)
%%    ),
%%    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), <<"share">>)),
%%    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
%%
%%    % Open file to ensure it's creation on storage
%%    {ok, Handle} = lfm_proxy:open(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), write),
%%    ok = lfm_proxy:close(Node, Handle),
%%
%%    AssertProperStorageAttrsFun = fun(ExpMode) ->
%%        permissions_test_utils:assert_user_is_file_owner_on_storage(
%%            Node, ?SPACE_ID, FilePath, FileOwnerUserSessionId, #{mode => ?FILE_MODE(ExpMode)}
%%        )
%%    end,
%%
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    %% POSIX
%%
%%    % file owner can always change file perms if he has access to it
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#677, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#100, FileGuid => 8#000}),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),
%%    AssertProperStorageAttrsFun(8#000),
%%
%%    % but not if that access is via shared guid
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EPERM},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    % other users from space can't change perms no matter what
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    % with exception being space owner who can always change perms no matter what
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#000, FileGuid => 8#000}),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), 8#555)),
%%    AssertProperStorageAttrsFun(8#555),
%%
%%    % but even space owner cannot perform write operation on space dir
%%    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(<<"space1">>),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#000)),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#555)),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#777)),
%%
%%    % users outside of space shouldn't even see the file
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?ENOENT},
%%        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    %% ACL
%%
%%    % file owner can always change file perms if he has access to it
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => [?traverse_container],
%%        FileGuid => []
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),
%%
%%    % but not if that access is via shared guid
%%    ?assertMatch(
%%        {error, ?EPERM},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
%%    ),
%%
%%    % file owner cannot change acl after his access was denied by said acl
%%    permissions_test_utils:set_acls(Node, #{}, #{
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, ?everyone, ?no_flags_mask),
%%
%%    PermsBitmask = permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS),
%%
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_acl(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), [
%%            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
%%        ])
%%    ),
%%
%%    % but space owner always can change acl for any file
%%    ?assertMatch(
%%        ok,
%%        lfm_proxy:set_acl(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), [
%%            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
%%        ])
%%    ),
%%
%%    % other users from space can't change perms no matter what
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS,
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%
%%    % users outside of space shouldn't even see the file
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS,
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?ENOENT},
%%        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ).


check_read_perms_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_write_perms_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_rdwr_perms_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


create_share_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_dir_spec{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(opt_shares:create(Node, SessionId, DirKey, <<"create_share">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


%%%% TODO
%%remove_share_test(Config) ->
%%    [_, _, W] = ?config(op_worker_nodes, Config),
%%
%%    SpaceOwnerSessionId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),
%%    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [?ROOT_SESS_ID, ?SPACE_ID]),
%%    ScenariosRootDirPath = filename:join(["/", SpaceName, ?SCENARIO_NAME]),
%%    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SpaceOwnerSessionId, ScenariosRootDirPath, 8#700)),
%%
%%    FilePath = filename:join([ScenariosRootDirPath, ?RAND_STR()]),
%%    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SpaceOwnerSessionId, FilePath, 8#700)),
%%    {ok, ShareId} = opt_shares:create(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), <<"share">>),
%%
%%    RemoveShareFun = fun(SessionId) ->
%%        % Remove share via gs call to test token data caveats
%%        % (middleware should reject any api call with data caveats)
%%        rpc:call(Node, middleware, handle, [#op_req{
%%            auth = permissions_test_utils:get_auth(Node, SessionId),
%%            gri = #gri{type = op_share, id = ShareId, aspect = instance},
%%            operation = delete
%%        }])
%%    end,
%%
%%    UserId = <<"user2">>,
%%    UserSessionId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
%%
%%    % Assert share removal requires only ?SPACE_MANAGE_SHARES space priv
%%    % and no file permissions
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, []),
%%    ?assertEqual(?ERROR_POSIX(?EPERM), RemoveShareFun(UserSessionId)),
%%
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, privileges:space_admin()),
%%    MainToken = initializer:create_access_token(UserId),
%%
%%    % Assert api operations are unauthorized in case of data caveats
%%    Token1 = tokens:confine(MainToken, #cv_data_readonly{}),
%%    CaveatSessionId1 = permissions_test_utils:create_session(Node, UserId, Token1),
%%    ?assertEqual(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_readonly})),
%%        RemoveShareFun(CaveatSessionId1)
%%    ),
%%    Token2 = tokens:confine(MainToken, #cv_data_path{whitelist = [ScenariosRootDirPath]}),
%%    CaveatSessionId2 = permissions_test_utils:create_session(Node, UserId, Token2),
%%    ?assertMatch(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_path, [ScenariosRootDirPath]})),
%%        RemoveShareFun(CaveatSessionId2)
%%    ),
%%    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
%%    Token3 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [ObjectId]}),
%%    CaveatSessionId3 = permissions_test_utils:create_session(Node, UserId, Token3),
%%    ?assertMatch(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_objectid, [ObjectId]})),
%%        RemoveShareFun(CaveatSessionId3)
%%    ),
%%
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, [?SPACE_MANAGE_SHARES]),
%%    ?assertEqual(ok, RemoveShareFun(UserSessionId)).
%%
%%
%%%% TODO
%%share_perms_test(Config) ->
%%    [_, _, W] = ?config(op_worker_nodes, Config),
%%    FileOwner = <<"user1">>,
%%
%%    FileOwnerUserSessionId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
%%    GroupUserSessionId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
%%
%%    ScenarioDirName = ?SCENARIO_NAME,
%%    ScenarioDirPath = <<"/space1/", ScenarioDirName/binary>>,
%%    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, ScenarioDirPath, 8#700)),
%%
%%    MiddleDirPath = <<ScenarioDirPath/binary, "/dir2">>,
%%    {ok, MiddleDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, MiddleDirPath, 8#777)),
%%
%%    BottomDirPath = <<MiddleDirPath/binary, "/dir3">>,
%%    {ok, BottomDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, BottomDirPath), 8#777),
%%
%%    FilePath = <<BottomDirPath/binary, "/file1">>,
%%    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, FileOwnerUserSessionId, FilePath, 8#777)),
%%
%%    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(Node, FileOwnerUserSessionId, ?FILE_REF(MiddleDirGuid), <<"share">>)),
%%    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
%%
%%    % Accessing file in normal mode by space user should result in eacces (dir1 perms -> 8#700)
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(FileGuid))
%%    ),
%%    % But accessing it in share mode should succeed as perms should be checked only up to
%%    % share root (dir1/dir2 -> 8#777) and not space root
%%    ?assertMatch(
%%        {ok, #file_attr{guid = ShareFileGuid}},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(ShareFileGuid))
%%    ),
%%
%%    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
%%    ?assertEqual(ok, lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, ?FILE_REF(BottomDirGuid), 8#770)),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(ShareFileGuid))
%%    ).


get_acl_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_acl(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_acl_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_acl(Node, SessionId, FileKey, [
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
    }).


remove_acl_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_acl(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_transfer_encoding_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_transfer_encoding(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"base64">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_transfer_encoding(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_transfer_encoding_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_transfer_encoding(Node, SessionId, FileKey, <<"base64">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_cdmi_completion_status_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_cdmi_completion_status(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"Completed">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_cdmi_completion_status(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_cdmi_completion_status_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_cdmi_completion_status(Node, SessionId, FileKey, <<"Completed">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_mimetype_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_mimetype(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"mimetype">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:get_mimetype(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_mimetype_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_cdmi:set_mimetype(Node, SessionId, FileKey, <<"mimetype">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_metadata_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_file_metadata:set_custom_metadata(Node, FileOwnerSessionId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:get_custom_metadata(Node, SessionId, FileKey, json, [], false))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_metadata_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:set_custom_metadata(Node, SessionId, FileKey, json, <<"VAL">>, []))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


remove_metadata_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_file_metadata:set_custom_metadata(Node, FileOwnerSessionId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_file_metadata:remove_custom_metadata(Node, SessionId, FileKey, json))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_xattr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_xattr(Node, SessionId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


list_xattr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:list_xattr(Node, SessionId, FileKey, false, false))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_xattr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_xattr(Node, SessionId, FileKey, #xattr{
                name = <<"myxattr">>, value = <<"VAL">>
            }))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


remove_xattr_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_xattr(Node, SessionId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


add_qos_entry_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:add_qos_entry(Node, SessionId, FileKey, <<"country=FR">>, 1))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_qos_entry_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:get_qos_entry(Node, SessionId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


remove_qos_entry_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:remove_qos_entry(Node, SessionId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_effective_file_qos_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                {ok, _QosEntryId} = opt_qos:add_qos_entry(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:get_effective_file_qos(Node, SessionId, FileKey))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_qos_fulfillment_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                {ok, QosEntryId} = opt_qos:add_qos_entry(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(opt_qos:check_qos_status(Node, SessionId, QosEntryId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, authz_test_runner],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = ?RAND_ELEMENT([
            "1op_posix"
%%            "1op_s3"  %% TODO
        ]),
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            % clean space
            lists:foreach(fun(SpaceSelector) ->
                {ok, FileEntries} = onenv_file_test_utils:ls(space_owner, SpaceSelector, 0, 10000),

                lists_utils:pforeach(fun({Guid, _}) ->
                    onenv_file_test_utils:rm_and_sync_file(space_owner, Guid)
                end, FileEntries)
            end, [space_krk]),

            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
