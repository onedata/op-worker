%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of file metadata operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_file_metadata_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    test_get_custom_metadata/1,
    test_set_custom_metadata/1,
    test_remove_custom_metadata/1,

    test_get_xattr/1,
    test_list_xattr/1,
    test_set_xattr/1,
    test_remove_xattr/1,

    test_get_file_distribution/1,
    test_get_historical_dir_size_stats/1,
    test_get_file_storage_locations/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_get_custom_metadata(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_file_metadata:set_custom_metadata(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), json, <<"VAL">>, []
                ),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                opt_file_metadata:get_custom_metadata(Node, SessionId, FileKey, json, [], false);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir
        ],
        special_dirs_error = ?ERROR_FORBIDDEN
    }).


test_set_custom_metadata(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                opt_file_metadata:set_custom_metadata(Node, SessionId, FileKey, json, <<"VAL">>, []);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir
        ],
        special_dirs_error = ?ERROR_FORBIDDEN
    }).


test_remove_custom_metadata(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_file_metadata:set_custom_metadata(
                    Node, FileOwnerSessionId, ?FILE_REF(Guid), json, <<"VAL">>, []
                ),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_file_metadata:remove_custom_metadata(Node, SessionId, FileKey, json)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_get_xattr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                lfm_proxy:get_xattr(Node, SessionId, FileKey, <<"myxattr">>);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir, archive_dir
        ],
        special_dirs_error = {error, ?EPERM}
    }).


test_list_xattr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                lfm_proxy:list_xattr(Node, SessionId, FileKey, false, false);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir, archive_dir
        ],
        special_dirs_error = {error, ?EPERM}
    }).


test_set_xattr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                lfm_proxy:set_xattr(Node, SessionId, FileKey, #xattr{name = <<"myxattr">>, value = <<"VAL">>});
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir, archive_dir
        ],
        special_dirs_error =  {error, ?EPERM}
    }).


test_remove_xattr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_metadata],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(Node, FileOwnerSessionId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_xattr(Node, SessionId, FileKey, <<"myxattr">>)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_get_file_distribution(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                opt_file_metadata:get_distribution_deprecated(Node, SessionId, FileKey);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir, archive_dir
        ],
        special_dirs_error = ?ERROR_FORBIDDEN
    }).


test_get_historical_dir_size_stats(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?read_metadata],
            children = [
                #ct_authz_file_spec{
                    name = <<"file1">>,
                    required_perms = []
                }
            ]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                ProviderId = opw_test_rpc:get_provider_id(Node),
                opt_file_metadata:get_historical_dir_size_stats(
                    Node, SessionId, FileKey, ProviderId, #time_series_layout_get_request{}
                );
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/dir1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, opened_deleted_files_dir, share_container
        ],
        special_dirs_error = ?ERROR_FORBIDDEN
    }).


test_get_file_storage_locations(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun
            F(guid, Node, SessionId, FileKey) ->
                opt_file_metadata:get_storage_locations(Node, SessionId, FileKey);
            F(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
                FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
                F(guid, Node, SessionId, maps:get(FilePath, ExtraData))
            end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        forbidden_special_dirs = [
            user_root_dir, trash, tmp_dir, opened_deleted_files_dir, share_container,
            space_archives_root_dir, dataset_archives_root_dir, archive_dir
        ],
        special_dirs_error = ?ERROR_FORBIDDEN
    }).
