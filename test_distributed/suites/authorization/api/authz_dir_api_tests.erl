%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of directory specific operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_dir_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    test_mkdir/1,
    test_get_children_attrs/1,
    test_get_child_attr/1,
    test_mv_dir/1,
    test_rm_dir/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_mkdir(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ?FILE_REF(ParentDirGuid) = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath,
                <<"/dir1">>, ExtraData),
            case lfm_proxy:mkdir(Node, SessionId, ParentDirGuid, <<"dir2">>, 8#777) of
                {ok, DirGuid} ->
                    storage_test_utils:ensure_dir_created_on_storage(Node, DirGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_assign_ownership, <<TestCaseRootDirPath/binary, "/dir1/dir2">>}
        end,
        allowed_special_dirs = [space_dir, archive_dir, tmp_dir]
    }).


test_get_children_attrs(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_public_data_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirKey = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath, <<"/dir1">>, ExtraData),
            lfm_proxy:get_children_attrs(Node, SessionId, DirKey, #{
                offset => 0, limit => 100, tune_for_large_continuous_listing => false
            })
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end,
        allowed_special_dirs = ?ALL_SPECIAL_DIRS -- [global_root_dir]
    }).


test_get_child_attr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?traverse_container],
            children = [#ct_authz_file_spec{name = <<"file1">>}]
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_public_data_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ?FILE_REF(ParentDirGuid) = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath,
                <<"/dir1">>, ExtraData),
            lfm_proxy:get_child_attr(Node, SessionId, ParentDirGuid, <<"file1">>)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end,
        allowed_special_dirs = ?ALL_SPECIAL_DIRS -- [global_root_dir],
        special_dirs_ok_value = {error, ?ENOENT} % this file does not exist in special dirs
    }).


test_mv_dir(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                required_perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #ct_authz_dir_spec{
                        name = <<"dir11">>,
                        required_perms = [?delete]
                    }
                ]
            },
            #ct_authz_dir_spec{
                name = <<"dir2">>,
                required_perms = [?traverse_container, ?add_subcontainer]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            SrcDirKey = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath, <<"/dir1/dir11">>, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            lfm_proxy:mv(Node, SessionId, SrcDirKey, DstDirKey, <<"dir21">>)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/dir21">>}
        end,
        allowed_special_dirs = []
    }).


test_rm_dir(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                required_perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #ct_authz_dir_spec{
                        name = <<"dir2">>,
                        required_perms = [?delete, ?list_container]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            lfm_proxy:unlink(Node, SessionId, authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath,
                <<"/dir1/dir2">>, ExtraData))
        end,
        final_ownership_check = fun(_) -> {inapplicable_due_to, dir_removal} end,
        allowed_special_dirs = []
    }).