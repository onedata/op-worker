%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of ACL operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_acl_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    test_get_acl/1,
    test_set_acl/1,
    test_remove_acl/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_get_acl(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun (Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FileKey = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath, <<"/file1">>, ExtraData),
            lfm_proxy:get_acl(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        allowed_special_dirs = [space_dir]
    }).


test_set_acl(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun (Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FileKey = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath, <<"/file1">>, ExtraData),
            lfm_proxy:set_acl(Node, SessionId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    authz_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ])
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        allowed_special_dirs = [space_dir],
        special_dirs_ok_value = {error, ?EACCES} % space dir does not have required perms set
    }).


test_remove_acl(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_public_data_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FileKey = authz_api_test_runner:extract_test_file_key(TestCaseRootDirPath, <<"/file1">>, ExtraData),
            lfm_proxy:remove_acl(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end,
        allowed_special_dirs = [space_dir],
        special_dirs_ok_value = {error, ?EACCES} % space dir does not have required perms set
    }).
