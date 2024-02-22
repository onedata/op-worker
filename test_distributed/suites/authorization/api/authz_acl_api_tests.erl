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
    get_acl/1,
    set_acl/1,
    remove_acl/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


get_acl(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_acl(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_acl(SpaceId) ->
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
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_acl(Node, SessionId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ])
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


remove_acl(SpaceId) ->
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
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_acl(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
