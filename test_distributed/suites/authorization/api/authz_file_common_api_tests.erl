%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of file common operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_file_common_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    get_parent/1,
    get_file_path/1,
    resolve_guid/1,
    stat/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


get_parent(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_parent(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_path(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_for_share_guid = false, % TODO VFS-6057
        available_in_open_handle_mode = false, % TODO VFS-6057
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            ?FILE_REF(FileGuid) = maps:get(FilePath, ExtraData),
            lfm_proxy:get_file_path(Node, SessionId, FileGuid)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


resolve_guid(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_for_share_guid = not_a_file_guid_based_operation,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            lfm_proxy:resolve_guid(Node, SessionId, FilePath)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


stat(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:stat(Node, SessionId, FileKey)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
