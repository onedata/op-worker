%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of cdmi metadata operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_cdmi_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    get_transfer_encoding/1,
    set_transfer_encoding/1,
    get_cdmi_completion_status/1,
    set_cdmi_completion_status/1,
    get_mimetype/1,
    set_mimetype/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


get_transfer_encoding(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_transfer_encoding(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"base64">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:get_transfer_encoding(Node, SessionId, FileKey)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_transfer_encoding(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:set_transfer_encoding(Node, SessionId, FileKey, <<"base64">>)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_cdmi_completion_status(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_cdmi_completion_status(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"Completed">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:get_cdmi_completion_status(Node, SessionId, FileKey)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_cdmi_completion_status(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:set_cdmi_completion_status(Node, SessionId, FileKey, <<"Completed">>)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_mimetype(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_attributes],
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_mimetype(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"mimetype">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:get_mimetype(Node, SessionId, FileKey)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_mimetype(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_cdmi:set_mimetype(Node, SessionId, FileKey, <<"mimetype">>)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
