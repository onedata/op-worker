%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of regular file specific operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_reg_file_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    create_file/1,
    open_for_read/1,
    open_for_write/1,
    open_for_rdwr/1,
    create_and_open/1,
    truncate/1,
    mv_file/1,
    rm_file/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


create_file(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?traverse_container, ?add_object]
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
            {should_assign_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }).


open_for_read(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(Node, SessionId, FileKey, read)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


open_for_write(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(Node, SessionId, FileKey, write)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


open_for_rdwr(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(Node, SessionId, FileKey, rdwr)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


create_and_open(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            required_perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create_and_open(Node, SessionId, ParentDirGuid, <<"file1">>, 8#777)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_assign_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }).


truncate(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:truncate(Node, SessionId, FileKey, 0)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


mv_file(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                required_perms = [?traverse_container, ?delete_object],
                children = [
                    #ct_authz_file_spec{
                        name = <<"file11">>,
                        required_perms = [?delete]
                    }
                ]
            },
            #ct_authz_dir_spec{
                name = <<"dir2">>,
                required_perms = [?traverse_container, ?add_object]
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
            lfm_proxy:mv(Node, SessionId, SrcFileKey, DstDirKey, <<"file21">>)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/file21">>}
        end
    }).


rm_file(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [
            #ct_authz_dir_spec{
                name = <<"dir1">>,
                required_perms = [?traverse_container, ?delete_object],
                children = [
                    #ct_authz_file_spec{
                        name = <<"file1">>,
                        required_perms = [?delete]
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
            lfm_proxy:unlink(Node, SessionId, FileKey)
        end
    }).
