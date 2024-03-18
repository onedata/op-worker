%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of QoS operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_qos_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").

-export([
    test_add_qos_entry/1,
    test_get_qos_entry/1,
    test_remove_qos_entry/1,
    test_get_effective_file_qos/1,
    test_check_qos_status/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_add_qos_entry(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = false,
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_qos:add_qos_entry(Node, SessionId, FileKey, <<"country=FR">>, 1)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_get_qos_entry(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
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
        available_for_share_guid = not_a_file_guid_based_operation,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            opt_qos:get_qos_entry(Node, SessionId, QosEntryId)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_remove_qos_entry(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
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
        available_for_share_guid = not_a_file_guid_based_operation,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            opt_qos:remove_qos_entry(Node, SessionId, QosEntryId)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_get_effective_file_qos(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
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
        available_for_share_guid = not_a_file_guid_based_operation,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            opt_qos:get_effective_file_qos(Node, SessionId, FileKey)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_check_qos_status(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
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
        available_for_share_guid = not_a_file_guid_based_operation,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            opt_qos:check_qos_status(Node, SessionId, QosEntryId)
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
