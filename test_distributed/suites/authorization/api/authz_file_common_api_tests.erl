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
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    test_get_parent/1,
    test_get_file_path/1,
    test_resolve_guid/1,
    test_stat/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_get_parent(SpaceId) ->
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


test_get_file_path(SpaceId) ->
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


test_resolve_guid(SpaceId) ->
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


test_stat(SpaceId) ->
    Xattrs = ?RAND_ELEMENT([
        ?attr_xattrs([<<"key">>]),
        ?attr_xattrs([<<"cdmi_acl">>]),
        ?attr_xattrs([<<"cdmi_mimetype">>]),
        ?attr_xattrs([<<"onedata_json">>])]
    ),
    Attributes = ?RAND_SUBLIST([Xattrs | ?ALL_ATTRS]),
    RequiredPerms = lists:usort(lists:flatmap(fun get_attr_required_perms/1, Attributes)),
    RequiredSpacePrivs = case lists:member(?read_metadata, RequiredPerms) of
        true -> [?SPACE_READ_DATA];
        false -> []
    end,

    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            required_perms = RequiredPerms,
            on_create = fun(Node, FileOwnerSessionId, Guid) ->
                opt_cdmi:set_mimetype(Node, FileOwnerSessionId, ?FILE_REF(Guid), <<"mimetype">>),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = RequiredSpacePrivs,
        acl_requires_space_privs = RequiredSpacePrivs,
        available_in_readonly_mode = true,
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:stat(Node, SessionId, FileKey, Attributes)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% ATTENTION!!!
%% Rather than reusing ready macros for all possible attributes they are listed
%% by hand to ensure that if any new attribute is added the test will crash
%% (if such attribute is selected so it may not happen immediately though)
%% and implementer will have to add case to this function considering if
%% any permission is required to get this new attribute.
%% @end
%%--------------------------------------------------------------------
-spec get_attr_required_perms(lfm_attrs:file_attributes()) -> [binary()].
get_attr_required_perms(guid) -> [];
get_attr_required_perms(index) -> [];
get_attr_required_perms(type) -> [];
get_attr_required_perms(active_permissions_type) -> [];
get_attr_required_perms(mode) -> [];
get_attr_required_perms(acl) -> [?read_acl];
get_attr_required_perms(name) -> [];
get_attr_required_perms(conflicting_name) -> [];
get_attr_required_perms(path) -> [];
get_attr_required_perms(parent_guid) -> [];
get_attr_required_perms(gid) -> [];
get_attr_required_perms(uid) -> [];
get_attr_required_perms(atime) -> [];
get_attr_required_perms(mtime) -> [];
get_attr_required_perms(ctime) -> [];
get_attr_required_perms(size) -> [];
get_attr_required_perms(is_fully_replicated) -> [];
get_attr_required_perms(local_replication_rate) -> [];
get_attr_required_perms(provider_id) -> [];
get_attr_required_perms(shares) -> [];
get_attr_required_perms(owner_id) -> [];
get_attr_required_perms(hardlink_count) -> [];
get_attr_required_perms(symlink_value) -> [];
get_attr_required_perms(has_custom_metadata) -> [];
get_attr_required_perms(eff_protection_flags) -> [];
get_attr_required_perms(eff_dataset_protection_flags) -> [];
get_attr_required_perms(eff_dataset_inheritance_path) -> [];
get_attr_required_perms(eff_qos_inheritance_path) -> [];
get_attr_required_perms(qos_status) -> [];
get_attr_required_perms(recall_root_id) -> [];
get_attr_required_perms(is_deleted) -> [];
get_attr_required_perms(conflicting_files) -> [];
get_attr_required_perms({xattrs, _XattrNames}) -> [?read_metadata].
