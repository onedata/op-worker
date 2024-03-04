%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of perms operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_perms_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("storage_files_test_SUITE.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    test_set_perms/1,
    test_check_read_perms/1,
    test_check_write_perms/1,
    test_check_rdwr_perms/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


test_set_perms(SpaceId) ->
    Node = oct_background:get_random_provider_node(krakow),

    SpaceOwnerSessionId = oct_background:get_user_session_id(space_owner, krakow),
    FileOwnerSessionId = oct_background:get_user_session_id(user1, krakow),
    FileOwnerUserId = oct_background:get_user_id(user1),
    SpaceMemberSessionId = oct_background:get_user_session_id(user2, krakow),
    NonSpaceMemberSessionId = oct_background:get_user_session_id(user3, krakow),

    SpaceDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    #object{
        guid = DirGuid,
        children = [
            #object{guid = FileGuid}
        ]
    } = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, SpaceDirGuid, krakow, #dir_spec{
            name = <<"root_dir">>,
            mode = ?FILE_MODE(8#700),
            children = [#file_spec{
                name = <<"file">>,
                mode = ?FILE_MODE(8#777)
            }]
        }
    ),
    storage_test_utils:ensure_file_created_on_storage(Node, FileGuid),
    FileRef = ?FILE_REF(FileGuid),

    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SpaceOwnerSessionId, FileRef, <<"share">>)
    ),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    ShareFileRef = ?FILE_REF(ShareFileGuid),

    FilePath = <<"/test_set_perms/root_dir/file">>,

    AssertAttrsOnStorage = fun(ExpMode) ->
        storage_test_utils:assert_file_attrs_on_posix_storage(
            Node, SpaceId, FilePath, FileOwnerSessionId, #{mode => ?FILE_MODE(ExpMode)}
        )
    end,
    AssertAttrsOnStorage(8#777),

    %% POSIX

    % file owner can always change file perms if he has access to it
    authz_test_utils:set_modes(Node, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, FileOwnerSessionId, FileRef, 8#000)),
    authz_test_utils:set_modes(Node, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerSessionId, FileRef, 8#000)),
    AssertAttrsOnStorage(8#000),

    % but not if that access is via shared guid
    authz_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, FileOwnerSessionId, ShareFileRef, 8#000)),
    AssertAttrsOnStorage(8#777),

    % other users from space can't change perms no matter what
    authz_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, SpaceMemberSessionId, FileRef, 8#000)),
    AssertAttrsOnStorage(8#777),

    % with exception being space owner who can always change perms no matter what
    authz_test_utils:set_modes(Node, #{DirGuid => 8#000, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, FileRef, 8#555)),
    AssertAttrsOnStorage(8#555),

    % but even space owner cannot perform write operation on space dir
    SpaceDirRef = ?FILE_REF(SpaceDirGuid),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, SpaceDirRef, 8#000)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, SpaceDirRef, 8#555)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, SpaceDirRef, 8#777)),

    % users outside of space shouldn't even see the file
    authz_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, NonSpaceMemberSessionId, FileRef, 8#000)),
    AssertAttrsOnStorage(8#777),

    %% ACL

    SetAclFun = fun(AclPerFile) ->
        authz_test_utils:set_acls(Node, AclPerFile, #{}, ?everyone, ?no_flags_mask)
    end,

    % file owner can always change file perms if he has access to it
    SetAclFun(#{DirGuid => ?ALL_DIR_PERMS -- [?traverse_container], FileGuid => ?ALL_FILE_PERMS}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, FileOwnerSessionId, FileRef, 8#000)),

    SetAclFun(#{DirGuid => [?traverse_container], FileGuid => []}),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerSessionId, FileRef, 8#000)),

    % but not if that access is via shared guid
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, FileOwnerSessionId, ShareFileRef, 8#000)),

    % but space owner always can change acl for any file

    % other users from space can't change perms no matter what
    SetAclFun(#{DirGuid => ?ALL_DIR_PERMS, FileGuid => ?ALL_FILE_PERMS}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, SpaceMemberSessionId, FileRef, 8#000)),

    % users outside of space shouldn't even see the file
    SetAclFun(#{DirGuid => ?ALL_DIR_PERMS, FileGuid => ?ALL_FILE_PERMS}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(Node, NonSpaceMemberSessionId, FileRef, 8#000)).


test_check_read_perms(SpaceId) ->
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
        available_for_share_guid = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(Node, SessionId, FileKey, read)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_check_write_perms(SpaceId) ->
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
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(Node, SessionId, FileKey, write)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


test_check_rdwr_perms(SpaceId) ->
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
        available_for_share_guid = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(Node, SessionId, FileKey, rdwr)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
