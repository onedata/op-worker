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
-include("storage_files_test_SUITE.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    set_perms/1,
    check_read_perms/1,
    check_write_perms/1,
    check_rdwr_perms/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


set_perms(SpaceId) ->
    Node = oct_background:get_random_provider_node(krakow),

    FileOwnerUserSessionId = oct_background:get_user_session_id(user1, krakow),
    GroupUserSessionId = oct_background:get_user_session_id(user2, krakow),
    OtherUserSessionId = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessionId = oct_background:get_user_session_id(space_owner, krakow),

    DirPath = <<"/space1/dir1">>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(Node, FileOwnerUserSessionId, DirPath)
    ),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(Node, FileOwnerUserSessionId, FilePath, 8#777)
    ),
    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Open file to ensure it's creation on storage
    {ok, Handle} = lfm_proxy:open(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(Node, Handle),

    AssertProperStorageAttrsFun = fun(ExpMode) ->
        permissions_test_utils:assert_user_is_file_owner_on_storage(
            Node, SpaceId, FilePath, FileOwnerUserSessionId, #{mode => ?FILE_MODE(ExpMode)}
        )
    end,

    AssertProperStorageAttrsFun(8#777),

    %% POSIX

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ),
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),
    AssertProperStorageAttrsFun(8#000),

    % but not if that access is via shared guid
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % other users from space can't change perms no matter what
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % with exception being space owner who can always change perms no matter what
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#000, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), 8#555)),
    AssertProperStorageAttrsFun(8#555),

    % but even space owner cannot perform write operation on space dir
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(<<"space1">>),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#000)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#555)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#777)),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    %% ACL

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_acls(Node, #{
        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ),

    permissions_test_utils:set_acls(Node, #{
        DirGuid => [?traverse_container],
        FileGuid => []
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),

    % but not if that access is via shared guid
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
    ),

    % but space owner always can change acl for any file

    % other users from space can't change perms no matter what
    permissions_test_utils:set_acls(Node, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_acls(Node, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
    ).


check_read_perms(SpaceId) ->
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
            lfm_proxy:check_perms(Node, SessionId, FileKey, read)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_write_perms(SpaceId) ->
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
            lfm_proxy:check_perms(Node, SessionId, FileKey, write)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_rdwr_perms(SpaceId) ->
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
            lfm_proxy:check_perms(Node, SessionId, FileKey, rdwr)
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).
