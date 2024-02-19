%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-11734 Port permission tests to authz_api_test_runner
%%% This test suite verifies correct behaviour of authorization mechanism
%%% with corresponding lfm (logical_file_manager) functions.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    mkdir/1,
    get_children/1,
    get_children_attrs/1,
    get_child_attr/1,
    mv_dir/1,
    rm_dir/1,

    create_file/1,
    open_for_read/1,
    open_for_write/1,
    open_for_rdwr/1,
    create_and_open/1,
    truncate/1,
    mv_file/1,
    rm_file/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,
    get_file_attr_test/1,

%%    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
%%    remove_share_test/1,
%%    share_perms_test/1,

    get_acl_test/1,
    set_acl_test/1,
    remove_acl_test/1,

    get_transfer_encoding/1,
    set_transfer_encoding/1,
    get_cdmi_completion_status/1,
    set_cdmi_completion_status/1,
    get_mimetype/1,
    set_mimetype/1,

    get_custom_metadata/1,
    set_custom_metadata/1,
    remove_custom_metadata/1,
    get_xattr/1,
    list_xattr/1,
    set_xattr/1,
    remove_xattr/1,
    get_file_distribution/1,
    get_historical_dir_size_stats/1,
    get_file_storage_locations/1,

    add_qos_entry/1,
    get_qos_entry/1,
    remove_qos_entry/1,
    get_effective_file_qos/1,
    check_qos_status/1
]).

all() -> [
    mkdir,
    get_children,
    get_children_attrs,
    get_child_attr,
    mv_dir,
    rm_dir,

    create_file,
    open_for_read,
    open_for_write,
    open_for_rdwr,
    create_and_open,
    truncate,
    mv_file,
    rm_file,

    get_parent_test,
    get_file_path_test,
    get_file_guid_test,
    get_file_attr_test,

%%    set_perms_test,
    check_read_perms_test,
    check_write_perms_test,
    check_rdwr_perms_test,

    create_share_test,
%%    remove_share_test,
%%    share_perms_test,

    get_acl_test,
    set_acl_test,
    remove_acl_test,

    get_transfer_encoding,
    set_transfer_encoding,
    get_cdmi_completion_status,
    set_cdmi_completion_status,
    get_mimetype,
    set_mimetype,

    get_custom_metadata,
    set_custom_metadata,
    remove_custom_metadata,
    get_xattr,
    list_xattr,
    set_xattr,
    remove_xattr,
    get_file_distribution,
    get_historical_dir_size_stats,
    get_file_storage_locations,

    add_qos_entry,
    get_qos_entry,
    remove_qos_entry,
    get_effective_file_qos,
    check_qos_status
].


-define(RUN_AUTHZ_DIR_API_TEST(__CONFIG),
    authz_dir_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_REG_FILE_API_TEST(__CONFIG),
    authz_reg_file_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_CDMI_API_TEST(__CONFIG),
    authz_cdmi_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_FILE_METADATA_API_TEST(__CONFIG),
    authz_file_metadata_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).
-define(RUN_AUTHZ_QOS_API_TEST(__CONFIG),
    authz_qos_api_tests:?FUNCTION_NAME(?config(space_id, Config))
).

-define(ATTEMPTS, 10).


%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_children(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_children_attrs(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


get_child_attr(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


mv_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


rm_dir(Config) ->
    ?RUN_AUTHZ_DIR_API_TEST(Config).


create_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_read(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_write(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


open_for_rdwr(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


create_and_open(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


truncate(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


mv_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


rm_file(Config) ->
    ?RUN_AUTHZ_REG_FILE_API_TEST(Config).


get_parent_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:get_parent(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_path_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = false, % TODO VFS-6057
        available_in_open_handle_mode = false, % TODO VFS-6057
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            ?FILE_REF(FileGuid) = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:get_file_path(Node, SessionId, FileGuid))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_guid_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            authz_api_test_utils:extract_ok(lfm_proxy:resolve_guid(Node, SessionId, FilePath))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_file_attr_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:stat(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


%%%% TODO
%%set_perms_test(Config) ->
%%    [_, _, Node] = ?config(op_worker_nodes, Config),
%%    FileOwner = <<"user1">>,
%%
%%    FileOwnerUserSessionId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
%%    GroupUserSessionId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
%%    OtherUserSessionId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),
%%    SpaceOwnerSessionId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),
%%
%%    DirPath = <<"/space1/dir1">>,
%%    {ok, DirGuid} = ?assertMatch(
%%        {ok, _},
%%        lfm_proxy:mkdir(Node, FileOwnerUserSessionId, DirPath)
%%    ),
%%
%%    FilePath = <<"/space1/dir1/file1">>,
%%    {ok, FileGuid} = ?assertMatch(
%%        {ok, _},
%%        lfm_proxy:create(Node, FileOwnerUserSessionId, FilePath, 8#777)
%%    ),
%%    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), <<"share">>)),
%%    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
%%
%%    % Open file to ensure it's creation on storage
%%    {ok, Handle} = lfm_proxy:open(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), write),
%%    ok = lfm_proxy:close(Node, Handle),
%%
%%    AssertProperStorageAttrsFun = fun(ExpMode) ->
%%        permissions_test_utils:assert_user_is_file_owner_on_storage(
%%            Node, ?SPACE_ID, FilePath, FileOwnerUserSessionId, #{mode => ?FILE_MODE(ExpMode)}
%%        )
%%    end,
%%
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    %% POSIX
%%
%%    % file owner can always change file perms if he has access to it
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#677, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#100, FileGuid => 8#000}),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),
%%    AssertProperStorageAttrsFun(8#000),
%%
%%    % but not if that access is via shared guid
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EPERM},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    % other users from space can't change perms no matter what
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    % with exception being space owner who can always change perms no matter what
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#000, FileGuid => 8#000}),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), 8#555)),
%%    AssertProperStorageAttrsFun(8#555),
%%
%%    % but even space owner cannot perform write operation on space dir
%%    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(<<"space1">>),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#000)),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#555)),
%%    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(Node, SpaceOwnerSessionId, ?FILE_REF(SpaceGuid), 8#777)),
%%
%%    % users outside of space shouldn't even see the file
%%    permissions_test_utils:set_modes(Node, #{DirGuid => 8#777, FileGuid => 8#777}),
%%    ?assertMatch(
%%        {error, ?ENOENT},
%%        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%    AssertProperStorageAttrsFun(8#777),
%%
%%    %% ACL
%%
%%    % file owner can always change file perms if he has access to it
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => [?traverse_container],
%%        FileGuid => []
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), 8#000)),
%%
%%    % but not if that access is via shared guid
%%    ?assertMatch(
%%        {error, ?EPERM},
%%        lfm_proxy:set_perms(Node, FileOwnerUserSessionId, ?FILE_REF(ShareFileGuid), 8#000)
%%    ),
%%
%%    % file owner cannot change acl after his access was denied by said acl
%%    permissions_test_utils:set_acls(Node, #{}, #{
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, ?everyone, ?no_flags_mask),
%%
%%    PermsBitmask = permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS),
%%
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_acl(Node, FileOwnerUserSessionId, ?FILE_REF(FileGuid), [
%%            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
%%        ])
%%    ),
%%
%%    % but space owner always can change acl for any file
%%    ?assertMatch(
%%        ok,
%%        lfm_proxy:set_acl(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), [
%%            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
%%        ])
%%    ),
%%
%%    % other users from space can't change perms no matter what
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS,
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:set_perms(Node, GroupUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ),
%%
%%    % users outside of space shouldn't even see the file
%%    permissions_test_utils:set_acls(Node, #{
%%        DirGuid => ?ALL_DIR_PERMS,
%%        FileGuid => ?ALL_FILE_PERMS
%%    }, #{}, ?everyone, ?no_flags_mask),
%%    ?assertMatch(
%%        {error, ?ENOENT},
%%        lfm_proxy:set_perms(Node, OtherUserSessionId, ?FILE_REF(FileGuid), 8#000)
%%    ).


check_read_perms_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        available_in_open_handle_mode = true,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_write_perms_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


check_rdwr_perms_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:check_perms(Node, SessionId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


create_share_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_dir_spec{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        blocked_by_data_access_caveats = {true, ?ERROR_POSIX(?EAGAIN)},
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            authz_api_test_utils:extract_ok(opt_shares:create(Node, SessionId, DirKey, <<"create_share">>))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


%%%% TODO
%%remove_share_test(Config) ->
%%    [_, _, W] = ?config(op_worker_nodes, Config),
%%
%%    SpaceOwnerSessionId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),
%%    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [?ROOT_SESS_ID, ?SPACE_ID]),
%%    ScenariosRootDirPath = filename:join(["/", SpaceName, ?SCENARIO_NAME]),
%%    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SpaceOwnerSessionId, ScenariosRootDirPath, 8#700)),
%%
%%    FilePath = filename:join([ScenariosRootDirPath, ?RAND_STR()]),
%%    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SpaceOwnerSessionId, FilePath, 8#700)),
%%    {ok, ShareId} = opt_shares:create(Node, SpaceOwnerSessionId, ?FILE_REF(FileGuid), <<"share">>),
%%
%%    RemoveShareFun = fun(SessionId) ->
%%        % Remove share via gs call to test token data caveats
%%        % (middleware should reject any api call with data caveats)
%%        rpc:call(Node, middleware, handle, [#op_req{
%%            auth = permissions_test_utils:get_auth(Node, SessionId),
%%            gri = #gri{type = op_share, id = ShareId, aspect = instance},
%%            operation = delete
%%        }])
%%    end,
%%
%%    UserId = <<"user2">>,
%%    UserSessionId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
%%
%%    % Assert share removal requires only ?SPACE_MANAGE_SHARES space priv
%%    % and no file permissions
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, []),
%%    ?assertEqual(?ERROR_POSIX(?EPERM), RemoveShareFun(UserSessionId)),
%%
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, privileges:space_admin()),
%%    MainToken = initializer:create_access_token(UserId),
%%
%%    % Assert api operations are unauthorized in case of data caveats
%%    Token1 = tokens:confine(MainToken, #cv_data_readonly{}),
%%    CaveatSessionId1 = permissions_test_utils:create_session(Node, UserId, Token1),
%%    ?assertEqual(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_readonly})),
%%        RemoveShareFun(CaveatSessionId1)
%%    ),
%%    Token2 = tokens:confine(MainToken, #cv_data_path{whitelist = [ScenariosRootDirPath]}),
%%    CaveatSessionId2 = permissions_test_utils:create_session(Node, UserId, Token2),
%%    ?assertMatch(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_path, [ScenariosRootDirPath]})),
%%        RemoveShareFun(CaveatSessionId2)
%%    ),
%%    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
%%    Token3 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [ObjectId]}),
%%    CaveatSessionId3 = permissions_test_utils:create_session(Node, UserId, Token3),
%%    ?assertMatch(
%%        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED({cv_data_objectid, [ObjectId]})),
%%        RemoveShareFun(CaveatSessionId3)
%%    ),
%%
%%    initializer:testmaster_mock_space_user_privileges([W], ?SPACE_ID, UserId, [?SPACE_MANAGE_SHARES]),
%%    ?assertEqual(ok, RemoveShareFun(UserSessionId)).


%% TODO
%%share_perms_test(Config) ->
%%    [_, _, W] = ?config(op_worker_nodes, Config),
%%    FileOwner = <<"user1">>,
%%
%%    FileOwnerUserSessionId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
%%    GroupUserSessionId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
%%
%%    ScenarioDirName = ?SCENARIO_NAME,
%%    ScenarioDirPath = <<"/space1/", ScenarioDirName/binary>>,
%%    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, ScenarioDirPath, 8#700)),
%%
%%    MiddleDirPath = <<ScenarioDirPath/binary, "/dir2">>,
%%    {ok, MiddleDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, MiddleDirPath, 8#777)),
%%
%%    BottomDirPath = <<MiddleDirPath/binary, "/dir3">>,
%%    {ok, BottomDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, FileOwnerUserSessionId, BottomDirPath), 8#777),
%%
%%    FilePath = <<BottomDirPath/binary, "/file1">>,
%%    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, FileOwnerUserSessionId, FilePath, 8#777)),
%%
%%    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(Node, FileOwnerUserSessionId, ?FILE_REF(MiddleDirGuid), <<"share">>)),
%%    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
%%
%%    % Accessing file in normal mode by space user should result in eacces (dir1 perms -> 8#700)
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(FileGuid))
%%    ),
%%    % But accessing it in share mode should succeed as perms should be checked only up to
%%    % share root (dir1/dir2 -> 8#777) and not space root
%%    ?assertMatch(
%%        {ok, #file_attr{guid = ShareFileGuid}},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(ShareFileGuid))
%%    ),
%%
%%    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
%%    ?assertEqual(ok, lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, ?FILE_REF(BottomDirGuid), 8#770)),
%%    ?assertMatch(
%%        {error, ?EACCES},
%%        lfm_proxy:stat(Node, GroupUserSessionId, ?FILE_REF(ShareFileGuid))
%%    ).


get_acl_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:get_acl(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


set_acl_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:set_acl(Node, SessionId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ]))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


remove_acl_test(Config) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = ?config(space_id, Config),
        files = [#ct_authz_file_spec{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            authz_api_test_utils:extract_ok(lfm_proxy:remove_acl(Node, SessionId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }).


get_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_transfer_encoding(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_cdmi_completion_status(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


set_mimetype(Config) ->
    ?RUN_AUTHZ_CDMI_API_TEST(Config).


get_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


set_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


remove_custom_metadata(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


list_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


set_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


remove_xattr(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_file_distribution(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_historical_dir_size_stats(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


get_file_storage_locations(Config) ->
    ?RUN_AUTHZ_FILE_METADATA_API_TEST(Config).


add_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


get_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


remove_qos_entry(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


get_effective_file_qos(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


check_qos_status(Config) ->
    ?RUN_AUTHZ_QOS_API_TEST(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
%%    StorageType = ?RAND_ELEMENT([posix, s3]),
    StorageType = posix,  % TODO

    ModulesToLoad = [?MODULE, authz_api_test_runner],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = get_onenv_scenario(StorageType),
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            delete_spaces_from_previous_run(),
            [{storage_type, StorageType}, {storage_id, find_storage_id(StorageType)} | NewConfig]
        end
    }).


%% @private
-spec get_onenv_scenario(posix | s3) -> list().
get_onenv_scenario(posix) -> "1op_posix";
get_onenv_scenario(s3) -> "1op_s3".


%% @private
-spec delete_spaces_from_previous_run() -> ok.
delete_spaces_from_previous_run() ->
    AllTestCases = all(),

    RemovedSpaces = lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),

        Exists = lists:member(binary_to_atom(SpaceName), AllTestCases),
        Exists andalso ozw_test_rpc:delete_space(SpaceId),

        Exists
    end, ozw_test_rpc:list_spaces()),

    ?assertEqual([], lists_utils:intersect(opw_test_rpc:get_spaces(krakow), RemovedSpaces), ?ATTEMPTS),

    ok.


%% @private
-spec find_storage_id(posix | s3) -> storage:id().
find_storage_id(StorageType) ->
    StorageTypeBin = atom_to_binary(StorageType),

    [StorageId] = lists:filter(fun(StorageId) ->
        StorageDetails = opw_test_rpc:storage_describe(krakow, StorageId),
        StorageTypeBin == maps:get(<<"type">>, StorageDetails)
    end, opw_test_rpc:get_storages(krakow)),

    StorageId.


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = space_owner,
        users = [user1, user2],
        supports = [#support_spec{
            provider = krakow,
            size = 10000000,
            storage_spec = ?config(storage_id, Config)
        }]
    }),
    [{space_id, SpaceId} | lfm_proxy:init(Config)].


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
