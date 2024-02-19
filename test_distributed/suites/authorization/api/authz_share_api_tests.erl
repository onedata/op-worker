%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests concerning authorization of share operations.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_share_api_tests).
-author("Bartosz Walkowicz").

-include("authz_api_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include("storage_files_test_SUITE.hrl").

-export([
    create_share/1,
    remove_share/1,
    share_perms_are_checked_only_up_to_share_root/1
]).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_share(SpaceId) ->
    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
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


remove_share(SpaceId) ->
    SpaceOwnerSessionId = oct_background:get_user_session_id(space_owner, krakow),

    authz_api_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        space_id = SpaceId,
        files = [#ct_authz_dir_spec{
            name = <<"dir1">>,
            on_create = fun(Node, _FileOwnerSessionId, Guid) ->
                {ok, ShareId} = opt_shares:create(Node, SpaceOwnerSessionId, ?FILE_REF(Guid), <<"share">>),
                ShareId
            end
        }],
        requires_traverse_ancestors = false,
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        blocked_by_data_access_caveats = {true, ?ERROR_POSIX(?EACCES)},
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ShareId = maps:get(DirPath, ExtraData),
            authz_api_test_utils:extract_ok(opt_shares:remove(Node, SessionId, ShareId))
        end,
        returned_errors = api_errors,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }).


%% TODO random posix/acl ??
share_perms_are_checked_only_up_to_share_root(SpaceId) ->
    SpaceDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    FilesOwnerId = oct_background:get_user_id(user1),

    #object{children = [#object{
        shares = [ShareId],
        children = [#object{
            guid = BottomDirGuid,
            children = [#object{guid = FileGuid}]
        }]
    }]} = onenv_file_test_utils:create_file_tree(FilesOwnerId, SpaceDirGuid, krakow, #dir_spec{
        name = <<"root_dir">>,
        mode = ?FILE_MODE(8#700),
        children = [#dir_spec{
            name = <<"middle_dir">>,
            shares = [#share_spec{}],
            children = [#dir_spec{
                name = <<"bottom_dir">>,
                children = [#file_spec{}]
            }]
        }]
    }),

    Node = oct_background:get_random_provider_node(krakow),
    SpaceUserSessionId = oct_background:get_user_session_id(user2, krakow),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Accessing file in normal mode by space user should result in eacces (root_dir perms -> 8#700)
    ?assertMatch({error, ?EACCES}, lfm_proxy:stat(Node, SpaceUserSessionId, ?FILE_REF(FileGuid))),

    % But accessing it in share mode should succeed as perms should be checked only up to
    % share root (root_dir/middle_dir -> 8#777) and not space root
    ?assertMatch(
        {ok, #file_attr{guid = ShareFileGuid}},
        lfm_proxy:stat(Node, SpaceUserSessionId, ?FILE_REF(ShareFileGuid))
    ),

    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
    ?assertEqual(ok, lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, ?FILE_REF(BottomDirGuid), 8#770)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:stat(Node, SpaceUserSessionId, ?FILE_REF(ShareFileGuid))).
