%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of sharing files - creating and removing shares, and reaching the
%%% shared file tree through a share guid. A share guid opens the subtree rooted
%%% at the shared file to anyone, including the guest (unauthenticated) session,
%%% while hiding everything that identifies its owner: the share root reports no
%%% parent, and every file in the subtree reports only its 'other' permission
%%% bits and an anonymous uid/gid.
%%%
%%% Which space privileges guard creating and removing a share is not tested
%%% here - that is covered against the full posix/acl/privilege matrix by
%%% authz_share_api_tests. The tests below are given the privilege upfront (see
%%% ensure_share_management_privileges/0) and check what sharing does.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_shares_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    create_share_test/0,
    remove_share_test/0,

    share_root_getattr_test/0,
    share_child_getattr_test/0,
    share_get_parent_test/0,
    share_list_test/0,
    share_read_test/0,

    guest_cannot_access_unshared_file_test/0
]).

%% suite setup helpers
-export([grant_share_management_privileges/0]).

-define(SHARE_NAME, <<"share_name">>).

% Grants everyone read and traverse access, so that a share can be followed all
% the way down to a file by a session other than the one that created it.
-define(PUBLICLY_ACCESSIBLE_PERMS, 8#707).


%%%===================================================================
%%% Tests of managing shares
%%%===================================================================


create_share_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    CreateDir = fun() ->
        lfm_proxy:mkdir(Node, SessId, RootDirGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS)
    end,
    CreateFile = fun() ->
        lfm_proxy:create(Node, SessId, RootDirGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS)
    end,

    lists:foreach(fun(CreateSharedFile) ->
        {ok, Guid} = ?assertMatch({ok, _}, CreateSharedFile()),

        {ok, ShareId1} = ?assertMatch({ok, _}, opt_shares:create(
            Node, SessId, ?FILE_REF(Guid), ?SHARE_NAME)),

        % the same file may be shared any number of times, each share standing
        % on its own
        {ok, ShareId2} = ?assertMatch({ok, _}, opt_shares:create(
            Node, SessId, ?FILE_REF(Guid), ?SHARE_NAME)),
        ?assertNotEqual(ShareId1, ShareId2),

        ?assertMatch(
            {ok, #file_attr{shares = [ShareId2, ShareId1]}},
            lfm_proxy:stat(Node, SessId, ?FILE_REF(Guid))
        )
    end, [CreateDir, CreateFile]).


remove_share_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),
    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),

    ShareGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, ?GUEST_SESS_ID, ?FILE_REF(ShareGuid))),

    ?assertEqual(ok, opt_shares:remove(Node, SessId, ShareId)),

    % the share is gone from both the file it was rooted at and the zone, so it
    % neither grants access any longer nor can be removed again
    ?assertMatch(
        {ok, #file_attr{shares = []}},
        lfm_proxy:stat(Node, SessId, ?FILE_REF(DirGuid))
    ),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, ?GUEST_SESS_ID, ?FILE_REF(ShareGuid))),
    ?assertEqual(?ERROR_NOT_FOUND, opt_shares:remove(Node, SessId, ShareId)).


%%%===================================================================
%%% Tests of accessing a file tree through a share
%%%===================================================================


share_root_getattr_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    DirName = generator:gen_name(),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, RootDirGuid, DirName, 8#704)),

    {ok, ShareId1} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    {ok, ShareId2} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    ShareGuid = file_id:guid_to_share_guid(DirGuid, ShareId1),

    UserId = oct_background:get_user_id(?USER_SELECTOR),
    ProviderId = oct_background:get_provider_id(?PROVIDER_SELECTOR),

    {ok, #file_attr{uid = Uid, gid = Gid}} = ?assertMatch({ok, #file_attr{
        mode = 8#704,
        name = DirName,
        type = ?DIRECTORY_TYPE,
        guid = DirGuid,
        parent_guid = RootDirGuid,
        owner_id = UserId,
        provider_id = ProviderId,
        shares = [ShareId2, ShareId1]
    }}, lfm_proxy:stat(Node, SessId, ?FILE_REF(DirGuid))),
    ?assertNotEqual({?SHARE_UID, ?SHARE_GID}, {Uid, Gid}),

    % accessed through the share, the directory tells nothing about who owns it
    % or where it sits, and this holds for its owner just as for the guest
    lists:foreach(fun(SessionId) ->
        ?assertMatch({ok, #file_attr{
            mode = 8#004,
            name = DirName,
            type = ?DIRECTORY_TYPE,
            guid = ShareGuid,
            uid = ?SHARE_UID,
            gid = ?SHARE_GID,
            parent_guid = undefined,
            owner_id = undefined,
            provider_id = undefined,
            shares = [ShareId1]
        }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ShareGuid)))
    end, [SessId, ?GUEST_SESS_ID]).


share_child_getattr_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),
    FileName = generator:gen_name(),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, DirGuid, FileName, 8#700)),

    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    {ok, [{ShareChildGuid, _}]} = ?assertMatch({ok, [_]}, lfm_proxy:get_children(
        Node, ?GUEST_SESS_ID, ?FILE_REF(ShareDirGuid), 0, 1)),

    % unlike the share root, a file below it does point at its parent - up to the
    % share root - but is stripped of everything else just the same
    ?assertMatch({ok, #file_attr{
        mode = 8#000,
        name = FileName,
        type = ?REGULAR_FILE_TYPE,
        guid = ShareChildGuid,
        parent_guid = ShareDirGuid,
        shares = []
    }}, lfm_proxy:stat(Node, ?GUEST_SESS_ID, ?FILE_REF(ShareChildGuid))).


share_get_parent_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, DirGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS)),

    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    ?assertEqual({ok, RootDirGuid}, lfm_proxy:get_parent(Node, SessId, ?FILE_REF(DirGuid))),
    ?assertEqual({ok, DirGuid}, lfm_proxy:get_parent(Node, SessId, ?FILE_REF(FileGuid))),

    % the share root is where the tree ends when walked up through the share
    ?assertEqual({ok, undefined}, lfm_proxy:get_parent(Node, SessId, ?FILE_REF(ShareDirGuid))),
    ?assertEqual({ok, ShareDirGuid}, lfm_proxy:get_parent(Node, SessId, ?FILE_REF(ShareFileGuid))).


share_list_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),
    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    MakeDir = fun(ParentGuid, Name) ->
        {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
            Node, SessId, ParentGuid, Name, ?PUBLICLY_ACCESSIBLE_PERMS)),
        Guid
    end,
    MakeFile = fun(ParentGuid, Name) ->
        {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, ParentGuid, Name, ?DEFAULT_FILE_PERMS)),
        Guid
    end,
    ToShareEntries = fun(GuidsWithNames) ->
        [{file_id:guid_to_share_guid(Guid, ShareId), Name} || {Guid, Name} <- GuidsWithNames]
    end,

    % listing the share root yields its children as share guids, so that the
    % tree can be walked down without ever leaving the share
    SubDirGuid = MakeDir(DirGuid, <<"1">>),
    Children = [
        {SubDirGuid, <<"1">>},
        {MakeDir(DirGuid, <<"2">>), <<"2">>},
        {MakeFile(DirGuid, <<"3">>), <<"3">>}
    ],
    ?assertEqual({ok, ToShareEntries(Children)}, lfm_proxy:get_children(
        Node, ?GUEST_SESS_ID, ?FILE_REF(ShareDirGuid), 0, 10)),

    GrandChildren = [
        {MakeDir(SubDirGuid, <<"1">>), <<"1">>},
        {MakeFile(SubDirGuid, <<"2">>), <<"2">>}
    ],
    ?assertEqual({ok, ToShareEntries(GrandChildren)}, lfm_proxy:get_children(
        Node, ?GUEST_SESS_ID, ?FILE_REF(file_id:guid_to_share_guid(SubDirGuid, ShareId)), 0, 10)).


share_read_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),

    % whether a file was there when the share was created or was added afterwards
    % makes no difference to reading it through the share
    WriteFile = fun(Name) ->
        {ok, {_Guid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
            Node, SessId, DirGuid, Name, ?PUBLICLY_ACCESSIBLE_PERMS)),
        ?assertEqual({ok, byte_size(Name)}, lfm_proxy:write(Node, Handle, 0, Name)),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle))
    end,

    WriteFile(<<"shared_before">>),
    {ok, ShareId} = ?assertMatch({ok, _}, opt_shares:create(
        Node, SessId, ?FILE_REF(DirGuid), ?SHARE_NAME)),
    WriteFile(<<"shared_after">>),

    {ok, ShareChildren} = ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(
        Node, ?GUEST_SESS_ID, ?FILE_REF(file_id:guid_to_share_guid(DirGuid, ShareId)), 0, 10)),

    lists:foreach(fun({ShareChildGuid, Name}) ->
        {ok, Handle} = ?assertMatch({ok, <<_/binary>>}, lfm_proxy:open(
            Node, ?GUEST_SESS_ID, ?FILE_REF(ShareChildGuid), read)),
        ?assertEqual({ok, Name}, lfm_proxy:read(Node, Handle, 0, byte_size(Name))),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle))
    end, ShareChildren).


guest_cannot_access_unshared_file_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    % the permission bits alone grant the guest nothing - it may reach a file
    % only through a share
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?PUBLICLY_ACCESSIBLE_PERMS)),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, ?GUEST_SESS_ID, ?FILE_REF(DirGuid))).


%%%===================================================================
%%% Suite setup helpers
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Managing shares takes a space privilege that a plain space member does not
%% have by default. To be called from init_per_group, as it is what this group
%% of tests needs of the environment rather than something the tests set and
%% unset. It states the privileges outright instead of adding to whatever is
%% there, so a run interrupted midway leaves nothing to repair.
%% @end
%%--------------------------------------------------------------------
-spec grant_share_management_privileges() -> ok.
grant_share_management_privileges() ->
    ozt_spaces:set_privileges(?SPACE_SELECTOR, ?USER_SELECTOR, [
        ?SPACE_MANAGE_SHARES | privileges:space_member()
    ]).
