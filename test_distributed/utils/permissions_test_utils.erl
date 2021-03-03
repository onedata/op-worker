%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in lfm_permissions framework and tests.
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_utils).
-author("Bartosz Walkowicz").

-include("storage_files_test_SUITE.hrl").
-include("permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    assert_user_is_file_owner_on_storage/4,
    assert_user_is_file_owner_on_storage/5,

    ensure_file_created_on_storage/2,
    ensure_dir_created_on_storage/2,

    create_session/3, create_session/4,

    all_perms/2, complementary_perms/3,
    perms_to_bitmask/1, perm_to_bitmask/1,
    perm_to_posix_perms/1, posix_perm_to_mode/2,

    set_modes/2,
    set_acls/5
]).


% See p1/p2_local_feed_luma.json
-define(EXP_POSIX_STORAGE_CONFIG, #{
    p1 => #{
        <<"owner">> => #{uid => 3001, gid => 3000},
        <<"user1">> => #{uid => 3002, gid => 3000},
        <<"user2">> => #{uid => 3003, gid => 3000},
        <<"user3">> => #{uid => 3004, gid => 3000}
    },
    p2 => #{
        <<"owner">> => #{uid => 6001, gid => 6000},
        <<"user1">> => #{uid => 6002, gid => 6000},
        <<"user2">> => #{uid => 6003, gid => 6000},
        <<"user3">> => #{uid => 6004, gid => 6000}
    }
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_user_is_file_owner_on_storage(node(), od_space:id(), file_meta:path(), session:id()) ->
    ok | no_return().
assert_user_is_file_owner_on_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessId) ->
    assert_user_is_file_owner_on_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessId, #{}).


-spec assert_user_is_file_owner_on_storage(
    node(),
    od_space:id(),
    file_meta:path(),
    session:id(),
    AdditionalAttrs :: map()
) ->
    ok | no_return().
assert_user_is_file_owner_on_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessId, AdditionalAttrs) ->
    ?EXEC_IF_SUPPORTED_BY_POSIX(Node, SpaceId, fun() ->
        ?ASSERT_FILE_INFO(
            get_exp_owner_posix_attrs(Node, ExpOwnerSessId, AdditionalAttrs),
            Node,
            storage_file_path(Node, SpaceId, LogicalFilePath)
        )
    end).


-spec ensure_file_created_on_storage(node(), file_id:file_guid()) -> ok.
ensure_file_created_on_storage(Node, FileGuid) ->
    % Open and close file in dir to ensure it is created on storage.
    {ok, Handle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, {guid, FileGuid}, write),
    ok = lfm_proxy:close(Node, Handle).


-spec ensure_dir_created_on_storage(node(), file_id:file_guid()) -> ok.
ensure_dir_created_on_storage(Node, DirGuid) ->
    % Create and open file in dir to ensure it is created on storage.
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, ?ROOT_SESS_ID, DirGuid, <<"__tmp_file">>, 8#777
    )),
    {ok, Handle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, {guid, FileGuid}, write),
    ok = lfm_proxy:close(Node, Handle),

    % Remove file to ensure it will not disturb tests
    ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, {guid, FileGuid}).


-spec create_session(node(), od_user:id(), tokens:serialized()) ->
    session:id().
create_session(Node, UserId, AccessToken) ->
    create_session(Node, UserId, AccessToken, normal).


-spec create_session(node(), od_user:id(), tokens:serialized(), session:mode()) ->
    session:id().
create_session(Node, UserId, AccessToken, SessionMode) ->
    Nonce = crypto:strong_rand_bytes(10),
    Identity = ?SUB(user, UserId),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ),
    {ok, SessionId} = ?assertMatch({ok, _}, rpc:call(
        Node,
        session_manager,
        reuse_or_create_fuse_session,
        [Nonce, Identity, SessionMode, TokenCredentials]
    )),
    SessionId.


-spec all_perms(node(), file_id:file_guid()) -> Perms :: [binary()].
all_perms(Node, Guid) ->
    case is_dir(Node, Guid) of
        true -> ?ALL_DIR_PERMS;
        false -> ?ALL_FILE_PERMS
    end.


-spec complementary_perms(node(), file_id:file_guid(), Perms :: [binary()]) ->
    ComplementaryPerms :: [binary()].
complementary_perms(Node, Guid, Perms) ->
    ComplementaryPerms = all_perms(Node, Guid) -- Perms,
    % Special case: because ?delete_object and ?delete_subcontainer translates
    % to the same bitmask if even one of them is present other must also be removed
    case lists:member(?delete_object, Perms) or lists:member(?delete_subcontainer, Perms) of
        true -> ComplementaryPerms -- [?delete_object, ?delete_subcontainer];
        false -> ComplementaryPerms
    end.


-spec perms_to_bitmask([binary()]) -> ace:bitmask().
perms_to_bitmask(Permissions) ->
    lists:foldl(fun(Perm, BitMask) ->
        BitMask bor perm_to_bitmask(Perm)
    end, ?no_flags_mask, Permissions).


-spec perm_to_bitmask(binary()) -> ace:bitmask().
perm_to_bitmask(?read_object) -> ?read_object_mask;
perm_to_bitmask(?list_container) -> ?list_container_mask;
perm_to_bitmask(?write_object) -> ?write_object_mask;
perm_to_bitmask(?add_object) -> ?add_object_mask;
perm_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
perm_to_bitmask(?read_metadata) -> ?read_metadata_mask;
perm_to_bitmask(?write_metadata) -> ?write_metadata_mask;
perm_to_bitmask(?traverse_container) -> ?traverse_container_mask;
perm_to_bitmask(?delete_object) -> ?delete_child_mask;
perm_to_bitmask(?delete_subcontainer) -> ?delete_child_mask;
perm_to_bitmask(?read_attributes) -> ?read_attributes_mask;
perm_to_bitmask(?write_attributes) -> ?write_attributes_mask;
perm_to_bitmask(?delete) -> ?delete_mask;
perm_to_bitmask(?read_acl) -> ?read_acl_mask;
perm_to_bitmask(?write_acl) -> ?write_acl_mask.


-spec perm_to_posix_perms(Perm :: binary()) -> PosixPerms :: [atom()].
perm_to_posix_perms(?read_object) -> [read];
perm_to_posix_perms(?list_container) -> [read];
perm_to_posix_perms(?write_object) -> [write];
perm_to_posix_perms(?add_object) -> [write, exec];
perm_to_posix_perms(?add_subcontainer) -> [write];
perm_to_posix_perms(?read_metadata) -> [read];
perm_to_posix_perms(?write_metadata) -> [write];
perm_to_posix_perms(?traverse_container) -> [exec];
perm_to_posix_perms(?delete_object) -> [write, exec];
perm_to_posix_perms(?delete_subcontainer) -> [write, exec];
perm_to_posix_perms(?read_attributes) -> [];
perm_to_posix_perms(?write_attributes) -> [write];
perm_to_posix_perms(?delete) -> [owner_if_parent_sticky];
perm_to_posix_perms(?read_acl) -> [];
perm_to_posix_perms(?write_acl) -> [owner].


-spec posix_perm_to_mode(PosixPerm :: atom(), Type :: owner | group) ->
    non_neg_integer().
posix_perm_to_mode(read, owner)  -> 8#4 bsl 6;
posix_perm_to_mode(write, owner) -> 8#2 bsl 6;
posix_perm_to_mode(exec, owner)  -> 8#1 bsl 6;
posix_perm_to_mode(read, group)  -> 8#4 bsl 3;
posix_perm_to_mode(write, group) -> 8#2 bsl 3;
posix_perm_to_mode(exec, group)  -> 8#1 bsl 3;
posix_perm_to_mode(read, other)  -> 8#4;
posix_perm_to_mode(write, other) -> 8#2;
posix_perm_to_mode(exec, other)  -> 8#1;
posix_perm_to_mode(_, _)         -> 8#0.


-spec set_modes(node(), #{file_id:file_guid() => file_meta:mode()}) ->
    ok.
set_modes(Node, ModePerFile) ->
    maps:fold(fun(Guid, Mode, _) ->
        ?assertEqual(ok, lfm_proxy:set_perms(
            Node, ?ROOT_SESS_ID, {guid, Guid}, Mode
        ))
    end, ok, ModePerFile).


-spec set_acls(
    Node :: node(),
    AllowedPermsPerFile :: #{file_id:file_guid() => [binary()]},
    DeniedPermsPerFile :: #{file_id:file_guid() => [binary()]},
    AceWho :: ace:bitmask(),
    AceFlags :: ace:bitmask()
) ->
    ok.
set_acls(Node, AllowedPermsPerFile, DeniedPermsPerFile, AceWho, AceFlags) ->
    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(Perms))])
        )
    end, ok, maps:without(maps:keys(DeniedPermsPerFile), AllowedPermsPerFile)),

    maps:fold(fun(Guid, Perms, _) ->
        AllPerms = all_perms(Node, Guid),

        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [
                ?DENY_ACE(AceWho, AceFlags, perms_to_bitmask(Perms)),
                ?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(AllPerms))
            ]
        ))
    end, ok, DeniedPermsPerFile).


%%%===================================================================
%%% API
%%%===================================================================


%% @private
-spec is_dir(node(), file_id:file_guid()) -> boolean().
is_dir(Node, Guid) ->
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, #document{value = #file_meta{type = FileType}}} = ?assertMatch(
        {ok, _},
        rpc:call(Node, file_meta, get, [Uuid])
    ),
    FileType == ?DIRECTORY_TYPE.


%% @private
-spec get_exp_owner_posix_attrs(node(), session:id(), map()) ->
    #{uid => integer(), gid => integer()}.
get_exp_owner_posix_attrs(Worker, SessionId, AdditionalAttrs) ->
    {ok, UserId} = rpc:call(Worker, session, get_user_id, [SessionId]),
    ProviderName = list_to_atom(
        lists:nth(2, string:tokens(atom_to_list(?GET_HOSTNAME(Worker)), "."))
    ),
    maps:merge(
        AdditionalAttrs,
        maps:get(UserId, maps:get(ProviderName, ?EXP_POSIX_STORAGE_CONFIG))
    ).


%% @private
-spec storage_file_path(node(), od_space:id(), file_meta:path()) -> binary().
storage_file_path(W, SpaceId, LogicalPath) ->
    [<<"/">>, <<"/", LogicalPathWithoutSpaceId/binary>>] = binary:split(
        LogicalPath, SpaceId, [global]
    ),
    storage_test_utils:file_path(W, SpaceId, LogicalPathWithoutSpaceId).
