%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in authorization framework and tests.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_test_utils).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include("storage_files_test_SUITE.hrl").

-export([
    all_perms/2,
    complementary_perms/3,

    perms_to_bitmask/1,
    perm_to_bitmask/1,

    perm_to_posix_perms/1,
    posix_perm_to_mode/2,

    set_modes/2,
    set_acls/5
]).


%%%===================================================================
%%% API
%%%===================================================================


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


-spec perms_to_bitmask([binary()]) -> data_access_control:bitmask().
perms_to_bitmask(Permissions) ->
    lists:foldl(fun(Perm, BitMask) ->
        BitMask bor perm_to_bitmask(Perm)
    end, ?no_flags_mask, Permissions).


-spec perm_to_bitmask(binary()) -> data_access_control:bitmask().
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


-spec posix_perm_to_mode(read | write | exec, owner | group | other) ->
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
            Node, ?ROOT_SESS_ID, ?FILE_REF(Guid), Mode
        ))
    end, ok, ModePerFile).


-spec set_acls(
    Node :: node(),
    AllowedPermsPerFile :: #{file_id:file_guid() => [binary()]},
    DeniedPermsPerFile :: #{file_id:file_guid() => [binary()]},
    AceWho :: data_access_control:bitmask(),
    AceFlags :: data_access_control:bitmask()
) ->
    ok.
set_acls(Node, AllowedPermsPerFile, DeniedPermsPerFile, AceWho, AceFlags) ->
    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, ?FILE_REF(Guid),
            [?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(Perms))])
        )
    end, ok, maps:without(maps:keys(DeniedPermsPerFile), AllowedPermsPerFile)),

    maps:fold(fun(Guid, Perms, _) ->
        AllPerms = all_perms(Node, Guid),

        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, ?FILE_REF(Guid),
            [
                ?DENY_ACE(AceWho, AceFlags, perms_to_bitmask(Perms)),
                ?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(AllPerms))
            ]
        ))
    end, ok, DeniedPermsPerFile).


%%%===================================================================
%%% Internal functions
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
