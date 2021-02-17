%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module handles access requirements checks for files.
%%% @end
%%%--------------------------------------------------------------------
-module(data_access_rights).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    assert_granted/3,
    assert_operation_available_in_readonly_mode/1
]).

-type type() ::
    owner
    | share
    | traverse_ancestors            % Means ancestors' exec permission
    | {permissions, ace:bitmask()}.

-type requirement() ::
    root
    | type()
    | {type(), 'or', type()}.

% User permission matrix holds information about user overall granted and denied
% permissions. It is build incrementally (e.g. in case of acl not entire acl is
% checked but only up to fragment where permission is granted or denied) and
% that is why it contains pointer to where it stopped calculating previously.
-type user_perms_matrix() :: {non_neg_integer(), ace:bitmask(), ace:bitmask()}.

-export_type([type/0, requirement/0, user_perms_matrix/0]).

% Permissions respected in readonly mode (operation requiring any other
% permission, even if granted by posix mode or acl, will be denied)
-define(READONLY_MODE_RESPECTED_PERMS, (
    ?read_attributes_mask bor
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask bor
    ?read_acl_mask bor
    ?traverse_container_mask
)).

% Permissions denied by lack of ?SPACE_WRITE_DATA/?SPACE_READ_DATA space privilege
-define(SPACE_WRITE_PERMS, (
    ?write_attributes_mask bor
    ?write_object_mask bor
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?delete_mask bor
    ?delete_child_mask bor
    ?write_metadata_mask bor
    ?write_acl_mask
)).
-define(SPACE_READ_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).

% Permissions granted by 'rwx' posix mode bits
-define(POSIX_ALWAYS_GRANTED_PERMS, (
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(POSIX_READ_ONLY_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).
-define(POSIX_FILE_WRITE_ONLY_PERMS, (
    ?write_object_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_DIR_WRITE_ONLY_PERMS, (
    ?add_subcontainer_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_EXEC_ONLY_PERMS, (
    ?traverse_container_mask
)).
-define(POSIX_FILE_WRITE_EXEC_PERMS, (
    ?POSIX_FILE_WRITE_ONLY_PERMS bor
    ?POSIX_EXEC_ONLY_PERMS
)).
-define(POSIX_DIR_WRITE_EXEC_PERMS, (
    ?POSIX_DIR_WRITE_ONLY_PERMS bor
    ?POSIX_EXEC_ONLY_PERMS bor

    % Special permissions that are granted only when both 'write' and 'exec'
    % mode bits are set
    ?add_object_mask bor
    ?delete_child_mask
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_granted(user_ctx:ctx(), file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_granted(UserCtx, FileCtx0, AccessRequirements0) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx0),
    IsSpaceOwner = user_ctx:is_space_owner(UserCtx, SpaceId),
    IsSpaceDir = file_ctx:is_space_dir_const(FileCtx0),

    case user_ctx:is_root(UserCtx) orelse (IsSpaceOwner andalso not IsSpaceDir) of
        true ->
            FileCtx0;
        false ->
            AccessRequirements1 = case user_ctx:is_guest(UserCtx) of
                true ->
                    [share | AccessRequirements0];
                false ->
                    case file_ctx:is_in_user_space_const(FileCtx0, UserCtx) of
                        true -> AccessRequirements0;
                        false -> throw(?ENOENT)
                    end
            end,
            lists:foldl(fun(AccessRequirement, FileCtx1) ->
                assert_access_requirement(UserCtx, FileCtx1, AccessRequirement)
            end, FileCtx0, AccessRequirements1)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether operation requiring specified access requirements
%% can be executed in readonly mode.
%% @end
%%--------------------------------------------------------------------
-spec assert_operation_available_in_readonly_mode([requirement()]) ->
    ok | no_return().
assert_operation_available_in_readonly_mode(AccessRequirements) ->
    case lists:all(fun is_available_in_readonly_mode/1, AccessRequirements) of
        true -> ok;
        false -> throw(?EACCES)
    end.


%%%===================================================================
%%% Internal Functions
%%%===================================================================


%% @private
-spec is_available_in_readonly_mode(requirement()) -> boolean().
is_available_in_readonly_mode(?PERMISSIONS(Perms)) ->
    case ?reset_flags(Perms, ?READONLY_MODE_RESPECTED_PERMS) of
        0 -> true;
        _ -> false
    end;
is_available_in_readonly_mode(Requirement) when
    Requirement == owner;
    Requirement == share;
    Requirement == traverse_ancestors;
    Requirement == root
->
    true;
is_available_in_readonly_mode({AccessType1, 'or', AccessType2}) ->
    is_available_in_readonly_mode(AccessType1) andalso is_available_in_readonly_mode(AccessType2).


%% @private
-spec assert_access_requirement(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    file_ctx:ctx() | no_return().
assert_access_requirement(UserCtx, FileCtx, ?PERMISSIONS(RequiredPerms)) ->
    check_user_perms_matrix(UserCtx, FileCtx, RequiredPerms);

assert_access_requirement(UserCtx, FileCtx0, Requirement) when
    Requirement == owner;
    Requirement == share;
    Requirement == traverse_ancestors
->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    CacheKey = {Requirement, UserId, Guid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, granted} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                {ok, FileCtx1} = check_access_requirement(UserCtx, FileCtx0, Requirement),
                permissions_cache:cache_permission(CacheKey, granted),
                FileCtx1
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES),
                throw(?EACCES)
            end
    end;

assert_access_requirement(UserCtx, FileCtx, root) ->
    case user_ctx:is_root(UserCtx) of
        true -> FileCtx;
        false -> throw(?EACCES)
    end;

assert_access_requirement(UserCtx, FileCtx, {AccessType1, 'or', AccessType2}) ->
    try
        assert_access_requirement(UserCtx, FileCtx, AccessType1)
    catch _:?EACCES ->
        assert_access_requirement(UserCtx, FileCtx, AccessType2)
    end.


%% @private
-spec check_access_requirement(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    {ok, file_ctx:ctx()} | no_return().
check_access_requirement(UserCtx, FileCtx0, owner) ->
    {OwnerId, FileCtx1} = file_ctx:get_owner(FileCtx0),

    case user_ctx:get_user_id(UserCtx) =:= OwnerId of
        true -> {ok, FileCtx1};
        false -> throw(?EACCES)
    end;

check_access_requirement(UserCtx, FileCtx0, share) ->
    case file_ctx:is_root_dir_const(FileCtx0) of
        true ->
            throw(?ENOENT);
        false ->
            {#document{value = #file_meta{
                shares = Shares
            }}, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),

            ShareId = file_ctx:get_share_id_const(FileCtx1),

            case lists:member(ShareId, Shares) of
                true ->
                    {ok, FileCtx1};
                false ->
                    {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx1, UserCtx),
                    assert_access_requirement(UserCtx, ParentCtx, share),
                    {ok, FileCtx2}
            end
    end;

check_access_requirement(UserCtx, FileCtx0, traverse_ancestors) ->
    case file_ctx:get_and_check_parent(FileCtx0, UserCtx) of
        {undefined, FileCtx1} ->
            {ok, FileCtx1};
        {ParentCtx0, FileCtx1} ->
            ParentCtx1 = check_user_perms_matrix(
                UserCtx, ParentCtx0, ?traverse_container_mask
            ),
            assert_access_requirement(UserCtx, ParentCtx1, traverse_ancestors),
            {ok, FileCtx1}
    end.


%% @private
-spec check_user_perms_matrix(user_ctx:ctx(), file_ctx:ctx(), ace:bitmask()) ->
    file_ctx:ctx() | no_return().
check_user_perms_matrix(UserCtx, FileCtx0, RequiredPerms) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    CacheKey = {user_perms_matrix, UserId, Guid},

    Result = case permissions_cache:check_permission(CacheKey) of
        {ok, {_No, AllowedPerms, DeniedPerms} = UserPermsMatrix} ->
            case ?reset_flags(RequiredPerms, AllowedPerms) of
                0 ->
                    {allowed, FileCtx0};
                LeftoverRequiredPerms ->
                    case LeftoverRequiredPerms band DeniedPerms of
                        0 ->
                            calc_and_check_user_perms_matrix(
                                UserCtx, FileCtx0, LeftoverRequiredPerms, UserPermsMatrix
                            );
                        _ ->
                            throw(?EACCES)
                    end
            end;
        calculate ->
            calc_and_check_user_perms_matrix(UserCtx, FileCtx0, RequiredPerms)
    end,

    case Result of
        {allowed, FileCtx1} ->
            FileCtx1;
        {allowed, FileCtx1, NewUserPermsMatrix} ->
            permissions_cache:cache_permission(CacheKey, NewUserPermsMatrix),
            FileCtx1;
        {denied, FileCtx1, NewUserPermsMatrix} ->
            permissions_cache:cache_permission(CacheKey, NewUserPermsMatrix),
            throw(?EACCES)
    end.


%% @private
-spec calc_and_check_user_perms_matrix(user_ctx:ctx(), file_ctx:ctx(), ace:bitmask()) ->
    {allowed | denied, file_ctx:ctx(), user_perms_matrix()}.
calc_and_check_user_perms_matrix(UserCtx, FileCtx0, RequiredPermissions) ->
    ShareId = file_ctx:get_share_id_const(FileCtx0),
    DeniedPerms = get_perms_denied_by_lack_of_space_privs(UserCtx, FileCtx0, ShareId),

    UserPermsMatrix = {0, 0, DeniedPerms},

    case RequiredPermissions band DeniedPerms of
        0 ->
            calc_and_check_user_perms_matrix(
                UserCtx, FileCtx0, RequiredPermissions, UserPermsMatrix
            );
        _ ->
            {denied, FileCtx0, UserPermsMatrix}
    end.


%% @private
-spec get_perms_denied_by_lack_of_space_privs(
    user_ctx:ctx(), file_ctx:ctx(), undefined | od_share:id()
) ->
    ace:bitmask().
get_perms_denied_by_lack_of_space_privs(UserCtx, FileCtx, undefined) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            % All write permissions are denied for user root dir
            ?SPACE_WRITE_PERMS;
        false ->
            {ok, EffUserSpacePrivs} = space_logic:get_eff_privileges(SpaceId, UserId),

            lists:foldl(fun
                (?SPACE_READ_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_READ_PERMS);
                (?SPACE_WRITE_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_WRITE_PERMS);
                (_, Bitmask) -> Bitmask
            end, ?SPACE_READ_PERMS bor ?SPACE_WRITE_PERMS, EffUserSpacePrivs)
    end;

get_perms_denied_by_lack_of_space_privs(_UserCtx, _FileCtx, _ShareId) ->
    % All write permissions are denied in share mode
    ?SPACE_WRITE_PERMS.


%% @private
-spec calc_and_check_user_perms_matrix(
    user_ctx:ctx(), file_ctx:ctx(), ace:bitmask(), user_perms_matrix()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_matrix()}.
calc_and_check_user_perms_matrix(UserCtx, FileCtx0, RequiredPerms, UserPermsMatrix) ->
    case file_ctx:get_active_perms_type(FileCtx0, include_deleted) of
        {posix, FileCtx1} ->
            calc_and_check_user_perms_matrix_from_posix_mode(
                UserCtx, FileCtx1, RequiredPerms, UserPermsMatrix
            );
        {acl, FileCtx1} ->
            calc_and_check_user_perms_matrix_from_acl(
                UserCtx, FileCtx1, RequiredPerms, UserPermsMatrix
            )
    end.


%% @private
-spec calc_and_check_user_perms_matrix_from_posix_mode(
    user_ctx:ctx(), file_ctx:ctx(), ace:bitmask(), user_perms_matrix()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_matrix()}.
calc_and_check_user_perms_matrix_from_posix_mode(
    UserCtx, FileCtx0, RequiredPerms, {0, 0, DeniedPerms}
) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        type = FileType,
        mode = Mode
    }}, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),

    {RelevantModeBits0, SpecialPosixPerms, FileCtx3} = case user_ctx:get_user_id(UserCtx) of
        OwnerId ->
            {Mode bsr 6, ?write_acl_mask bor ?delete_mask, FileCtx1};
        _ ->
            ShiftedMode = case file_ctx:is_in_user_space_const(FileCtx1, UserCtx) of
                true -> Mode bsr 3;
                false -> Mode
            end,
            {HasParentStickyBitSet, FileCtx2} = has_parent_sticky_bit_set(UserCtx, FileCtx1),
            UserSpecialPerms = case HasParentStickyBitSet of
                true -> 0;
                false -> ?delete_mask
            end,
            {ShiftedMode, UserSpecialPerms, FileCtx2}
    end,
    RelevantModeBits1 = RelevantModeBits0 band 2#111,
    AllowedPosixPerms = SpecialPosixPerms bor get_posix_allowed_perms(RelevantModeBits1, FileType),

    AllAllowedPerms = ?reset_flags(AllowedPosixPerms, DeniedPerms),
    AllDeniedPerms = bnot AllAllowedPerms,

    Result = case RequiredPerms band AllDeniedPerms of
        0 -> allowed;
        _ -> denied
    end,
    {Result, FileCtx3, {1, AllAllowedPerms, AllDeniedPerms}};

calc_and_check_user_perms_matrix_from_posix_mode(UserCtx, FileCtx, RequiredPerms, _) ->
    % Race between reading and clearing cache must have happened (user perms matrix
    % is calculated entirely at once from posix mode so it is not possible for it
    % to has No > 0 and not be full)
    calc_and_check_user_perms_matrix(UserCtx, FileCtx, RequiredPerms).


%% @private
-spec has_parent_sticky_bit_set(user_ctx:ctx(), file_ctx:ctx()) ->
    {boolean(), file_ctx:ctx()}.
has_parent_sticky_bit_set(UserCtx, FileCtx0) ->
    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),

    {#document{value = #file_meta{
        mode = Mode
    }}, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),

    {Mode band 2#1000000000 == 1, FileCtx1}.


%% @private
-spec get_posix_allowed_perms(0..7, file_meta:type()) -> ace:bitmask().
get_posix_allowed_perms(2#000, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS;
get_posix_allowed_perms(2#001, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_EXEC_ONLY_PERMS;
get_posix_allowed_perms(2#010, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_DIR_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#010, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_FILE_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#011, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_DIR_WRITE_EXEC_PERMS;
get_posix_allowed_perms(2#011, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_FILE_WRITE_EXEC_PERMS;
get_posix_allowed_perms(2#100, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS;
get_posix_allowed_perms(2#101, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_EXEC_ONLY_PERMS;
get_posix_allowed_perms(2#110, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_DIR_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#110, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_FILE_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#111, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_DIR_WRITE_EXEC_PERMS;
get_posix_allowed_perms(2#111, _) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_FILE_WRITE_EXEC_PERMS.


%% @private
-spec calc_and_check_user_perms_matrix_from_acl(
    user_ctx:ctx(), file_ctx:ctx(), ace:bitmask(), user_perms_matrix()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_matrix()}.
calc_and_check_user_perms_matrix_from_acl(
    UserCtx, FileCtx0, RequiredPerms, {No, _, _} = UserPermsMatrix
) ->
    UserDoc = user_ctx:get_user(UserCtx),
    {Acl, FileCtx1} = file_ctx:get_acl(FileCtx0),

    try
        acl:check_acl(
            lists:nthtail(No, Acl), UserDoc, FileCtx1,
            RequiredPerms, UserPermsMatrix
        )
    catch error:function_clause ->
        % Race between reading and clearing cache must have happened
        % (acl must have been replaced if it is shorter then should be)
        calc_and_check_user_perms_matrix(UserCtx, FileCtx0, RequiredPerms)
    end.
