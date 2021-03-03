%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module checks user access rights for file.
%%% @end
%%%--------------------------------------------------------------------
-module(data_access_control).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("modules/fslogic/data_access_control.hrl").
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
    ownership
    | public_access
    | traverse_ancestors            % Means ancestors' exec permission
    | {permissions, ace:bitmask()}.

-type requirement() ::
    root
    | type()
    | {type(), 'or', type()}.

-type user_perms_check_progress() :: #user_perms_check_progress{}.

-export_type([type/0, requirement/0, user_perms_check_progress/0]).


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
                    [?PUBLIC_ACCESS | AccessRequirements0];
                false ->
                    case file_ctx:is_in_user_space_const(FileCtx0, UserCtx) of
                        true -> AccessRequirements0;
                        false -> throw(?ENOENT)
                    end
            end,
            lists:foldl(fun(AccessRequirement, FileCtx1) ->
                assert_meets_access_requirement(UserCtx, FileCtx1, AccessRequirement)
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
        ?no_flags_mask -> true;
        _ -> false
    end;
is_available_in_readonly_mode(Requirement) when
    Requirement == ?OWNERSHIP;
    Requirement == ?PUBLIC_ACCESS;
    Requirement == ?TRAVERSE_ANCESTORS;
    Requirement == root
->
    true;
is_available_in_readonly_mode(?OR(AccessType1, AccessType2)) ->
    is_available_in_readonly_mode(AccessType1) andalso is_available_in_readonly_mode(AccessType2).


%% @private
-spec assert_meets_access_requirement(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    file_ctx:ctx() | no_return().
assert_meets_access_requirement(UserCtx, FileCtx, ?PERMISSIONS(RequiredPerms)) ->
    assert_has_permissions(UserCtx, FileCtx, RequiredPerms);

assert_meets_access_requirement(UserCtx, FileCtx0, Requirement) when
    Requirement == ?OWNERSHIP;
    Requirement == ?PUBLIC_ACCESS;
    Requirement == ?TRAVERSE_ANCESTORS
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

assert_meets_access_requirement(UserCtx, FileCtx, root) ->
    case user_ctx:is_root(UserCtx) of
        true -> FileCtx;
        false -> throw(?EACCES)
    end;

assert_meets_access_requirement(UserCtx, FileCtx, ?OR(AccessType1, AccessType2)) ->
    try
        assert_meets_access_requirement(UserCtx, FileCtx, AccessType1)
    catch _:?EACCES ->
        assert_meets_access_requirement(UserCtx, FileCtx, AccessType2)
    end.


%% @private
-spec check_access_requirement(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    {ok, file_ctx:ctx()} | no_return().
check_access_requirement(UserCtx, FileCtx0, ?OWNERSHIP) ->
    {OwnerId, FileCtx1} = file_ctx:get_owner(FileCtx0),

    case user_ctx:get_user_id(UserCtx) =:= OwnerId of
        true -> {ok, FileCtx1};
        false -> throw(?EACCES)
    end;

check_access_requirement(UserCtx, FileCtx0, ?PUBLIC_ACCESS) ->
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
                    assert_meets_access_requirement(UserCtx, ParentCtx, ?PUBLIC_ACCESS),
                    {ok, FileCtx2}
            end
    end;

check_access_requirement(UserCtx, FileCtx0, ?TRAVERSE_ANCESTORS) ->
    case file_ctx:get_and_check_parent(FileCtx0, UserCtx) of
        {undefined, FileCtx1} ->
            {ok, FileCtx1};
        {ParentCtx0, FileCtx1} ->
            ParentCtx1 = assert_has_permissions(
                UserCtx, ParentCtx0, ?traverse_container_mask
            ),
            assert_meets_access_requirement(UserCtx, ParentCtx1, ?TRAVERSE_ANCESTORS),
            {ok, FileCtx1}
    end.


%% @private
-spec assert_has_permissions(user_ctx:ctx(), file_ctx:ctx(), ace:bitmask()) ->
    file_ctx:ctx() | no_return().
assert_has_permissions(UserCtx, FileCtx0, RequiredPerms) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    % TODO VFS-6224 do not construct cache key outside of permissions_cache module
    CacheKey = {user_perms_matrix, UserId, Guid},

    Result = case permissions_cache:check_permission(CacheKey) of
        {ok, #user_perms_check_progress{
            granted = GrantedPerms,
            denied = DeniedPerms
        } = UserPermsCheckProgress} ->
            case ?reset_flags(RequiredPerms, GrantedPerms) of
                ?no_flags_mask ->
                    {allowed, FileCtx0};
                LeftoverRequiredPerms ->
                    case ?common_flags(LeftoverRequiredPerms, DeniedPerms) of
                        ?no_flags_mask ->
                            check_permissions(
                                UserCtx, FileCtx0, LeftoverRequiredPerms, UserPermsCheckProgress
                            );
                        _ ->
                            throw(?EACCES)
                    end
            end;
        calculate ->
            check_permissions(UserCtx, FileCtx0, RequiredPerms)
    end,

    case Result of
        {allowed, FileCtx1} ->
            FileCtx1;
        {allowed, FileCtx1, NewUserPermsCheckProgress} ->
            permissions_cache:cache_permission(CacheKey, NewUserPermsCheckProgress),
            FileCtx1;
        {denied, FileCtx1, NewUserPermsCheckProgress} ->
            permissions_cache:cache_permission(CacheKey, NewUserPermsCheckProgress),
            throw(?EACCES)
    end.


%% @private
-spec check_permissions(user_ctx:ctx(), file_ctx:ctx(), ace:bitmask()) ->
    {allowed | denied, file_ctx:ctx(), user_perms_check_progress()}.
check_permissions(UserCtx, FileCtx0, RequiredPerms) ->
    ShareId = file_ctx:get_share_id_const(FileCtx0),
    DeniedPerms = get_perms_denied_by_lack_of_space_privs(UserCtx, FileCtx0, ShareId),

    UserPermsCheckProgress = #user_perms_check_progress{
        finished_step = ?SPACE_PRIVILEGES_CHECK,
        granted = ?no_flags_mask,
        denied = DeniedPerms
    },
    case ?common_flags(RequiredPerms, DeniedPerms) of
        ?no_flags_mask ->
            check_permissions(UserCtx, FileCtx0, RequiredPerms, UserPermsCheckProgress);
        _ ->
            {denied, FileCtx0, UserPermsCheckProgress}
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
            ?SPACE_DENIED_WRITE_PERMS;
        false ->
            {ok, EffUserSpacePrivs} = space_logic:get_eff_privileges(SpaceId, UserId),

            lists:foldl(fun
                (?SPACE_READ_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_DENIED_READ_PERMS);
                (?SPACE_WRITE_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_DENIED_WRITE_PERMS);
                (_, Bitmask) -> Bitmask
            end, ?SPACE_DENIED_READ_PERMS bor ?SPACE_DENIED_WRITE_PERMS, EffUserSpacePrivs)
    end;

get_perms_denied_by_lack_of_space_privs(_UserCtx, _FileCtx, _ShareId) ->
    % All write permissions are denied in share mode
    ?SPACE_DENIED_WRITE_PERMS.


%% @private
-spec check_permissions(
    user_ctx:ctx(),
    file_ctx:ctx(),
    ace:bitmask(),
    user_perms_check_progress()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_check_progress()}.
check_permissions(UserCtx, FileCtx0, RequiredPerms, UserPermsCheckProgress) ->
    case file_ctx:get_active_perms_type(FileCtx0, include_deleted) of
        {posix, FileCtx1} ->
            check_posix_permissions(UserCtx, FileCtx1, RequiredPerms, UserPermsCheckProgress);
        {acl, FileCtx1} ->
            check_acl_permissions(UserCtx, FileCtx1, RequiredPerms, UserPermsCheckProgress)
    end.


%% @private
-spec check_posix_permissions(
    user_ctx:ctx(), file_ctx:ctx(), ace:bitmask(), user_perms_check_progress()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_check_progress()}.
check_posix_permissions(UserCtx, FileCtx0, RequiredPerms, #user_perms_check_progress{
    finished_step = ?SPACE_PRIVILEGES_CHECK,
    granted = ?no_flags_mask,
    denied = DeniedPerms
}) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        type = FileType,
        mode = Mode
    }}, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),

    {UserAffiliation, UserSpecialPerms, FileCtx3} = case user_ctx:get_user_id(UserCtx) of
        OwnerId ->
            {owner, ?write_acl_mask bor ?delete_mask, FileCtx1};
        _ ->
            Affiliation = case file_ctx:is_in_user_space_const(FileCtx1, UserCtx) of
                true -> group;
                false -> other
            end,
            case has_parent_sticky_bit_set(UserCtx, FileCtx1) of
                {true, FileCtx2} -> {Affiliation, ?no_flags_mask, FileCtx2};
                {false, FileCtx2} -> {Affiliation, ?delete_mask, FileCtx2}
            end
    end,
    RelevantModeBits = get_mode_bits_triplet(Mode, UserAffiliation),

    GrantedPosixPerms = ?set_flags(UserSpecialPerms, get_posix_allowed_perms(
        RelevantModeBits, FileType
    )),
    AllAllowedPerms = ?reset_flags(GrantedPosixPerms, DeniedPerms),
    AllDeniedPerms = ?complement_flags(AllAllowedPerms),

    PermissionsCheckResult = case ?common_flags(RequiredPerms, AllDeniedPerms) of
        ?no_flags_mask -> allowed;
        _ -> denied
    end,
    {PermissionsCheckResult, FileCtx3, #user_perms_check_progress{
        finished_step = ?POSIX_MODE_CHECK,
        granted = AllAllowedPerms,
        denied = AllDeniedPerms
    }};

check_posix_permissions(UserCtx, FileCtx, RequiredPerms, _) ->
    % Race between reading and clearing cache must have happened (user perms matrix
    % is calculated entirely at once from posix mode so it is not possible for it
    % to has pointer > 0 and not be fully constructed)
    check_permissions(UserCtx, FileCtx, RequiredPerms).


%% @private
-spec get_mode_bits_triplet(file_meta:mode(), owner | group | other) -> 0..7.
get_mode_bits_triplet(Mode, owner) -> ?common_flags(Mode bsr 6, 2#111);
get_mode_bits_triplet(Mode, group) -> ?common_flags(Mode bsr 3, 2#111);
get_mode_bits_triplet(Mode, other) -> ?common_flags(Mode, 2#111).


%% @private
-spec has_parent_sticky_bit_set(user_ctx:ctx(), file_ctx:ctx()) ->
    {boolean(), file_ctx:ctx()}.
has_parent_sticky_bit_set(UserCtx, FileCtx0) ->
    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),

    {#document{value = #file_meta{
        mode = Mode
    }}, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),

    {?has_flags(Mode, ?STICKY_BIT), FileCtx1}.


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
-spec check_acl_permissions(
    user_ctx:ctx(),
    file_ctx:ctx(),
    ace:bitmask(),
    user_perms_check_progress()
) ->
    {allowed | denied, file_ctx:ctx(), user_perms_check_progress()}.
check_acl_permissions(UserCtx, FileCtx0, RequiredPerms, #user_perms_check_progress{
    finished_step = ?ACL_CHECK(AceNo)
} = UserPermsCheckProgress) ->
    {Acl, FileCtx1} = file_ctx:get_acl(FileCtx0),

    case AceNo >= length(Acl) of
        true ->
            % Race between reading and clearing cache must have happened
            % (acl must have been replaced if it is shorter then should be)
            check_permissions(UserCtx, FileCtx0, RequiredPerms);
        false ->
            acl:check_acl(
                lists:nthtail(AceNo, Acl),
                user_ctx:get_user(UserCtx),
                FileCtx1, RequiredPerms, UserPermsCheckProgress
            )
    end.
