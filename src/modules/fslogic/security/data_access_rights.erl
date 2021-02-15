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

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
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

-export_type([type/0, requirement/0]).


-define(READONLY_MODE_ALLOWED_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask bor
    ?read_attributes_mask bor
    ?read_acl_mask bor
    ?traverse_container_mask
)).


-define(SPACE_WRITE_PERMS, (
    ?write_attributes_mask bor
    ?write_metadata_mask bor
    ?write_object_mask bor
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?delete_mask bor
    ?delete_child_mask bor
    ?write_acl_mask
)).
-define(SPACE_READ_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).


-define(POSIX_ALWAYS_GRANTED_PERMS, (
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(POSIX_READ_ONLY_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).
-define(POSIX_WRITE_ONLY_PERMS, (
    ?write_object_mask bor
    ?add_subcontainer_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_EXEC_ONLY_PERMS, (
    ?traverse_container_mask
)).

-define(POSIX_WRITE_EXEC_PERMS, (
    ?POSIX_WRITE_ONLY_PERMS bor
    ?POSIX_EXEC_ONLY_PERMS bor

    % some permissions are granted only when both 'write' and 'exec' bits are set
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
                check_and_cache_result(UserCtx, FileCtx1, AccessRequirement)
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
is_available_in_readonly_mode(root) ->
    true;
is_available_in_readonly_mode(share) ->
    true;
is_available_in_readonly_mode(owner) ->
    true;
is_available_in_readonly_mode(traverse_ancestors) ->
    true;
is_available_in_readonly_mode(?PERMISSIONS(Perms)) ->
    case Perms band (bnot ?READONLY_MODE_ALLOWED_PERMS) of
        0 -> true;
        _ -> false
    end;
is_available_in_readonly_mode({AccessType1, 'or', AccessType2}) ->
    is_available_in_readonly_mode(AccessType1) andalso is_available_in_readonly_mode(AccessType2).


%% @private
-spec check_and_cache_result(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    file_ctx:ctx() | no_return().
check_and_cache_result(UserCtx, FileCtx0, ?PERMISSIONS(RequiredPerms)) ->
    check_permissions(UserCtx, FileCtx0, RequiredPerms);

check_and_cache_result(UserCtx, FileCtx0, Requirement) ->
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
                {ok, FileCtx1} = check_access(UserCtx, FileCtx0, Requirement),
                permissions_cache:cache_permission(CacheKey, granted),
                FileCtx1
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES),
                throw(?EACCES)
            end
    end.


%% @private
-spec check_access(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    {ok, file_ctx:ctx()} | no_return().
check_access(UserCtx, FileCtx, {AccessType1, 'or', AccessType2}) ->
    case catch check_access(UserCtx, FileCtx, AccessType1) of
        {ok, _} = Res ->
            Res;
        _ ->
            check_access(UserCtx, FileCtx, AccessType2)
    end;

check_access(UserCtx, FileCtx, root) ->
    case user_ctx:is_root(UserCtx) of
        true ->
            {ok, FileCtx};
        false ->
            throw(?EACCES)
    end;

check_access(UserCtx, FileCtx0, owner) ->
    {OwnerId, FileCtx1} = file_ctx:get_owner(FileCtx0),

    case user_ctx:get_user_id(UserCtx) =:= OwnerId of
        true ->
            {ok, FileCtx1};
        false ->
            throw(?EACCES)
    end;

check_access(UserCtx, FileCtx0, share) ->
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
                    {ParentCtx, FileCtx2} = file_ctx:get_parent(
                        FileCtx1, UserCtx
                    ),
                    check_and_cache_result(UserCtx, ParentCtx, share),
                    {ok, FileCtx2}
            end
    end;

check_access(UserCtx, FileCtx0, traverse_ancestors) ->
    case file_ctx:get_and_check_parent(FileCtx0, UserCtx) of
        {undefined, FileCtx1} ->
            {ok, FileCtx1};
        {ParentCtx0, FileCtx1} ->
            ParentCtx1 = check_permissions(
                UserCtx, ParentCtx0, ?traverse_container_mask
            ),
            check_and_cache_result(UserCtx, ParentCtx1, traverse_ancestors),
            {ok, FileCtx1}
    end.







%% @private
check_permissions(UserCtx, FileCtx0, RequiredPerms) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    CacheKey = {user_perms_matrix, UserId, Guid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, {_No, AllowedPerms, DeniedPerms} = UserPermsMatrix} ->
            case RequiredPerms band (bnot AllowedPerms) of
                0 ->
                    FileCtx0;
                LeftoverRequiredPerms ->
                    case LeftoverRequiredPerms band DeniedPerms of
                        0 ->
                            propagate_user_perms_matrix(
                                CacheKey, UserCtx, FileCtx0,
                                LeftoverRequiredPerms, UserPermsMatrix
                            );
                        _ ->
                            throw(?EACCES)
                    end
            end;
        calculate ->
            calculate_user_perms_matrix(CacheKey, UserCtx, FileCtx0, RequiredPerms)
    end.


%% @private
calculate_user_perms_matrix(CacheKey, UserCtx, FileCtx0, Permission) ->
    ShareId = file_ctx:get_share_id_const(FileCtx0),
    DeniedPerms = get_perms_denied_by_lack_of_space_privs(UserCtx, FileCtx0, ShareId),

    UserPermsMatrix = {0, 0, DeniedPerms},

    case Permission band DeniedPerms of
        0 ->
            propagate_user_perms_matrix(CacheKey, UserCtx, FileCtx0, Permission, UserPermsMatrix);
        _ ->
            permissions_cache:cache_permission(CacheKey, UserPermsMatrix),
            throw(?EACCES)
    end.


%% @private
propagate_user_perms_matrix(CacheKey, UserCtx, FileCtx0, RequiredPerms, UserPermsMatrix) ->
    case file_ctx:get_active_perms_type(FileCtx0, include_deleted) of
        {acl, FileCtx1} ->
            calculate_acl_perms(CacheKey, UserCtx, FileCtx1, RequiredPerms, UserPermsMatrix);
        {posix, FileCtx1} ->
            calculate_posix_perms(CacheKey, UserCtx, FileCtx1, RequiredPerms, UserPermsMatrix)
    end.


%% @private
get_perms_denied_by_lack_of_space_privs(UserCtx, FileCtx, undefined) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            ?SPACE_WRITE_PERMS;
        false ->
            {ok, EffUserSpacePrivs} = space_logic:get_eff_privileges(SpaceId, UserId),

            lists:foldl(fun
                (?SPACE_READ_DATA, Bitmask) -> Bitmask band (bnot ?SPACE_READ_PERMS);
                (?SPACE_WRITE_DATA, Bitmask) -> Bitmask band (bnot ?SPACE_WRITE_PERMS);
                (_, Bitmask) -> Bitmask
            end, ?SPACE_READ_PERMS bor ?SPACE_WRITE_PERMS, EffUserSpacePrivs)
    end;

get_perms_denied_by_lack_of_space_privs(_UserCtx, _FileCtx, _ShareId) ->
    ?SPACE_WRITE_PERMS.


%% @private
calculate_posix_perms(CacheKey, UserCtx, FileCtx0, RequiredPerms, {0, 0, DeniedPerms}) ->
    {#document{value = #file_meta{
        owner = OwnerId,
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
    AllowedPosixPerms = SpecialPosixPerms bor get_posix_allowed_perms(RelevantModeBits1),

    AllAllowedPerms = AllowedPosixPerms band (bnot DeniedPerms),
    AllDeniedPerms = bnot AllAllowedPerms,

    UserPermsMatrix = {1, AllAllowedPerms, AllDeniedPerms},
    permissions_cache:cache_permission(CacheKey, UserPermsMatrix),

    case RequiredPerms band AllDeniedPerms of
        0 -> FileCtx3;
        _ -> throw(?EACCES)
    end;

calculate_posix_perms(CacheKey, UserCtx, FileCtx, RequiredPerms, {_, _, _}) ->
    % Race between reading and clearing cache must have happened - TODO WRITEME
    calculate_user_perms_matrix(CacheKey, UserCtx, FileCtx, RequiredPerms).


%% @private
has_parent_sticky_bit_set(UserCtx, FileCtx0) ->
    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),

    {#document{value = #file_meta{
        mode = Mode
    }}, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),

    {Mode band 2#1000000000 == 1, FileCtx1}.


%% @private
get_posix_allowed_perms(2#000) ->
    ?POSIX_ALWAYS_GRANTED_PERMS;
get_posix_allowed_perms(2#001) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_EXEC_ONLY_PERMS;
get_posix_allowed_perms(2#010) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#011) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_WRITE_EXEC_PERMS;
get_posix_allowed_perms(2#100) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS;
get_posix_allowed_perms(2#101) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_EXEC_ONLY_PERMS;
get_posix_allowed_perms(2#110) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_WRITE_ONLY_PERMS;
get_posix_allowed_perms(2#111) ->
    ?POSIX_ALWAYS_GRANTED_PERMS bor ?POSIX_READ_ONLY_PERMS bor ?POSIX_WRITE_EXEC_PERMS.


%% @private
calculate_acl_perms(CacheKey, UserCtx, FileCtx0, RequiredPerms, {No, _, _} = UserPermsMatrix) ->
    {Acl0, FileCtx1} = file_ctx:get_acl(FileCtx0),

    % TODO handle No > length(Acl) - error
    Acl1 = lists:nthtail(No, Acl0),

    check_acl(
        Acl1, user_ctx:get_user(UserCtx), RequiredPerms, FileCtx1,
        CacheKey, UserPermsMatrix
    ).


%% @private
check_acl(_Acl, _User, ?no_flags_mask, FileCtx, _CacheKey, _UserPermsMatrix) ->
    FileCtx;
check_acl([], _User, _Operations, _FileCtx, CacheKey, {No, AllowedPerms, _DeniedPerms}) ->
    % After reaching then end of ACL all not explicitly granted perms are denied
    permissions_cache:cache_permission(CacheKey, {No, AllowedPerms, bnot AllowedPerms}),
    throw(?EACCES);
check_acl([Ace | Rest], User, Operations, FileCtx, CacheKey, {No, PrevAllowedPerms, PrevDeniedPerms}) ->
    case ace:is_applicable(User, FileCtx, Ace) of
        {true, FileCtx2} ->
            case check_against(Operations, PrevAllowedPerms, PrevDeniedPerms, Ace) of
                {allowed, AllAllowedPerms, AllDeniedPerms} ->
                    permissions_cache:cache_permission(
                        CacheKey, {No+1, AllAllowedPerms, AllDeniedPerms}
                    ),
                    FileCtx2;
                {inconclusive, LeftoverOperations, AllAllowedPerms, AllDeniedPerms} ->
                    check_acl(Rest, User, LeftoverOperations, FileCtx2,
                        CacheKey, {No+1, AllAllowedPerms, AllDeniedPerms}
                    );
                {denied, AllAllowedPerms, AllDeniedPerms} ->
                    permissions_cache:cache_permission(
                        CacheKey, {No+1, AllAllowedPerms, AllDeniedPerms}
                    ),
                    throw(?EACCES)
            end;
        {false, FileCtx2} ->
            check_acl(
                Rest, User, Operations, FileCtx2,
                CacheKey, {No+1, PrevAllowedPerms, PrevDeniedPerms}
            )
    end.


%% @private
check_against(Operations, PrevAllowedPerms, DeniedPerms, #access_control_entity{
    acetype = ?allow_mask,
    acemask = AceMask
}) ->
    AllAllowedPerms = PrevAllowedPerms bor (AceMask band (bnot DeniedPerms)),

    case (Operations band AceMask) of
        Operations ->
            {allowed, AllAllowedPerms, DeniedPerms};
        AllowedOperations ->
            {inconclusive, Operations bxor AllowedOperations, AllAllowedPerms, DeniedPerms}
    end;

check_against(Operations, AllowedPerms, PrevDeniedPerms, #access_control_entity{
    acetype = ?deny_mask,
    acemask = AceMask
}) ->
    AllDeniedPerms = PrevDeniedPerms bor (AceMask band (bnot AllowedPerms)),

    case (Operations band AceMask) of
        ?no_flags_mask ->
            {inconclusive, Operations, AllowedPerms, AllDeniedPerms};
        _ ->
            {denied, AllowedPerms, AllDeniedPerms}
    end.
