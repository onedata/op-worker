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
%%% @TODO VFS-10318 - standardize concepts regarding protection flags (restricted/blocked/forbidden/denied; dataset/file protection flags)
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
    assert_available_in_readonly_mode/1
]).

-type bitmask() :: integer().

-type type() ::
    ownership
    | public_access
    | traverse_ancestors            % Means ancestors' exec permission
    | {operations, bitmask()}.

-type requirement() ::
    root
    | type()
    | {type(), 'or', type()}.

-type user_access_check_progress() :: #user_access_check_progress{}.

-export_type([bitmask/0, type/0, requirement/0, user_access_check_progress/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_granted(user_ctx:ctx(), file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_granted(UserCtx, FileCtx, AccessRequirements) ->
    case user_ctx:is_root(UserCtx) of
        true ->
            % Root omits all access checks with exception to file protection flags
            case file_ctx:is_space_dir_const(FileCtx) of
                false ->
                    assert_access_permitted_by_protection_flags(FileCtx, AccessRequirements);
                true ->
                    %% @TODO VFS-8589 - properly handle getting space dir metadata on proxy providers
                    FileCtx
            end;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),

            case user_ctx:is_space_owner(UserCtx, SpaceId) of
                true ->
                    assert_access_granted_for_space_owner(UserCtx, FileCtx, AccessRequirements);
                false ->
                    assert_access_granted_for_user(UserCtx, FileCtx, AccessRequirements)
            end
    end.


-spec assert_available_in_readonly_mode([requirement()]) ->
    ok | no_return().
assert_available_in_readonly_mode(AccessRequirements) ->
    case lists:all(fun is_available_in_readonly_mode/1, AccessRequirements) of
        true -> ok;
        false -> throw(?EACCES)
    end.


%%%===================================================================
%%% Internal Functions
%%%===================================================================


%% @private
-spec is_available_in_readonly_mode(requirement()) -> boolean().
is_available_in_readonly_mode(?OPERATIONS(Operations)) ->
    case ?reset_flags(Operations, ?READONLY_MODE_AVAILABLE_OPERATIONS) of
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
-spec assert_access_granted_for_space_owner(user_ctx:ctx(), file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_access_granted_for_space_owner(UserCtx, FileCtx0, AccessRequirements) ->
    IsSpaceDir = file_ctx:is_space_dir_const(FileCtx0),
    IsInOpenHandleMode = user_ctx:is_in_open_handle_mode(UserCtx),
    
    case IsSpaceDir orelse IsInOpenHandleMode of
        true ->
            % In case of space dir or 'open_handle' session mode space owner
            % is treated as any other user
            assert_access_granted_for_user(UserCtx, FileCtx0, AccessRequirements);
        false ->
            % For any other file or directory space owner omits all access checks
            % with exceptions to file protection flags
            assert_access_permitted_by_protection_flags(FileCtx0, AccessRequirements)
    end.


%% @private
-spec assert_access_granted_for_user(user_ctx:ctx(), file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_access_granted_for_user(UserCtx, FileCtx0, AccessRequirements0) ->
    AccessRequirements1 = case user_ctx:is_guest(UserCtx) of
        true ->
            [?PUBLIC_ACCESS | AccessRequirements0];
        false ->
            case file_ctx:is_in_user_space_const(FileCtx0, UserCtx) of
                true -> AccessRequirements0;
                false -> throw(?EACCES)
            end
    end,
    % Special case - user in 'open_handle' mode should be treated as ?GUEST
    % when checking permissions
    UserCtx2 = case user_ctx:is_in_open_handle_mode(UserCtx) of
        true -> user_ctx:set_session_mode(user_ctx:new(?GUEST_SESS_ID), open_handle);
        false -> UserCtx
    end,
    lists:foldl(fun(AccessRequirement, FileCtx1) ->
        assert_meets_access_requirement(UserCtx2, FileCtx1, AccessRequirement)
    end, FileCtx0, AccessRequirements1).


%% @private
-spec assert_access_permitted_by_protection_flags(file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_access_permitted_by_protection_flags(FileCtx, AccessRequirements) ->
    {ForbiddenOps, FileCtx1} = get_operations_blocked_by_file_protection_flags(FileCtx),
    {ForbiddenOps2, FileCtx2} = get_operations_blocked_by_dataset_protection_flags(FileCtx1),
    AllForbiddenOps = ForbiddenOps bor ForbiddenOps2,

    IsRequirementMet = fun
        F(?OPERATIONS(RequiredOps)) ->
            case ?common_flags(RequiredOps, AllForbiddenOps) of
                ?no_flags_mask -> true;
                _ -> false
            end;
        F(?OR(AccessRequirement1, AccessRequirement2)) ->
            F(AccessRequirement1) orelse F(AccessRequirement2);
        F(_) ->
            true
    end,
    case lists:all(IsRequirementMet, AccessRequirements) of
        true -> FileCtx2;
        false -> throw(?EPERM)
    end.


%% @private
-spec assert_meets_access_requirement(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    file_ctx:ctx() | no_return().
assert_meets_access_requirement(UserCtx, FileCtx, ?OPERATIONS(RequiredOps)) ->
    assert_operations_allowed(UserCtx, FileCtx, RequiredOps);

assert_meets_access_requirement(UserCtx, FileCtx0, Requirement) when
    Requirement == ?OWNERSHIP;
    Requirement == ?PUBLIC_ACCESS;
    Requirement == ?TRAVERSE_ANCESTORS
->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_logical_guid_const(FileCtx0),
    CacheKey = {Requirement, UserId, Guid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, granted} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            % permissions_cache update is tagged with Timestamp to prevent races with cache invalidation.
            % All data used to calculate cached value has to be get from datastore after Timestamp so
            % file_ctx reset is needed.
            Timestamp = bounded_cache:get_timestamp(),
            FileCtx1 = file_ctx:reset(FileCtx0),
            try
                {ok, FileCtx2} = check_access_requirement(UserCtx, FileCtx1, Requirement),
                permissions_cache:cache_permission(CacheKey, granted, Timestamp),
                FileCtx2
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES, Timestamp),
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
                    {ParentCtx, FileCtx2} = file_tree:get_parent(FileCtx1, UserCtx),
                    assert_meets_access_requirement(UserCtx, ParentCtx, ?PUBLIC_ACCESS),
                    {ok, FileCtx2}
            end
    end;

check_access_requirement(UserCtx, FileCtx0, ?TRAVERSE_ANCESTORS) ->
    {ParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),

    case file_ctx:equals(FileCtx1, ParentCtx) of
        true ->
            {ok, FileCtx1};
        false ->
            ParentCtx1 = assert_operations_allowed(
                UserCtx, ParentCtx, ?traverse_container_mask
            ),
            assert_meets_access_requirement(UserCtx, ParentCtx1, ?TRAVERSE_ANCESTORS),
            {ok, FileCtx1}
    end.


%% @private
-spec assert_operations_allowed(user_ctx:ctx(), file_ctx:ctx(), bitmask()) ->
    file_ctx:ctx() | no_return().
assert_operations_allowed(UserCtx, FileCtx0, RequiredOps) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_logical_guid_const(FileCtx0),
    % TODO VFS-6224 do not construct cache key outside of permissions_cache module
    CacheKey = {user_perms_matrix, UserId, Guid},

    % permissions_cache update is tagged with Timestamp to prevent races with cache invalidation.
    % All data used to calculate cached value has to be get from datastore after Timestamp so file_ctx reset is needed.
    Timestamp = bounded_cache:get_timestamp(),
    FileCtx1 = file_ctx:reset(FileCtx0),

    Result = case permissions_cache:check_permission(CacheKey) of
        {ok, #user_access_check_progress{
            forbidden = ForbiddenOps,
            allowed = AllowedOps,
            denied = DeniedOps
        } = UserAccessCheckProgress} ->
            case ?common_flags(RequiredOps, ForbiddenOps) of
                ?no_flags_mask ->
                    case ?reset_flags(RequiredOps, AllowedOps) of
                        ?no_flags_mask ->
                            {allowed, FileCtx0}; % permissions_cache will not be updated - original ctx can be used
                        LeftoverRequiredOps ->
                            case ?common_flags(LeftoverRequiredOps, DeniedOps) of
                                ?no_flags_mask ->
                                    check_file_permissions(
                                        UserCtx, FileCtx1, LeftoverRequiredOps,
                                        UserAccessCheckProgress
                                    );
                                _ ->
                                    throw(?EACCES)
                            end
                    end;
                _ ->
                    throw(?EPERM)
            end;
        _ ->
            check_operations(UserCtx, FileCtx1, RequiredOps)
    end,

    case Result of
        {allowed, FileCtx2} ->
            FileCtx2;
        {allowed, FileCtx2, NewUserAccessCheckProgress} ->
            permissions_cache:cache_permission(CacheKey, NewUserAccessCheckProgress, Timestamp),
            FileCtx2;
        {denied, _FileCtx2, NewUserAccessCheckProgress} ->
            permissions_cache:cache_permission(CacheKey, NewUserAccessCheckProgress, Timestamp),
            throw(?EACCES);
        {forbidden, _FileCtx2, NewUserAccessCheckProgress} ->
            permissions_cache:cache_permission(CacheKey, NewUserAccessCheckProgress, Timestamp),
            throw(?EPERM)
    end.


%% @private
-spec check_operations(user_ctx:ctx(), file_ctx:ctx(), bitmask()) ->
    {forbidden | allowed | denied, file_ctx:ctx(), user_access_check_progress()}.
check_operations(UserCtx, FileCtx0, RequiredOps) ->
    ShareId = file_ctx:get_share_id_const(FileCtx0),
    ForbiddenOps1 = get_operations_blocked_by_lack_of_space_privs(UserCtx, FileCtx0, ShareId),
    {ForbiddenOps2, FileCtx1} = get_operations_blocked_by_file_protection_flags(FileCtx0),
    {ForbiddenOps3, FileCtx2} = get_operations_blocked_by_dataset_protection_flags(FileCtx1),
    AllForbiddenOps = ForbiddenOps1 bor ForbiddenOps2 bor ForbiddenOps3,

    UserAccessCheckProgress = #user_access_check_progress{
        finished_step = ?OPERATIONS_AVAILABILITY_CHECK,
        forbidden = AllForbiddenOps,
        allowed = ?no_flags_mask,
        denied = ?no_flags_mask
    },
    case ?common_flags(RequiredOps, AllForbiddenOps) of
        ?no_flags_mask ->
            check_file_permissions(UserCtx, FileCtx2, RequiredOps, UserAccessCheckProgress);
        _ ->
            {forbidden, FileCtx2, UserAccessCheckProgress}
    end.


%% @private
-spec get_operations_blocked_by_lack_of_space_privs(
    user_ctx:ctx(), file_ctx:ctx(), undefined | od_share:id()
) ->
    bitmask().
get_operations_blocked_by_lack_of_space_privs(UserCtx, FileCtx, undefined) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    IsInOpenHandleMode = user_ctx:is_in_open_handle_mode(UserCtx),

    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) orelse IsInOpenHandleMode of
        true ->
            ?SPACE_BLOCKED_WRITE_OPERATIONS;
        false ->
            {ok, EffUserSpacePrivs} = space_logic:get_eff_privileges(SpaceId, UserId),

            lists:foldl(fun
                (?SPACE_READ_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_BLOCKED_READ_OPERATIONS);
                (?SPACE_WRITE_DATA, Bitmask) -> ?reset_flags(Bitmask, ?SPACE_BLOCKED_WRITE_OPERATIONS);
                (_, Bitmask) -> Bitmask
            end, ?SPACE_BLOCKED_READ_OPERATIONS bor ?SPACE_BLOCKED_WRITE_OPERATIONS, EffUserSpacePrivs)
    end;

get_operations_blocked_by_lack_of_space_privs(_UserCtx, _FileCtx, _ShareId) ->
    % All write operations are denied in share mode
    ?SPACE_BLOCKED_WRITE_OPERATIONS.


%% @private
-spec get_operations_blocked_by_file_protection_flags(file_ctx:ctx()) ->
    {bitmask(), file_ctx:ctx()}.
get_operations_blocked_by_file_protection_flags(FileCtx0) ->
    {FileDoc, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),
    {ok, Flags} = dataset_eff_cache:get_eff_protection_flags(FileDoc),
    AllRestrictedOps = [
        {?DATA_PROTECTION, ?FILE_DATA_PROTECTION_BLOCKED_OPERATIONS},
        {?METADATA_PROTECTION, ?FILE_METADATA_PROTECTION_BLOCKED_OPERATIONS}
    ],
    {get_operations_blocked_by_protection_flags(Flags, AllRestrictedOps), FileCtx1}.


%% @private
-spec get_operations_blocked_by_dataset_protection_flags(file_ctx:ctx()) ->
    {bitmask(), file_ctx:ctx()}.
get_operations_blocked_by_dataset_protection_flags(FileCtx0) ->
    {FileDoc, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),
    {ok, Flags} = dataset_eff_cache:get_eff_dataset_protection_flags(FileDoc),
    AllRestrictedOps = [{?DATA_PROTECTION, ?DATASET_DATA_PROTECTION_BLOCKED_OPERATIONS}],
    {get_operations_blocked_by_protection_flags(Flags, AllRestrictedOps), FileCtx1}.


%% @private
-spec get_operations_blocked_by_protection_flags(Flags :: bitmask(), [{ProtectionFlag :: bitmask(), RestrictedOps :: bitmask()}]) ->
    DeniedOps :: bitmask().
get_operations_blocked_by_protection_flags(Flags, AllRestrictedOps) ->
    lists:foldl(fun({ProtectionFlag, RestrictedOps}, DeniedOpsAcc) ->
        case ?has_all_flags(Flags, ProtectionFlag) of
            true -> ?set_flags(DeniedOpsAcc, RestrictedOps);
            false -> DeniedOpsAcc
        end
    end, ?no_flags_mask, AllRestrictedOps).


%% @private
-spec check_file_permissions(user_ctx:ctx(), file_ctx:ctx(), bitmask(), user_access_check_progress()) ->
    {allowed | denied, file_ctx:ctx(), user_access_check_progress()}.
check_file_permissions(UserCtx, FileCtx0, RequiredOps, UserAccessCheckProgress) ->
    case file_ctx:get_active_perms_type(FileCtx0, include_deleted) of
        {posix, FileCtx1} ->
            check_posix_mode(UserCtx, FileCtx1, RequiredOps, UserAccessCheckProgress);
        {acl, FileCtx1} ->
            check_acl(UserCtx, FileCtx1, RequiredOps, UserAccessCheckProgress)
    end.


%% @private
-spec check_posix_mode(user_ctx:ctx(), file_ctx:ctx(), bitmask(), user_access_check_progress()) ->
    {allowed | denied, file_ctx:ctx(), user_access_check_progress()}.
check_posix_mode(UserCtx, FileCtx0, RequiredOps, #user_access_check_progress{
    finished_step = ?OPERATIONS_AVAILABILITY_CHECK,
    allowed = ?no_flags_mask,
    denied = DeniedOps
} = UserAccessCheckProgress) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        type = FileType,
        mode = Mode
    }}, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),

    {UserAffiliation, UserSpecialOps, FileCtx3} = case user_ctx:get_user_id(UserCtx) of
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

    AllowedPosixOps = ?set_flags(UserSpecialOps, get_posix_allowed_ops(
        RelevantModeBits, FileType
    )),
    AllAllowedOps = ?reset_flags(AllowedPosixOps, DeniedOps),
    AllDeniedOps = ?complement_flags(AllAllowedOps),

    PermissionsCheckResult = case ?common_flags(RequiredOps, AllDeniedOps) of
        ?no_flags_mask -> allowed;
        _ -> denied
    end,
    {PermissionsCheckResult, FileCtx3, UserAccessCheckProgress#user_access_check_progress{
        finished_step = ?POSIX_MODE_CHECK,
        allowed = AllAllowedOps,
        denied = AllDeniedOps
    }};

check_posix_mode(UserCtx, FileCtx, RequiredOps, _) ->
    % Race between reading and clearing cache must have happened (user perms matrix
    % is calculated entirely at once from posix mode so it is not possible for it
    % to has pointer > 0 and not be fully constructed)
    check_operations(UserCtx, FileCtx, RequiredOps).


%% @private
-spec get_mode_bits_triplet(file_meta:mode(), owner | group | other) -> 0..7.
get_mode_bits_triplet(Mode, owner) -> ?common_flags(Mode bsr 6, 2#111);
get_mode_bits_triplet(Mode, group) -> ?common_flags(Mode bsr 3, 2#111);
get_mode_bits_triplet(Mode, other) -> ?common_flags(Mode, 2#111).


%% @private
-spec has_parent_sticky_bit_set(user_ctx:ctx(), file_ctx:ctx()) ->
    {boolean(), file_ctx:ctx()}.
has_parent_sticky_bit_set(UserCtx, FileCtx0) ->
    {ParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),

    {#document{value = #file_meta{
        mode = Mode
    }}, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),

    {?has_all_flags(Mode, ?STICKY_BIT), FileCtx1}.


%% @private
-spec get_posix_allowed_ops(0..7, onedata_file:type()) -> bitmask().
get_posix_allowed_ops(2#000, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS;
get_posix_allowed_ops(2#001, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_EXEC_ONLY_OPS;
get_posix_allowed_ops(2#010, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_DIR_WRITE_ONLY_OPS;
get_posix_allowed_ops(2#010, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_FILE_WRITE_ONLY_OPS;
get_posix_allowed_ops(2#011, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_DIR_WRITE_EXEC_OPS;
get_posix_allowed_ops(2#011, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_FILE_WRITE_EXEC_OPS;
get_posix_allowed_ops(2#100, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS;
get_posix_allowed_ops(2#101, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS bor ?POSIX_EXEC_ONLY_OPS;
get_posix_allowed_ops(2#110, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS bor ?POSIX_DIR_WRITE_ONLY_OPS;
get_posix_allowed_ops(2#110, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS bor ?POSIX_FILE_WRITE_ONLY_OPS;
get_posix_allowed_ops(2#111, ?DIRECTORY_TYPE) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS bor ?POSIX_DIR_WRITE_EXEC_OPS;
get_posix_allowed_ops(2#111, _) ->
    ?POSIX_ALWAYS_ALLOWED_OPS bor ?POSIX_READ_ONLY_OPS bor ?POSIX_FILE_WRITE_EXEC_OPS.


%% @private
-spec check_acl(user_ctx:ctx(), file_ctx:ctx(), bitmask(), user_access_check_progress()) ->
    {allowed | denied, file_ctx:ctx(), user_access_check_progress()}.
check_acl(UserCtx, FileCtx0, RequiredOps, #user_access_check_progress{
    finished_step = ?ACL_CHECK(AceNo)
} = UserAccessCheckProgress) ->
    {Acl, FileCtx1} = file_ctx:get_acl(FileCtx0),

    case AceNo >= length(Acl) of
        true ->
            % Race between reading and clearing cache must have happened
            % (acl must have been replaced if it is shorter then should be)
            check_operations(UserCtx, FileCtx0, RequiredOps);
        false ->
            acl:check_acl(
                lists:nthtail(AceNo, Acl),
                user_ctx:get_user(UserCtx),
                FileCtx1, RequiredOps, UserAccessCheckProgress
            )
    end.
