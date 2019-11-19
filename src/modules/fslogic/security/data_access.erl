%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module handles access requirements checks for files.
%%% @end
%%%--------------------------------------------------------------------
-module(data_access).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([assert_granted/3]).

-type type() ::
    owner
    | owner_if_parent_sticky
    | share
    | traverse_ancestors            % Means ancestors' exec permission
    | binary().                     % Permissions defined in acl.hrl

-type requirement() ::
    root
    | type()
    | {type(), 'or', type()}.

-type posix_perm() :: write | read | exec.

-export_type([type/0, requirement/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_granted(user_ctx:ctx(), file_ctx:ctx(), [requirement()]) ->
    file_ctx:ctx() | no_return().
assert_granted(UserCtx, FileCtx0, AccessRequirements0) ->
    case user_ctx:is_root(UserCtx) of
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


%%%===================================================================
%%% Internal Functions
%%%===================================================================


%% @private
-spec check_and_cache_result(user_ctx:ctx(), file_ctx:ctx(), requirement()) ->
    file_ctx:ctx() | no_return().
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

check_access(UserCtx, FileCtx0, owner_if_parent_sticky) ->
    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),
    {#document{value = #file_meta{
        mode = Mode
    }}, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),

    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            check_access(UserCtx, FileCtx1, owner);
        false ->
            {ok, FileCtx1}
    end;

check_access(UserCtx, FileCtx0, share) ->
    case file_ctx:is_root_dir_const(FileCtx0) of
        true ->
            throw(?EACCES);
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
    case file_ctx:is_root_dir_const(FileCtx0) of
        true ->
            {ok, FileCtx0};
        false ->
            {ParentCtx0, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),
            ParentCtx1 = check_and_cache_result(
                UserCtx, ParentCtx0, ?traverse_container
            ),
            check_and_cache_result(UserCtx, ParentCtx1, traverse_ancestors),
            {ok, FileCtx1}
    end;

check_access(UserCtx, FileCtx0, Permission) ->
    ShareId = file_ctx:get_share_id_const(FileCtx0),
    case file_ctx:get_active_perms_type(FileCtx0) of
        {acl, FileCtx1} ->
            {Acl, _} = file_ctx:get_acl(FileCtx1),
            check_acl_access(Permission, UserCtx, ShareId, Acl, FileCtx1);
        {posix, FileCtx1} ->
            check_posix_access(Permission, UserCtx, ShareId, FileCtx1)
    end.


%% @private
-spec check_acl_access(type(), user_ctx:ctx(), undefined | od_share:id(),
    acl:acl(), file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
check_acl_access(?read_object, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_object_mask, FileCtx0
    ),
    check_space_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?list_container, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?list_container_mask, FileCtx0
    ),
    check_space_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?write_object, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_object_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?add_object, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_object_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?add_subcontainer, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_subcontainer_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_metadata, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_metadata_mask, FileCtx0
    ),
    check_space_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?write_metadata, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_metadata_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?traverse_container, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?traverse_container_mask, FileCtx
    );
check_acl_access(?delete_object, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?delete_subcontainer, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_attributes, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_attributes_mask, FileCtx
    );
check_acl_access(?write_attributes, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_attributes_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?delete, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_acl, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_acl_mask, FileCtx
    );
check_acl_access(?write_acl, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_acl_mask, FileCtx0
    ),
    check_space_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(Perm, User, ShareId, Acl, FileCtx) ->
    ?error(
        "Unknown acl permission check rule: (~p, ~p, ~p, ~p, ~p)",
        [Perm, file_ctx:get_guid_const(FileCtx), User, ShareId, Acl]
    ),
    throw(?EACCES).


%% @private
-spec check_posix_access(type(), user_ctx:ctx(), undefined | od_share:id(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
check_posix_access(?read_object, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, UserCtx, ShareId, FileCtx);
check_posix_access(?list_container, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, UserCtx, ShareId, FileCtx);
check_posix_access(?write_object, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, UserCtx, ShareId, FileCtx);
check_posix_access(?add_object, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, UserCtx, ShareId, FileCtx2);
check_posix_access(?add_subcontainer, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, UserCtx, ShareId, FileCtx);
check_posix_access(?read_metadata, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, UserCtx, ShareId, FileCtx);
check_posix_access(?write_metadata, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, UserCtx, ShareId, FileCtx);
check_posix_access(?traverse_container, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(exec, UserCtx, ShareId, FileCtx);
check_posix_access(?delete_object, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, UserCtx, ShareId, FileCtx2);
check_posix_access(?delete_subcontainer, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, UserCtx, ShareId, FileCtx2);
check_posix_access(?read_attributes, _UserCtx, _ShareId, FileCtx) ->
    {ok, FileCtx};
check_posix_access(?write_attributes, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, UserCtx, ShareId, FileCtx);
check_posix_access(?delete, UserCtx, _ShareId, FileCtx) ->
    check_access(UserCtx, FileCtx, owner_if_parent_sticky);
check_posix_access(?read_acl, _UserCtx, _ShareId, FileCtx) ->
    {ok, FileCtx};
check_posix_access(?write_acl, UserCtx, _ShareId, FileCtx) ->
    check_access(UserCtx, FileCtx, owner);
check_posix_access(Perm, User, ShareId, FileCtx) ->
    ?error(
        "Unknown posix permission check rule: (~p, ~p, ~p, ~p)",
        [Perm, file_ctx:get_guid_const(FileCtx), User, ShareId]
    ),
    throw(?EACCES).


%% @private
-spec check_posix_perm(posix_perm(), user_ctx:ctx(),
    od_share:id() | undefined, file_ctx:ctx()
) ->
    {ok, file_ctx:ctx()} | no_return().
check_posix_perm(PosixPerm, UserCtx, ShareId, FileCtx0) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        mode = Mode
    }}, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx0),

    ReqBit0 = case PosixPerm of
        read -> 8#4;
        write -> 8#2;
        exec -> 8#1
    end,

    ReqBit1 = case user_ctx:get_user_id(UserCtx) of
        OwnerId ->
            ReqBit0 bsl 6;          % shift to owner posix mode bits
        _ ->
            case file_ctx:is_in_user_space_const(FileCtx1, UserCtx) of
                true ->
                    ReqBit0 bsl 3;  % shift to group posix mode bits
                false ->
                    ReqBit0         % remain at other posix mode bits
            end
    end,

    case ?has_flag(Mode, ReqBit1) of
        true ->
            check_space_privs(PosixPerm, FileCtx1, UserCtx, ShareId);
        false ->
            throw(?EACCES)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to access given file with
%% respect to space settings.
%% Exception to this are shared files which can be read without
%% SPACE_READ_DATA privilege.
%% @end
%%--------------------------------------------------------------------
-spec check_space_privs(posix_perm(), file_ctx:ctx(), user_ctx:ctx(),
    od_share:id() | undefined) -> {ok, file_ctx:ctx()} | no_return().
check_space_privs(read, FileCtx, UserCtx, undefined) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {ok, FileCtx};
        false ->
            UserId = user_ctx:get_user_id(UserCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_READ_DATA) of
                true -> {ok, FileCtx};
                false -> throw(?EACCES)
            end
    end;
check_space_privs(write, FileCtx, UserCtx, _ShareId) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_WRITE_DATA) of
        true -> {ok, FileCtx};
        false -> throw(?EACCES)
    end;
check_space_privs(_, FileCtx, _, _) ->
    {ok, FileCtx}.
