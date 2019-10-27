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
-module(fslogic_access).
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
    file_ctx:ctx().
assert_granted(UserCtx, FileCtx0, AccessRequirements0) ->
    case user_ctx:is_root(UserCtx) of
        true ->
            FileCtx0;
        false ->
            AccessRequirements1 = case user_ctx:is_guest(UserCtx) of
                true -> [share | AccessRequirements0];
                false -> AccessRequirements0
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
        {ok, ok} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                {ok, FileCtx1} = check_access(UserCtx, FileCtx0, Requirement),
                permissions_cache:cache_permission(CacheKey, ok),
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
    case catch check_access(AccessType1, UserCtx, FileCtx) of
        {ok, _} = Res ->
            Res;
        _ ->
            check_access(AccessType2, UserCtx, FileCtx)
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

check_access(UserCtx, FileCtx, Permission) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx2),
    case file_ctx:get_active_perms_type(FileCtx2) of
        {acl, FileCtx3} ->
            {Acl, _} = file_ctx:get_acl(FileCtx3),
            check_acl_access(
                Permission, FileDoc, UserCtx, ShareId, Acl, FileCtx3
            );
        {posix, FileCtx3} ->
            check_posix_access(Permission, FileDoc, UserCtx, ShareId, FileCtx3)
    end.


%% @private
-spec check_acl_access(type(), file_meta:doc(), user_ctx:ctx(),
    od_share:id() | undefined, acl:acl(), file_ctx:ctx()
) ->
    {ok, file_ctx:ctx()} | no_return().
check_acl_access(?read_object, _, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_object_mask, FileCtx0
    ),
    validate_scope_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?list_container, _, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?list_container_mask, FileCtx0
    ),
    validate_scope_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?write_object, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_object_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?add_object, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_object_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?add_subcontainer, _, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?add_subcontainer_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_metadata, _, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_metadata_mask, FileCtx0
    ),
    validate_scope_privs(read, FileCtx1, UserCtx, ShareId);
check_acl_access(?write_metadata, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_metadata_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?traverse_container, #document{value = #file_meta{is_scope = true}},
    UserCtx, ShareId, Acl, FileCtx
) ->
    validate_scope_access(FileCtx, UserCtx, ShareId),
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?traverse_container_mask, FileCtx
    );
check_acl_access(?traverse_container, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?traverse_container_mask, FileCtx
    );
check_acl_access(?delete_object, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?delete_subcontainer, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_child_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_attributes, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_attributes_mask, FileCtx
    );
check_acl_access(?write_attributes, _, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_attributes_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?delete, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?delete_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(?read_acl, _, UserCtx, _, Acl, FileCtx) ->
    acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?read_acl_mask, FileCtx
    );
check_acl_access(?write_acl, _Doc, UserCtx, ShareId, Acl, FileCtx0) ->
    {ok, FileCtx1} = acl:assert_permitted(
        Acl, user_ctx:get_user(UserCtx),
        ?write_acl_mask, FileCtx0
    ),
    validate_scope_privs(write, FileCtx1, UserCtx, ShareId);
check_acl_access(Perm, File, User, ShareId, Acl, _FileCtx) ->
    ?error(
        "Unknown acl permission check rule: (~p, ~p, ~p, ~p, ~p)",
        [Perm, File, User, ShareId, Acl]
    ),
    throw(?EACCES).


%% @private
-spec check_posix_access(type(), file_meta:doc(), user_ctx:ctx(),
    od_share:id() | undefined, file_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | no_return().
check_posix_access(?read_object, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?list_container, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?write_object, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?add_object, Doc, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, Doc, UserCtx, ShareId, FileCtx2);
check_posix_access(?add_subcontainer, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?read_metadata, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(read, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?write_metadata, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?traverse_container, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(exec, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?delete_object, Doc, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, Doc, UserCtx, ShareId, FileCtx2);
check_posix_access(?delete_subcontainer, Doc, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx),
    check_posix_perm(exec, Doc, UserCtx, ShareId, FileCtx2);
check_posix_access(?read_attributes, _Doc, _UserCtx, _ShareId, FileCtx) ->
    {ok, FileCtx};
check_posix_access(?write_attributes, Doc, UserCtx, ShareId, FileCtx) ->
    check_posix_perm(write, Doc, UserCtx, ShareId, FileCtx);
check_posix_access(?delete, _Doc, UserCtx, _ShareId, FileCtx) ->
    check_access(UserCtx, FileCtx, owner_if_parent_sticky);
check_posix_access(?read_acl, _Doc, _UserCtx, _ShareId, FileCtx) ->
    {ok, FileCtx};
check_posix_access(?write_acl, _Doc, UserCtx, _ShareId, FileCtx) ->
    check_access(UserCtx, FileCtx, owner);
check_posix_access(Perm, File, User, ShareId, _FileCtx) ->
    ?error(
        "Unknown posix permission check rule: (~p, ~p, ~p, ~p)",
        [Perm, File, User, ShareId]
    ),
    throw(?EACCES).


%% @private
-spec check_posix_perm(posix_perm(), file_meta:doc(), user_ctx:ctx(),
    od_share:id() | undefined, file_ctx:ctx()
) ->
    {ok, file_ctx:ctx()} | no_return().
check_posix_perm(PosixPerm, #document{value = #file_meta{
    is_scope = IsScope
}}, UserCtx, ShareId, FileCtx) ->
    {ok, FileCtx2} = case IsScope of
        true -> validate_scope_access(FileCtx, UserCtx, ShareId);
        false -> {ok, FileCtx}
    end,
    {ok, FileCtx3} = check_posix_perm(
        PosixPerm, FileCtx2, UserCtx, ShareId
    ),
    validate_scope_privs(PosixPerm, FileCtx3, UserCtx, ShareId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has given permission on given file
%% (POSIX permission check).
%% @end
%%--------------------------------------------------------------------
-spec check_posix_perm(posix_perm(), file_ctx:ctx(), user_ctx:ctx(),
    od_share:id() | undefined) -> {ok, file_ctx:ctx()} | no_return().
check_posix_perm(PosixPerm, FileCtx, UserCtx, _ShareId) ->
    {#document{value = #file_meta{
        owner = OwnerId,
        mode = Mode
    }}, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),

    ReqBit0 = case PosixPerm of
        read -> 8#4;
        write -> 8#2;
        exec -> 8#1
    end,

    ReqBit1 = case user_ctx:get_user_id(UserCtx) of
        OwnerId ->
            ReqBit0 bsl 6;          % shift to owner posix mode bits
        _ ->
            case file_ctx:is_in_user_space_const(FileCtx, UserCtx) of
                true ->
                    ReqBit0 bsl 3;  % shift to group posix mode bits
                false ->
                    ReqBit0         % remain at other posix mode bits
            end
    end,

    case ?has_flag(Mode, ReqBit1) of
        true -> {ok, FileCtx2};
        false -> throw(?EACCES)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to see given scope file.
%% This function is always called before validate_posix_access/3 and shall
%% handle all special cases.
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_access(file_ctx:ctx(), user_ctx:ctx(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_access(FileCtx, UserCtx, undefined) ->
    case user_ctx:is_guest(UserCtx)
        orelse file_ctx:is_in_user_space_const(FileCtx, UserCtx)
    of
        true ->
            {ok, FileCtx};
        false ->
            throw(?ENOENT)
    end;
validate_scope_access(FileCtx, _UserCtx, _ShareId) ->
    {ok, FileCtx}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to access given file with
%% respect to scope settings.
%% Exception to this are shared files which can be read without
%% SPACE_READ_DATA privilege.
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_privs(posix_perm(), file_ctx:ctx(), user_ctx:ctx(),
    od_share:id() | undefined) -> {ok, file_ctx:ctx()} | no_return().
validate_scope_privs(read, FileCtx, UserCtx, undefined) ->
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
validate_scope_privs(write, FileCtx, UserCtx, _ShareId) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_WRITE_DATA) of
        true -> {ok, FileCtx};
        false -> throw(?EACCES)
    end;
validate_scope_privs(_, FileCtx, _, _) ->
    {ok, FileCtx}.
