%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Security rules.
%%% @end
%%%--------------------------------------------------------------------
-module(rules).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([check_normal_or_default_def/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if given access_definition is granted to given user.
%% Accepts default defs (that use default file context).
%% Returns updated default file context record.
%% @end
%%--------------------------------------------------------------------
-spec check_normal_or_default_def(check_permissions:check_type(), user_ctx:ctx(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
check_normal_or_default_def({Type, SubjectCtx}, UserCtx, FileCtx) ->
    {ok, _} = check_normal_def({Type, SubjectCtx}, UserCtx, FileCtx),
    {ok, FileCtx};
check_normal_or_default_def(Type, UserCtx, FileCtx) ->
    {ok, _FileCtx2} = check_normal_def({Type, FileCtx}, UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user. Accepts only full
%% definitions with file context. Return updated file context. The functions
%% gets as an argument also DefaultFileCtx, 'share' check depends on its
%% 'share_id'.
%% @end
%%--------------------------------------------------------------------
-spec check_normal_def(check_permissions:check_type(),  user_ctx:user_ctx(),
    file_ctx:ctx()) -> ok | no_return().
check_normal_def({share, SubjectCtx}, UserCtx, DefaultFileCtx) ->
    case file_ctx:is_root_dir_const(SubjectCtx) of
        true ->
            throw(?EACCES);
        false ->
            {#document{value = #file_meta{shares = Shares}}, SubjectCtx2} =
                file_ctx:get_file_doc(SubjectCtx),
            ShareId = file_ctx:get_share_id_const(DefaultFileCtx),

            case lists:member(ShareId, Shares) of
                true ->
                    {ok, SubjectCtx2};
                false ->
                    {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, user_ctx:get_user_id(UserCtx)),
                    check_normal_def({share, ParentCtx}, UserCtx, DefaultFileCtx),
                    {ok, SubjectCtx3}
            end
    end;
check_normal_def({traverse_ancestors, SubjectCtx}, UserCtx, _DefaultFileCtx) ->
    case file_ctx:is_root_dir_const(SubjectCtx) of
        true ->
            {ok, SubjectCtx};
        false ->
            SubjectCtx2 = case file_ctx:is_space_dir_const(SubjectCtx) of
                true ->
                    {ok, SubjectCtx2_} = check_normal_def({?traverse_container, SubjectCtx}, UserCtx, _DefaultFileCtx),
                    SubjectCtx2_;
                false ->
                    SubjectCtx
            end,
            {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, user_ctx:get_user_id(UserCtx)),
            check_normal_def({?traverse_container, ParentCtx}, UserCtx, _DefaultFileCtx),
            check_normal_def({traverse_ancestors, ParentCtx}, UserCtx, _DefaultFileCtx),
            {ok, SubjectCtx3}
    end;
check_normal_def({Type, SubjectCtx}, UserCtx, _FileCtx) ->
    {FileDoc, SubjectCtx2} = file_ctx:get_file_doc(SubjectCtx),
    UserDoc = user_ctx:get_user(UserCtx),
    ShareId = file_ctx:get_share_id_const(SubjectCtx2),
    {Acl, _} = file_ctx:get_acl(SubjectCtx2),
    check(Type, FileDoc, UserDoc, ShareId, Acl, SubjectCtx).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user.
%% @end
%%--------------------------------------------------------------------
-spec check(check_permissions:check_type(), file_meta:doc(), od_user:doc(),
    od_share:id() | undefined, acl:acl() | undefined, file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
% standard posix checks
check(root, _, _, _, _, _) ->
    throw(?EACCES);
check(owner, #document{value = #file_meta{owner = OwnerId}}, #document{key = OwnerId}, _, _, FileCtx) ->
    {ok, FileCtx};
check(owner, _, _, _, _, _) ->
    throw(?EACCES);
check(owner_if_parent_sticky, Doc, UserDoc, ShareId, Acl, FileCtx) ->
    {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserDoc#document.key),
    {#document{value = #file_meta{mode = Mode}}, _ParentCtx2} =
        file_ctx:get_file_doc(ParentCtx),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            check(owner, Doc, UserDoc, ShareId, Acl, FileCtx2);
        false ->
            {ok, FileCtx2}
    end;
check({AccessType1, 'or', AccessType2}, Doc, User, ShareId, Acl, FileCtx) ->
    case
        {
                catch check(AccessType1, Doc, User, ShareId, Acl, FileCtx),
                catch check(AccessType2, Doc, User, ShareId, Acl, FileCtx)
        }
    of
        {{ok, Ctx}, _} -> {ok, Ctx};
        {_, {ok, Ctx}} -> {ok, Ctx};
        _ -> throw(?EACCES)
    end;
check(AccessType, #document{value = #file_meta{is_scope = true}},
    UserDoc, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr
    ->
    {ok, FileCtx2} = validate_scope_access(FileCtx, UserDoc, ShareId),
    {ok, FileCtx3} = validate_posix_access(AccessType, FileCtx2, UserDoc, ShareId),
    validate_scope_privs(AccessType, FileCtx3, UserDoc, ShareId);
check(AccessType, _Doc, UserDoc, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    {ok, FileCtx2} = validate_posix_access(AccessType, FileCtx, UserDoc, ShareId),
    validate_scope_privs(AccessType, FileCtx2, UserDoc, ShareId);

% if no acl specified, map access masks checks to posix checks
check(?read_object, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?list_container, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?write_object, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?add_object, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserDoc, ShareId, undefined, FileCtx),
    check(exec, Doc, UserDoc, ShareId, undefined, FileCtx2);
check(?append_data, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?add_subcontainer, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?read_metadata, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?write_metadata, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?execute, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(exec, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?traverse_container, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(exec, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?delete_object, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserDoc, ShareId, undefined, FileCtx),
    check(exec, Doc, UserDoc, ShareId, undefined, FileCtx2);
check(?delete_subcontainer, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserDoc, ShareId, undefined, FileCtx),
    check(exec, Doc, UserDoc, ShareId, undefined, FileCtx2);
check(?read_attributes, _Doc, _UserDoc, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_attributes, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?delete, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(owner_if_parent_sticky, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?read_acl, _Doc, _UserDoc, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_acl, Doc, UserDoc, ShareId, undefined, FileCtx) ->
    check(owner, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?write_owner, _, UserDoc, ShareId, undefined, FileCtx) ->
    check(root, undefined, UserDoc, ShareId, undefined, FileCtx);

% acl is specified but the request is for shared file, check posix perms
check(?read_object, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_object, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?list_container, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?list_container, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?read_metadata, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_metadata, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?execute, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?execute, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?traverse_container, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?traverse_container, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?read_attributes, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_attributes, Doc, UserDoc, ShareId, undefined, FileCtx);
check(?read_acl, Doc, UserDoc, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_acl, Doc, UserDoc, ShareId, undefined, FileCtx);

% acl is specified, check access masks
check(?read_object, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_object_mask),
    {ok, FileCtx};
check(?list_container, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?list_container_mask),
    {ok, FileCtx};
check(?write_object, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_object_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?add_object, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_object_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?append_data, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?append_data_mask),
    {ok, FileCtx};
check(?add_subcontainer, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_subcontainer_mask),
    {ok, FileCtx};
check(?read_metadata, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_metadata_mask),
    {ok, FileCtx};
check(?write_metadata, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_metadata_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?execute, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?execute_mask),
    {ok, FileCtx};
check(?traverse_container, #document{value = #file_meta{is_scope = true}}, UserDoc, ShareId, Acl, FileCtx) ->
    validate_scope_access(FileCtx, UserDoc, ShareId),
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask),
    {ok, FileCtx};
check(?traverse_container, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask),
    {ok, FileCtx};
check(?delete_object, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_object_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?delete_subcontainer, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_subcontainer_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?read_attributes, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_attributes_mask),
    {ok, FileCtx};
check(?write_attributes, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_attributes_mask),
    {ok, FileCtx};
check(?delete, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?read_acl, _, UserDoc, _, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_acl_mask),
    {ok, FileCtx};
check(?write_acl, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_acl_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(?write_owner, _Doc, UserDoc, ShareId, Acl, FileCtx) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_owner_mask),
    validate_scope_privs(write, FileCtx, UserDoc, ShareId);
check(Perm, File, User, ShareId, Acl, _FileCtx) ->
    ?error("Unknown permission check rule: (~p, ~p, ~p, ~p, ~p)", [Perm, File, User, ShareId, Acl]),
    throw(?EACCES).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has given permission on given file
%% (POSIX permission check).
%% @end
%%--------------------------------------------------------------------
-spec validate_posix_access(check_permissions:check_type(), file_ctx:ctx(),
    od_user:doc(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_posix_access(rdwr, FileCtx, UserDoc, ShareId) ->
    {ok, FileCtx2} = validate_posix_access(write, FileCtx, UserDoc, ShareId),
    validate_posix_access(read, FileCtx2, UserDoc, ShareId);
validate_posix_access(AccessType, FileCtx, UserDoc, _ShareId) ->
    {#document{value = #file_meta{owner = OwnerId, mode = Mode}}, FileCtx2} =
        file_ctx:get_file_doc(FileCtx),
    ReqBit =
        case AccessType of
            read -> 8#4;
            write -> 8#2;
            exec -> 8#1
        end,

    CheckType =
        case UserDoc#document.key of
            OwnerId ->
                owner;
            _ ->
                #document{value = #od_user{space_aliases = Spaces}} = UserDoc,
                SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx2),
                case catch lists:keymember(fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirUuid), 1, Spaces) of
                    true ->
                        group;
                    _ ->
                        other
                end
        end,
    IsAccessible =
        case CheckType of
            owner ->
                ((ReqBit bsl 6) band Mode) =:= (ReqBit bsl 6);
            group ->
                ((ReqBit bsl 3) band Mode) =:= (ReqBit bsl 3);
            other ->
                (ReqBit band Mode) =:= ReqBit
        end,

    case IsAccessible of
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
-spec validate_scope_access(file_ctx:ctx(), od_user:doc(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_access(FileCtx, #document{key = ?GUEST_USER_ID}, undefined) ->
    {ok, FileCtx};
validate_scope_access(FileCtx, UserDoc, undefined) ->
    RootDirUUID = fslogic_uuid:user_root_dir_uuid(UserDoc#document.key),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case file_ctx:is_root_dir_const(FileCtx)
        orelse RootDirUUID =:= FileUuid of
        true ->
            {ok, FileCtx};
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            UserSpaces = UserDoc#document.value#od_user.space_aliases,
            case (is_list(UserSpaces) andalso lists:keymember(SpaceId, 1, UserSpaces)) of
                true ->
                    {ok, FileCtx};
                false ->
                    throw(?ENOENT)
            end
    end;
validate_scope_access(FileCtx, _UserDoc, _ShareId) ->
    {ok, FileCtx}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given user has permission to access given file with
%% respect to scope settings.
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_privs(check_permissions:check_type(), file_ctx:ctx(),
    od_user:doc(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_privs(write, FileCtx, #document{key = UserId, value = #od_user{eff_groups = UserGroups}}, _ShareId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, #document{value = #od_space{users = Users, groups = Groups}}} =
        od_space:get(SpaceId, UserId),
    SpeceWritePriv = space_write_files,

    UserPrivs = proplists:get_value(UserId, Users, []),
    case lists:member(SpeceWritePriv, UserPrivs) of
        true -> {ok, FileCtx};
        false ->
            SpaceGroupsSet = sets:from_list(proplists:get_keys(Groups)),
            UserGroupsSet = sets:from_list(UserGroups),
            CommonGroups = sets:to_list(sets:intersection(UserGroupsSet, SpaceGroupsSet)),

            ValidGroups = lists:foldl(fun(GroupId, AccIn) ->
                GroupPrivs = proplists:get_value(GroupId, Groups, []),
                case lists:member(SpeceWritePriv, GroupPrivs) of
                    true ->
                        [GroupId | AccIn];
                    false ->
                        AccIn
                end
            end, [], CommonGroups),
            case ValidGroups of
                [] -> throw(?EACCES);
                _ -> {ok, FileCtx}
            end
    end;
validate_scope_privs(_, FileCtx, _, _) ->
    {ok, FileCtx}.