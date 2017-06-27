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
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").

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
-spec check_normal_or_default_def(check_permissions:access_definition(), user_ctx:ctx(),
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
-spec check_normal_def(check_permissions:access_definition(),  user_ctx:user_ctx(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
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
                    {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, UserCtx),
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
            {ParentCtx, SubjectCtx3} = file_ctx:get_parent(SubjectCtx2, UserCtx),
            check_normal_def({?traverse_container, ParentCtx}, UserCtx, _DefaultFileCtx),
            check_normal_def({traverse_ancestors, ParentCtx}, UserCtx, _DefaultFileCtx),
            {ok, SubjectCtx3}
    end;
check_normal_def({Type, SubjectCtx}, UserCtx, _FileCtx) ->
    {FileDoc, SubjectCtx2} = file_ctx:get_file_doc(SubjectCtx),
    ShareId = file_ctx:get_share_id_const(SubjectCtx2),
    {Acl, _} = file_ctx:get_acl(SubjectCtx2),
    check(Type, FileDoc, UserCtx, ShareId, Acl, SubjectCtx).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user.
%% @end
%%--------------------------------------------------------------------
-spec check(check_permissions:access_definition(), file_meta:doc(), user_ctx:ctx(),
    od_share:id() | undefined, acl:acl() | undefined, file_ctx:ctx()) -> {ok, file_ctx:ctx()} | no_return().
% standard posix checks
check(root, _, _, _, _, _) ->
    throw(?EACCES);
check(owner, #document{value = #file_meta{owner = OwnerId}}, UserCtx, _, _, FileCtx) ->
    case user_ctx:get_user_id(UserCtx) =:= OwnerId of
        true ->
            {ok, FileCtx};
        false ->
            throw(?EACCES)
    end;
check(owner_if_parent_sticky, Doc, UserCtx, ShareId, Acl, FileCtx) ->
    {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    {#document{value = #file_meta{mode = Mode}}, _ParentCtx2} =
        file_ctx:get_file_doc(ParentCtx),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            check(owner, Doc, UserCtx, ShareId, Acl, FileCtx2);
        false ->
            {ok, FileCtx2}
    end;
check({AccessType1, 'or', AccessType2}, Doc, UserCtx, ShareId, Acl, FileCtx) ->
    case
        {
                catch check(AccessType1, Doc, UserCtx, ShareId, Acl, FileCtx),
                catch check(AccessType2, Doc, UserCtx, ShareId, Acl, FileCtx)
        }
    of
        {{ok, Ctx}, _} -> {ok, Ctx};
        {_, {ok, Ctx}} -> {ok, Ctx};
        _ -> throw(?EACCES)
    end;
check(AccessType, #document{value = #file_meta{is_scope = true}},
    UserCtx, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr
    ->
    {ok, FileCtx2} = validate_scope_access(FileCtx, UserCtx, ShareId),
    {ok, FileCtx3} = validate_posix_access(AccessType, FileCtx2, UserCtx, ShareId),
    validate_scope_privs(AccessType, FileCtx3, UserCtx, ShareId);
check(AccessType, _Doc, UserCtx, ShareId, undefined, FileCtx) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    {ok, FileCtx2} = validate_posix_access(AccessType, FileCtx, UserCtx, ShareId),
    validate_scope_privs(AccessType, FileCtx2, UserCtx, ShareId);

% if no acl specified, map access masks checks to posix checks
check(?read_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?list_container, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?write_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?add_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?append_data, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?add_subcontainer, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_metadata, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(read, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?write_metadata, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?execute, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?traverse_container, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?delete_object, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?delete_subcontainer, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    {ok, FileCtx2} = check(write, Doc, UserCtx, ShareId, undefined, FileCtx),
    check(exec, Doc, UserCtx, ShareId, undefined, FileCtx2);
check(?read_attributes, _Doc, _UserCtx, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_attributes, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(write, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?delete, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(owner_if_parent_sticky, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_acl, _Doc, _UserCtx, _ShareId, undefined, FileCtx) ->
    {ok, FileCtx};
check(?write_acl, Doc, UserCtx, ShareId, undefined, FileCtx) ->
    check(owner, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?write_owner, _Doc, _UserCtx, _ShareId, undefined, _FileCtx) ->
    throw(?EACCES);

% acl is specified but the request is for shared file, check posix perms
check(?read_object, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_object, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?list_container, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?list_container, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_metadata, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_metadata, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?execute, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?execute, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?traverse_container, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?traverse_container, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_attributes, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_attributes, Doc, UserCtx, ShareId, undefined, FileCtx);
check(?read_acl, Doc, UserCtx, ShareId, _, FileCtx) when is_binary(ShareId) ->
    check(?read_acl, Doc, UserCtx, ShareId, undefined, FileCtx);

% acl is specified, check access masks
check(?read_object, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?read_object_mask),
    {ok, FileCtx};
check(?list_container, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?list_container_mask),
    {ok, FileCtx};
check(?write_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?write_object_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?add_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?add_object_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?append_data, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?append_data_mask),
    {ok, FileCtx};
check(?add_subcontainer, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?add_subcontainer_mask),
    {ok, FileCtx};
check(?read_metadata, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?read_metadata_mask),
    {ok, FileCtx};
check(?write_metadata, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?write_metadata_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?execute, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?execute_mask),
    {ok, FileCtx};
check(?traverse_container, #document{value = #file_meta{is_scope = true}}, UserCtx, ShareId, Acl, FileCtx) ->
    validate_scope_access(FileCtx, UserCtx, ShareId),
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?traverse_container_mask),
    {ok, FileCtx};
check(?traverse_container, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?traverse_container_mask),
    {ok, FileCtx};
check(?delete_object, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?delete_object_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?delete_subcontainer, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?delete_subcontainer_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?read_attributes, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?read_attributes_mask),
    {ok, FileCtx};
check(?write_attributes, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?write_attributes_mask),
    {ok, FileCtx};
check(?delete, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?delete_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?read_acl, _, UserCtx, _, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?read_acl_mask),
    {ok, FileCtx};
check(?write_acl, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?write_acl_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
check(?write_owner, _Doc, UserCtx, ShareId, Acl, FileCtx) ->
    acl_logic:check_permission(Acl, user_ctx:get_user(UserCtx), ?write_owner_mask),
    validate_scope_privs(write, FileCtx, UserCtx, ShareId);
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
-spec validate_posix_access(check_permissions:access_definition(), file_ctx:ctx(),
    user_ctx:ctx(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_posix_access(rdwr, FileCtx, UserCtx, ShareId) ->
    {ok, FileCtx2} = validate_posix_access(write, FileCtx, UserCtx, ShareId),
    validate_posix_access(read, FileCtx2, UserCtx, ShareId);
validate_posix_access(AccessType, FileCtx, UserCtx, _ShareId) ->
    {#document{value = #file_meta{owner = OwnerId, mode = Mode}}, FileCtx2} =
        file_ctx:get_file_doc(FileCtx),
    ReqBit =
        case AccessType of
            read -> 8#4;
            write -> 8#2;
            exec -> 8#1
        end,
    CheckType =
        case user_ctx:get_user_id(UserCtx) of
            OwnerId ->
                owner;
            _ ->
                case file_ctx:is_in_user_space_const(FileCtx, UserCtx) of
                    true ->
                        group;
                    false ->
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
%% @end
%%--------------------------------------------------------------------
-spec validate_scope_privs(check_permissions:access_definition(), file_ctx:ctx(),
    user_ctx:ctx(), od_share:id() | undefined) ->
    {ok, file_ctx:ctx()} | no_return().
validate_scope_privs(write, FileCtx, UserCtx, _ShareId) ->
    #document{key = UserId, value = #od_user{eff_groups = UserGroups}} =
        user_ctx:get_user(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, #document{value = #od_space{users = Users, groups = Groups}}} =
        od_space:get(SpaceId, UserId),

    UserPrivs = proplists:get_value(UserId, Users, []),
    case lists:member(?SPACE_WRITE_DATA, UserPrivs) of
        true -> {ok, FileCtx};
        false ->
            SpaceGroupsSet = sets:from_list(proplists:get_keys(Groups)),
            UserGroupsSet = sets:from_list(UserGroups),
            CommonGroups = sets:to_list(sets:intersection(UserGroupsSet, SpaceGroupsSet)),

            ValidGroups = lists:foldl(fun(GroupId, AccIn) ->
                GroupPrivs = proplists:get_value(GroupId, Groups, []),
                case lists:member(?SPACE_WRITE_DATA, GroupPrivs) of
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