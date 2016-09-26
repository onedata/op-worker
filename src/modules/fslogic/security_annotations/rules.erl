%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Security rules
%%% @end
%%%--------------------------------------------------------------------
-module(rules).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([check/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given access_definition is granted to given user.
%% @end
%%--------------------------------------------------------------------
-spec check({term(), FileDoc :: datastore:document() | undefined, UserDoc :: datastore:document(),
    share_info:id(), Acl :: [#accesscontrolentity{}] | undefined}) -> ok | no_return().
% standard posix checks
check({_, _, #document{key = ?ROOT_USER_ID}, _, _}) ->
    ok;
check({root, _, _, _, _}) ->
    throw(?EACCES);
check({owner, #document{value = #file_meta{uid = OwnerId}}, #document{key = OwnerId}, _, _}) ->
    ok;
check({owner, _, _, _, _}) ->
    throw(?EACCES);
check({owner_if_parent_sticky, Doc, UserDoc, ShareId, Acl}) ->
    #document{value = #file_meta{mode = Mode}} = fslogic_utils:get_parent(Doc),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            ok = check({owner, Doc, UserDoc, ShareId, Acl});
        false ->
            ok
    end;
check({{owner, 'or', ?write_attributes}, Doc, #document{key = UserId} = User, ShareId, Acl}) ->
    case Doc#document.value#file_meta.uid of
        UserId ->
            ok;
        _ ->
            check({?write_attributes, Doc, User, ShareId, Acl})
    end;
check({{AccessType1, 'or', AccessType2}, Doc, User, ShareId, Acl}) ->
    case
        {
                catch check({AccessType1, Doc, User, ShareId, Acl}),
                catch check({AccessType2, Doc, User, ShareId, Acl})
        }
    of
        {ok, _} -> ok;
        {_, ok} -> ok;
        _ -> throw(?EACCES)
    end;
check({AccessType, #document{value = #file_meta{is_scope = true}} = Doc, #document{key = UserId} = UserDoc, ShareId, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_scope_access(Doc, UserId, ShareId),
    ok = validate_posix_access(AccessType, Doc, UserId, ShareId),
    ok = validate_scope_privs(AccessType, Doc, UserDoc, ShareId);
check({AccessType, Doc, #document{key = UserId} = UserDoc, ShareId, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_posix_access(AccessType, Doc, UserId, ShareId),
    ok = validate_scope_privs(AccessType, Doc, UserDoc, ShareId);


% if no acl specified, map access masks checks to posix checks
check({?read_object, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({read, Doc, UserDoc, ShareId, undefined});
check({?list_container, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({read, Doc, UserDoc, ShareId, undefined});
check({?write_object, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined});
check({?add_object, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined}),
    ok = check({exec, Doc, UserDoc, ShareId, undefined});
check({?append_data, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined});
check({?add_subcontainer, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined});
check({?read_metadata, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({read, Doc, UserDoc, ShareId, undefined});
check({?write_metadata, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined});
check({?execute, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({exec, Doc, UserDoc, ShareId, undefined});
check({?traverse_container, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({exec, Doc, UserDoc, ShareId, undefined});
check({?delete_object, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined}),
    ok = check({exec, Doc, UserDoc, ShareId, undefined});
check({?delete_subcontainer, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined}),
    ok = check({exec, Doc, UserDoc, ShareId, undefined});
check({?read_attributes, _Doc, _UserDoc, _ShareId, undefined}) ->
    ok;
check({?write_attributes, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({write, Doc, UserDoc, ShareId, undefined});
check({?delete, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({owner_if_parent_sticky, Doc, UserDoc, ShareId, undefined});
check({?read_acl, _Doc, _UserDoc, _ShareId, undefined}) ->
    ok;
check({?write_acl, Doc, UserDoc, ShareId, undefined}) ->
    ok = check({owner, Doc, UserDoc, ShareId, undefined});
check({?write_owner, _, UserDoc, ShareId, undefined}) ->
    ok = check({root, undefined, UserDoc, ShareId, undefined});

% acl is specified but the request is for shared file, check posix perms
check({?read_object, _, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?read_object, Doc, UserDoc, ShareId, undefined});
check({?list_container, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?list_container, Doc, UserDoc, ShareId, undefined});
check({?read_metadata, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?read_metadata, Doc, UserDoc, ShareId, undefined});
check({?execute, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?execute, Doc, UserDoc, ShareId, undefined});
check({?traverse_container, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?traverse_container, Doc, UserDoc, ShareId, undefined});
check({?read_attributes, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?read_attributes, Doc, UserDoc, ShareId, undefined});
check({?read_acl, Doc, UserDoc, ShareId, _}) when is_binary(ShareId) ->
    ok = check({?read_acl, Doc, UserDoc, ShareId, undefined});

% acl is specified, check access masks
check({?read_object, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_object_mask);
check({?list_container, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?list_container_mask);
check({?write_object, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_object_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?add_object, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_object_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?append_data, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?append_data_mask);
check({?add_subcontainer, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_subcontainer_mask);
check({?read_metadata, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_metadata_mask);
check({?write_metadata, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_metadata_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?execute, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?execute_mask);
check({?traverse_container, #document{value = #file_meta{is_scope = true}} = FileDoc, #document{key = UserId} = UserDoc, ShareId, Acl}) ->
    ok = validate_scope_access(FileDoc, UserId, ShareId),
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask);
check({?traverse_container, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask);
check({?delete_object, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_object_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?delete_subcontainer, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_subcontainer_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?read_attributes, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_attributes_mask);
check({?write_attributes, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_attributes_mask);
check({?delete, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?read_acl, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_acl_mask);
check({?write_acl, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_acl_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({?write_owner, Doc, UserDoc, ShareId, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_owner_mask),
    ok = validate_scope_privs(write, Doc, UserDoc, ShareId);
check({Perm, File, User, ShareId, Acl}) ->
    ?error_stacktrace("Unknown permission check rule: (~p, ~p, ~p, ~p, ~p)", [Perm, File, User, ShareId, Acl]),
    throw(?EACCES).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks whether given user has given permission on given file (POSIX permission check).
%%--------------------------------------------------------------------
-spec validate_posix_access(AccessType :: check_permissions:check_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id(), ShareId :: share_info:id()) -> ok | no_return().
validate_posix_access(rdwr, FileDoc, UserId, ShareId) ->
    ok = validate_posix_access(write, FileDoc, UserId, ShareId),
    ok = validate_posix_access(read, FileDoc, UserId, ShareId);
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId, ShareId) ->
    ReqBit =
        case AccessType of
            read -> 8#4;
            write -> 8#2;
            exec -> 8#1
        end,

    CheckType =
        case UserId of
            OwnerId ->
                owner;
            _ ->
                {ok, #document{value = #onedata_user{spaces = Spaces}}} = onedata_user:get(UserId),
                {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
                case catch lists:keymember(fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID), 1, Spaces) of
                    true ->
                        group;
                    _ ->
                        other
                end
        end,
    ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as ~p.", [UserId, ReqBit, FileDoc, Mode, CheckType]),

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
        true -> ok;
        false -> throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc Checks whether given user has permission to see given scope file.
%%      This function is always called before validate_posix_access/3 and shall handle all special cases.
%%--------------------------------------------------------------------
-spec validate_scope_access(FileDoc :: datastore:document(), UserId :: onedata_user:id(), ShareId :: share_info:id()) -> ok | no_return().
validate_scope_access(_FileDoc, ?GUEST_USER_ID, undefined) ->
    throw(?ENOENT);
validate_scope_access(FileDoc, UserId, undefined) ->
    RootDirUUID = fslogic_uuid:user_root_dir_uuid(UserId),
    case file_meta:is_root_dir(FileDoc)
        orelse RootDirUUID =:= FileDoc#document.key of
        true -> ok;
        false ->
            try fslogic_spaces:get_space(FileDoc, UserId) of
                _ -> ok
            catch
                {not_a_space, _} -> throw(?ENOENT)
            end
    end;
validate_scope_access(_FileDoc, _UserId, _ShareId) ->
    ok.


%%--------------------------------------------------------------------
%% @doc Checks whether given user has permission to access given file with respect to scope settings.
%%--------------------------------------------------------------------
-spec validate_scope_privs(AccessType :: check_permissions:check_type(), FileDoc :: datastore:document(),
    UserDoc :: datastore:document(), ShareId :: share_info:id()) -> ok | no_return().
validate_scope_privs(write, FileDoc, #document{key = UserId, value = #onedata_user{effective_group_ids = UserGroups}}, _ShareId) ->
    {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
    {ok, #document{value = #space_info{users = Users, groups = Groups}}} =
        space_info:get(fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID), UserId),

    SpeceWritePriv = space_write_files,

    UserPrivs = proplists:get_value(UserId, Users, []),
    case lists:member(SpeceWritePriv, UserPrivs) of
        true -> ok;
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
                _ -> ok
            end
    end;
validate_scope_privs(_, _, _, _) ->
    ok.