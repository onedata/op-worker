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
                catch check({AccessType1, Doc, User, ShareId,  Acl}),
                catch check({AccessType2, Doc, User, ShareId,  Acl})
        }
    of
        {ok, _} -> ok;
        {_, ok} -> ok;
        _ -> throw(?EACCES)
    end;
check({AccessType, #document{value = #file_meta{is_scope = true}} = Doc, #document{key = UserId}, ShareId, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_scope_access(Doc, UserId, ShareId),
    ok = validate_posix_access(AccessType, Doc, UserId, ShareId);
check({AccessType, Doc, #document{key = UserId}, ShareId, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_posix_access(AccessType, Doc, UserId, ShareId);


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
check({?write_object, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_object_mask);
check({?add_object, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_object_mask);
check({?append_data, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?append_data_mask);
check({?add_subcontainer, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_subcontainer_mask);
check({?read_metadata, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_metadata_mask);
check({?write_metadata, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_metadata_mask);
check({?execute, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?execute_mask);
check({?traverse_container, #document{value = #file_meta{is_scope = true}} = FileDoc, #document{key = UserId} = UserDoc, ShareId, Acl}) ->
    ok = validate_scope_access(FileDoc, UserId, ShareId),
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask);
check({?traverse_container, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask);
check({?delete_object, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_object_mask);
check({?delete_subcontainer, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_subcontainer_mask);
check({?read_attributes, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_attributes_mask);
check({?write_attributes, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_attributes_mask);
check({?delete, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_mask);
check({?read_acl, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_acl_mask);
check({?write_acl, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_acl_mask);
check({?write_owner, _, UserDoc, _, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_owner_mask);
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
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId, undefined) ->
    ReqBit =
        case AccessType of
            rdwr -> 8#6;
            read -> 8#4;
            write -> 8#2;
            exec -> 8#1
        end,

    IsAccessable =
        case UserId of
            OwnerId ->
                ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as owner.", [UserId, ReqBit, FileDoc, Mode]),
                ((ReqBit bsl 6) band Mode) =:= (ReqBit bsl 6);
            _ ->
                {ok, #document{value = #onedata_user{spaces = Spaces}}} = onedata_user:get(UserId),
                {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
                case catch lists:keymember(fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID), 1, Spaces) of
                    true ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as space member.", [UserId, ReqBit, FileDoc, Mode]),
                        ((ReqBit bsl 3) band Mode) =:= (ReqBit bsl 3);
                    _ ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as other (Spaces ~p, scope ~p).", [UserId, ReqBit, FileDoc, Mode, Spaces, ScopeUUID]),
                        (ReqBit band Mode) =:= ReqBit
                end
        end,

    case IsAccessable of
        true -> ok;
        false -> throw(?EACCES)
    end;
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId, _ShareId) ->
    validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId, undefined) orelse
        AccessType =:= read orelse AccessType =:= exec.

%%--------------------------------------------------------------------
%% @doc Checks whether given user has permission to see given scope file.
%%      This function is always called before validate_posix_access/3 and shall handle all special cases.
%%--------------------------------------------------------------------
-spec validate_scope_access(FileDoc :: datastore:document(), UserId :: onedata_user:id(), ShareId :: share_info:id()) -> ok | no_return().
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
