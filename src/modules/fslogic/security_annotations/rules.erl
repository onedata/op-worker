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
-include_lib("annotations/include/annotations.hrl").

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
-spec check({term(), FileDoc :: datastore:document(), UserDoc :: datastore:document(),
    Acl :: [#accesscontrolentity{}]}) -> ok | no_return().
% standard posix checks
check({_, _, #document{key = ?ROOT_USER_ID}, _}) ->
    ok;
check({root, _, _, _}) ->
    throw(?EACCES);
check({owner, #document{value = #file_meta{uid = OwnerId}}, #document{key = OwnerId}, _}) ->
    ok;
check({owner_if_parent_sticky, Doc, UserDoc, Acl}) ->
    #document{value = #file_meta{mode = Mode}} = fslogic_utils:get_parent(Doc),
    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            ok = check({owner, Doc, UserDoc, Acl});
        false ->
            ok
    end;
check({AccessType, #document{value = #file_meta{is_scope = true}} = Doc, #document{key = UserId}, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_scope_access(AccessType, Doc, UserId),
    ok = validate_posix_access(AccessType, Doc, UserId);
check({AccessType, Doc, #document{key = UserId}, undefined}) when
    AccessType =:= read orelse AccessType =:= write orelse AccessType =:= exec orelse AccessType =:= rdwr ->
    ok = validate_posix_access(AccessType, Doc, UserId);


% if no acl specified, map access masks checks to posix checks
check({?read_object, Doc, UserDoc, undefined}) ->
    ok = check({read, Doc, UserDoc, undefined});
check({?list_container, Doc, UserDoc, undefined}) ->
    ok = check({read, Doc, UserDoc, undefined}),
    ok = check({exec, Doc, UserDoc, undefined});
check({?write_object, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined});
check({?add_object, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined}),
    ok = check({exec, Doc, UserDoc, undefined});
check({?append_data, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined});
check({?add_subcontainer, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined});
check({?read_metadata, Doc, UserDoc, undefined}) ->
    ok = check({read, Doc, UserDoc, undefined});
check({?write_metadata, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined});
check({?execute, Doc, UserDoc, undefined}) ->
    ok = check({exec, Doc, UserDoc, undefined});
check({?traverse_container, Doc, UserDoc, undefined}) ->
    ok = check({exec, Doc, UserDoc, undefined});
check({?delete_object, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined}),
    ok = check({exec, Doc, UserDoc, undefined});
check({?delete_subcontainer, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined}),
    ok = check({exec, Doc, UserDoc, undefined});
check({?read_attributes, _Doc, _UserDoc, undefined}) ->
    ok;
check({?write_attributes, Doc, UserDoc, undefined}) ->
    ok = check({write, Doc, UserDoc, undefined});
check({?delete, Doc, UserDoc, undefined}) ->
    ok = check({owner_if_parent_sticky, Doc, UserDoc, undefined});
check({?read_acl, _Doc, _UserDoc, undefined}) ->
    ok;
check({?write_acl, Doc, UserDoc, undefined}) ->
    ok = check({owner, Doc, UserDoc, undefined});
check({?write_owner, _, UserDoc, undefined}) ->
    ok = check({root, undefined, UserDoc, undefined});

% acl is specified, check access masks
check({?read_object, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_object_mask);
check({?list_container, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?list_container_mask);
check({?write_object, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_object_mask);
check({?add_object, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_object_mask);
check({?append_data, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?append_data_mask);
check({?add_subcontainer, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?add_subcontainer_mask);
check({?read_metadata, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_metadata_mask);
check({?write_metadata, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_metadata_mask);
check({?execute, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?execute_mask);
check({?traverse_container, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?traverse_container_mask);
check({?delete_object, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_object_mask);
check({?delete_subcontainer, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_subcontainer_mask);
check({?read_attributes, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_attributes_mask);
check({?write_attributes, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_attributes_mask);
check({?delete, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?delete_mask);
check({?read_acl, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?read_acl_mask);
check({?write_acl, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_acl_mask);
check({?write_owner, _, UserDoc, Acl}) ->
    fslogic_acl:check_permission(Acl, UserDoc, ?write_owner_mask);

check({Perm, File, User, Acl}) ->
    ?error_stacktrace("Unknown permission check rule: (~p, ~p, ~p, ~p)", [Perm, File, User, Acl]),
    throw(?EACCES).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks whether given user has given permission on given file (POSIX permission check).
%%--------------------------------------------------------------------
-spec validate_posix_access(AccessType :: check_permissions:access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId) ->
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
                {ok, #document{value = #onedata_user{space_ids = Spaces}}} = onedata_user:get(UserId),
                {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
                case lists:member(fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID), Spaces) of
                    true ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as space member.", [UserId, ReqBit, FileDoc, Mode]),
                        ((ReqBit bsl 3) band Mode) =:= (ReqBit bsl 3);
                    false ->
                        ?debug("Require ~p to have ~.8B mode on file ~p with mode ~.8B as other (Spaces ~p, scope ~p).", [UserId, ReqBit, FileDoc, Mode, Spaces, ScopeUUID]),
                        (ReqBit band Mode) =:= ReqBit
                end
        end,

    case IsAccessable of
        true -> ok;
        false -> throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc Checks whether given user has given permission on scope given file.
%%      This function is always called before validate_posix_access/3 and shall handle all special cases.
%% @todo: Implement this method. Currently expected behaviour is to throw ENOENT instead EACCES for all spaces dirs.
%%--------------------------------------------------------------------
-spec validate_scope_access(AccessType :: check_permissions:access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_scope_access(_AccessType, _FileDoc, _UserId) ->
    ok.
