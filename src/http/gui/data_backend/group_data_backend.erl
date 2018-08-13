%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the group model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(group_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/api_errors.hrl").


%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-export([group_record/1, group_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(<<"group">>, GroupId) ->
    SessionId = gui_session:get_session_id(),
    % Check if the user belongs to this group - he should be able to get
    % protected group data.
    case group_logic:get_protected_data(SessionId, GroupId) of
        {ok, _} ->
            {ok, group_record(GroupId)};
        _ ->
            gui_error:unauthorized()
    end;

% PermissionsRecord matches <<"group-(user|group)-(list|permission)">>
find_record(PermissionsRecord, RecordId) ->
    GroupId = permission_record_to_group_id(PermissionsRecord, RecordId),
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    % Make sure that user is allowed to view requested privileges - he must have
    % view privileges in this group.
    Authorized = group_logic:has_eff_privilege(
        SessionId, GroupId, UserId, ?GROUP_VIEW
    ),
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            case PermissionsRecord of
                <<"group-user-list">> ->
                    {ok, group_user_list_record(RecordId)};
                <<"group-group-list">> ->
                    {ok, group_group_list_record(RecordId)};
                <<"group-user-permission">> ->
                    {ok, group_user_permission_record(RecordId)};
                <<"group-group-permission">> ->
                    {ok, group_group_permission_record(RecordId)}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"group">>) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(<<"group">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(<<"group">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"group">>, Data) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    Name = proplists:get_value(<<"name">>, Data),
    case Name of
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot create group with empty name.">>);
        _ ->
            case user_logic:create_group(SessionId, UserId, Name) of
                {ok, GroupId} ->
                    gui_async:push_updated(
                        <<"user">>,
                        user_data_backend:user_record(SessionId, UserId)
                    ),
                    % This group was created by this user -> he has view privs.
                    GroupRecord = group_record(GroupId, true),
                    {ok, GroupRecord};
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot create new group due to unknown error.">>)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"group">>, GroupId, [{<<"name">>, Name}]) ->
    SessionId = gui_session:get_session_id(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set group name to empty string.">>);
        NewName ->
            case group_logic:update_name(SessionId, GroupId, NewName) of
                ok ->
                    ok;
                ?ERROR_FORBIDDEN ->
                    gui_error:report_warning(
                        <<"You do not have privileges to modify this group.">>);
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change group name due to unknown error.">>)
            end
    end;

update_record(<<"group-user-permission">>, AssocId, Data) ->
    SessionId = gui_session:get_session_id(),
    {UserId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            direct_users = UsersAndPerms
        }}} = group_logic:get(SessionId, GroupId),
    UserPerms = maps:get(UserId, UsersAndPerms),
    OzVersion = oneprovider:get_oz_version(),

    {Granted, Revoked} = lists:foldl(
        fun({PermGui, Flag}, {GrantedAcc, RevokedAcc}) ->
            Perm = perm_gui_to_db(PermGui, OzVersion),
            case {Flag, lists:member(Perm, UserPerms)} of
                {false, false} ->
                    {GrantedAcc, RevokedAcc};
                {false, true} ->
                    {GrantedAcc, [Perm | RevokedAcc]};
                {true, false} ->
                    {[Perm | GrantedAcc], RevokedAcc};
                {true, true} ->
                    {GrantedAcc, RevokedAcc}
            end
        end, {[], []}, Data),

    GrantResult = group_logic:update_user_privileges(
        SessionId, GroupId, UserId, lists:usort(Granted), grant
    ),
    RevokeResult = group_logic:update_user_privileges(
        SessionId, GroupId, UserId, lists:usort(Revoked), revoke
    ),

    case {GrantResult, RevokeResult} of
        {ok, ok} ->
            ok;
        {?ERROR_FORBIDDEN, _} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify group privileges.">>);
        {_, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end;

update_record(<<"group-group-permission">>, AssocId, Data) ->
    SessionId = gui_session:get_session_id(),
    {ChildGroupId, ParentGroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            direct_children = GroupsAndPerms
        }}} = group_logic:get(SessionId, ParentGroupId),
    GroupPerms = maps:get(ChildGroupId, GroupsAndPerms),
    OzVersion = oneprovider:get_oz_version(),
    {Granted, Revoked} = lists:foldl(
        fun({PermGui, Flag}, {GrantedAcc, RevokedAcc}) ->
            Perm = perm_gui_to_db(PermGui, OzVersion),
            case {Flag, lists:member(Perm, GroupPerms)} of
                {false, false} ->
                    {GrantedAcc, RevokedAcc};
                {false, true} ->
                    {GrantedAcc, [Perm | RevokedAcc]};
                {true, false} ->
                    {[Perm | GrantedAcc], RevokedAcc};
                {true, true} ->
                    {GrantedAcc, RevokedAcc}
            end
        end, {[], []}, Data),

    GrantResult = group_logic:update_child_privileges(
        SessionId, ParentGroupId, ChildGroupId, lists:usort(Granted), grant
    ),
    RevokeResult = group_logic:update_child_privileges(
        SessionId, ParentGroupId, ChildGroupId, lists:usort(Revoked), revoke
    ),

    case {GrantResult, RevokeResult} of
        {ok, ok} ->
            ok;
        {?ERROR_FORBIDDEN, _} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify group privileges.">>);
        {_, _} ->
            gui_error:report_warning(
                <<"Cannot change group privileges due to unknown error.">>)
    end;

update_record(_ResourceType, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"group">>, GroupId) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case group_logic:delete(SessionId, GroupId) of
        ok ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            ok;
        ?ERROR_FORBIDDEN ->
            gui_error:report_warning(
                <<"You do not have privileges to modify this group.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot remove group due to unknown error.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant group record based on group id. Automatically
%% check if the user has view privileges in that group and returns proper data.
%% @end
%%--------------------------------------------------------------------
-spec group_record(GroupId :: binary()) -> proplists:proplist().
group_record(GroupId) ->
    SessionId = gui_session:get_session_id(),
    % Check if that user has view privileges in that group
    HasViewPrivileges = group_logic:has_eff_privilege(
        SessionId, GroupId, gui_session:get_user_id(), ?GROUP_VIEW
    ),
    group_record(GroupId, HasViewPrivileges).


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant group record based on group id. Allows to
%% override HasViewPrivileges.
%% @end
%%--------------------------------------------------------------------
-spec group_record(GroupId :: binary(), HasViewPrivileges :: boolean()) ->
    proplists:proplist().
group_record(GroupId, HasViewPrivs) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    {ok, #document{value = Group}} = case HasViewPrivs of
        true -> group_logic:get(SessionId, GroupId);
        false -> group_logic:get_protected_data(SessionId, GroupId)
    end,

    % Depending on view privileges, show or hide info about members and privs
    {GroupUserListId, GroupGroupListId, Parents, Children} = case HasViewPrivs of
        true ->
            #od_group{
                direct_children = ChildrenWithPerms,
                direct_parents = ParentGroups
            } = Group,
            ChildGroups = maps:keys(ChildrenWithPerms),
            {GroupId, GroupId, ParentGroups, ChildGroups};
        false ->
            {null, null, [], []}
    end,
    [
        {<<"id">>, GroupId},
        {<<"name">>, Group#od_group.name},
        {<<"hasViewPrivilege">>, HasViewPrivs},
        {<<"userList">>, GroupUserListId},
        {<<"groupList">>, GroupGroupListId},
        {<<"parentGroups">>, Parents},
        {<<"childGroups">>, Children},
        {<<"user">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-user-list record based on group id.
%% @end
%%--------------------------------------------------------------------
-spec group_user_list_record(SpaceId :: binary()) -> proplists:proplist().
group_user_list_record(GroupId) ->
    SessionId = gui_session:get_session_id(),
    {ok, #document{value = #od_group{
        direct_users = UsersWithPerms
    }}} = group_logic:get(SessionId, GroupId),
    UserPermissions = lists:map(
        fun(UsId) ->
            op_gui_utils:ids_to_association(UsId, GroupId)
        end, maps:keys(UsersWithPerms)),
    [
        {<<"id">>, GroupId},
        {<<"group">>, GroupId},
        {<<"permissions">>, UserPermissions}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-group-list record based on group id.
%% @end
%%--------------------------------------------------------------------
-spec group_group_list_record(SpaceId :: binary()) -> proplists:proplist().
group_group_list_record(GroupId) ->
    SessionId = gui_session:get_session_id(),
    {ok, #document{value = #od_group{
        direct_children = GroupsWithPerms
    }}} = group_logic:get(SessionId, GroupId),
    GroupPermissions = lists:map(
        fun(GrId) ->
            op_gui_utils:ids_to_association(GrId, GroupId)
        end, maps:keys(GroupsWithPerms)),
    [
        {<<"id">>, GroupId},
        {<<"group">>, GroupId},
        {<<"permissions">>, GroupPermissions}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant group-user-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec group_user_permission_record(AssocId :: binary()) -> proplists:proplist().
group_user_permission_record(AssocId) ->
    SessionId = gui_session:get_session_id(),
    {UserId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            direct_users = UsersAndPerms
        }}} = group_logic:get(SessionId, GroupId),
    UserPerms = maps:get(UserId, UsersAndPerms),
    PermsMapped = perms_db_to_gui(UserPerms),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"group">>, GroupId},
        {<<"systemUserId">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant group-group-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec group_group_permission_record(AssocId :: binary()) ->
    proplists:proplist().
group_group_permission_record(AssocId) ->
    SessionId = gui_session:get_session_id(),
    {ChildGroupId, ParentGroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            direct_children = GroupsAndPerms
        }}} = group_logic:get(SessionId, ParentGroupId),
    GroupPerms = maps:get(ChildGroupId, GroupsAndPerms),
    PermsMapped = perms_db_to_gui(GroupPerms),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"group">>, ParentGroupId},
        {<<"systemGroupId">>, ChildGroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a list of group permissions from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perms_db_to_gui(atom()) -> proplists:proplist().
perms_db_to_gui(Perms) ->
    OzVersion = oneprovider:get_oz_version(),
    lists:foldl(
        fun(Perm, Acc) ->
            case perm_db_to_gui(Perm, OzVersion) of
                undefined ->
                    Acc;
                PermBin ->
                    HasPerm = lists:member(Perm, Perms),
                    [{PermBin, HasPerm} | Acc]
            end
        end, [], all_group_privileges(OzVersion)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a group permission from internal form to client-compliant form.
%% The logic is different depending on OZ version to keep backward compatibility
%% with deprecated perms.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(atom(), binary()) -> binary() | undefined.
perm_db_to_gui(Perm, <<"18.02", _/binary>>) -> perm_db_to_gui_18_02(Perm);
perm_db_to_gui(Perm, _) -> perm_db_to_gui(Perm).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a group permission from client-compliant form to internal form.
%% The logic is different depending on OZ version to keep backward compatibility
%% with deprecated perms.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary(), binary()) -> atom().
perm_gui_to_db(Perm, <<"18.02", _/binary>>) -> perm_gui_to_db_18_02(Perm);
perm_gui_to_db(Perm, _) -> perm_gui_to_db(Perm).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the list of all group privileges.
%% The logic is different depending on OZ version to keep backward compatibility
%% with deprecated perms.
%% @end
%%--------------------------------------------------------------------
-spec all_group_privileges(binary()) -> [atom()].
all_group_privileges(<<"18.02", _/binary>>) ->
    privileges:group_privileges();
all_group_privileges(_) -> [
    ?GROUP_VIEW,
    ?GROUP_UPDATE,
    ?GROUP_SET_PRIVILEGES,
    ?GROUP_DELETE,
    ?GROUP_INVITE_USER,
    ?GROUP_REMOVE_USER,
    group_invite_child,
    group_remove_child,
    group_join_parent,
    group_leave_parent,
    ?GROUP_CREATE_SPACE,
    ?GROUP_JOIN_SPACE,
    ?GROUP_LEAVE_SPACE
].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a group permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(atom()) -> binary() | undefined.
perm_db_to_gui(?GROUP_VIEW) -> <<"permViewGroup">>;
perm_db_to_gui(?GROUP_UPDATE) -> <<"permModifyGroup">>;
perm_db_to_gui(?GROUP_SET_PRIVILEGES) -> <<"permSetPrivileges">>;
perm_db_to_gui(?GROUP_DELETE) -> <<"permRemoveGroup">>;
perm_db_to_gui(?GROUP_INVITE_USER) -> <<"permInviteUser">>;
perm_db_to_gui(?GROUP_REMOVE_USER) -> <<"permRemoveUser">>;
perm_db_to_gui(group_invite_child) -> <<"permInviteGroup">>;
perm_db_to_gui(group_remove_child) -> <<"permRemoveSubgroup">>;
perm_db_to_gui(group_join_parent) -> <<"permJoinGroup">>;
perm_db_to_gui(group_leave_parent) -> <<"permLeaveGroup">>;
perm_db_to_gui(?GROUP_CREATE_SPACE) -> <<"permCreateSpace">>;
perm_db_to_gui(?GROUP_JOIN_SPACE) -> <<"permJoinSpace">>;
perm_db_to_gui(?GROUP_LEAVE_SPACE) -> <<"permLeaveSpace">>;
perm_db_to_gui(_) -> undefined.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a group permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary()) -> atom().
perm_gui_to_db(<<"permViewGroup">>) -> ?GROUP_VIEW;
perm_gui_to_db(<<"permModifyGroup">>) -> ?GROUP_UPDATE;
perm_gui_to_db(<<"permSetPrivileges">>) -> ?GROUP_SET_PRIVILEGES;
perm_gui_to_db(<<"permRemoveGroup">>) -> ?GROUP_DELETE;
perm_gui_to_db(<<"permInviteUser">>) -> ?GROUP_INVITE_USER;
perm_gui_to_db(<<"permRemoveUser">>) -> ?GROUP_REMOVE_USER;
perm_gui_to_db(<<"permInviteGroup">>) -> group_invite_child;
perm_gui_to_db(<<"permRemoveSubgroup">>) -> group_remove_child;
perm_gui_to_db(<<"permJoinGroup">>) -> group_join_parent;
perm_gui_to_db(<<"permLeaveGroup">>) -> group_leave_parent;
perm_gui_to_db(<<"permCreateSpace">>) -> ?GROUP_CREATE_SPACE;
perm_gui_to_db(<<"permJoinSpace">>) -> ?GROUP_JOIN_SPACE;
perm_gui_to_db(<<"permLeaveSpace">>) -> ?GROUP_LEAVE_SPACE.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% DEPRECATED - changing in future release (> 18.02)
%% Converts a group permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui_18_02(atom()) -> binary() | undefined.
perm_db_to_gui_18_02(?GROUP_VIEW) -> <<"permViewGroup">>;
perm_db_to_gui_18_02(?GROUP_UPDATE) -> <<"permModifyGroup">>;
perm_db_to_gui_18_02(?GROUP_SET_PRIVILEGES) -> <<"permSetPrivileges">>;
perm_db_to_gui_18_02(?GROUP_DELETE) -> <<"permRemoveGroup">>;
perm_db_to_gui_18_02(?GROUP_INVITE_USER) -> <<"permInviteUser">>;
perm_db_to_gui_18_02(?GROUP_REMOVE_USER) -> <<"permRemoveUser">>;
perm_db_to_gui_18_02(?GROUP_INVITE_GROUP) -> <<"permInviteGroup">>;
perm_db_to_gui_18_02(?GROUP_REMOVE_GROUP) -> <<"permRemoveSubgroup">>;
perm_db_to_gui_18_02(?GROUP_JOIN_GROUP) -> <<"permJoinGroup">>;
perm_db_to_gui_18_02(?GROUP_LEAVE_GROUP) -> <<"permLeaveGroup">>;
perm_db_to_gui_18_02(?GROUP_CREATE_SPACE) -> <<"permCreateSpace">>;
perm_db_to_gui_18_02(?GROUP_JOIN_SPACE) -> <<"permJoinSpace">>;
perm_db_to_gui_18_02(?GROUP_LEAVE_SPACE) -> <<"permLeaveSpace">>;
perm_db_to_gui_18_02(_) -> undefined.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% DEPRECATED - changing in future release (> 18.02)
%% Converts a group permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db_18_02(binary()) -> atom().
perm_gui_to_db_18_02(<<"permViewGroup">>) -> ?GROUP_VIEW;
perm_gui_to_db_18_02(<<"permModifyGroup">>) -> ?GROUP_UPDATE;
perm_gui_to_db_18_02(<<"permSetPrivileges">>) -> ?GROUP_SET_PRIVILEGES;
perm_gui_to_db_18_02(<<"permRemoveGroup">>) -> ?GROUP_DELETE;
perm_gui_to_db_18_02(<<"permInviteUser">>) -> ?GROUP_INVITE_USER;
perm_gui_to_db_18_02(<<"permRemoveUser">>) -> ?GROUP_REMOVE_USER;
perm_gui_to_db_18_02(<<"permInviteGroup">>) -> ?GROUP_INVITE_GROUP;
perm_gui_to_db_18_02(<<"permRemoveSubgroup">>) -> ?GROUP_REMOVE_GROUP;
perm_gui_to_db_18_02(<<"permJoinGroup">>) -> ?GROUP_JOIN_GROUP;
perm_gui_to_db_18_02(<<"permLeaveGroup">>) -> ?GROUP_LEAVE_GROUP;
perm_gui_to_db_18_02(<<"permCreateSpace">>) -> ?GROUP_CREATE_SPACE;
perm_gui_to_db_18_02(<<"permJoinSpace">>) -> ?GROUP_JOIN_SPACE;
perm_gui_to_db_18_02(<<"permLeaveSpace">>) -> ?GROUP_LEAVE_SPACE.


-spec permission_record_to_group_id(binary(), binary()) -> binary().
permission_record_to_group_id(<<"group-user-list">>, RecordId) ->
    RecordId;
permission_record_to_group_id(<<"group-group-list">>, RecordId) ->
    RecordId;
permission_record_to_group_id(_, RecordId) ->
    % covers <<"group-(user|group)-permission">>
    {_, Id} = op_gui_utils:association_to_ids(RecordId),
    Id.
