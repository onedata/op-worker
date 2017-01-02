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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
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
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"group">>, GroupId) ->
    UserId = gui_session:get_user_id(),
    % Check if the user belongs to this group
    case group_logic:has_effective_user(GroupId, UserId) of
        false ->
            gui_error:unauthorized();
        true ->
            {ok, group_record(GroupId)}
    end;

% PermissionsRecord matches <<"group-(user|group)-(list|permission)">>
find(PermissionsRecord, RecordId) ->
    GroupId = case PermissionsRecord of
        <<"group-user-list">> ->
            RecordId;
        <<"group-group-list">> ->
            RecordId;
        _ -> % covers <<"group-(user|group)-permission">>
            {_, Id} = op_gui_utils:association_to_ids(RecordId),
            Id
    end,
    UserId = gui_session:get_user_id(),
    % Make sure that user is allowed to view requested privileges - he must have
    % view privileges in this group.
    Authorized = group_logic:has_effective_privilege(
        GroupId, UserId, group_view_data
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
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"group">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"group">>, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    Name = proplists:get_value(<<"name">>, Data),
    case Name of
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot create group with empty name.">>);
        _ ->
            case group_logic:create(UserAuth, #od_group{name = Name}) of
                {ok, GroupId} ->
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
    UserAuth = op_gui_utils:get_user_auth(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set group name to empty string.">>);
        NewName ->
            case group_logic:set_name(UserAuth, GroupId, NewName) of
                ok ->
                    ok;
                {error, {403, <<>>, <<>>}} ->
                    gui_error:report_warning(
                        <<"You do not have privileges to modify this group.">>);
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change group name due to unknown error.">>)
            end
    end;

update_record(<<"group-user-permission">>, AssocId, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {UserId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            users = UsersAndPerms
        }}} = group_logic:get(UserAuth, GroupId),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    NewUserPerms = lists:foldl(
        fun({PermGui, Flag}, PermsAcc) ->
            Perm = perm_gui_to_db(PermGui),
            case Flag of
                true ->
                    PermsAcc ++ [Perm];
                false ->
                    PermsAcc -- [Perm]
            end
        end, UserPerms, Data),

    Result = group_logic:set_user_privileges(
        UserAuth, GroupId, UserId, lists:usort(NewUserPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify group privileges.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end;

update_record(<<"group-group-permission">>, AssocId, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {ChildGroupId, ParentGroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            children = GroupsAndPerms
        }}} = group_logic:get(UserAuth, ParentGroupId),
    GroupPerms = proplists:get_value(ChildGroupId, GroupsAndPerms),
    NewGroupPerms = lists:foldl(
        fun({PermGui, Flag}, PermsAcc) ->
            Perm = perm_gui_to_db(PermGui),
            case Flag of
                true ->
                    PermsAcc ++ [Perm];
                false ->
                    PermsAcc -- [Perm]
            end
        end, GroupPerms, Data),

    Result = group_logic:set_group_privileges(
        UserAuth, ParentGroupId, ChildGroupId, lists:usort(NewGroupPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify group privileges.">>);
        {error, _} ->
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
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:delete(UserAuth, GroupId) of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
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
    % Check if that user has view privileges in that group
    HasViewPrivileges = group_logic:has_effective_privilege(
        GroupId, gui_session:get_user_id(), group_view_data
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
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #od_group{
            name = Name,
            children = ChildrenWithPerms,
            parents = ParentGroups
        }}} = group_logic:get(UserAuth, GroupId),
    {ChildGroups, _} = lists:unzip(ChildrenWithPerms),

    % Depending on view privileges, show or hide info about members and privs
    {GroupUserList, GroupGroupList, Parents, Children} = case HasViewPrivs of
        true ->
            {GroupId, GroupId, ParentGroups, ChildGroups};
        false ->
            {null, null, [], []}
    end,
    [
        {<<"id">>, GroupId},
        {<<"name">>, Name},
        {<<"hasViewPrivilege">>, HasViewPrivs},
        {<<"userList">>, GroupUserList},
        {<<"groupList">>, GroupGroupList},
        {<<"parentGroups">>, Parents},
        {<<"childGroups">>, Children}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-user-list record based on group id.
%% @end
%%--------------------------------------------------------------------
-spec group_user_list_record(SpaceId :: binary()) -> proplists:proplist().
group_user_list_record(GroupId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{value = #od_group{
        users = UsersWithPerms
    }}} = group_logic:get(UserAuth, GroupId),
    UserPermissions = lists:map(
        fun({UsId, _UsPerms}) ->
            op_gui_utils:ids_to_association(UsId, GroupId)
        end, UsersWithPerms),
    [
        {<<"id">>, GroupId},
        {<<"group">>, GroupId},
        {<<"userPermissions">>, UserPermissions}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-group-list record based on group id.
%% @end
%%--------------------------------------------------------------------
-spec group_group_list_record(SpaceId :: binary()) -> proplists:proplist().
group_group_list_record(GroupId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{value = #od_group{
        children = GroupsWithPerms
    }}} = group_logic:get(UserAuth, GroupId),
    GroupPermissions = lists:map(
        fun({GrId, _GrPerms}) ->
            op_gui_utils:ids_to_association(GrId, GroupId)
        end, GroupsWithPerms),
    [
        {<<"id">>, GroupId},
        {<<"group">>, GroupId},
        {<<"groupPermissions">>, GroupPermissions}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant group-user-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec group_user_permission_record(AssocId :: binary()) -> proplists:proplist().
group_user_permission_record(AssocId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {UserId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            users = UsersAndPerms
        }}} = group_logic:get(UserAuth, GroupId),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    PermsMapped = lists:map(
        fun(Perm) ->
            HasPerm = lists:member(Perm, UserPerms),
            {perm_db_to_gui(Perm), HasPerm}
        end, privileges:group_privileges()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"userList">>, GroupId},
        {<<"systemUser">>, UserId}
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
    UserAuth = op_gui_utils:get_user_auth(),
    {ChildGroupId, ParentGroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_group{
            children = GroupsAndPerms
        }}} = group_logic:get(UserAuth, ParentGroupId),
    GroupPerms = proplists:get_value(ChildGroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(Perm) ->
            HasPerm = lists:member(Perm, GroupPerms),
            {perm_db_to_gui(Perm), HasPerm}
        end, privileges:group_privileges()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"groupList">>, ParentGroupId},
        {<<"systemGroup">>, ChildGroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(atom()) -> binary().
perm_db_to_gui(group_view_data) -> <<"permViewGroup">>;
perm_db_to_gui(group_change_data) -> <<"permModifyGroup">>;
perm_db_to_gui(group_set_privileges) -> <<"permSetPrivileges">>;
perm_db_to_gui(group_remove) -> <<"permRemoveGroup">>;
perm_db_to_gui(group_invite_user) -> <<"permInviteUser">>;
perm_db_to_gui(group_remove_user) -> <<"permRemoveUser">>;
perm_db_to_gui(group_invite_group) -> <<"permInviteGroup">>;
perm_db_to_gui(group_remove_group) -> <<"permRemoveSubgroup">>;
perm_db_to_gui(group_join_group) -> <<"permJoinGroup">>;
perm_db_to_gui(group_create_space) -> <<"permCreateSpace">>;
perm_db_to_gui(group_join_space) -> <<"permJoinSpace">>;
perm_db_to_gui(group_leave_space) -> <<"permLeaveSpace">>;
perm_db_to_gui(group_create_space_token) -> <<"permGetSupport">>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary()) -> atom().
perm_gui_to_db(<<"permViewGroup">>) -> group_view_data;
perm_gui_to_db(<<"permModifyGroup">>) -> group_change_data;
perm_gui_to_db(<<"permSetPrivileges">>) -> group_set_privileges;
perm_gui_to_db(<<"permRemoveGroup">>) -> group_remove;
perm_gui_to_db(<<"permInviteUser">>) -> group_invite_user;
perm_gui_to_db(<<"permRemoveUser">>) -> group_remove_user;
perm_gui_to_db(<<"permInviteGroup">>) -> group_invite_group;
perm_gui_to_db(<<"permRemoveSubgroup">>) -> group_remove_group;
perm_gui_to_db(<<"permJoinGroup">>) -> group_join_group;
perm_gui_to_db(<<"permCreateSpace">>) -> group_create_space;
perm_gui_to_db(<<"permJoinSpace">>) -> group_join_space;
perm_gui_to_db(<<"permLeaveSpace">>) -> group_leave_space;
perm_gui_to_db(<<"permGetSupport">>) -> group_create_space_token.


