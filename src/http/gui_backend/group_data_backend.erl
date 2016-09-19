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
-export([group_record/1]).

%%%===================================================================
%%% API functions
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
    {ok, group_record(GroupId)};

find(<<"group-user-permission">>, AssocId) ->
    {ok, group_user_permission_record(AssocId)};

find(<<"group-group-permission">>, AssocId) ->
    {ok, group_group_permission_record(AssocId)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"group">>) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = g_session:get_user_id(),
    GroupIds = op_gui_utils:find_all_groups(UserAuth, UserId),
    Res = lists:map(
        fun(GroupId) ->
            {ok, GroupData} = find(<<"group">>, GroupId),
            GroupData
        end, GroupIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"group">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


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
            case group_logic:create(UserAuth, #onedata_group{name = Name}) of
                {ok, GroupId} ->
                    GroupRecord = group_record(GroupId),
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
                {error, {403,<<>>,<<>>}} ->
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
        value = #onedata_group{
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
        {error, {403,<<>>,<<>>}} ->
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
        value = #onedata_group{
            nested_groups = GroupsAndPerms
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
        {error, {403,<<>>,<<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify group privileges.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change group privileges due to unknown error.">>)
    end.


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
        {error, {403,<<>>,<<>>}} ->
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
%% @private
%% @doc
%% Returns a client-compliant group record based on group id.
%% @end
%%--------------------------------------------------------------------
-spec group_record(GroupId :: binary()) -> proplists:proplist().
group_record(CurrentGroupId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #onedata_group{
            name = Name,
            users = UsersAndPerms,
            nested_groups = GroupsAndPerms,
            parent_groups = ParentGroups
        }}} = group_logic:get(UserAuth, CurrentGroupId),

    {ChildGroups, _} = lists:unzip(GroupsAndPerms),
    UserPermissions = lists:map(
        fun({UsId, _UsPerms}) ->
            op_gui_utils:ids_to_association(UsId, CurrentGroupId)
        end, UsersAndPerms),
    GroupPermissions = lists:map(
        fun({ChildGroupId, _GroupPerms}) ->
            op_gui_utils:ids_to_association(ChildGroupId, CurrentGroupId)
        end, GroupsAndPerms),
    [
        {<<"id">>, CurrentGroupId},
        {<<"name">>, Name},
        {<<"userPermissions">>, UserPermissions},
        {<<"groupPermissions">>, GroupPermissions},
        {<<"parentGroups">>, ParentGroups},
        {<<"childGroups">>, ChildGroups}
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
        value = #onedata_group{
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
        {<<"group">>, GroupId},
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
        value = #onedata_group{
            nested_groups = GroupsAndPerms
        }}} = group_logic:get(UserAuth, ParentGroupId),
    GroupPerms = proplists:get_value(ChildGroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(Perm) ->
            HasPerm = lists:member(Perm, GroupPerms),
            {perm_db_to_gui(Perm), HasPerm}
        end, privileges:group_privileges()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"group">>, ParentGroupId},
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


