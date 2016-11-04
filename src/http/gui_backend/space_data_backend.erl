%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(space_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-export([space_record/1, space_record/2]).

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
find(<<"space">>, SpaceId) ->
    UserId = gui_session:get_user_id(),
    % Check if the user belongs to this space
    case space_logic:has_effective_user(SpaceId, UserId) of
        false ->
            gui_error:unauthorized();
        true ->
            {ok, space_record(SpaceId)}
    end;

% PermissionsRecord matches <<"space-(user|group)-permission">>
find(PermissionsRecord, AssocId) ->
    {_, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    UserId = gui_session:get_user_id(),
    % Make sure that user is allowed to view requested privileges - he must have
    % view privileges in this space.
    Authorized = space_logic:has_effective_privilege(
        SpaceId, UserId, space_view_data
    ),
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            case PermissionsRecord of
                <<"space-user-permission">> ->
                    {ok, space_user_permission_record(AssocId)};
                <<"space-group-permission">> ->
                    {ok, space_group_permission_record(AssocId)}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"space">>) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    SpaceIds = op_gui_utils:find_all_spaces(UserAuth, UserId),
    Res = lists:map(
        fun(SpaceId) ->
            {ok, SpaceData} = find(<<"space">>, SpaceId),
            SpaceData
        end, SpaceIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"space">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"space">>, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    Name = proplists:get_value(<<"name">>, Data),
    case Name of
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot create space with empty name.">>);
        _ ->
            case space_logic:create_user_space(UserAuth, #od_space{name = Name}) of
                {ok, SpaceId} ->
                    % This space was created by this user -> he has view privs.
                    SpaceRecord = space_record(SpaceId, true),
                    {ok, SpaceRecord};
                _ ->
                    gui_error:report_warning(
                        <<"Cannot create new space due to unknown error.">>)
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
update_record(<<"space">>, SpaceId, [{<<"isDefault">>, Flag}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case Flag of
        undefined ->
            ok;
        false ->
            ok;
        true ->
            case user_logic:set_default_space(UserAuth, SpaceId) of
                ok ->
                    ok;
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change default space due to unknown error.">>)
            end
    end;

update_record(<<"space">>, SpaceId, [{<<"name">>, Name}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set space name to empty string.">>);
        NewName ->
            case space_logic:set_name(UserAuth, SpaceId, NewName) of
                ok ->
                    ok;
                {error, {403, <<>>, <<>>}} ->
                    gui_error:report_warning(
                        <<"You do not have privileges to modify this space.">>);
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change space name due to unknown error.">>)
            end
    end;

update_record(<<"space-user-permission">>, AssocId, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    CurrentUser = gui_session:get_user_id(),
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            users = UsersAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, CurrentUser),
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

    Result = space_logic:set_user_privileges(
        UserAuth, SpaceId, UserId, lists:usort(NewUserPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify space privileges.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end;

update_record(<<"space-group-permission">>, AssocId, Data) ->
    UserAuth = op_gui_utils:get_user_auth(),
    CurrentUser = gui_session:get_user_id(),
    {GroupId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            groups = GroupsAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, CurrentUser),
    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
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

    Result = space_logic:set_group_privileges(
        UserAuth, SpaceId, GroupId, lists:usort(NewGroupPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify space privileges.">>);
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
delete_record(<<"space">>, SpaceId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:delete(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify this space.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot remove space due to unknown error.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space record based on space id. Automatically
%% check if the user has view privileges in that space and returns proper data.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary()) -> proplists:proplist().
space_record(SpaceId) ->
    % Check if that user has view privileges in that space
    HasViewPrivileges = space_logic:has_effective_privilege(
        SpaceId, gui_session:get_user_id(), space_view_data
    ),
    space_record(SpaceId, HasViewPrivileges).


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space record based on space id. Allows to
%% override HasViewPrivileges.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary(), HasViewPrivileges :: boolean()) ->
    proplists:proplist().
space_record(SpaceId, HasViewPrivileges) ->
    UserId = gui_session:get_user_id(),
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #od_space{
            name = Name,
            users = UsersAndPerms,
            groups = GroupsAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, UserId),
    DefaultSpaceId = user_logic:get_default_space(UserAuth, UserId),

    % Make sure that user is allowed to view requested space - he must have
    % view privileges in this space. If not, return only public data.
    case HasViewPrivileges of
        false ->
            [
                {<<"id">>, SpaceId},
                {<<"name">>, Name},
                {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
                {<<"hasViewPrivilege">>, false},
                {<<"userPermissions">>, []},
                {<<"groupPermissions">>, []}
            ];
        true ->
            UserPermissions = lists:map(
                fun({UsId, _UsPerms}) ->
                    op_gui_utils:ids_to_association(UsId, SpaceId)
                end, UsersAndPerms),

            GroupPermissions = lists:map(
                fun({GroupId, _GroupPerms}) ->
                    op_gui_utils:ids_to_association(GroupId, SpaceId)
                end, GroupsAndPerms),
            [
                {<<"id">>, SpaceId},
                {<<"name">>, Name},
                {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
                {<<"hasViewPrivilege">>, true},
                {<<"userPermissions">>, UserPermissions},
                {<<"groupPermissions">>, GroupPermissions}
            ]
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-user-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_user_permission_record(AssocId :: binary()) -> proplists:proplist().
space_user_permission_record(AssocId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    CurrentUser = gui_session:get_user_id(),
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            users = UsersAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, CurrentUser),
    UserPermsAtoms = proplists:get_value(UserId, UsersAndPerms),
    UserPerms = [str_utils:to_binary(P) || P <- UserPermsAtoms],
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(str_utils:to_binary(SpacePerm), UserPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, privileges:space_privileges()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"systemUser">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-group-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_permission_record(AssocId :: binary()) ->
    proplists:proplist().
space_group_permission_record(AssocId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    CurrentUser = gui_session:get_user_id(),
    {GroupId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            groups = GroupsAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, CurrentUser),
    GroupPermsAtoms = proplists:get_value(GroupId, GroupsAndPerms),
    GroupPerms = [str_utils:to_binary(P) || P <- GroupPermsAtoms],
    PermsMapped = lists:map(
        fun(Perm) ->
            HasPerm = lists:member(str_utils:to_binary(Perm), GroupPerms),
            {perm_db_to_gui(Perm), HasPerm}
        end, privileges:space_privileges()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"systemGroup">>, GroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(atom()) -> binary().
perm_db_to_gui(space_view_data) -> <<"permViewSpace">>;
perm_db_to_gui(space_change_data) -> <<"permModifySpace">>;
perm_db_to_gui(space_remove) -> <<"permRemoveSpace">>;
perm_db_to_gui(space_set_privileges) -> <<"permSetPrivileges">>;
perm_db_to_gui(space_invite_user) -> <<"permInviteUser">>;
perm_db_to_gui(space_remove_user) -> <<"permRemoveUser">>;
perm_db_to_gui(space_invite_group) -> <<"permInviteGroup">>;
perm_db_to_gui(space_remove_group) -> <<"permRemoveGroup">>;
perm_db_to_gui(space_add_provider) -> <<"permInviteProvider">>;
perm_db_to_gui(space_remove_provider) -> <<"permRemoveProvider">>;
perm_db_to_gui(space_manage_shares) -> <<"permManageShares">>;
perm_db_to_gui(space_write_files) -> <<"permWriteFiles">>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary()) -> atom().
perm_gui_to_db(<<"permViewSpace">>) -> space_view_data;
perm_gui_to_db(<<"permModifySpace">>) -> space_change_data;
perm_gui_to_db(<<"permRemoveSpace">>) -> space_remove;
perm_gui_to_db(<<"permSetPrivileges">>) -> space_set_privileges;
perm_gui_to_db(<<"permInviteUser">>) -> space_invite_user;
perm_gui_to_db(<<"permRemoveUser">>) -> space_remove_user;
perm_gui_to_db(<<"permInviteGroup">>) -> space_invite_group;
perm_gui_to_db(<<"permRemoveGroup">>) -> space_remove_group;
perm_gui_to_db(<<"permInviteProvider">>) -> space_add_provider;
perm_gui_to_db(<<"permRemoveProvider">>) -> space_remove_provider;
perm_gui_to_db(<<"permManageShares">>) -> space_manage_shares;
perm_gui_to_db(<<"permWriteFiles">>) -> space_write_files.
