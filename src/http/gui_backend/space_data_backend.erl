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
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

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
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"space">>, SpaceIds) ->
    Res = lists:map(
        fun(SpaceId) ->
            space_record(SpaceId)
        end, SpaceIds),
    {ok, Res};

find(<<"space-user-permission">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_user_permission_record(AssocId)
        end, AssocIds),
    {ok, Res};

find(<<"space-user">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_user_record(AssocId)
        end, AssocIds),
    {ok, Res};

find(<<"space-group-permission">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_group_permission_record(AssocId)
        end, AssocIds),
    {ok, Res};

find(<<"space-group">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_group_record(AssocId)
        end, AssocIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"space">>) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    UserId = g_session:get_user_id(),
    {ok, Spaces} = user_logic:get_spaces(UserAuth, UserId),
    {SpaceIds, _} = lists:unzip(Spaces),
    Res = lists:map(
        fun(SpaceId) ->
            {ok, [SpaceData]} = find(<<"space">>, [SpaceId]),
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
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"space">>, Data) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    % @todo error handling
    Name = proplists:get_value(<<"name">>, Data, <<"">>),
    {ok, SpaceId} = space_logic:create_user_space(
        UserAuth, #space_info{name = Name}),
    {ok, [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"isDefault">>, false},
        {<<"userPermissions">>, []},
        {<<"groupPermissions">>, []}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"space">>, SpaceId, Data) ->
    % @TODO Use space_info modify!!!!
    % @todo error handling
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case proplists:get_value(<<"isDefault">>, Data, undefined) of
        undefined ->
            ok;
        false ->
            ok;
        true ->
            user_logic:set_default_space(UserAuth, SpaceId)
    end,
    case proplists:get_value(<<"name">>, Data, undefined) of
        undefined ->
            ok;
        NewName ->
            space_logic:set_name(UserAuth, SpaceId, NewName)
    end,
    ok;

update_record(<<"space-user-permission">>, AssocId, Data) ->
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, #document{
        value = #space_info{
            users = UsersAndPerms
        }}} = space_logic:get(UserAuth, SpaceId, UserId),
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
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"space">>, SpaceId) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:delete(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot remove space due to unknown error.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary()) -> proplists:proplist().
space_record(SpaceId) ->
    UserId = g_session:get_user_id(),
    Auth = op_gui_utils:get_user_rest_auth(),
    {ok, #document{
        value = #space_info{
            name = Name,
            users = UsersAndPerms,
            groups = GroupsAndPerms
        }}} = space_logic:get(Auth, SpaceId, UserId),

    UserPermissions = lists:map(
        fun({UsId, _UsPerms}) ->
            op_gui_utils:ids_to_association(UsId, SpaceId)
        end, UsersAndPerms),

    GroupPermissions = lists:map(
        fun({GroupId, _GroupPerms}) ->
            op_gui_utils:ids_to_association(GroupId, SpaceId)
        end, GroupsAndPerms),

    DefaultSpaceId = user_logic:get_default_space(Auth, UserId),
    [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
        {<<"userPermissions">>, UserPermissions},
        {<<"groupPermissions">>, GroupPermissions}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space_user_permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_user_permission_record(AssocId :: binary()) -> proplists:proplist().
space_user_permission_record(AssocId) ->
    Auth = op_gui_utils:get_user_rest_auth(),
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    CurrentUser = g_session:get_user_id(),
    {ok, #document{
        value = #space_info{
            users = UsersAndPerms
        }}} = space_logic:get(Auth, SpaceId, CurrentUser),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, UserPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"user">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space_user record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_user_record(UserId :: binary()) -> proplists:proplist().
space_user_record(UserId) ->
    CurrentUserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, #document{value = #onedata_user{name = UserName}}} =
        user_logic:get(CurrentUserAuth, UserId),
    [
        {<<"id">>, UserId},
        {<<"name">>, UserName}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space_group_permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_permission_record(AssocId :: binary()) ->
    proplists:proplist().
space_group_permission_record(AssocId) ->
    Auth = op_gui_utils:get_user_rest_auth(),
    {GroupId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #space_info{
            groups = GroupsAndPerms
        }}} = space_logic:get(Auth, SpaceId, g_session:get_user_id()),
    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, GroupPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"group">>, GroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space_group record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_record(GroupId :: binary()) -> proplists:proplist().
space_group_record(GroupId) ->
    {ok, #document{value = #onedata_group{name = GroupName}}} =
        onedata_group:get(GroupId),
    [
        {<<"id">>, GroupId},
        {<<"name">>, GroupName}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns all allowed space permissions.
%% @end
%%--------------------------------------------------------------------
-spec all_space_perms() -> [binary()].
all_space_perms() -> [
    space_invite_user, space_remove_user,
    space_invite_group, space_remove_group,
    space_add_provider, space_remove_provider,
    space_set_privileges, space_change_data,
    space_remove, space_view_data
].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(binary()) -> binary().
perm_db_to_gui(space_invite_user) -> <<"permInviteUser">>;
perm_db_to_gui(space_remove_user) -> <<"permRemoveUser">>;
perm_db_to_gui(space_invite_group) -> <<"permInviteGroup">>;
perm_db_to_gui(space_remove_group) -> <<"permRemoveGroup">>;
perm_db_to_gui(space_set_privileges) -> <<"permSetPrivileges">>;
perm_db_to_gui(space_remove) -> <<"permRemoveSpace">>;
perm_db_to_gui(space_add_provider) -> <<"permInviteProvider">>;
perm_db_to_gui(space_remove_provider) -> <<"permRemoveProvider">>;
perm_db_to_gui(space_change_data) -> <<"permModifySpace">>;
perm_db_to_gui(space_view_data) -> <<"permViewSpace">>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary()) -> binary().
perm_gui_to_db(<<"permInviteUser">>) -> space_invite_user;
perm_gui_to_db(<<"permRemoveUser">>) -> space_remove_user;
perm_gui_to_db(<<"permInviteGroup">>) -> space_invite_group;
perm_gui_to_db(<<"permRemoveGroup">>) -> space_remove_group;
perm_gui_to_db(<<"permSetPrivileges">>) -> space_set_privileges;
perm_gui_to_db(<<"permRemoveSpace">>) -> space_remove;
perm_gui_to_db(<<"permInviteProvider">>) -> space_add_provider;
perm_gui_to_db(<<"permRemoveProvider">>) -> space_remove_provider;
perm_gui_to_db(<<"permModifySpace">>) -> space_change_data;
perm_gui_to_db(<<"permViewSpace">>) -> space_view_data.
