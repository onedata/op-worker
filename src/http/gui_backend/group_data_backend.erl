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
-module(group_data_backend).
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
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"group">>) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
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
    UserAuth = op_gui_utils:get_user_rest_auth(),
    Name = proplists:get_value(<<"name">>, Data),
    case Name of
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot create group with empty name.">>);
        _ ->
            case group_logic:create(UserAuth, #onedata_group{name = Name}) of
                {ok, GroupId} ->
                    {ok, [
                        {<<"id">>, GroupId},
                        {<<"name">>, Name},
                        {<<"userPermissions">>, []},
                        {<<"groupPermissions">>, []}
                    ]};
                _ ->
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
    UserAuth = op_gui_utils:get_user_rest_auth(),
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
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change group name due to unknown error.">>)
            end
    end;

update_record(<<"group-user-permission">>, AssocId, Data) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
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
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end;

update_record(<<"group-group-permission">>, _AssocId, _Data) ->
    gui_error:report_error(<<"Not implemented">>).
%%    UserAuth = op_gui_utils:get_user_rest_auth(),
%%    CurrentUser = g_session:get_user_id(),
%%    {GroupId, GroupId} = op_gui_utils:association_to_ids(AssocId),
%%    {ok, #document{
%%        value = #onedata_group{
%%            groups = GroupsAndPerms
%%        }}} = group_logic:get(UserAuth, GroupId, CurrentUser),
%%    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
%%    NewGroupPerms = lists:foldl(
%%        fun({PermGui, Flag}, PermsAcc) ->
%%            Perm = perm_gui_to_db(PermGui),
%%            case Flag of
%%                true ->
%%                    PermsAcc ++ [Perm];
%%                false ->
%%                    PermsAcc -- [Perm]
%%            end
%%        end, GroupPerms, Data),
%%
%%    Result = group_logic:set_group_privileges(
%%        UserAuth, GroupId, GroupId, lists:usort(NewGroupPerms)),
%%    case Result of
%%        ok ->
%%            ok;
%%        {error, _} ->
%%            gui_error:report_warning(
%%                <<"Cannot change user privileges due to unknown error.">>)
%%    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"group">>, GroupId) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:delete(UserAuth, GroupId) of
        ok ->
            ok;
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
group_record(GroupId) ->
    CurrentUser = g_session:get_user_id(),
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, #document{
        value = #onedata_group{
            name = Name,
            users = UsersAndPerms,
%%            groups = GroupsAndPerms
        }}} = group_logic:get(UserAuth, GroupId),
    %% @todo wait for groups from zbyszek
    GroupsAndPerms = [],

    UserPermissions = lists:map(
        fun({UsId, _UsPerms}) ->
            op_gui_utils:ids_to_association(UsId, GroupId)
        end, UsersAndPerms),

    GroupPermissions = lists:map(
        fun({GroupId, _GroupPerms}) ->
            op_gui_utils:ids_to_association(GroupId, GroupId)
        end, GroupsAndPerms),
    [
        {<<"id">>, GroupId},
        {<<"name">>, Name},
        {<<"userPermissions">>, UserPermissions},
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
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {UserId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #onedata_group{
            users = UsersAndPerms
        }}} = group_logic:get(UserAuth, GroupId),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    PermsMapped = lists:map(
        fun(GroupPerm) ->
            HasPerm = lists:member(GroupPerm, UserPerms),
            {perm_db_to_gui(GroupPerm), HasPerm}
        end, all_group_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"group">>, GroupId},
        {<<"user">>, UserId}
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
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {GroupId, GroupId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #onedata_group{
%%            groups = GroupsAndPerms
        }}} = group_logic:get(UserAuth, GroupId),
    %% @todo wait for groups from zbyszek
    GroupsAndPerms = [],
    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(GroupPerm) ->
            HasPerm = lists:member(GroupPerm, GroupPerms),
            {perm_db_to_gui(GroupPerm), HasPerm}
        end, all_group_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"group">>, GroupId},
        {<<"group">>, GroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns all allowed space permissions.
%% @end
%%--------------------------------------------------------------------
-spec all_group_perms() -> [atom()].
all_group_perms() -> [
    group_view_data, group_change_data,
    group_remove, group_set_privileges,
    group_invite_user, group_remove_user,
    group_create_space, group_join_space,
    group_leave_space, group_create_space_token
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
perm_db_to_gui(group_remove) -> <<"permRemoveGroup">>;
perm_db_to_gui(group_set_privileges) -> <<"permSetPrivileges">>;
perm_db_to_gui(group_invite_user) -> <<"permInviteUser">>;
perm_db_to_gui(group_remove_user) -> <<"permRemoveUser">>;
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
perm_gui_to_db(<<"permRemoveGroup">>) -> group_remove;
perm_gui_to_db(<<"permSetPrivileges">>) -> group_set_privileges;
perm_gui_to_db(<<"permInviteUser">>) -> group_invite_user;
perm_gui_to_db(<<"permRemoveUser">>) -> group_remove_user;
perm_gui_to_db(<<"permCreateSpace">>) -> group_create_space;
perm_gui_to_db(<<"permJoinSpace">>) -> group_join_space;
perm_gui_to_db(<<"permLeaveSpace">>) -> group_leave_space;
perm_gui_to_db(<<"permGetSupport">>) -> group_create_space_token.
