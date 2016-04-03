%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
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
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/oz/oz_groups.hrl").

% @todo currently unused - every time taken from OZ
%% Key under which default space is stored in session memory.
-define(DEFAULT_SPACE_KEY, default_space).

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).


%%% @todo dev
-export([support/1]).
support(Token) ->
    {ok, SpaceId} = oz_providers:support_space(provider, [
        {<<"token">>, Token}, {<<"size">>, <<"10000000">>}
    ]),
    {ok, Storage} = storage:get_by_name(<<"/mnt/st1">>),
    StorageId = storage:id(Storage),
    {ok, _} = space_storage:add(SpaceId, StorageId).


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
    op_gui_utils:register_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    op_gui_utils:unregister_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"space">>, SpaceDirIds) ->
    Res = lists:map(
        fun(SpaceDirId) ->
            space_record(SpaceDirId)
        end, SpaceDirIds),
    {ok, Res};


find(<<"space-user-permission">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_user_permission_record(AssocId)
        end, AssocIds),
    {ok, Res};


find(<<"space-user">>, UserIds) ->
    Res = lists:map(
        fun(UserId) ->
            space_user_record(UserId)
        end, UserIds),
    {ok, Res};


find(<<"space-group-permission">>, AssocIds) ->
    Res = lists:map(
        fun(AssocId) ->
            space_group_permission_record(AssocId)
        end, AssocIds),
    {ok, Res};


find(<<"space-group">>, GroupIds) ->
    Res = lists:map(
        fun(GroupId) ->
            space_group_record(GroupId)
        end, GroupIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"space">>) ->
    UserId = op_gui_utils:get_user_id(),
    {ok, SpaceIds} = onedata_user:get_spaces(UserId),
    Res = lists:filtermap(
        fun(SpaceId) ->
            try
                SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                {ok, [SpaceData]} = find(<<"space">>, [SpaceDirId]),
                {true, SpaceData}
            catch _:_ ->
                ?error("Cannot resolve space data: ~p", [SpaceId]),
                false
            end
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
    % todo error handling
    Name = proplists:get_value(<<"name">>, Data, <<"">>),
    {ok, SpaceId} = oz_users:create_space(UserAuth, [{<<"name">>, Name}]),
    SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    {ok, [
        {<<"id">>, SpaceDirId},
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
update_record(<<"space">>, SpaceDirId, Data) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    % todo error handling
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case proplists:get_value(<<"isDefault">>, Data, undefined) of
        undefined ->
            ok;
        false ->
            ok;
        true ->
            % @todo change when onedata_user holds default space
            oz_users:set_default_space(UserAuth, [{<<"spaceId">>, SpaceId}])
    end,
    case proplists:get_value(<<"name">>, Data, undefined) of
        undefined ->
            ok;
        NewName ->
            oz_spaces:modify_details(UserAuth, SpaceId, [{<<"name">>, NewName}])
    end,
    ok;


update_record(<<"space-user-permission">>, AssocId, Data) ->
    {UserId, SpaceDirId} = association_to_ids(AssocId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    UserAuth = op_gui_utils:get_user_rest_auth(),
    {ok, #document{
        value = #space_info{
            users = UsersAndPerms
        }}} = space_info:get_or_fetch(UserAuth, SpaceId),
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

    Res = oz_spaces:set_user_privileges(UserAuth, SpaceId, UserId, [
        % usort - make sure there are no duplicates
        {<<"privileges">>, lists:usort(NewUserPerms)}
    ]),
    ?dump(NewUserPerms),
    ?dump(Res),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"space">>, SpaceDirId) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    oz_spaces:remove(UserAuth, SpaceId),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


space_record(SpaceDirId) ->
    Auth = op_gui_utils:get_user_rest_auth(),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            name = Name,
            size = _Size,
            users = UsersAndPerms,
            groups = GroupsAndPerms,
            providers = _Providers
        }}} = space_info:get_or_fetch(Auth, SpaceId),

    ?dump(UsersAndPerms),
    ?dump(GroupsAndPerms),

    UserPermissions = lists:map(
        fun({UserId, _UserPerms}) ->
            ids_to_association(UserId, SpaceDirId)
        end, UsersAndPerms),

    GroupPermissions = lists:map(
        fun({GroupId, _GroupPerms}) ->
            ids_to_association(GroupId, SpaceDirId)
        end, GroupsAndPerms),

    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(
        op_gui_utils:get_users_default_space()),
    [
        {<<"id">>, SpaceDirId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
        {<<"userPermissions">>, UserPermissions},
        {<<"groupPermissions">>, GroupPermissions}
    ].


space_user_permission_record(AssocId) ->
    Auth = op_gui_utils:get_user_rest_auth(),
    {UserId, SpaceDirId} = association_to_ids(AssocId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            users = UsersAndPerms
        }}} = space_info:get_or_fetch(Auth, SpaceId),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, UserPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceDirId},
        % @todo this is assoc id so we can get user details via
        % space REST endpoint (we need space id to do that)
        % This is required when user hasn't logged in in this provider yet
        {<<"user">>, AssocId}
    ].


space_user_record(AssocId) ->
    {UserId, SpaceDirId} = association_to_ids(AssocId),
    UserName = case onedata_user:get(UserId) of
        {ok, #document{value = #onedata_user{name = Name}}} ->
            Name;
        _ ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
            CurrentUserAuth = op_gui_utils:get_user_rest_auth(),
            {ok, #user_details{
                name = Name
            }} = oz_spaces:get_user_details(CurrentUserAuth, SpaceId, UserId),
            Name
    end,
    [
        {<<"id">>, AssocId},
        {<<"name">>, UserName}
    ].


space_group_permission_record(AssocId) ->
    Auth = op_gui_utils:get_user_rest_auth(),
    {GroupId, SpaceDirId} = association_to_ids(AssocId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            groups = GroupsAndPerms
        }}} = space_info:get_or_fetch(Auth, SpaceId),
    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, GroupPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceDirId},
        % @todo this is assoc id so we can get group details via
        % space REST endpoint (we need space id to do that)
        % This is required when user hasn't logged in in this provider yet
        {<<"group">>, AssocId}
    ].


space_group_record(AssocId) ->
    {GroupId, SpaceDirId} = association_to_ids(AssocId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    GroupName = case onedata_group:get(GroupId) of
        {ok, #document{value = #onedata_group{name = Name}}} ->
            Name;
        _ ->
            CurrentUserAuth = op_gui_utils:get_user_rest_auth(),
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
            {ok, #group_details{
                name = Name
            }} = oz_spaces:get_user_details(CurrentUserAuth, SpaceId, GroupId),
            Name
    end,
    [
        {<<"id">>, AssocId},
        {<<"name">>, GroupName}
    ].


ids_to_association(FirstId, SecondId) ->
    <<FirstId/binary, "@", SecondId/binary>>.


association_to_ids(AssocId) ->
    [FirstId, SecondId] = binary:split(AssocId, <<"@">>, [global]),
    {FirstId, SecondId}.


all_space_perms() -> [
    <<"space_invite_user">>, <<"space_remove_user">>,
    <<"space_invite_group">>, <<"space_remove_group">>,
    <<"space_add_provider">>, <<"space_remove_provider">>,
    <<"space_set_privileges">>, <<"space_change_data">>,
    <<"space_remove">>, <<"space_view_data">>
].


perm_db_to_gui(<<"space_invite_user">>) -> <<"permInviteUser">>;
perm_db_to_gui(<<"space_remove_user">>) -> <<"permRemoveUser">>;
perm_db_to_gui(<<"space_invite_group">>) -> <<"permInviteGroup">>;
perm_db_to_gui(<<"space_remove_group">>) -> <<"permRemoveGroup">>;
perm_db_to_gui(<<"space_set_privileges">>) -> <<"permSetPrivileges">>;
perm_db_to_gui(<<"space_remove">>) -> <<"permRemoveSpace">>;
perm_db_to_gui(<<"space_add_provider">>) -> <<"permInviteProvider">>;
perm_db_to_gui(<<"space_remove_provider">>) -> <<"permRemoveProvider">>;
perm_db_to_gui(<<"space_change_data">>) -> <<"permModifySpace">>;
perm_db_to_gui(<<"space_view_data">>) -> <<"permViewSpace">>.


perm_gui_to_db(<<"permInviteUser">>) -> <<"space_invite_user">>;
perm_gui_to_db(<<"permRemoveUser">>) -> <<"space_remove_user">>;
perm_gui_to_db(<<"permInviteGroup">>) -> <<"space_invite_group">>;
perm_gui_to_db(<<"permRemoveGroup">>) -> <<"space_remove_group">>;
perm_gui_to_db(<<"permSetPrivileges">>) -> <<"space_set_privileges">>;
perm_gui_to_db(<<"permRemoveSpace">>) -> <<"space_remove">>;
perm_gui_to_db(<<"permInviteProvider">>) -> <<"space_add_provider">>;
perm_gui_to_db(<<"permRemoveProvider">>) -> <<"space_remove_provider">>;
perm_gui_to_db(<<"permModifySpace">>) -> <<"space_change_data">>;
perm_gui_to_db(<<"permViewSpace">>) -> <<"space_view_data">>.

