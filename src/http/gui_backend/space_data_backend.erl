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

%% Key under which default space is stored in session memory.
-define(DEFAULT_SPACE_KEY, default_space).

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

% @todo dev
-export([support/1, process_space_info_change/3]).
support(Token) ->
    {ok, SpaceId} = oz_providers:support_space(provider, [
        {<<"token">>, Token}, {<<"size">>, <<"10000000">>}
    ]),
    {ok, Storage} = storage:get_by_name(<<"/mnt/st1">>),
    StorageId = storage:id(Storage),
    {ok, _} = space_storage:add(SpaceId, StorageId).


%%--------------------------------------------------------------------
%% @doc
%% @todo temporal solution - until events are used in GUI
%% Processes file_meta model changes and informs Ember client about changes.
%% @end
%%--------------------------------------------------------------------
-spec process_space_info_change(Method :: model_behaviour:model_action(),
    Context :: term(), ReturnValue :: term()) -> ok.
% @TODO FILTROWANIE - PACZ NA fslogic_context:set_space_id
process_space_info_change(create, [#document{key = SpaceId} = Doc], _ReturnValue) ->
    ?alert("CreateSpace: ~p", [SpaceId]),
    ok;
process_space_info_change(update, [SpaceId, _Changes], _ReturnValue) ->
    ?alert("UpdateSpace: ~p", [SpaceId]),
    ok;
process_space_info_change(delete, [SpaceId, _DeleteFun], _ReturnValue) ->
    ?alert("DeleteSpace: ~p", [SpaceId]),
    ok.


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
    g_session:put_value(?DEFAULT_SPACE_KEY,
        op_gui_utils:get_users_default_space()),
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
find(<<"space">>, [SpaceId]) ->
    {ok, #document{
        value = #space_info{
            name = Name,
            size = _Size,
            users = UsersAndPerms,
            groups = GroupPerms,
            providers = _Providers
        }}} = space_info:get(SpaceId),

    UserPermissions = lists:map(
        fun({UserId, _UserPerms}) ->
            ids_to_association(UserId, SpaceId)
        end, UsersAndPerms),

    GroupPermissions = lists:map(
        fun({GroupId, _GroupPerms}) ->
            ids_to_association(GroupId, SpaceId)
        end, GroupPerms),

    DefaultSpaceId = g_session:get_value(?DEFAULT_SPACE_KEY),
    Res = [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
        {<<"userPermissions">>, UserPermissions},
        {<<"groupPermissions">>, GroupPermissions}
    ],
    {ok, Res};


find(<<"space-user-permission">>, [AssocId]) ->
    {UserId, SpaceId} = association_to_ids(AssocId),
    {ok, #document{
        value = #space_info{
            users = UsersAndPerms
        }}} = space_info:get(SpaceId),
    UserPerms = proplists:get_value(UserId, UsersAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, UserPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    Res = PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"user">>, UserId}
    ],
    {ok, Res};


find(<<"space-user">>, [UserId]) ->
    {ok, #document{
        value = #onedata_user{
            name = Name
        }}} = onedata_user:get(UserId),
    Res = [
        {<<"id">>, UserId},
        {<<"name">>, Name}
    ],
    {ok, Res};


find(<<"space-group-permission">>, [AssocId]) ->
    {GroupId, SpaceId} = association_to_ids(AssocId),
    {ok, #document{
        value = #space_info{
            groups = GroupsAndPerms
        }}} = space_info:get(SpaceId),
    GroupPerms = proplists:get_value(GroupId, GroupsAndPerms),
    PermsMapped = lists:map(
        fun(SpacePerm) ->
            HasPerm = lists:member(SpacePerm, GroupPerms),
            {perm_db_to_gui(SpacePerm), HasPerm}
        end, all_space_perms()),
    Res = PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"group">>, GroupId}
    ],
    {ok, Res};


find(<<"space-group">>, [GroupId]) ->
    {ok, #document{
        value = #onedata_group{
            name = Name
        }}} = onedata_group:get(GroupId),
    Res = [
        {<<"id">>, GroupId},
        {<<"name">>, Name}
    ],
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
    {ok, #document{
        value = #onedata_user{
            space_ids = SpaceIds}}} = onedata_user:get(UserId),
    Res = lists:map(
        fun(SpaceId) ->
            {ok, SpaceData} = find(<<"space">>, [SpaceId]),
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
    % todo error handling
    Name = proplists:get_value(<<"name">>, Data, <<"">>),
    {ok, SpaceId} = space_info:create(
        #document{value = #space_info{name = Name}}),
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
    % todo error handling
    ?dump({SpaceId, Data}),
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
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"space">>, _Id) ->
    gui_error:report_error(<<"Not iplemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================


ids_to_association(FirstId, SecondId) ->
    <<FirstId/binary, "@", SecondId/binary>>.


association_to_ids(AssocId) ->
    [FirstId, SecondId] = binary:split(AssocId, <<"@">>, [global]),
    {FirstId, SecondId}.


all_space_perms() -> [
    space_invite_user, space_remove_user,
    space_invite_group, space_remove_group,
    space_add_provider, space_remove_provider,
    space_set_privileges, space_change_data,
    space_remove, space_view_data
].


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

