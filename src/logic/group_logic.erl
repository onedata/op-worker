%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_group records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_group records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(group_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([get/2, get_protected_data/2, get_shared_data/3]).
-export([get_name/2, get_name/3]).
-export([get_eff_children/2, get_eff_spaces/2]).
-export([has_eff_user/2, has_eff_user/3]).
-export([has_eff_child/2, has_eff_child/3]).
-export([has_eff_space/2, has_eff_space/3]).
-export([has_eff_privilege/3, has_eff_privilege/4]).
-export([can_view_user_through_group/3, can_view_user_through_group/4]).
-export([can_view_child_through_group/3, can_view_child_through_group/4]).
-export([create/2, create/3, update_name/3, delete/2]).
-export([update_user_privileges/4, update_user_privileges/5]).
-export([update_child_privileges/4, update_child_privileges/5]).
-export([create_user_invite_token/2, create_group_invite_token/2]).
-export([join_group/3, leave_group/3]).
-export([leave_space/3, join_space/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves group doc by given GroupId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_group:id()) ->
    {ok, od_group:doc()} | gs_protocol:error().
get(SessionId, GroupId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_group, id = GroupId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group doc restricted to protected data by given GroupId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_group:id()) ->
    {ok, od_group:doc()} | gs_protocol:error().
get_protected_data(SessionId, GroupId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_group, id = GroupId, aspect = instance, scope = protected},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group doc restricted to shared data by given GroupId and AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(gs_client_worker:client(), od_group:id(), gs_protocol:auth_hint()) ->
    {ok, od_group:doc()} | gs_protocol:error().
get_shared_data(SessionId, GroupId, AuthHint) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_group, id = GroupId, aspect = instance, scope = shared},
        auth_hint = AuthHint,
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_group:id()) ->
    {ok, od_group:name()} | gs_protocol:error().
get_name(SessionId, GroupId) ->
    get_name(SessionId, GroupId, undefined).

get_name(SessionId, GroupId, AuthHint) ->
    case get_shared_data(SessionId, GroupId, AuthHint) of
        {ok, #document{value = #od_group{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec get_eff_children(gs_client_worker:client(), od_group:id()) ->
    {ok, [od_group:id()]} | gs_protocol:error().
get_eff_children(SessionId, GroupId) ->
    case get(SessionId, GroupId) of
        {ok, #document{value = #od_group{eff_children = EffChildren}}} ->
            {ok, EffChildren};
        {error, _} = Error ->
            Error
    end.


-spec get_eff_spaces(gs_client_worker:client(), od_group:id()) ->
    {ok, [od_space:id()]} | gs_protocol:error().
get_eff_spaces(SessionId, GroupId) ->
    case get(SessionId, GroupId) of
        {ok, #document{value = #od_group{eff_spaces = EffSpaces}}} ->
            {ok, EffSpaces};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_group:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_group{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_group:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, GroupId, UserId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            has_eff_user(GroupDoc, UserId);
        _ ->
            false
    end.


-spec has_eff_child(od_group:doc(), od_group:id()) -> boolean().
has_eff_child(#document{value = #od_group{eff_children = EffChildren}}, ChildId) ->
    maps:is_key(ChildId, EffChildren).


-spec has_eff_child(gs_client_worker:client(), od_group:id(),
    ChildId :: od_group:id()) -> boolean().
has_eff_child(SessionId, GroupId, ChildId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            has_eff_child(GroupDoc, ChildId);
        _ ->
            false
    end.


-spec has_eff_space(od_group:doc(), od_space:id()) -> boolean().
has_eff_space(#document{value = #od_group{eff_spaces = EffSpaces}}, SpaceId) ->
    lists:member(SpaceId, EffSpaces).


-spec has_eff_space(gs_client_worker:client(), od_group:id(), od_space:id()) ->
    boolean().
has_eff_space(SessionId, GroupId, SpaceId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            has_eff_space(GroupDoc, SpaceId);
        _ ->
            false
    end.


-spec has_eff_privilege(od_group:doc(), od_user:id(), privileges:group_privilege()) ->
    boolean().
has_eff_privilege(#document{value = #od_group{eff_users = EffUsers}}, UserId, Privilege) ->
    lists:member(Privilege, maps:get(UserId, EffUsers, [])).


-spec has_eff_privilege(gs_client_worker:client(), od_group:id(), od_user:id(),
    privileges:group_privilege()) -> boolean().
has_eff_privilege(SessionId, GroupId, UserId, Privilege) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            has_eff_privilege(GroupDoc, UserId, Privilege);
        _ ->
            false
    end.


-spec can_view_user_through_group(gs_client_worker:client(), od_group:id(),
    ClientUserId :: od_user:id(), TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_group(SessionId, GroupId, ClientUserId, TargetUserId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            can_view_user_through_group(GroupDoc, ClientUserId, TargetUserId);
        _ ->
            false
    end.


-spec can_view_user_through_group(od_group:doc(), ClientUserId :: od_user:id(),
    TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_group(GroupDoc, ClientUserId, TargetUserId) ->
    has_eff_privilege(GroupDoc, ClientUserId, ?GROUP_VIEW) andalso
        has_eff_user(GroupDoc, TargetUserId).


-spec can_view_child_through_group(gs_client_worker:client(), od_group:id(),
    ClientUserId :: od_user:id(), ChildId :: od_group:id()) -> boolean().
can_view_child_through_group(SessionId, GroupId, ClientUserId, ChildId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            can_view_child_through_group(GroupDoc, ClientUserId, ChildId);
        _ ->
            false
    end.


-spec can_view_child_through_group(od_group:doc(), ClientUserId :: od_user:id(),
    ChildId :: od_group:id()) -> boolean().
can_view_child_through_group(GroupDoc, ClientUserId, ChildId) ->
    has_eff_privilege(GroupDoc, ClientUserId, ?GROUP_VIEW) andalso
        has_eff_child(GroupDoc, ChildId).


-spec create(gs_client_worker:client(), od_group:name() | maps:map()) ->
    {ok, od_group:id()} | gs_protocol:error().
create(SessionId, NameOrData) ->
    {ok, UserId} = session:get_user_id(SessionId),
    create(SessionId, UserId, NameOrData).


-spec create(gs_client_worker:client(), od_user:id(),
    od_group:name() | maps:map()) -> {ok, od_group:id()} | gs_protocol:error().
create(SessionId, UserId, Name) when is_binary(Name) ->
    create(SessionId, UserId, #{<<"name">> => Name});
create(SessionId, UserId, Data) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_group, id = undefined, aspect = instance},
        auth_hint = ?AS_USER(UserId),
        data = Data,
        subscribe = true
    })),
    ?ON_SUCCESS(Res, fun(_) ->
        {ok, UserId} = session:get_user_id(SessionId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec update_name(gs_client_worker:client(), od_group:id(), od_group:name()) ->
    ok | gs_protocol:error().
update_name(SessionId, GroupId, Name) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_group, id = GroupId, aspect = instance},
        data = #{<<"name">> => Name}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec delete(gs_client_worker:client(), od_group:id()) -> ok | gs_protocol:error().
delete(SessionId, GroupId) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_group, id = GroupId, aspect = instance}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec update_user_privileges(gs_client_worker:client(), od_group:id(),
    od_user:id(), [privileges:group_privilege()]) -> ok | gs_protocol:error().
update_user_privileges(SessionId, GroupId, UserId, Privileges) ->
    update_user_privileges(SessionId, GroupId, UserId, Privileges, set).


-spec update_user_privileges(gs_client_worker:client(), od_group:id(),
    od_user:id(), [privileges:group_privilege()], set | grant | revoke) ->
    ok | gs_protocol:error().
update_user_privileges(SessionId, GroupId, UserId, Privileges, Operation) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_group, id = GroupId, aspect = {user_privileges, UserId}},
        data = #{<<"privileges">> => Privileges, <<"operation">> => Operation}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec update_child_privileges(gs_client_worker:client(), od_group:id(),
    ChildGroupId :: od_group:id(), [privileges:group_privilege()]) -> ok | gs_protocol:error().
update_child_privileges(SessionId, GroupId, ChildGroupId, Privileges) ->
    update_child_privileges(SessionId, GroupId, ChildGroupId, Privileges, set).


-spec update_child_privileges(gs_client_worker:client(), od_group:id(),
    ChildGroupId :: od_group:id(), [privileges:group_privilege()], set | grant | revoke) ->
    ok | gs_protocol:error().
update_child_privileges(SessionId, GroupId, ChildGroupId, Privileges, Operation) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_group, id = GroupId, aspect = {child_privileges, ChildGroupId}},
        data = #{<<"privileges">> => Privileges, <<"operation">> => Operation}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec create_user_invite_token(gs_client_worker:client(), od_group:id()) ->
    {ok, Token :: binary()} | gs_protocol:error().
create_user_invite_token(SessionId, GroupId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_group, id = GroupId, aspect = invite_user_token},
        data = #{}
    })).


-spec create_group_invite_token(gs_client_worker:client(), od_group:id()) ->
    {ok, Token :: binary()} | gs_protocol:error().
create_group_invite_token(SessionId, GroupId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_group, id = GroupId, aspect = invite_group_token},
        data = #{}
    })).


-spec join_group(gs_client_worker:client(), od_group:id(), Token :: binary()) ->
    {ok, od_group:id()} | gs_protocol:error().
join_group(SessionId, GroupId, Token) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_group, id = undefined, aspect = join},
        auth_hint = ?AS_GROUP(GroupId),
        data = #{<<"token">> => Token}
    })),
    ?ON_SUCCESS(Res, fun({ok, JoinedGroupId}) ->
        gs_client_worker:invalidate_cache(od_group, JoinedGroupId),
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec leave_group(gs_client_worker:client(), od_group:id(),
    ParentGroupId :: od_group:id()) -> ok | gs_protocol:error().
leave_group(SessionId, GroupId, ParentGroupId) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_group, id = GroupId, aspect = {parent, ParentGroupId}}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_group, GroupId),
        gs_client_worker:invalidate_cache(od_group, ParentGroupId)
    end).


-spec join_space(gs_client_worker:client(), od_group:id(), Token :: binary()) ->
    {ok, od_space:id()} | gs_protocol:error().
join_space(SessionId, GroupId, Token) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = undefined, aspect = join},
        auth_hint = ?AS_GROUP(GroupId),
        data = #{<<"token">> => Token}
    })),
    ?ON_SUCCESS(Res, fun({ok, JoinedSpaceId}) ->
        gs_client_worker:invalidate_cache(od_space, JoinedSpaceId),
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).


-spec leave_space(gs_client_worker:client(), od_group:id(), od_space:id()) ->
    ok | gs_protocol:error().
leave_space(SessionId, GroupId, SpaceId) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_group, id = GroupId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_group, GroupId)
    end).
