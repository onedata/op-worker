%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_space records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_space records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([get/2, get_protected_data/2]).
-export([get_name/2]).
-export([get_eff_users/2, get_shares/2]).
-export([get_provider_ids/2, get_providers_supports/2]).
-export([has_eff_user/2, has_eff_user/3]).
-export([has_eff_group/2, has_eff_group/3]).
-export([is_supported/2, is_supported/3]).
-export([has_eff_privilege/3, has_eff_privilege/4]).
-export([can_view_user_through_space/3, can_view_user_through_space/4]).
-export([can_view_group_through_space/3, can_view_group_through_space/4]).
-export([create/2, create/3, update_name/3, delete/2]).
-export([update_user_privileges/4, update_group_privileges/4]).
-export([create_user_invite_token/2, create_group_invite_token/2]).
-export([create_provider_invite_token/2]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | gs_protocol:error().
get(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc restricted to protected data by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | gs_protocol:error().
get_protected_data(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = protected},
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:name()} | gs_protocol:error().
get_name(SessionId, SpaceId) ->
    case get_protected_data(SessionId, SpaceId) of
        {ok, #document{value = #od_space{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec get_eff_users(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_user:id()]} | gs_protocol:error().
get_eff_users(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_users = EffUsers}}} ->
            {ok, EffUsers};
        {error, _} = Error ->
            Error
    end.


-spec get_shares(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_share:id()]} | gs_protocol:error().
get_shares(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{shares = Shares}}} ->
            {ok, Shares};
        {error, _} = Error ->
            Error
    end.


-spec get_provider_ids(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_provider:id()]} | gs_protocol:error().
get_provider_ids(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            {ok, maps:keys(Providers)};
        {error, _} = Error ->
            Error
    end.


-spec get_providers_supports(gs_client_worker:client(), od_space:id()) ->
    {ok, maps:map(od_provider:id(), Size :: integer())} | gs_protocol:error().
get_providers_supports(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            {ok, Providers};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_space:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_space{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_space:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, SpaceId, UserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_user(SpaceDoc, UserId);
        _ ->
            false
    end.


-spec has_eff_group(od_space:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_space{eff_groups = EffGroups}}, GroupId) ->
    maps:is_key(GroupId, EffGroups).


-spec has_eff_group(gs_client_worker:client(), od_space:id(), od_group:id()) ->
    boolean().
has_eff_group(SessionId, SpaceId, GroupId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_group(SpaceDoc, GroupId);
        _ ->
            false
    end.


-spec is_supported(od_space:doc(), od_provider:id()) -> boolean().
is_supported(#document{value = #od_space{providers = Providers}}, ProviderId) ->
    maps:is_key(ProviderId, Providers).


-spec is_supported(gs_client_worker:client(), od_space:id(), od_provider:id()) ->
    boolean().
is_supported(SessionId, SpaceId, ProviderId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            is_supported(SpaceDoc, ProviderId);
        _ ->
            false
    end.


-spec has_eff_privilege(od_space:doc(), od_user:id(), privileges:space_privilege()) ->
    boolean().
has_eff_privilege(#document{value = #od_space{eff_users = EffUsers}}, UserId, Privilege) ->
    lists:member(Privilege, maps:get(UserId, EffUsers, [])).


-spec has_eff_privilege(gs_client_worker:client(), od_space:id(), od_user:id(),
    privileges:space_privilege()) -> boolean().
has_eff_privilege(SessionId, SpaceId, UserId, Privilege) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_privilege(SpaceDoc, UserId, Privilege);
        _ ->
            false
    end.


-spec can_view_user_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SessionId, SpaceId, ClientUserId, TargetUserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId);
        _ ->
            false
    end.


-spec can_view_user_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_user(SpaceDoc, TargetUserId).


-spec can_view_group_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), od_group:id()) -> boolean().
can_view_group_through_space(SessionId, SpaceId, ClientUserId, GroupId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_group_through_space(SpaceDoc, ClientUserId, GroupId);
        _ ->
            false
    end.


-spec can_view_group_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    od_group:id()) -> boolean().
can_view_group_through_space(SpaceDoc, ClientUserId, GroupId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_group(SpaceDoc, GroupId).


-spec create(gs_client_worker:client(), od_space:name()) ->
    {ok, od_space:id()} | gs_protocol:error().
create(SessionId, Name) ->
    {ok, UserId} = session:get_user_id(SessionId),
    create(SessionId, UserId, Name).


-spec create(gs_client_worker:client(), od_user:id(),
    od_space:name()) -> {ok, od_space:id()} | gs_protocol:error().
create(SessionId, UserId, Name) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = undefined, aspect = instance},
        auth_hint = ?AS_USER(UserId),
        data = #{<<"name">> => Name},
        subscribe = true
    })),
    ?ON_SUCCESS(Res, fun(_) ->
        {ok, UserId} = session:get_user_id(SessionId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec update_name(gs_client_worker:client(), od_space:id(), od_space:name()) ->
    ok | gs_protocol:error().
update_name(SessionId, SpaceId, Name) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance},
        data = #{<<"name">> => Name}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).


-spec delete(gs_client_worker:client(), od_space:id()) -> ok | gs_protocol:error().
delete(SessionId, SpaceId) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).


-spec update_user_privileges(gs_client_worker:client(), od_space:id(),
    od_user:id(), [privileges:space_privilege()]) -> ok | gs_protocol:error().
update_user_privileges(SessionId, SpaceId, UserId, Privileges) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_space, id = SpaceId, aspect = {user_privileges, UserId}},
        data = #{<<"privileges">> => Privileges}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).


-spec update_group_privileges(gs_client_worker:client(), od_space:id(),
    od_group:id(), [privileges:space_privilege()]) -> ok | gs_protocol:error().
update_group_privileges(SessionId, SpaceId, GroupId, Privileges) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_space, id = SpaceId, aspect = {group_privileges, GroupId}},
        data = #{<<"privileges">> => Privileges}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).


-spec create_user_invite_token(gs_client_worker:client(), od_space:id()) ->
    {ok, Token :: binary()} | gs_protocol:error().
create_user_invite_token(SessionId, SpaceId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = SpaceId, aspect = invite_user_token},
        data = #{}
    })).


-spec create_group_invite_token(gs_client_worker:client(), od_space:id()) ->
    {ok, Token :: binary()} | gs_protocol:error().
create_group_invite_token(SessionId, SpaceId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = SpaceId, aspect = invite_group_token},
        data = #{}
    })).


-spec create_provider_invite_token(gs_client_worker:client(), od_space:id()) ->
    {ok, Token :: binary()} | gs_protocol:error().
create_provider_invite_token(SessionId, SpaceId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = SpaceId, aspect = invite_provider_token},
        data = #{}
    })).
