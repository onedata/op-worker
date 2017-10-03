%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_user records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by OneZone.
%%% NOTE: This is the only valid way to interact with od_user records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(user_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get_by_auth/1]).
-export([get/2, get_protected_data/2, get_shared_data/3]).
-export([exists/2]).
-export([get_name/2, get_name/3]).
-export([get_default_space/2]).
-export([get_eff_groups/2, get_eff_spaces/2]).
-export([get_eff_handle_services/2, get_eff_handles/2]).
-export([set_default_space/3]).
-export([has_eff_group/2, has_eff_group/3]).
-export([has_eff_space/2, has_eff_space/3]).
-export([get_space_by_name/3]).
-export([create_group/3, create_space/3]).
-export([join_group/3, leave_group/3]).
-export([join_space/3, leave_space/3]).
-export([authorize/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves user by given authorization. No UserId is needed as it can be
%% deduced from auth.
%% @end
%%--------------------------------------------------------------------
-spec get_by_auth(Auth :: session:auth()) ->
    {ok, od_user:doc()} | gs_protocol:error().
get_by_auth(Auth) ->
    gs_client_worker:request(Auth, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = ?SELF, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc by given UserId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_user:id()) ->
    {ok, od_user:doc()} | gs_protocol:error().
get(_, ?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
get(_, ?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{name = <<"nobody">>}}};
get(Client, UserId) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc restricted to protected data by given UserId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_user:id()) ->
    {ok, od_user:doc()} | gs_protocol:error().
get_protected_data(_, ?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
get_protected_data(_, ?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{name = <<"nobody">>}}};
get_protected_data(Client, UserId) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = protected},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc restricted to shared data by given UserId and AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(gs_client_worker:client(), od_user:id(), gs_protocol:auth_hint()) ->
    {ok, od_user:doc()} | gs_protocol:error().
get_shared_data(_, ?ROOT_USER_ID, _) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
get_shared_data(_, ?GUEST_USER_ID, _) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{name = <<"nobody">>}}};
get_shared_data(Client, UserId, AuthHint) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = shared},
        auth_hint = AuthHint,
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given user exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(gs_client_worker:client(), od_user:id()) -> boolean().
exists(Client, UserId) ->
    % Shared scope is enough to determine existence, provider has access to
    % shared scope of all supported users.
    case get_shared_data(Client, UserId, undefined) of
        {ok, _} -> true;
        _ -> false
    end.


-spec get_name(gs_client_worker:client(), od_user:id()) ->
    {ok, od_user:name()} | gs_protocol:error().
get_name(Client, UserId) ->
    get_name(Client, UserId, undefined).


-spec get_name(gs_client_worker:client(), od_user:id(), gs_protocol:auth_hint()) ->
    od_user:name() | gs_protocol:error().
get_name(Client, UserId, AuthHint) ->
    case get_shared_data(Client, UserId, AuthHint) of
        {ok, #document{value = #od_user{name = Name}}} ->
            {ok, Name};
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_default_space(gs_client_worker:client(), od_user:id()) ->
    undefined | od_space:id() | gs_protocol:error().
get_default_space(Client, UserId) ->
    case get(Client, UserId) of
        {ok, #document{value = #od_user{default_space = DefaultSpace}}} ->
            {ok, DefaultSpace};
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_eff_groups(gs_client_worker:client(), od_user:id()) ->
    {ok, [od_group:id()]} | gs_protocol:error().
get_eff_groups(Client, UserId) ->
    case get(Client, UserId) of
        {ok, #document{value = #od_user{eff_groups = GroupsIds}}} ->
            {ok, GroupsIds};
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_eff_spaces(od_user:doc()) ->
    {ok, [od_space:id()]} | gs_protocol:error().
get_eff_spaces(#document{value = #od_user{eff_spaces = EffSpaces}}) ->
    {ok, EffSpaces}.


-spec get_eff_spaces(gs_client_worker:client(), od_user:id()) ->
    {ok, [od_space:id()]} | gs_protocol:error().
get_eff_spaces(Client, UserId) ->
    case get(Client, UserId) of
        {ok, Doc} ->
            get_eff_spaces(Doc);
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_eff_handle_services(gs_client_worker:client(), od_user:id()) ->
    {ok, [od_handle_service:id()]} | gs_protocol:error().
get_eff_handle_services(Client, UserId) ->
    case get(Client, UserId) of
        {ok, #document{value = #od_user{eff_handle_services = HServices}}} ->
            {ok, HServices};
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_eff_handles(gs_client_worker:client(), od_user:id()) ->
    {ok, [od_handle:id()]} | gs_protocol:error().
get_eff_handles(Client, UserId) ->
    case get(Client, UserId) of
        {ok, #document{value = #od_user{eff_handles = Handles}}} ->
            {ok, Handles};
        {error, Reason} ->
            {error, Reason}
    end.


-spec has_eff_group(od_user:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_user{eff_groups = EffGroups}}, GroupId) ->
    lists:member(GroupId, EffGroups).


-spec has_eff_group(gs_client_worker:client(), od_user:id(), od_group:id()) -> boolean().
has_eff_group(Client, UserId, GroupId) when is_binary(UserId) ->
    case get(Client, UserId) of
        {ok, UserDoc = #document{}} ->
            has_eff_group(UserDoc, GroupId);
        {error, _} ->
            false
    end.


-spec has_eff_space(od_user:doc(), od_space:id()) -> boolean().
has_eff_space(#document{value = #od_user{eff_spaces = EffSpaces}}, SpaceId) ->
    lists:member(SpaceId, EffSpaces).


-spec has_eff_space(gs_client_worker:client(), od_user:id(), od_space:id()) ->
    boolean().
has_eff_space(Client, UserId, SpaceId) when is_binary(UserId) ->
    case get(Client, UserId) of
        {ok, UserDoc = #document{}} ->
            has_eff_space(UserDoc, SpaceId);
        {error, _} ->
            false
    end.


-spec get_space_by_name(gs_client_worker:client(), od_user:id() | od_user:doc(),
    od_space:name()) -> {true, od_space:id()} | false.
get_space_by_name(Client, UserDoc = #document{}, SpaceName) ->
    {ok, Spaces} = get_eff_spaces(UserDoc),
    get_space_by_name_internal(Client, SpaceName, Spaces);
get_space_by_name(Client, UserId, SpaceName) ->
    case get(Client, UserId) of
        {error, _} ->
            false;
        {ok, UserDoc} ->
            get_space_by_name(Client, UserDoc, SpaceName)
    end.


-spec get_space_by_name_internal(gs_client_worker:client(), od_space:name(),
    [od_space:id()]) -> {true, od_space:id()} | false.
get_space_by_name_internal(_Client, _SpaceName, []) ->
    false;
get_space_by_name_internal(Client, SpaceName, [SpaceId | Rest]) ->
    case space_logic:get_name(Client, SpaceId) of
        {ok, SpaceName} ->
            {true, SpaceId};
        _ ->
            get_space_by_name_internal(Client, SpaceName, Rest)
    end.


-spec set_default_space(gs_client_worker:client(), od_user:id(), od_space:id()) ->
    ok | gs_protocol:error().
set_default_space(Client, UserId, SpaceId) ->
    Res = ?CREATE_RETURN_OK(gs_client_worker:request(Client, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_user, id = UserId, aspect = default_space},
        data = #{<<"spaceId">> => SpaceId}
    })),
    ?ON_SUCCESS(Res, fun(_) ->
        {ok, UserId} = session:get_user_id(Client),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec create_group(gs_client_worker:client(), od_user:id(),
    od_group:name() | maps:map()) -> {ok, od_group:id()} | gs_protocol:error().
create_group(Client, UserId, NameOrData) ->
    group_logic:create(Client, UserId, NameOrData).


-spec create_space(gs_client_worker:client(), od_user:id(), od_space:name()) ->
    {ok, od_space:id()} | gs_protocol:error().
create_space(Client, UserId, Name) ->
    space_logic:create(Client, UserId, Name).


-spec join_group(gs_client_worker:client(), od_user:id(), Token :: binary()) ->
    {ok, od_group:id()} | gs_protocol:error().
join_group(Client, UserId, Token) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(Client, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_group, id = undefined, aspect = join},
        auth_hint = ?AS_USER(UserId),
        data = #{<<"token">> => Token}
    })),
    ?ON_SUCCESS(Res, fun({ok, JoinedGroupId}) ->
        {ok, UserId} = session:get_user_id(Client),
        gs_client_worker:invalidate_cache(od_group, JoinedGroupId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec leave_group(gs_client_worker:client(), od_user:id(), od_group:id()) ->
    ok | gs_protocol:error().
leave_group(Client, UserId, GroupId) ->
    Res = gs_client_worker:request(Client, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_user, id = UserId, aspect = {group, GroupId}}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        {ok, UserId} = session:get_user_id(Client),
        gs_client_worker:invalidate_cache(od_group, GroupId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec join_space(gs_client_worker:client(), od_user:id(), Token :: binary()) ->
    {ok, od_space:id()} | gs_protocol:error().
join_space(Client, UserId, Token) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(Client, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = undefined, aspect = join},
        auth_hint = ?AS_USER(UserId),
        data = #{<<"token">> => Token}
    })),
    ?ON_SUCCESS(Res, fun({ok, JoinedSpaceId}) ->
        {ok, UserId} = session:get_user_id(Client),
        gs_client_worker:invalidate_cache(od_space, JoinedSpaceId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


-spec leave_space(gs_client_worker:client(), od_user:id(), od_space:id()) ->
    ok | gs_protocol:error().
leave_space(Client, UserId, SpaceId) ->
    Res = gs_client_worker:request(Client, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_user, id = UserId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        {ok, UserId} = session:get_user_id(Client),
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_user, UserId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Collects discharge macaroon from OZ to verify if user is authenticated.
%% @end
%%--------------------------------------------------------------------
-spec authorize(CaveatId :: binary()) ->
    {ok, DischMacaroon :: binary()} | gs_protocol:error().
authorize(CaveatId) ->
    gs_client_worker:request(#gs_req_rpc{
        function = <<"authorizeUser">>,
        args = #{<<"identifier">> => CaveatId}
    }).
