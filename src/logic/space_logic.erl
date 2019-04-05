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

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([get/2, get_protected_data/2]).
-export([get_name/2]).
-export([get_eff_users/2, has_eff_user/2, has_eff_user/3]).
-export([has_eff_privilege/3, has_eff_privilege/4]).
-export([get_eff_groups/2, get_shares/2]).
-export([get_provider_ids/2]).
-export([is_supported/2, is_supported/3]).
-export([can_view_user_through_space/3, can_view_user_through_space/4]).
-export([can_view_group_through_space/3, can_view_group_through_space/4]).


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
    {ok, maps:map(od_user:id(), [privileges:space_privilege()])} | gs_protocol:error().
get_eff_users(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_users = EffUsers}}} ->
            {ok, EffUsers};
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


-spec has_eff_privilege(gs_client_worker:client(), od_space:id(), od_user:id(),
    privileges:space_privilege()) -> boolean().
has_eff_privilege(SessionId, SpaceId, UserId, Privilege) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_privilege(SpaceDoc, UserId, Privilege);
        _ ->
            false
    end.


-spec get_eff_groups(gs_client_worker:client(), od_space:id()) ->
    {ok, maps:map(od_group:id(), [privileges:space_privilege()])} | gs_protocol:error().
get_eff_groups(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_groups = EffGroups}}} ->
            {ok, EffGroups};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_group(od_space:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_space{eff_groups = EffGroups}}, GroupId) ->
    maps:is_key(GroupId, EffGroups).


-spec has_eff_privilege(od_space:doc(), od_user:id(), privileges:space_privilege()) ->
    boolean().
has_eff_privilege(#document{value = #od_space{eff_users = EffUsers}}, UserId, Privilege) ->
    lists:member(Privilege, maps:get(UserId, EffUsers, [])).


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

