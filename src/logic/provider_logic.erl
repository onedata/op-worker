%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_provider records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by OneZone.
%%% NOTE: This is the only valid way to interact with od_provider records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_logic).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/0, get/1, get/2, get_protected_data/2]).
-export([get_name/0, get_name/1, get_name/2]).
-export([get_spaces/0, get_spaces/1, get_spaces/2]).
-export([get_urls/0, get_urls/1, get_urls/2]).
-export([has_eff_user/2, has_eff_user/3]).
-export([supports_space/2, supports_space/3]).
-export([map_idp_group_to_onedata/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get() ->
    {ok, od_provider:doc()} | gs_protocol:error().
get() ->
    get(?ROOT_SESS_ID, oneprovider:get_provider_id()).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get(od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get(ProviderId) ->
    get(?ROOT_SESS_ID, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc restricted to protected data by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get_protected_data(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance, scope = protected},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_name() -> {ok, od_provider:name()} | gs_protocol:error().
get_name() ->
    get_name(?ROOT_SESS_ID, oneprovider:get_provider_id()).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_name(od_provider:id()) -> {ok, od_provider:name()} | gs_protocol:error().
get_name(ProviderId) ->
    get_name(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_name(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:name()} | gs_protocol:error().
get_name(SessionId, ProviderId) ->
    case get_protected_data(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces() -> {ok, [od_space:id()]} | gs_protocol:error().
get_spaces() ->
    get_spaces(?ROOT_SESS_ID, oneprovider:get_provider_id()).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(od_provider:id()) -> {ok, [od_space:id()]} | gs_protocol:error().
get_spaces(ProviderId) ->
    get_spaces(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(gs_client_worker:client(), od_provider:id()) ->
    {ok, [od_space:id()]} | gs_protocol:error().
get_spaces(SessionId, ProviderId) ->
    case get(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{spaces = Spaces}}} ->
            {ok, maps:keys(Spaces)};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves urls of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_urls() -> {ok, od_provider:urls()} | gs_protocol:error().
get_urls() ->
    get_urls(?ROOT_SESS_ID, oneprovider:get_provider_id()).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves urls of provider by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_urls(od_provider:id()) -> {ok, od_provider:urls()} | gs_protocol:error().
get_urls(ProviderId) ->
    get_urls(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves urls of provider by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_urls(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:urls()} | gs_protocol:error().
get_urls(SessionId, ProviderId) ->
    case get_protected_data(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{urls = URLs}}} ->
            {ok, URLs};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_provider:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_provider{eff_users = EffUsers}}, UserId) ->
    lists:member(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_provider:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, ProviderId, UserId) ->
    case get(SessionId, ProviderId) of
        {ok, ProviderDoc = #document{}} ->
            has_eff_user(ProviderDoc, UserId);
        _ ->
            false
    end.


-spec supports_space(od_provider:doc(), od_space:id()) -> boolean().
supports_space(#document{value = #od_provider{spaces = Spaces}}, SpaceId) ->
    maps:is_key(SpaceId, Spaces).


-spec supports_space(gs_client_worker:client(), od_provider:id(), od_space:id()) ->
    boolean().
supports_space(SessionId, ProviderId, SpaceId) ->
    case get(SessionId, ProviderId) of
        {ok, ProviderDoc = #document{}} ->
            supports_space(ProviderDoc, SpaceId);
        _ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Calls OZ to learn what's the onedata id of given group from certain IdP.
%% @end
%%--------------------------------------------------------------------
-spec map_idp_group_to_onedata(Idp :: binary(), IdpGroupId :: binary()) ->
    {ok, od_group:id()} | gs_protocol:error().
map_idp_group_to_onedata(Idp, IdpGroupId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_provider, id = undefined, aspect = map_idp_group},
        data = #{
            <<"idp">> => Idp,
            <<"groupId">> => IdpGroupId
        }
    })).
