%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_handle_service records
%%% synchronized via Graph Sync. Requests are delegated to gs_client_worker,
%%% which decides if they should be served from cache or handled by OneZone.
%%% NOTE: This is the only valid way to interact with od_handle_service records,
%%% to ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_service_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([get/2]).
-export([has_eff_user/2, has_eff_user/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves handle_service doc by given HandleServiceId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_handle_service:id()) ->
    {ok, od_handle_service:doc()} | gs_protocol:error().
get(SessionId, HServiceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_handle_service, id = HServiceId, aspect = instance, scope = private},
        subscribe = true
    }).


-spec has_eff_user(od_handle_service:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_handle_service{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_handle_service:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, HServiceId, UserId) ->
    case get(SessionId, HServiceId) of
        {ok, HServiceDoc = #document{}} ->
            has_eff_user(HServiceDoc, UserId);
        _ ->
            false
    end.
