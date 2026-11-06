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
%%% which decides if they should be served from cache or handled by Onezone.
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

-export([get_public_data/2]).

%%%===================================================================
%%% API
%%%===================================================================


-spec get_public_data(gs_client_worker:client(), od_handle_service:id()) ->
    {ok, od_handle_service:doc()} | errors:error().
get_public_data(SessionId, HServiceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_handle_service, id = HServiceId, aspect = instance, scope = public},
        subscribe = true
    }).

