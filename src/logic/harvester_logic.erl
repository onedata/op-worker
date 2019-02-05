%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_harvester records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_harvester records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(harvester_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([get/2, submit/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves harvester doc by given HarvesterId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_harvester:id()) ->
    {ok, od_harvester:doc()} | gs_protocol:error().
get(SessionId, HarvesterId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_harvester, id = HarvesterId, aspect = instance, scope = protected},
        subscribe = true
    }).

%%--------------------------------------------------------------------
%% @doc
%% Submits metadata for given HarvesterId to Onezone.
%% @end
%%--------------------------------------------------------------------
-spec submit(gs_client_worker:client(), od_harvester:id(), gs_protocol:data()) ->
    ok | gs_protocol:error().
submit(SessionId, HarvesterId, Data) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_harvester, id = HarvesterId, aspect = submit, scope = private},
        data = Data
    }).
