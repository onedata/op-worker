%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for manipulating automation inventories via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_inventory_logic).
-author("Michal Stanisz").

-include("middleware/middleware.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([
    get/2, get_name/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get(gs_client_worker:client(), od_atm_inventory:id()) ->
    {ok, od_atm_inventory:doc()} | errors:error().
get(SessionId, AtmInventoryId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_atm_inventory, id = AtmInventoryId, aspect = instance, scope = private},
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_atm_inventory:id()) ->
    {ok, automation:name()} | errors:error().
get_name(SessionId, AtmInventoryId) ->
    case get(SessionId, AtmInventoryId) of
        {ok, #document{value = #od_atm_inventory{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.
