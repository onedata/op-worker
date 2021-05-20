%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for manipulating automation workflow_schemas via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_logic).
-author("Lukasz Opiola").

-include("middleware/middleware.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([get/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(gs_client_worker:client(), od_atm_workflow_schema:id()) ->
    {ok, od_atm_workflow_schema:doc()} | errors:error().
get(SessionId, AtmWorkflowSchemaId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_atm_workflow_schema, id = AtmWorkflowSchemaId, aspect = instance, scope = private},
        subscribe = true
    }).
