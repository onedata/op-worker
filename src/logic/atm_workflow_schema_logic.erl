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
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    get/2,
    get_revision/2,
    assert_executable_revision/1
]).

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


-spec get_revision(
    atm_workflow_schema_revision:revision_number(),
    od_atm_workflow_schema:record() | od_atm_workflow_schema:doc()
) ->
    {ok, atm_workflow_schema_revision:record()} | ?ERROR_NOT_FOUND.
get_revision(RevisionNumber, #od_atm_workflow_schema{revision_registry = RevisionRegistry}) ->
    case atm_workflow_schema_revision_registry:has_revision(RevisionNumber, RevisionRegistry) of
        true ->
            {ok, atm_workflow_schema_revision_registry:get_revision(RevisionNumber, RevisionRegistry)};
        false ->
            ?ERROR_NOT_FOUND
    end;
get_revision(RevisionNumber, #document{value = AtmWorkflowSchema}) ->
    get_revision(RevisionNumber, AtmWorkflowSchema).


%%-------------------------------------------------------------------
%% @doc
%% Checks whether given atm workflow schema revision can be executed (not all
%% valid features are supported yet - e.g. empty lanes, etc.).
%% @end
%%-------------------------------------------------------------------
-spec assert_executable_revision(atm_workflow_schema_revision:record()) ->
    ok | no_return().
assert_executable_revision(#atm_workflow_schema_revision{lanes = []}) ->
    throw(?ERROR_ATM_WORKFLOW_EMPTY);

assert_executable_revision(#atm_workflow_schema_revision{lanes = AtmLaneSchemas}) ->
    lists:foreach(fun assert_lane_schema_executable/1, AtmLaneSchemas).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_lane_schema_executable(atm_lane_schema:record()) -> ok | no_return().
assert_lane_schema_executable(#atm_lane_schema{id = AtmLaneSchemaId, parallel_boxes = []}) ->
    throw(?ERROR_ATM_LANE_EMPTY(AtmLaneSchemaId));

assert_lane_schema_executable(#atm_lane_schema{parallel_boxes = AtmParallelBoxSchemas}) ->
    lists:foreach(fun assert_parallel_box_schema_executable/1, AtmParallelBoxSchemas).


%% @private
-spec assert_parallel_box_schema_executable(atm_parallel_box_schema:record()) ->
    ok | no_return().
assert_parallel_box_schema_executable(#atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId,
    tasks = []
}) ->
    throw(?ERROR_ATM_PARALLEL_BOX_EMPTY(AtmParallelBoxSchemaId));

assert_parallel_box_schema_executable(#atm_parallel_box_schema{}) ->
    ok.
