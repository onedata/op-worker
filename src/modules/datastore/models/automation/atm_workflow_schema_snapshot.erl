%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing automation workflow schema snapshot.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_snapshot).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, get/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type id() :: binary().
-type record() :: #atm_workflow_schema_snapshot{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(atm_workflow_execution:id(), od_atm_workflow_schema:doc()) ->
    {ok, id()} | {error, term()}.
create(AtmWorkflowExecutionId, #document{
    key = AtmWorkflowSchemaId,
    value = AtmWorkflowSchemaRecord = #od_atm_workflow_schema{
        name = AtmWorkflowSchemaName,
        description = AtmWorkflowSchemaDescription,
        stores = AtmStoreSchemas,
        lanes = AtmLaneSchemas,
        state = AtmWorkflowSchemaState,
        atm_inventory = AtmInventoryId,
        atm_lambdas = AtmLambdaIds
    }
}) ->
    assert_executable(AtmWorkflowSchemaRecord),

    %% TODO VFS-7685 add ref count and gen snapshot id based on doc revision
    ?extract_key(datastore_model:create(?CTX, #document{
        key = AtmWorkflowExecutionId,
        value = #atm_workflow_schema_snapshot{
            schema_id = AtmWorkflowSchemaId,
            name = AtmWorkflowSchemaName,
            description = AtmWorkflowSchemaDescription,
            stores = AtmStoreSchemas,
            lanes = AtmLaneSchemas,
            state = AtmWorkflowSchemaState,
            atm_inventory = AtmInventoryId,
            atm_lambdas = AtmLambdaIds
        }
    })).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmWorkflowSchemaSnapshotId) ->
    datastore_model:get(?CTX, AtmWorkflowSchemaSnapshotId).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmWorkflowSchemaSnapshotId) ->
    datastore_model:delete(?CTX, AtmWorkflowSchemaSnapshotId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given atm workflow schema can be executed as not all
%% valid features are supported yet (e.g. empty lanes, etc.).
%% @end
%%-------------------------------------------------------------------
-spec assert_executable(od_atm_workflow_schema:record()) ->
    ok | no_return().
assert_executable(#od_atm_workflow_schema{lanes = []}) ->
    throw(?ERROR_ATM_WORKFLOW_EMPTY);

assert_executable(#od_atm_workflow_schema{lanes = AtmLaneSchemas}) ->
    lists:foreach(fun assert_lane_schema_executable/1, AtmLaneSchemas).


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


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {schema_id, string},
        {name, string},
        {description, string},

        {stores, [{custom, string, {persistent_record, encode, decode, atm_store_schema}}]},
        {lanes, [{custom, string, {persistent_record, encode, decode, atm_lane_schema}}]},

        {state, {custom, string, {automation, workflow_schema_state_to_json, workflow_schema_state_from_json}}},

        {atm_inventory, string},
        {atm_lambdas, [string]}
    ]}.
