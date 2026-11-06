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
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([create/3, get/1, get_lane_schemas/1, delete/1]).

%%% field encoding/decoding procedures
-export([legacy_state_to_json/1, legacy_state_from_json/1]).
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


-spec create(
    atm_workflow_execution:id(),
    atm_workflow_schema_revision:revision_number(),
    od_atm_workflow_schema:doc()
) ->
    {ok, id()} | {error, term()}.
create(AtmWorkflowExecutionId, RevisionNum, AtmWorkflowSchemaDoc = #document{
    key = AtmWorkflowSchemaId,
    value = #od_atm_workflow_schema{
        name = AtmWorkflowSchemaName,
        summary = AtmWorkflowSchemaSummary,
        atm_inventory = AtmInventoryId
    }
}) ->
    {ok, Revision} = atm_workflow_schema_logic:get_revision(RevisionNum, AtmWorkflowSchemaDoc),

    %% TODO VFS-7685 add ref count and gen snapshot id based on doc.revision and schema.revision_number
    ?extract_key(datastore_model:create(?CTX, #document{
        key = AtmWorkflowExecutionId,
        value = #atm_workflow_schema_snapshot{
            schema_id = AtmWorkflowSchemaId,
            name = AtmWorkflowSchemaName,
            summary = AtmWorkflowSchemaSummary,

            revision_number = RevisionNum,
            revision = Revision,

            atm_inventory = AtmInventoryId
        }
    })).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmWorkflowSchemaSnapshotId) ->
    datastore_model:get(?CTX, AtmWorkflowSchemaSnapshotId).


-spec get_lane_schemas(record() | doc()) -> [atm_lane_schema:record()].
get_lane_schemas(#atm_workflow_schema_snapshot{revision = #atm_workflow_schema_revision{
    lanes = AtmLaneSchemas
}}) ->
    AtmLaneSchemas;
get_lane_schemas(#document{value = AtmWorkflowSchemaSnapshot}) ->
    get_lane_schemas(AtmWorkflowSchemaSnapshot).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmWorkflowSchemaSnapshotId) ->
    datastore_model:delete(?CTX, AtmWorkflowSchemaSnapshotId).


%%%===================================================================
%%% field encoding/decoding procedures
%%%===================================================================


%% NOTE: used only in record version 1
-spec legacy_state_to_json(atom()) -> json_utils:json_term().
legacy_state_to_json(incomplete) -> <<"incomplete">>;
legacy_state_to_json(ready) -> <<"ready">>;
legacy_state_to_json(deprecated) -> <<"deprecated">>.


%% NOTE: used only in record version 1
-spec legacy_state_from_json(json_utils:json_term()) -> atom().
legacy_state_from_json(<<"incomplete">>) -> incomplete;
legacy_state_from_json(<<"ready">>) -> ready;
legacy_state_from_json(<<"deprecated">>) -> deprecated.


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
        {summary, string},

        {revision_number, integer},
        {revision, {custom, string, {persistent_record, to_string, from_string, atm_workflow_schema_revision}}},

        {atm_inventory, string}
    ]}.
