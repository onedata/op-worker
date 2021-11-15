%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing automation lambda snapshot.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_snapshot).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, get/1, get_revision/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).


-type id() :: binary().
-type record() :: #atm_lambda_snapshot{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(atm_workflow_execution:id(), od_atm_lambda:doc()) ->
    {ok, id()} | {error, term()}.
create(AtmWorkflowExecutionId, #document{key = AtmLambdaId, value = #od_atm_lambda{
    revision_registry = RevisionRegistry,
    atm_inventories = AtmInventories
}}) ->
    %% TODO VFS-7685 add ref count and gen snapshot id based on doc revision and lambda revisions
    %% (not used in given workflow execution lambda revisions are removed from revision registry)
    ?extract_key(datastore_model:create(?CTX, #document{
        key = datastore_key:new_from_digest([AtmWorkflowExecutionId, AtmLambdaId]),
        value = #atm_lambda_snapshot{
            lambda_id = AtmLambdaId,
            revision_registry = RevisionRegistry,
            atm_inventories = AtmInventories
        }
    })).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmLambdaSnapshotId) ->
    datastore_model:get(?CTX, AtmLambdaSnapshotId).


-spec get_revision(atm_lambda_revision:revision_number(), record()) ->
    atm_lambda_revision:record().
get_revision(RevisionNum, #atm_lambda_snapshot{revision_registry = RevisionRegistry}) ->
    atm_lambda_revision_registry:get_revision(RevisionNum, RevisionRegistry).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmLambdaSnapshotId) ->
    datastore_model:delete(?CTX, AtmLambdaSnapshotId).


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
    2.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {lambda_id, string},

        {name, string},
        {summary, string},
        {description, string},

        {operation_spec, {custom, string, {persistent_record, encode, decode, atm_lambda_operation_spec}}},
        {argument_specs, [{custom, string, {persistent_record, encode, decode, atm_lambda_argument_spec}}]},
        {result_specs, [{custom, string, {persistent_record, encode, decode, atm_lambda_result_spec}}]},

        {atm_inventories, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {lambda_id, string},
        % new field - name, summary, description, operation_spec, argument_specs, result_specs
        % are now stored per revision in it
        {revision_registry, {custom, string, {persistent_record, encode, decode, atm_lambda_revision_registry}}},
        {atm_inventories, [string]}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {
    ?MODULE,
    LambdaId,
    Name,
    Summary,
    Description,
    OperationSpec,
    ArgumentSpecs,
    ResultSpecs,
    InventoryIds
}) ->
    % Missing resource spec is constructed from some random values as they are irrelevant
    % due cluster upgrade from 21.02-alpha21 procedure purging all atm related docs
    % (record upgrade procedure is executed before cluster upgrade so some random
    % junk must be, unfortunately, specified)
    ResourceSpec = #atm_resource_spec{
        cpu_requested = 0.1,
        cpu_limit = undefined,
        memory_requested = 104857600,
        memory_limit = undefined,
        ephemeral_storage_requested = 104857600,
        ephemeral_storage_limit = undefined
    },
    Revision = #atm_lambda_revision{
        name = Name,
        summary = Summary,
        description = Description,
        operation_spec = OperationSpec,
        argument_specs = ArgumentSpecs,
        result_specs = ResultSpecs,
        resource_spec = ResourceSpec,
        checksum = <<>>,
        state = stable
    },
    RevisionRegistry = atm_lambda_revision_registry:add_revision(
        1,
        Revision#atm_lambda_revision{checksum = atm_lambda_revision:calculate_checksum(Revision)},
        atm_lambda_revision_registry:empty()
    ),

    {2, #atm_lambda_snapshot{
        lambda_id = LambdaId,
        revision_registry = RevisionRegistry,
        atm_inventories = InventoryIds
    }}.
