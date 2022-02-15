%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper functions for creation of automation schemas used in CT tests.
%%% Schemas are created from schema drafts which are records in most cases
%%% identical to them with the only difference being the optional fields.
%%% If field is not explicitly set in draft the created schema will have
%%% automatically generated correct value for it (it is not possible to use
%%% target schema records for this as their unset fields will have 'undefined'
%%% value which in turn can be correct value for them - it is not possible to
%%% tell if field was set or not in such case).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_schema_factory).
-author("Bartosz Walkowicz").

-include("atm_test_schema.hrl").

-export([create_from_draft/1]).


-type atm_openfaas_operation_spec_draft() :: #atm_openfaas_operation_spec_draft{}.
-type atm_lambda_revision_draft() :: #atm_lambda_revision_draft{}.

-type atm_store_iterator_spec_draft() :: #atm_store_iterator_spec_draft{}.
-type atm_task_schema_draft() :: #atm_task_schema_draft{}.
-type atm_parallel_box_schema_draft() :: #atm_parallel_box_schema_draft{}.
-type atm_lane_schema_draft() :: #atm_lane_schema_draft{}.

-type atm_store_schema_draft() :: #atm_store_schema_draft{}.

-type atm_workflow_schema_revision_draft() :: #atm_workflow_schema_revision_draft{}.

-type atm_workflow_schema_dump_draft() :: #atm_workflow_schema_dump_draft{}.

-export_type([
    atm_openfaas_operation_spec_draft/0,
    atm_lambda_revision_draft/0,

    atm_store_iterator_spec_draft/0,
    atm_task_schema_draft/0,
    atm_parallel_box_schema_draft/0,
    atm_lane_schema_draft/0,

    atm_store_schema_draft/0,

    atm_workflow_schema_revision_draft/0,

    atm_workflow_schema_dump_draft/0
]).


-define(RAND_INT(From, To), From + rand:uniform(To - From + 1) - 1).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_from_draft
    (atm_workflow_schema_dump_draft()) -> atm_test_inventory:atm_workflow_schema_dump();
    (atm_lambda_revision_draft()) -> atm_lambda_revision:record();
    (atm_openfaas_operation_spec_draft()) -> atm_openfaas_operation_spec:record();
    (atm_workflow_schema_revision_draft()) -> atm_workflow_schema_revision:record();
    (atm_store_schema_draft()) -> atm_store_schema:record();
    (atm_lane_schema_draft()) -> atm_lane_schema:record();
    (atm_parallel_box_schema_draft()) -> atm_parallel_box_schema:record();
    (atm_task_schema_draft()) -> atm_task_schema:record();
    (atm_store_iterator_spec_draft()) -> atm_store_iterator_spec:record().
create_from_draft(#atm_workflow_schema_dump_draft{
    name = PlaceholderOrName,
    summary = PlaceholderOrSummary,
    revision_num = AtmWorkflowSchemaRevisionNum,
    revision = AtmWorkflowSchemaRevision,
    supplementary_lambdas = SupplementaryLambdaDrafts
}) ->
    SupplementaryLambdas = maps:map(fun(_AtmLambdaId, AtmLambdaRevisionRegistryDraft) ->
        #atm_lambda_revision_registry{registry = maps:map(fun(_RevisionNum, AtmLambdaRevisionDraft) ->
            create_from_draft(AtmLambdaRevisionDraft)
        end, AtmLambdaRevisionRegistryDraft)}
    end, SupplementaryLambdaDrafts),

    #atm_workflow_schema_dump{
        name = ensure_name(PlaceholderOrName),
        summary = ensure_summary(PlaceholderOrSummary),
        revision_num = AtmWorkflowSchemaRevisionNum,
        revision = create_from_draft(AtmWorkflowSchemaRevision),
        supplementary_lambdas = SupplementaryLambdas
    };

create_from_draft(#atm_lambda_revision_draft{
    name = PlaceholderOrName,
    summary = PlaceholderOrSummary,
    description = PlaceholderOrDescription,

    operation_spec = PlaceholderOrOperationSpec,
    argument_specs = ArgSpecs,
    result_specs = ResultSpecs,

    preferred_batch_size = PlaceholderOrPreferredBatchSize,
    resource_spec = PlaceholderOrResourceSpec,
    state = PlaceholderOrState
}) ->
    AtmLambdaRevision = #atm_lambda_revision{
        name = ensure_name(PlaceholderOrName),
        summary = ensure_summary(PlaceholderOrSummary),
        description = ensure_description(PlaceholderOrDescription),

        operation_spec = case PlaceholderOrOperationSpec of
            ?ATM_AUTOGENERATE -> atm_test_utils:example_operation_spec();
            _ -> create_from_draft(PlaceholderOrOperationSpec)
        end,
        argument_specs = ArgSpecs,
        result_specs = ResultSpecs,

        preferred_batch_size = ensure_specified(PlaceholderOrPreferredBatchSize, ?RAND_INT(1, 100)),
        resource_spec = ensure_specified(PlaceholderOrResourceSpec, lists_utils:random_element(
            atm_test_utils:example_resource_specs()
        )),
        state = ensure_lifecycle_state(PlaceholderOrState),
        checksum = <<>>
    },

    AtmLambdaRevision#atm_lambda_revision{
        checksum = atm_lambda_revision:calculate_checksum(AtmLambdaRevision)
    };

create_from_draft(#atm_openfaas_operation_spec_draft{
    docker_image = PlaceholderOrDockerImage,
    docker_execution_options = PlaceholderOrDockerExecutionOptions
}) ->
    #atm_openfaas_operation_spec{
        docker_image = ensure_specified(PlaceholderOrDockerImage, str_utils:rand_hex(10)),
        docker_execution_options = case PlaceholderOrDockerExecutionOptions of
            ?ATM_AUTOGENERATE ->
                lists_utils:random_element(atm_test_utils:example_docker_execution_options());
            DockerExecutionOptions ->
                DockerExecutionOptions
        end
    };

create_from_draft(#atm_workflow_schema_revision_draft{
    description = PlaceholderOrDescription,
    stores = AtmStoreSchemaDrafts,
    lanes = AtmLaneSchemaDrafts,
    state = PlaceholderOrState
}) ->
    #atm_workflow_schema_revision{
        description = ensure_description(PlaceholderOrDescription),
        stores = lists:map(fun create_from_draft/1, AtmStoreSchemaDrafts),
        lanes = lists:map(fun create_from_draft/1, AtmLaneSchemaDrafts),
        state = ensure_lifecycle_state(PlaceholderOrState)
    };

create_from_draft(#atm_store_schema_draft{
    id = Id,
    name = PlaceholderOrName,
    description = PlaceholderOrDescription,
    type = Type,
    data_spec = AtmDataSpec,
    requires_initial_value = RequiresInitialValue,
    default_initial_value = DefaultInitialValue
}) ->
    #atm_store_schema{
        id = Id,
        name = ensure_name(PlaceholderOrName),
        description = ensure_description(PlaceholderOrDescription),
        type = Type,
        data_spec = AtmDataSpec,
        requires_initial_value = RequiresInitialValue,
        default_initial_value = DefaultInitialValue
    };

create_from_draft(#atm_lane_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,

    parallel_boxes = AtmParallelBoxSchemaDrafts,

    store_iterator_spec = AtmStoreIteratorSpecDraft,
    max_retries = PlaceholderOrMaxRetries
}) ->
    #atm_lane_schema{
        id = ensure_id(PlaceholderOrId),
        name = ensure_name(PlaceholderOrName),

        parallel_boxes = lists:map(fun create_from_draft/1, AtmParallelBoxSchemaDrafts),

        store_iterator_spec = create_from_draft(AtmStoreIteratorSpecDraft),
        max_retries = ensure_specified(PlaceholderOrMaxRetries, ?RAND_INT(1, 4))
    };

create_from_draft(#atm_parallel_box_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    tasks = AtmTaskSchemaDrafts
}) ->
    #atm_parallel_box_schema{
        id = ensure_id(PlaceholderOrId),
        name = ensure_name(PlaceholderOrName),
        tasks = lists:map(fun create_from_draft/1, AtmTaskSchemaDrafts)
    };

create_from_draft(#atm_task_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    lambda_id = AtmLambdaId,
    lambda_revision_number = AtmLambdaRevisionNum,

    argument_mappings = ArgMappings,
    result_mappings = ResultMappings,

    resource_spec_override = PlaceholderOrResourceSpecOverride
}) ->
    #atm_task_schema{
        id = ensure_id(PlaceholderOrId),
        name = ensure_name(PlaceholderOrName),
        lambda_id = AtmLambdaId,
        lambda_revision_number = AtmLambdaRevisionNum,

        argument_mappings = ArgMappings,
        result_mappings = ResultMappings,

        resource_spec_override = case PlaceholderOrResourceSpecOverride of
            ?ATM_AUTOGENERATE ->
                lists_utils:random_element([undefined | atm_test_utils:example_resource_specs()]);
            ResourceSpecOverride ->
                ResourceSpecOverride
        end
    };

create_from_draft(#atm_store_iterator_spec_draft{
    store_schema_id = AtmStoreSchemaId,
    max_batch_size = PlaceholderOrMaxBatchSize
}) ->
    #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId,
        max_batch_size = ensure_specified(PlaceholderOrMaxBatchSize, ?RAND_INT(1, 10))
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_specified(Value, DefaultValue) -> Value | DefaultValue.
ensure_specified(?ATM_AUTOGENERATE, Default) -> Default;
ensure_specified(Value, _Default) -> Value.


%% @private
-spec ensure_id(?ATM_AUTOGENERATE | automation:id()) -> automation:id().
ensure_id(?ATM_AUTOGENERATE) -> atm_test_utils:example_id();
ensure_id(Id) -> Id.


%% @private
-spec ensure_name(?ATM_AUTOGENERATE | automation:name()) -> automation:name().
ensure_name(?ATM_AUTOGENERATE) -> atm_test_utils:example_name();
ensure_name(Name) -> Name.


%% @private
-spec ensure_summary(?ATM_AUTOGENERATE | automation:summary()) ->
    automation:summary().
ensure_summary(?ATM_AUTOGENERATE) -> atm_test_utils:example_summary();
ensure_summary(Description) -> Description.


%% @private
-spec ensure_description(?ATM_AUTOGENERATE | automation:description()) ->
    automation:description().
ensure_description(?ATM_AUTOGENERATE) -> atm_test_utils:example_description();
ensure_description(Description) -> Description.


%% @private
-spec ensure_lifecycle_state(?ATM_AUTOGENERATE | json_utils:json_term()) ->
    json_utils:json_term().
ensure_lifecycle_state(?ATM_AUTOGENERATE) -> atm_test_utils:example_lifecycle_state();
ensure_lifecycle_state(State) -> State.
