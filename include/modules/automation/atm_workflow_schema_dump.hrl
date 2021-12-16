%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used when creating random
%%% automation workflow schema dumps.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_SCHEMA_DUMP_DRAFT_HRL).
-define(ATM_WORKFLOW_SCHEMA_DUMP_DRAFT_HRL, 1).


-include_lib("ctool/include/automation/automation.hrl").


-define(ATM_PLACEHOLDER, placeholder).


-record(atm_workflow_schema_dump, {
    supplementary_lambdas :: #{automation:id() => atm_lambda_revision_registry:record()},
    revision_num :: atm_workflow_schema_revision:revision_number(),
    revision :: atm_workflow_schema_revision:record()
}).

-record(atm_workflow_schema_dump_draft, {
    supplementary_lambdas = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | #{
        automation:id() => ?ATM_PLACEHOLDER | #{
            atm_lambda_revision:revision_number() => ?ATM_PLACEHOLDER | atm_lambda_revision_draft()
        }
    },
    revision_num = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_workflow_schema_revision:revision_number(),
    revision = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_workflow_schema_revision_draft()
}).

-record(atm_lambda_revision_draft, {
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    summary = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:summary(),
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),

    operation_spec = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_openfaas_operation_spec_draft(),
    argument_specs = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_lambda_argument_spec_draft()],
    result_specs = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_lambda_result_spec_draft()],

    preferred_batch_size = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | pos_integer(),
    resource_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_resource_spec:record(),
    state = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:lifecycle_state()
}).

-record(atm_openfaas_operation_spec_draft, {
    docker_image = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | binary(),
    docker_execution_options = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | #atm_docker_execution_options{}
}).

-record(atm_lambda_argument_spec_draft, {
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER |  automation:name(),
    data_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_data_spec:record(),
    is_optional = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | boolean(),
    default_value = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | undefined | json_utils:json_term()
}).

-record(atm_lambda_result_spec_draft, {
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    data_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_data_spec:record()
}).

-record(atm_workflow_schema_revision_draft, {
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),
    stores = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | [atm_workflow_schema_dump:atm_store_schema_draft()],
    lanes = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | [atm_workflow_schema_dump:atm_lane_schema_draft()],
    state = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:lifecycle_state()
}).

-record(atm_store_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),
    type = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:store_type(),
    data_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_data_spec:record(),
    requires_initial_value = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | boolean(),
    default_initial_value = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | undefined | json_utils:json_term()
}).

-record(atm_lane_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),

    parallel_boxes = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_parallel_box_schema_draft()],

    store_iterator_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_store_iterator_spec_draft(),
    max_retries = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | non_neg_integer()
}).

-record(atm_parallel_box_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    tasks = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_task_schema_draft()]
}).

-record(atm_task_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    lambda_id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    lambda_revision_number = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_lambda_revision:revision_number(),

    argument_mappings = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_task_schema_argument_mapper_draft()],
    result_mappings = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_task_schema_result_mapper_draft()],

    resource_spec_override = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | undefined | atm_resource_spec:record()
}).

-record(atm_task_schema_argument_mapper_draft, {
    argument_name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    value_builder = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_workflow_schema_dump:atm_task_argument_value_builder_draft()
}).

-record(atm_task_argument_value_builder_draft, {
    type = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_task_argument_value_builder:type(),
    recipe = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_task_argument_value_builder:recipe()
}).

-record(atm_task_schema_result_mapper_draft, {
    result_name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    store_schema_id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    dispatch_function = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_task_schema_result_mapper:dispatch_function()
}).

-record(atm_store_iterator_spec_draft, {
    store_schema_id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    max_batch_size = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | pos_integer()
}).


-endif.
