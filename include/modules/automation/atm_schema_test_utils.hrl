%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in automation CT test utils.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_SCHEMA_DUMP_DRAFT_HRL).
-define(ATM_WORKFLOW_SCHEMA_DUMP_DRAFT_HRL, 1).


-include_lib("ctool/include/automation/automation.hrl").


-define(ATM_PLACEHOLDER, placeholder).


-record(atm_workflow_schema_dump, {
    name :: automation:name(),
    summary :: automation:summary(),
    revision_num :: atm_workflow_schema_revision:revision_number(),
    revision :: atm_workflow_schema_revision:record(),
    supplementary_lambdas :: #{automation:id() => atm_lambda_revision_registry:record()}
}).

-record(atm_workflow_schema_dump_draft, {
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    summary = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:summary(),
    revision_num :: atm_workflow_schema_revision:revision_number(),
    revision :: atm_test_schema_factory:atm_workflow_schema_revision_draft(),
    supplementary_lambdas = #{} :: #{automation:id() => #{
        atm_lambda_revision:revision_number() => atm_test_schema_factory:atm_lambda_revision_draft()
    }}
}).

-record(atm_lambda_revision_draft, {
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    summary = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:summary(),
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),

    operation_spec = ?ATM_PLACEHOLDER ::
        ?ATM_PLACEHOLDER | atm_test_schema_factory:atm_openfaas_operation_spec_draft(),
    argument_specs :: [atm_lambda_argument_spec:record()],
    result_specs :: [atm_lambda_result_spec:record()],

    preferred_batch_size = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | pos_integer(),
    resource_spec = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | atm_resource_spec:record(),
    state = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:lifecycle_state()
}).

-record(atm_openfaas_operation_spec_draft, {
    docker_image = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | binary(),
    docker_execution_options = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | #atm_docker_execution_options{}
}).

-record(atm_workflow_schema_revision_draft, {
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),
    stores :: [atm_test_schema_factory:atm_store_schema_draft()],
    lanes :: [atm_test_schema_factory:atm_lane_schema_draft()],
    state = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:lifecycle_state()
}).

-record(atm_store_schema_draft, {
    id :: automation:name(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    description = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:description(),
    type :: automation:store_type(),
    data_spec :: atm_data_spec:record(),
    requires_initial_value :: boolean(),
    default_initial_value :: undefined | json_utils:json_term()
}).

-record(atm_lane_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),

    parallel_boxes :: [atm_test_schema_factory:atm_parallel_box_schema_draft()],

    store_iterator_spec :: atm_test_schema_factory:atm_store_iterator_spec_draft(),
    max_retries = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | non_neg_integer()
}).

-record(atm_parallel_box_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    tasks :: [atm_test_schema_factory:atm_task_schema_draft()]
}).

-record(atm_task_schema_draft, {
    id = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:id(),
    name = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | automation:name(),
    lambda_id :: automation:id(),
    lambda_revision_number :: atm_lambda_revision:revision_number(),

    argument_mappings :: [atm_task_schema_argument_mapper:record()],
    result_mappings :: [atm_task_schema_result_mapper:record()],

    resource_spec_override = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | undefined | atm_resource_spec:record()
}).

-record(atm_store_iterator_spec_draft, {
    store_schema_id :: automation:id(),
    max_batch_size = ?ATM_PLACEHOLDER :: ?ATM_PLACEHOLDER | pos_integer()
}).


-endif.
