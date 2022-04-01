%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of automation schema related record drafts
%%% used in CT tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TEST_SCHEMA_HRL).
-define(ATM_TEST_SCHEMA_HRL, 1).


-include_lib("ctool/include/automation/automation.hrl").


% sentinel value indicating that marked field should be filled with automatically
% generated value ('undefined' atom can not be used as it is valid value for some
% fields)
-define(ATM_AUTOGENERATE, atm_autogenerate).


-record(atm_workflow_schema_dump, {
    name :: automation:name(),
    summary :: automation:summary(),
    revision_num :: atm_workflow_schema_revision:revision_number(),
    revision :: atm_workflow_schema_revision:record(),
    supplementary_lambdas :: #{automation:id() => atm_lambda_revision_registry:record()}
}).

-record(atm_workflow_schema_dump_draft, {
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),
    summary = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:summary(),
    revision_num :: atm_workflow_schema_revision:revision_number(),
    revision :: atm_test_schema_factory:atm_workflow_schema_revision_draft(),
    supplementary_lambdas = #{} :: #{automation:id() => #{
        atm_lambda_revision:revision_number() => atm_test_schema_factory:atm_lambda_revision_draft()
    }}
}).

-record(atm_lambda_revision_draft, {
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),
    summary = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:summary(),
    description = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:description(),

    operation_spec = ?ATM_AUTOGENERATE ::
        ?ATM_AUTOGENERATE | atm_test_schema_factory:atm_openfaas_operation_spec_draft(),
    argument_specs :: [atm_lambda_argument_spec:record()],
    result_specs :: [atm_lambda_result_spec:record()],

    preferred_batch_size = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | pos_integer(),
    resource_spec = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | atm_resource_spec:record(),
    state = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:lifecycle_state()
}).

-record(atm_openfaas_operation_spec_draft, {
    docker_image = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | binary(),
    docker_execution_options = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | #atm_docker_execution_options{}
}).

-record(atm_workflow_schema_revision_draft, {
    description = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:description(),
    stores :: [atm_test_schema_factory:atm_store_schema_draft()],
    lanes :: [atm_test_schema_factory:atm_lane_schema_draft()],
    state = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:lifecycle_state()
}).

-record(atm_store_schema_draft, {
    id :: automation:name(),
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),
    description = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:description(),
    type :: automation:store_type(),
    config :: atm_store_config:record(),
    requires_initial_content :: boolean(),
    default_initial_content :: undefined | json_utils:json_term()
}).

-record(atm_lane_schema_draft, {
    id = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:id(),
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),

    parallel_boxes :: [atm_test_schema_factory:atm_parallel_box_schema_draft()],

    store_iterator_spec :: atm_test_schema_factory:atm_store_iterator_spec_draft(),
    max_retries = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | non_neg_integer()
}).

-record(atm_parallel_box_schema_draft, {
    id = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:id(),
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),
    tasks :: [atm_test_schema_factory:atm_task_schema_draft()]
}).

-record(atm_task_schema_draft, {
    id = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:id(),
    name = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | automation:name(),
    % TODO VFS-9060 allow to specify lambda draft instead of {id, revision_num}
    lambda_id :: automation:id(),
    lambda_revision_number :: atm_lambda_revision:revision_number(),

    argument_mappings :: [atm_task_schema_argument_mapper:record()],
    result_mappings :: [atm_task_schema_result_mapper:record()],

    resource_spec_override = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | undefined | atm_resource_spec:record()
}).

-record(atm_store_iterator_spec_draft, {
    store_schema_id :: automation:id(),
    max_batch_size = ?ATM_AUTOGENERATE :: ?ATM_AUTOGENERATE | pos_integer()
}).


-endif.
