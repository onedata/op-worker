%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros and records for temporary use - after integration with ctool/oz
%%% should be removed.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TMP_HRL).
-define(ATM_TMP_HRL, 1).

-type automation_name() :: binary().
-type automation_summary() :: binary().
-type automation_description() :: binary().

-record(atm_data_spec, {
    type :: atm_data_type:type(),
    value_constraints = #{} :: atm_data_type:value_constraints()
}).

-record(atm_store_schema, {
    name :: atm_store:name(),
    summary :: atm_store:summary(),
    description :: atm_store:description(),
    is_input_store :: boolean(),
    store_type :: atm_store:type(),
    data_spec :: atm_data_spec:record()
}).
-type atm_store_schema() :: #atm_store_schema{}.

-record(serial_mode, {}).
-record(bulk_mode, {size :: pos_integer()}).

-type atm_stream_mode() :: #serial_mode{} | #bulk_mode{}.

-record(atm_stream_schema, {
    mode :: atm_stream_mode()
}).
-type atm_stream_schema() :: #atm_stream_schema{}.

-record(atm_lambda_argument_spec, {
    name :: binary(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean(),
    is_optional :: boolean(),
    default_value = undefined :: undefined | json_utils:json_term()
}).
-type atm_lambda_argument_spec() :: #atm_lambda_argument_spec{}.

-record(atm_lambda_result_spec, {
    name :: binary(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean()
}).
-type atm_lambda_result_spec() :: #atm_lambda_result_spec{}.

-record(atm_task_schema_argument_mapper, {
    name :: binary(),
    input_spec :: map()
}).
-type atm_task_schema_argument_mapper() :: #atm_task_schema_argument_mapper{}.

-record(atm_docker_execution_options, {
    readonly = false :: boolean(),
    mount_oneclient = false :: boolean(),
    oneclient_mount_point = <<"/mnt/onedata">> :: binary(),
    oneclient_options = <<"">> :: binary()
}).
-type atm_docker_execution_options() :: #atm_docker_execution_options{}.

-record(atm_openfaas_operation_spec, {
    docker_image :: binary(),
    docker_execution_options :: #atm_docker_execution_options{}
}).
-type atm_openfaas_operation_spec() :: #atm_openfaas_operation_spec{}.

-record(atm_lambda_operation_spec, {
    spec :: atm_openfaas_operation_spec()
}).
-type atm_lambda_operation_spec() :: #atm_lambda_operation_spec{}.

-record(od_atm_lambda, {
    name :: automation_name(),
    summary :: automation_summary(),
    description :: automation_description(),

    operation_spec :: atm_lambda_operation_spec(),
    argument_specs = [] :: [atm_lambda_argument_spec()]
%%    result_specs = [] :: [atm_lambda_result_spec:record()]
}).
-type od_atm_lambda() :: #od_atm_lambda{}.

-record(atm_workflow_execution_ctx, {
    id :: binary()
}).
-type atm_workflow_execution_ctx() :: #atm_workflow_execution_ctx{}.

-record(atm_task_schema, {
    id :: binary(),
    name :: binary(),
    lambda_id :: binary(),
    argument_mappings :: [atm_task_schema_argument_mapper()]
}).
-type atm_task_schema() :: #atm_task_schema{}.

-define(ERROR_ATM_NO_TASK_MAPPER_FOR_REQUIRED_LAMBDA_ARG(__ARG_NAME),
    {error, {no_task_mapper_for, __ARG_NAME}}
).
-define(ERROR_ATM_TASK_MAPPER_FOR_NONEXISTENT_LAMBDA_ARG(__ARG_NAME),
    {error, {task_mapper_for_nonexistent_arg, __ARG_NAME}}
).

-define(ERROR_ATM_OPENFAAS_NOT_CONFIGURED, {error, openfaas_not_configured}).
-define(ERROR_ATM_OPENFAAS_QUERY_FAILED, {error, openfaas_query_failed}).
-define(ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILURE,
    {error, openfaas_function_registration_failure}
).

-define(ERROR_ATM_DATA_TYPE_UNVERIFIED(__VALUE, __EXP_TYPE),
    {error, {atm_data_type_unverified, __VALUE, __EXP_TYPE}}
).

-define(ERROR_ATM_TASK_ARG_MAPPING_FAILED(__ARG_NAME, __REASON),
    {error, {atm_task_arg_mapping_failed, __ARG_NAME, __REASON}}
).
-define(ERROR_TASK_ARG_MAPPER_INVALID_INPUT_SPEC,
    {error, atm_task_mapper_invalid_input_spec}
).

-type operation_spec_engine() :: openfaas.

-endif.
