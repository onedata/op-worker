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

-record(atm_task_schema_argument_mapper, {
    name :: binary(),
    input_spec :: map()
}).
-type atm_task_schema_argument_mapper() :: #atm_task_schema_argument_mapper{}.

-record(od_atm_lambda, {
    name :: automation:name(),
    summary :: automation:summary(),
    description :: automation:description(),

    operation_spec :: atm_lambda_operation_spec:record(),
    argument_specs = [] :: [atm_lambda_argument_spec:record()]
%%    result_specs = [] :: [atm_lambda_result_spec:record()]
}).
-type od_atm_lambda() :: #od_atm_lambda{}.

-record(atm_task_schema, {
    id :: binary(),
    name :: binary(),
    lambda_id :: binary(),
    argument_mappings :: [atm_task_schema_argument_mapper()]
}).
-type atm_task_schema() :: #atm_task_schema{}.

%% TODO VFS-7637 better error handling and logging
-define(ERROR_ATM_OPENFAAS_NOT_CONFIGURED, {error, openfaas_not_configured}).
-define(ERROR_ATM_OPENFAAS_QUERY_FAILED, {error, openfaas_query_failed}).
-define(ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED,
    {error, openfaas_function_registration_failed}
).

-define(ERROR_ATM_DATA_TYPE_UNVERIFIED(__VALUE, __EXP_TYPE),
    {error, {atm_data_type_unverified, __VALUE, __EXP_TYPE}}
).

-define(ERROR_ATM_NO_TASK_ARG_MAPPER_FOR_REQUIRED_LAMBDA_ARG(__ARG_NAME),
    {error, {no_task_arg_mapper_for, __ARG_NAME}}
).
-define(ERROR_ATM_TASK_ARG_MAPPER_FOR_NONEXISTENT_LAMBDA_ARG(__ARG_NAME),
    {error, {task_arg_mapper_for_nonexistent_arg, __ARG_NAME}}
).
-define(ERROR_ATM_TASK_ARG_MAPPING_FAILED(__ARG_NAME, __REASON),
    {error, {atm_task_arg_mapping_failed, __ARG_NAME, __REASON}}
).
-define(ERROR_TASK_ARG_MAPPER_INVALID_INPUT_SPEC,
    {error, atm_task_arg_mapper_invalid_input_spec}
).
-define(ERROR_TASK_ARG_MAPPER_NONEXISTENT_STORE(__STORE_SCHEMA_ID),
    {error, {atm_task_arg_mapper_nonexistent_store, __STORE_SCHEMA_ID}}
).
-define(ERROR_TASK_ARG_MAPPER_ITEM_QUERY_FAILED(__ITEM, __QUERY),
    {error, {atm_task_arg_item_query_failed, __ITEM, __QUERY}}
).

-endif.
