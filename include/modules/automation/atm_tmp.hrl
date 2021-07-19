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


%% TODO VFS-7637 better error handling and logging
-define(ERROR_ATM_INTERNAL_SERVER_ERROR, {error, atm_internal_server_error}).
-define(ERROR_ATM_OPENFAAS_NOT_CONFIGURED, {error, openfaas_not_configured}).
-define(ERROR_ATM_OPENFAAS_UNREACHABLE, {error, openfaas_unreachable}).
-define(ERROR_ATM_OPENFAAS_QUERY_FAILED, {error, openfaas_query_failed}).
-define(ERROR_ATM_OPENFAAS_QUERY_FAILED(__REASON),
    {error, {openfaas_query_failed, __REASON}}
).
-define(ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED,
    {error, openfaas_function_registration_failed}
).

-define(ERROR_ATM_DATA_TYPE_UNVERIFIED(__VALUE, __EXP_TYPE),
    {error, {atm_data_type_unverified, __VALUE, __EXP_TYPE}}
).

-define(ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(__VALUE_CONSTRAINT),
    {error, {atm_data_value_constraint_unverified, __VALUE_CONSTRAINT}}
).

-define(ERROR_ATM_REFERENCED_NONEXISTENT_STORE(__STORE_SCHEMA_ID),
    {error, {atm_task_arg_mapper_nonexistent_store, __STORE_SCHEMA_ID}}
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
-define(ERROR_ATM_TASK_ARG_MAPPER_INVALID_INPUT_SPEC,
    {error, atm_task_arg_mapper_invalid_input_spec}
).
-define(ERROR_ATM_TASK_ARG_MAPPER_ITEM_QUERY_FAILED(__ITEM, __QUERY),
    {error, {atm_task_arg_item_query_failed, __ITEM, __QUERY}}
).
-define(ERROR_ATM_TASK_MISSING_RESULT(__RESULT_NAME),
    {error, {atm_task_missing_result, __RESULT_NAME}}
).
-define(ERROR_ATM_TASK_RESULT_DISPATCH_FAILED(__STORE_SCHEMA_ID, __REASON),
    {error, {atm_task_result_dispatch_failed, __STORE_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_TASK_RESULT_MAPPING_FAILED(__RESULT_NAME, __REASON),
    {error, {atm_task_result_mapping_failed, __RESULT_NAME, __REASON}}
).
-define(ERROR_ATM_BAD_DATA, {error, atm_bad_data}).
-define(ERROR_ATM_BAD_DATA(__KEY, __REASON),
    {error, {atm_bad_data, __KEY, __REASON}}
).
-define(ERROR_ATM_UNSUPPORTED_DATA_TYPE(__UNSUPPORTED_TYPE, __SUPPORTED_TYPES),
    {error, {atm_unsupported_data_type, __UNSUPPORTED_TYPE, __SUPPORTED_TYPES}}
).
-define(ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE,
    {error, atm_store_missing_required_initial_value}
).
-define(ERROR_ATM_STORE_CREATION_FAILED(__STORE_SCHEMA_ID, __REASON),
    {error, {atm_store_creation_failed, __STORE_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(__TASK_SCHEMA_ID, __REASON),
    {error, {atm_task_execution_creation_failed, __TASK_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_EMPTY_PARALLEL_BOX(__PARALLEL_BOX_SCHEMA_ID),
    {error, {atm_parallel_box_empty, __PARALLEL_BOX_SCHEMA_ID}}
).
-define(ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(__PARALLEL_BOX_SCHEMA_ID, __REASON),
    {error, {atm_parallel_box_execution_creation_failed, __PARALLEL_BOX_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_EMPTY_LANE(__LANE_SCHEMA_ID),
    {error, {atm_lane_empty, __LANE_SCHEMA_ID}}
).
-define(ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(__LANE_SCHEMA_ID, __REASON),
    {error, {atm_lane_execution_creation_failed, __LANE_SCHEMA_ID, __REASON}}
).

-define(ERROR_ATM_TASK_EXECUTION_PREPARATION_FAILED(__TASK_SCHEMA_ID, __REASON),
    {error, {atm_task_execution_preparation_failed, __TASK_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_PARALLEL_BOX_EXECUTION_PREPARATION_FAILED(__PARALLEL_BOX_SCHEMA_ID, __REASON),
    {error, {atm_parallel_box_preparation_creation_failed, __PARALLEL_BOX_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(__LANE_SCHEMA_ID, __REASON),
    {error, {atm_lane_execution_preparation_failed, __LANE_SCHEMA_ID, __REASON}}
).
-define(ERROR_ATM_INVALID_STATUS_TRANSITION(__PREV_STATUS, __NEW_STATUS),
    {error, {atm_invalid_status_transition, __PREV_STATUS, __NEW_STATUS}}
).
-define(ERROR_ATM_STORE_FROZEN(__ATM_STORE_SCHEMA__ID),
    {error, {atm_store_frozen, __ATM_STORE_SCHEMA__ID}}
).
-define(ERROR_ATM_STORE_TYPE_DISALLOWED(__TYPE, __ALLOWED_TYPES),
    {error, {atm_store_type_unverified, __TYPE, __ALLOWED_TYPES}}
).
-define(ERROR_ATM_STORE_EMPTY(__ATM_STORE_SCHEMA__ID),
    {error, {atm_store_empty, __ATM_STORE_SCHEMA__ID}}
).

-define(ERROR_ATM_TASK_EXECUTION_ENDED, {error, atm_task_execution_ended}).

-endif.
