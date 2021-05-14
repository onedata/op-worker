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
    id :: binary(),
    name :: automation:name(),
    description :: automation:description(),
    type :: automation:store_type(),
    data_spec :: atm_data_spec:record(),
    requires_initial_value :: boolean()
}).
-type atm_store_schema() :: #atm_store_schema{}.

-record(atm_store_iterator_serial_strategy, {}).
-record(atm_store_iterator_batch_strategy, {size :: pos_integer()}).

-type atm_store_iterator_strategy() ::
    #atm_store_iterator_serial_strategy{} |
    #atm_store_iterator_batch_strategy{}.

-record(atm_store_iterator_spec, {
    strategy :: atm_store_iterator_strategy(),
    store_schema_id :: binary()
}).
-type atm_store_iterator_spec() :: #atm_store_iterator_spec{}.

-record(atm_task_schema_argument_mapper, {
    argument_name :: binary(),
    value_builder :: map()
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
    id :: automation:id(),
    name :: automation:name(),
    lambda_id :: automation:id(),
    argument_mappings :: [atm_task_schema_argument_mapper()]
%%    result_mappings :: [atm_task_schema_result_mapper()]
}).
-type atm_task_schema() :: #atm_task_schema{}.

-record(atm_parallel_box_schema, {
    id :: automation:id(),
    name :: automation:name(),
    tasks :: [atm_task_schema()]
}).
-type atm_parallel_box_schema() :: #atm_parallel_box_schema{}.

-record(atm_lane_schema, {
    id :: automation:id(),
    name :: automation:name(),
    parallel_boxes :: [atm_parallel_box_schema()],
    store_iterator_spec :: atm_store_iterator_spec()
}).
-type atm_lane_schema() :: #atm_lane_schema{}.

-record(atm_workflow_schema, {
    id :: automation:id(),
    name :: automation:name(),
    description :: automation:description(),
    stores :: [atm_store_schema()],
    lanes :: [atm_lane_schema()],
    state :: incomplete | ready | deprecated
}).

%% TODO VFS-7637 better error handling and logging
-define(ERROR_ATM_OPENFAAS_NOT_CONFIGURED, {error, openfaas_not_configured}).
-define(ERROR_ATM_OPENFAAS_QUERY_FAILED, {error, openfaas_query_failed}).
-define(ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED,
    {error, openfaas_function_registration_failed}
).

-define(ERROR_ATM_DATA_TYPE_UNVERIFIED(__VALUE, __EXP_TYPE),
    {error, {atm_data_type_unverified, __VALUE, __EXP_TYPE}}
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


-endif.
