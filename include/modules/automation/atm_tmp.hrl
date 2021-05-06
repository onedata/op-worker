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
    is_array :: boolean(),
    is_optional :: boolean(),
    default_value = undefined :: term()
}).

-type atm_lambda_argument_spec() :: #atm_lambda_argument_spec{}.

-record(atm_task_schema_argument_mapper, {
    name :: binary(),
    input_spec :: map()
}).

-type atm_task_schema_argument_mapper() :: #atm_task_schema_argument_mapper{}.

-endif.
