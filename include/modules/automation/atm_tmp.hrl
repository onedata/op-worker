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

-record(atm_data_spec2, {
    type :: atm_data_type2:type(),
    value_constraints = #{} :: atm_data_type2:value_constraints()
}).

-record(atm_store_schema, {
    name :: atm_store:name(),
    summary :: atm_store:summary(),
    description :: atm_store:description(),
    is_input_store :: boolean(),
    store_type :: atm_store:type(),
    data_spec :: atm_data_spec2:record()
}).

-type atm_store_schema() :: #atm_store_schema{}.

-record(serial_mode, {}).
-record(bulk_mode, {size :: pos_integer()}).

-type atm_stream_mode() :: #serial_mode{} | #bulk_mode{}.

-record(atm_stream_schema, {
    mode :: atm_stream_mode()
}).

-type atm_stream_schema() :: #atm_stream_schema{}.

-endif.
