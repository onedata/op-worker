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
    type :: atm_data_spec:type(),
    value_constraints = #{} :: json_utils:json_map()
}).

-record(atm_store_schema, {
    name :: atm_store:name(),
    summary :: atm_store:summary(),
    description :: atm_store:description(),
    is_input_store :: boolean(),
    store_type :: atm_store:type(),
    data_spec :: atm_data_spec:spec()
}).

-type atm_store_schema() :: #atm_store_schema{}.

-endif.
