%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tmp module - remove after integration with ctool and lambdas.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_type).
-author("Bartosz Walkowicz").

-type type() :: atm_integer_type | atm_string_type | atm_object_type.
-type value_constraints() :: map().

-export_type([type/0, value_constraints/0]).
