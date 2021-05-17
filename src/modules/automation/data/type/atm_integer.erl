%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` functionality for
%%% `atm_integer_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_integer).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).

%% atm_data_validator callbacks
-export([is_instance/2]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec is_instance(term(), atm_data_type2:value_constraints()) -> boolean().
is_instance(Value, _ValueConstraints) when is_integer(Value) ->
    true;
is_instance(_Value, _ValueConstraints) ->
    false.
