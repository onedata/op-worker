%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` functionality for
%%% `atm_object_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_object_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).

-include("modules/automation/atm_tmp.hrl").

%% atm_data_validator callbacks
-export([assert_instance/2]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_instance(atm_execution:item(), atm_data_type:value_constraints()) ->
    ok | no_return().
assert_instance(Value, _ValueConstraints) when is_map(Value) ->
    ok;
assert_instance(Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_object_type)).
