%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor` 
%%% functionality for `atm_object_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_object_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).

-include("modules/automation/atm_tmp.hrl").

%% atm_data_validator callbacks
-export([validate/3]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec validate(
    atm_workflow_execution_ctx:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
validate(_, Value, _ValueConstraints) when is_map(Value) ->
    ok;
validate(_, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_object_type)).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded()) -> map().
compress(Value) -> Value.

-spec expand(atm_workflow_execution_ctx:record(), map()) ->
    {ok, atm_value:expanded()}.
expand(_, Value) ->
    {ok, Value}.
