%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor` 
%%% functionality for `atm_number_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_number_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3, resolve/3]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_number_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    try
        check_integer_only_constraint(Value, AtmDataSpec),
        check_allowed_values_constraint(Value, AtmDataSpec)
    catch throw:{unverified_constraints, UnverifiedConstraints} ->
        throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_number_type, UnverifiedConstraints))
    end.


-spec resolve(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_number_data_spec:record()
) ->
    atm_value:expanded() | no_return().
resolve(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    assert_meets_constraints(AtmWorkflowExecutionAuth, Value, AtmDataSpec),
    Value.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_number_data_spec:record()) -> number().
compress(Value, _AtmDataSpec) -> Value.


-spec expand(atm_workflow_execution_auth:record(), number(), atm_number_data_spec:record()) ->
    {ok, atm_value:expanded()}.
expand(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_integer_only_constraint(number(), atm_number_data_spec:record()) ->
    ok | no_return().
check_integer_only_constraint(Number, #atm_number_data_spec{integers_only = IntegersOnly}) when
    IntegersOnly =:= false;
    (IntegersOnly =:= true andalso is_integer(Number))
->
    ok;

check_integer_only_constraint(_Number, #atm_number_data_spec{integers_only = IntegersOnly}) ->
    throw(throw({unverified_constraints, #{<<"integersOnly">> => IntegersOnly}})).


%% @private
-spec check_allowed_values_constraint(number(), atm_number_data_spec:record()) ->
    ok | no_return().
check_allowed_values_constraint(_Number, #atm_number_data_spec{allowed_values = undefined}) ->
    ok;
check_allowed_values_constraint(Number, #atm_number_data_spec{allowed_values = AllowedValues}) ->
    case lists:member(Number, AllowedValues) of
        true -> ok;
        false -> throw({unverified_constraints, #{<<"allowedValues">> => AllowedValues}})
    end.
