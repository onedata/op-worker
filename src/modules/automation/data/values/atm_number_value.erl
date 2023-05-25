%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_number_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_number_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").


%% atm_value callbacks
-export([
    validate/3,
    to_store_item/2,
    from_store_item/3,
    describe/3,
    resolve_lambda_parameter/3
]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_number_data_spec:record()
) ->
    ok | no_return().
validate(_AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    try
        check_integer_only_constraint(Value, AtmDataSpec),
        check_allowed_values_constraint(Value, AtmDataSpec)
    catch throw:{unverified_constraints, UnverifiedConstraints} ->
        throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
            Value, atm_number_type, UnverifiedConstraints
        ))
    end.


-spec to_store_item(automation:item(), atm_number_data_spec:record()) ->
    atm_store:item().
to_store_item(Value, _AtmDataSpec) ->
    Value.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_number_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


-spec describe(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_number_data_spec:record()
) ->
    {ok, automation:item()}.
describe(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_number_data_spec:record()
) ->
    automation:item().
resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec) ->
    validate(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec),
    Value.


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
