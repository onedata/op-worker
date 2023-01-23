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

-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, Value, ValueConstraints) ->
    try
        lists:foreach(fun
            ({_, undefined}) -> ok;
            (ConstraintRule) -> assert_meets_constraint(ConstraintRule, Value)
        end, maps:to_list(ValueConstraints))
    catch throw:{unverified_constraints, UnverifiedConstraints} ->
        throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_number_type, UnverifiedConstraints))
    end.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_data_type:value_constraints()) -> number().
compress(Value, _ValueConstraints) -> Value.


-spec expand(atm_workflow_execution_auth:record(), number(), atm_data_type:value_constraints()) ->
    {ok, atm_value:expanded()}.
expand(_AtmWorkflowExecutionAuth, Value, _ValueConstraints) ->
    {ok, Value}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_meets_constraint(ConstraintRule :: {atom(), term()}, number()) ->
    ok | no_return().
assert_meets_constraint({integers_only, IntegersOnly}, Number) when
    IntegersOnly =:= false;
    (IntegersOnly =:= true andalso is_integer(Number))
->
    ok;

assert_meets_constraint({integers_only, IntegersOnly}, _Number) ->
    throw(throw({unverified_constraints, #{<<"integersOnly">> => IntegersOnly}}));

assert_meets_constraint({allowed_values, AllowedValues}, Number) ->
    case lists:member(Number, AllowedValues) of
        true -> ok;
        false -> throw(throw({unverified_constraints, #{<<"allowedValues">> => AllowedValues}}))
    end.
