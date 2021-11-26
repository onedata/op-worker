%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor`
%%% functionality for `atm_array_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_array_value).
-author("Michal Stanisz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionAuth, ItemsArray, #{
    item_data_spec := ItemDataSpec
}) ->
    lists:foreach(fun(Item) ->
        %% TODO catch errors and into ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED for atm_array_type
        atm_value:validate(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
    end, ItemsArray).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


%% TODO compress and expand array items
-spec compress(atm_value:expanded()) -> list().
compress(Array) -> Array.


-spec expand(atm_workflow_execution_auth:record(), list()) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(_AtmWorkflowExecutionAuth, Array) ->
    Array.
