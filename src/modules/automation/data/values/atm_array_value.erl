%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_array_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_array_value).
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
    [automation:item()],
    atm_array_data_spec:record()
) ->
    ok | no_return().
validate(AtmWorkflowExecutionAuth, Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    lists:foreach(fun({Idx, Item}) ->
        try
            atm_value:validate(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
        catch throw:ItemError ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Array, atm_array_type, #{
                str_utils:format_bin("$[~B]", [Idx]) => errors:to_json(ItemError)
            }))
        end
    end, lists:enumerate(0, Array)).


-spec to_store_item(automation:item(), atm_array_data_spec:record()) ->
    [atm_store:item()].
to_store_item(Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    lists:map(fun(Item) -> atm_value:to_store_item(Item, ItemDataSpec) end, Array).


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    [atm_store:item()],
    atm_array_data_spec:record()
) ->
    {ok, [automation:item()]} | errors:error().
from_store_item(AtmWorkflowExecutionAuth, Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    try
        {ok, lists:map(fun(Item) ->
            case atm_value:from_store_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) of
                {ok, ExpandedItem} -> ExpandedItem;
                {error, _} = Error -> throw(Error)
            end
        end, Array)}
    catch throw:{error, _} = Error ->
        Error
    end.


-spec describe(
    atm_workflow_execution_auth:record(),
    [atm_store:item()],
    atm_array_data_spec:record()
) ->
    {ok, [automation:item()]} | errors:error().
describe(AtmWorkflowExecutionAuth, Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    try
        {ok, lists:map(fun(Item) ->
            case atm_value:describe(AtmWorkflowExecutionAuth, Item, ItemDataSpec) of
                {ok, ExpandedItem} -> ExpandedItem;
                {error, _} = Error -> throw(Error)
            end
        end, Array)}
    catch throw:{error, _} = Error ->
        Error
    end.


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    [automation:item()],
    atm_array_data_spec:record()
) ->
    [automation:item()] | no_return().
resolve_lambda_parameter(AtmWorkflowExecutionAuth, ItemsArray, #atm_array_data_spec{
    item_data_spec = ItemDataSpec
}) ->
    lists:map(fun({Idx, Item}) ->
        try
            atm_value:resolve_lambda_parameter(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
        catch throw:ItemError ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(ItemsArray, atm_array_type, #{
                str_utils:format_bin("$[~B]", [Idx]) => errors:to_json(ItemError)
            }))
        end
    end, lists:enumerate(0, ItemsArray)).
