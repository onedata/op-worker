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
    atm_array_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionAuth, ItemsArray, #atm_array_data_spec{
    item_data_spec = ItemDataSpec
}) ->
    lists:foreach(fun({Idx, Item}) ->
        try
            atm_value:validate(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
        catch throw:ItemError ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(ItemsArray, atm_array_type, #{
                str_utils:format_bin("$[~B]", [Idx]) => errors:to_json(ItemError)
            }))
        end
    end, lists:enumerate(0, ItemsArray)).


-spec resolve(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_array_data_spec:record()
) ->
    atm_value:expanded() | no_return().
resolve(AtmWorkflowExecutionAuth, ItemsArray, #atm_array_data_spec{
    item_data_spec = ItemDataSpec
}) ->
    lists:map(fun({Idx, Item}) ->
        try
            atm_value:resolve(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
        catch throw:ItemError ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(ItemsArray, atm_array_type, #{
                str_utils:format_bin("$[~B]", [Idx]) => errors:to_json(ItemError)
            }))
        end
    end, lists:enumerate(0, ItemsArray)).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_array_data_spec:record()) ->
    [atm_value:compressed()].
compress(Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    lists:map(fun(Item) -> atm_value:compress(Item, ItemDataSpec) end, Array).


-spec expand(
    atm_workflow_execution_auth:record(),
    [atm_value:compressed()],
    atm_array_data_spec:record()
) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionAuth, Array, #atm_array_data_spec{item_data_spec = ItemDataSpec}) ->
    try
        {ok, lists:map(fun(Item) ->
            case atm_value:expand(AtmWorkflowExecutionAuth, Item, ItemDataSpec) of
                {ok, ExpandedItem} -> ExpandedItem;
                {error, _} = Error -> throw(Error)
            end
        end, Array)}
    catch throw:{error, _} = Error ->
        Error
    end.
