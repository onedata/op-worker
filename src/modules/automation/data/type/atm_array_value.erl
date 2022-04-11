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
assert_meets_constraints(AtmWorkflowExecutionAuth, ItemsArray, #{
    item_data_spec := ItemDataSpec
}) ->
    lists:foreach(fun({Idx, Item}) ->
        try
            atm_value:validate(AtmWorkflowExecutionAuth, Item, ItemDataSpec)
        catch throw:ItemError ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(ItemsArray, atm_array_type, #{
                str_utils:format_bin("item[~B]", [Idx]) => errors:to_json(ItemError)
            }))
        end
    end, lists:zip(lists:seq(0, length(ItemsArray) - 1), ItemsArray)).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_data_type:value_constraints()) ->
    [atm_value:compressed()].
compress(Array, #{item_data_spec := ItemDataSpec}) ->
    lists:map(fun(Item) -> atm_value:compress(Item, ItemDataSpec) end, Array).


-spec expand(
    atm_workflow_execution_auth:record(),
    [atm_value:compressed()],
    atm_data_type:value_constraints()
) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionAuth, Array, #{item_data_spec := ItemDataSpec}) ->
    {ok, lists:map(
        fun(Item) -> atm_value:expand(AtmWorkflowExecutionAuth, Item, ItemDataSpec) end,
        Array
    )}.
