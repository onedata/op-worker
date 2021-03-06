%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor` 
%%% functionality for `atm_string_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_string_value).
-author("Bartosz Walkowicz").

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
    atm_workflow_execution_ctx:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionCtx, _Value, _ValueConstraints) ->
    ok.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded()) -> binary().
compress(Value) -> Value.


-spec expand(atm_workflow_execution_ctx:record(), binary()) ->
    {ok, atm_value:expanded()}.
expand(_, Value) ->
    {ok, Value}.
