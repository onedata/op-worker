%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor` 
%%% functionality for `atm_integer_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_integer_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).

-include("modules/automation/atm_tmp.hrl").

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


-spec compress(atm_value:expanded()) -> integer().
compress(Value) -> Value.


-spec expand(atm_workflow_execution_ctx:record(), integer()) ->
    {ok, atm_value:expanded()}.
expand(_, Value) ->
    {ok, Value}.
