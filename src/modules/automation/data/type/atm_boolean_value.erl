%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor` 
%%% functionality for `atm_boolean_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_boolean_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

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
    atm_boolean_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, _Value, _AtmDataSpec) ->
    ok.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_boolean_data_spec:record()) -> boolean().
compress(Value, _AtmDataSpec) -> Value.


-spec expand(atm_workflow_execution_auth:record(), boolean(), atm_boolean_data_spec:record()) ->
    {ok, atm_value:expanded()}.
expand(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.
