%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_boolean_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_boolean_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

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
    atm_boolean_data_spec:record()
) ->
    ok | no_return().
validate(_AtmWorkflowExecutionAuth, _Value, _AtmDataSpec) ->
    ok.


-spec to_store_item(automation:item(), atm_boolean_data_spec:record()) ->
    atm_store:item().
to_store_item(Value, _AtmDataSpec) ->
    Value.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_boolean_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


-spec describe(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_boolean_data_spec:record()
) ->
    {ok, automation:item()}.
describe(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_boolean_data_spec:record()
) ->
    automation:item().
resolve_lambda_parameter(_AtmWorkflowExecutionAuth, Value, _AtmParameterDataSpec) ->
    Value.
