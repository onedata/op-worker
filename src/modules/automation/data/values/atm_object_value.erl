%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_object_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_object_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

%% atm_value callbacks
-export([
    validate_constraints/3,
    to_store_item/2,
    from_store_item/3,
    describe_store_item/3,
    transform_to_data_spec_conformant/3
]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate_constraints(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_object_data_spec:record()
) ->
    ok | no_return().
validate_constraints(_AtmWorkflowExecutionAuth, _Value, _AtmDataSpec) ->
    ok.


-spec to_store_item(automation:item(), atm_object_data_spec:record()) ->
    atm_store:item().
to_store_item(Value, _AtmDataSpec) ->
    Value.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_object_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


-spec describe_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_object_data_spec:record()
) ->
    {ok, automation:item()}.
describe_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec transform_to_data_spec_conformant(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_object_data_spec:record()
) ->
    automation:item().
transform_to_data_spec_conformant(_AtmWorkflowExecutionAuth, Value, _AtmParameterDataSpec) ->
    Value.
