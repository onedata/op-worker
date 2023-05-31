%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles operations on automation values.
%%% Atm values are Oneprovider specific implementation of atm_data_type's.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_value).
-author("Michal Stanisz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    validate/3,

    to_store_item/2,
    from_store_item/3,

    describe/3,
    resolve_lambda_parameter/3
]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Asserts that all value constraints hold for specified value.
%%
%%                              !!! NOTE !!!
%% Beside explicit constraints given as an function argument some values may
%% also be bound by implicit ones that need to be checked (e.g. file/dataset
%% must exist within space in context of which workflow execution happens)
%% @end
%%--------------------------------------------------------------------
-callback validate(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    ok | no_return().


-callback to_store_item(
    automation:item(),
    atm_data_spec:record()
) ->
    atm_store:item().


-callback from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_data_spec:record()
) ->
    {ok, automation:item()} | errors:error().


-callback describe(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    {ok, automation:item()} | errors:error().


%%--------------------------------------------------------------------
%% @doc
%% Resolves required fields in case of reference types and asserts that all
%% value constraints hold.
%%
%%                              !!! NOTE !!!
%% Beside explicit constraints given as an function argument some values may
%% also be bound by implicit ones that need to be checked (e.g. file/dataset
%% must exist within space in context of which workflow execution happens)
%% @end
%%--------------------------------------------------------------------
-callback resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    automation:item() | no_return().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec validate(atm_workflow_execution_auth:record(), automation:item(), atm_data_spec:record()) ->
    ok | no_return().
validate(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    AtmDataType = atm_data_spec:get_data_type(AtmDataSpec),
    assert_is_instance_of_data_type(AtmDataType, Value),

    Module = get_callback_module(AtmDataType),
    Module:validate(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec to_store_item(automation:item(), atm_data_spec:record()) -> atm_store:item() | no_return().
to_store_item(Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_data_type(AtmDataSpec)),
    Module:to_store_item(Value, AtmDataSpec).


-spec from_store_item(atm_workflow_execution_auth:record(), atm_store:item(), atm_data_spec:record()) ->
    {ok, automation:item()} | {error, term()}.
from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_data_type(AtmDataSpec)),
    Module:from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec describe(atm_workflow_execution_auth:record(), atm_store:item(), atm_data_spec:record()) ->
    {ok, automation:item()} | {error, term()}.
describe(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_data_type(AtmDataSpec)),
    Module:describe(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    automation:item() | no_return().
resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    AtmDataType = atm_data_spec:get_data_type(AtmDataSpec),
    assert_is_instance_of_data_type(AtmDataType, Value),

    Module = get_callback_module(AtmDataType),
    Module:resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_array_type) -> atm_array_value;
get_callback_module(atm_boolean_type) -> atm_boolean_value;
get_callback_module(atm_dataset_type) -> atm_dataset_value;
get_callback_module(atm_file_type) -> atm_file_value;
get_callback_module(atm_number_type) -> atm_number_value;
get_callback_module(atm_object_type) -> atm_object_value;
get_callback_module(atm_range_type) -> atm_range_value;
get_callback_module(atm_string_type) -> atm_string_value;
get_callback_module(atm_time_series_measurement_type) -> atm_time_series_measurement_value.


%% @private
-spec assert_is_instance_of_data_type(atm_data_type:type(), automation:item()) -> ok | no_return().
assert_is_instance_of_data_type(AtmDataType, Value) ->
    case atm_data_type:is_instance(AtmDataType, Value) of
        true -> ok;
        false -> throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, AtmDataType))
    end.
