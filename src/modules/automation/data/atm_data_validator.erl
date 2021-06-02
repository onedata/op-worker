%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_data_validator` interface - an object which can be
%%% used to validate values against specific type and value constraints.
%%%
%%%                             !!! Caution !!!
%%% When adding validator for new type, the module must be registered in
%%% `get_callback_module` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_validator).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").

%% API
-export([sanitize/3, map_value/3]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Asserts that all value constraints hold for specified item. Returns sanitized item.
%% @end
%%--------------------------------------------------------------------
-callback sanitize(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    atm_api:item() | no_return().


-callback map_value(atm_workflow_execution_ctx:record(), atm_api:item()) -> 
    {true, atm_api:item()} | false.


%%%===================================================================
%%% API functions
%%%===================================================================


-spec sanitize(atm_workflow_execution_ctx:record(), atm_api:item(), atm_data_spec:record()) ->
    atm_api:item() | no_return().
sanitize(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    ValueConstraints = atm_data_spec:get_value_constraints(AtmDataSpec),
    Module:sanitize(AtmWorkflowExecutionCtx, Value, ValueConstraints).


-spec map_value(atm_workflow_execution_ctx:record(), atm_api:item(), atm_data_spec:record()) -> 
    {true, atm_api:item()} | false.
map_value(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:map_value(AtmWorkflowExecutionCtx, Value).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_integer_type) -> atm_integer_value;
get_callback_module(atm_string_type) -> atm_string_value;
get_callback_module(atm_object_type) -> atm_object_value;
get_callback_module(atm_file_type) -> atm_file_value;
get_callback_module(atm_dataset_type) -> atm_dataset_value.
