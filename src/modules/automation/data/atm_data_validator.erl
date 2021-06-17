%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_data_validator` interface - an object which can be
%%% used for validation of values against specific type and value constraints.
%%%
%%%                             !!! Caution !!!
%%% When adding validator for new type, the module must be registered in
%%% `atm_value:get_callback_module` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_validator).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([validate/3]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Asserts that all value constraints hold for specified value.
%% @end
%%--------------------------------------------------------------------
-callback assert_meets_constraints(
    atm_workflow_execution_ctx:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec validate(atm_workflow_execution_ctx:record(), atm_value:expanded(), atm_data_spec:record()) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    AtmDataType = atm_data_spec:get_type(AtmDataSpec),

    case atm_data_type:is_instance(AtmDataType, Value) of
        true ->
            Module = atm_value:get_callback_module(AtmDataType),
            ValueConstraints = atm_data_spec:get_value_constraints(AtmDataSpec),
            Module:assert_meets_constraints(AtmWorkflowExecutionCtx, Value, ValueConstraints);
        false ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, AtmDataType))
    end.
