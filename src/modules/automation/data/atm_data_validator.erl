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
%%% `atm_value:get_callback_module` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_validator).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").

%% API
-export([validate/3]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Asserts that all value constraints hold for specified item. Returns sanitized item.
%% @end
%%--------------------------------------------------------------------
-callback validate(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec validate(atm_workflow_execution_ctx:record(), atm_api:item(), atm_data_spec:record()) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    Module = atm_value:get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    ValueConstraints = atm_data_spec:get_value_constraints(AtmDataSpec),
    Module:validate(AtmWorkflowExecutionCtx, Value, ValueConstraints).
