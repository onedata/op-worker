%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_data_compressor` interface - an object which can be
%%% used for compressing and expanding values of given type.
%%%
%%%                             !!! Caution !!!
%%% When adding compressor for new type, the module must be registered in
%%% `atm_value:get_callback_module` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_compressor).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").

%% API
-export([compress/2, expand/3]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback compress(atm_api:item()) -> atm_api:item() | no_return().


-callback expand(atm_workflow_execution_ctx:record(), atm_api:item()) -> 
    {ok, atm_api:item()} | {error, term()}.


%%%===================================================================
%%% API functions
%%%===================================================================


-spec compress(atm_api:item(), atm_data_spec:record()) ->
    atm_api:item() | no_return().
compress(Value, AtmDataSpec) ->
    Module = atm_value:get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:compress(Value).


-spec expand(atm_workflow_execution_ctx:record(), atm_api:item(), atm_data_spec:record()) -> 
    {ok, atm_api:item()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    Module = atm_value:get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:expand(AtmWorkflowExecutionCtx, Value).
