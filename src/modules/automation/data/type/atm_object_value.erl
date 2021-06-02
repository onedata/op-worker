%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` functionality for
%%% `atm_object_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_object_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).

-include("modules/automation/atm_tmp.hrl").

%% atm_data_validator callbacks
-export([sanitize/3, map_value/2]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec sanitize(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    atm_api:item() | no_return().
sanitize(_, Value, _ValueConstraints) when is_map(Value) ->
    Value;
sanitize(_, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_object_type)).


-spec map_value(atm_workflow_execution_ctx:record(), atm_api:item()) ->
    {true, atm_api:item()} | false.
map_value(_, Value) ->
    {true, Value}.
