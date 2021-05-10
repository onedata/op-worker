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
-export([assert_instance/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Asserts that give value is of expected type and all value constraints holds.
%% @end
%%--------------------------------------------------------------------
-callback assert_instance(json_utils:json_term(), atm_data_type:value_constraints()) ->
    ok | no_return().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec assert_instance(json_utils:json_term(), atm_data_spec:record()) ->
    ok | no_return().
assert_instance(Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:assert_instance(Value, atm_data_spec:get_value_constraints(AtmDataSpec)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_integer_type) -> atm_integer;
get_callback_module(atm_string_type) -> atm_string;
get_callback_module(atm_object_type) -> atm_object.
