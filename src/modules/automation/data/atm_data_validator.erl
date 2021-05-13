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
-export([assert_instance/2, is_instance/2]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback is_instance(Value :: term(), atm_data_type2:value_constraints()) ->
    boolean().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec assert_instance(term(), atm_data_spec2:record()) -> ok | no_return().
assert_instance(Value, AtmDataSpec) ->
    case is_instance(Value, AtmDataSpec) of
        true -> ok;
        false -> throw(?EINVAL)
    end.


-spec is_instance(term(), atm_data_spec2:record()) -> boolean().
is_instance(Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec2:get_type(AtmDataSpec)),
    Module:assert_instance(Value, atm_data_spec2:get_value_constraints(AtmDataSpec)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_callback_module(atm_data_type2:type()) -> module().
get_callback_module(atm_integer_type) -> atm_integer.
