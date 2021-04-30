%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing atm integer type class.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_integer_type).
-author("Bartosz Walkowicz").

-behaviour(atm_data_spec).

-include_lib("ctool/include/errors.hrl").


%% atm_data_class callbacks
-export([assert_instance/2]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec assert_instance(term(), atm_data_spec:spec()) -> ok | no_return().
assert_instance(Value, _Type) when is_integer(Value) ->
    ok;
assert_instance(_Value, _Type) ->
    throw(?EINVAL).
