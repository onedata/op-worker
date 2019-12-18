%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions used by autocleaning modules
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_utils).
-author("Jakub Kudzia").

-include_lib("ctool/include/errors.hrl").

-type key() :: atom().

%% API
-export([assert_boolean/2, assert_not_greater_than/3,
    assert_non_negative_integer/2]).
-export([get_defined/3]).


%%%===================================================================
%%% API
%%%===================================================================

-spec assert_boolean(boolean(), key()) -> boolean().
assert_boolean(Value, _Key) when is_boolean(Value) ->
    Value;
assert_boolean(_Value, Key) ->
    throw(?ERROR_BAD_VALUE_BOOLEAN(atom_to_binary(Key, utf8))).


-spec assert_non_negative_integer(non_neg_integer(), key()) -> non_neg_integer().
assert_non_negative_integer(Value, _Key) when is_integer(Value) andalso Value >= 0 ->
    Value;
assert_non_negative_integer(Value, Key) when is_integer(Value) ->
    throw(?ERROR_BAD_VALUE_TOO_LOW(atom_to_binary(Key, utf8), 0));
assert_non_negative_integer(_Value, Key) ->
    throw(?ERROR_BAD_VALUE_INTEGER(atom_to_binary(Key, utf8))).


-spec assert_not_greater_than(integer(), integer(), key()) ->
    {integer(), integer()}.
assert_not_greater_than(Value1, Value2, _Key) when Value1 =< Value2 ->
    {Value1, Value2};
assert_not_greater_than(_Value1, Value2, Key) ->
    throw(?ERROR_BAD_VALUE_TOO_HIGH(atom_to_binary(Key, utf8), Value2)).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that undefined value won't be returned,
%% even if there is an entry key => undefined in the map.
%% @end
%%-------------------------------------------------------------------
-spec get_defined(term(), term(), term()) -> term().
get_defined(Key, Map, DefaultValue) ->
    case maps:get(Key, Map, DefaultValue) of
        undefined -> DefaultValue;
        Value -> Value
    end.
