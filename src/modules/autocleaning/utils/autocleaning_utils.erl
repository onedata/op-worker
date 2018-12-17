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

%% API
-export([assert_non_negative_integer/1, assert_non_negative_integer/2, get_defined/3]).
-export([assert_boolean/1, assert_boolean/2]).
-export([assert_not_greater_then/4]).

-define(ILLEGAL_TYPE_EXCEPTION, illegal_type).
-define(NEGATIVE_VALUE_EXCEPTION, negative_value).
-define(VALUE_GREATER_THAN_EXCEPTION, value_grater_than).

%%%===================================================================
%%% API
%%%===================================================================

-spec assert_boolean(term(), term()) -> boolean().
assert_boolean(Value, _Name) when is_boolean(Value) ->
    Value;
assert_boolean(_Value, Name) ->
    throw_illegal_type(Name).

-spec assert_boolean(term()) -> boolean().
assert_boolean(Value) when is_boolean(Value) ->
    Value;
assert_boolean(_Value) ->
    throw_illegal_type().

-spec assert_non_negative_integer(term()) -> non_neg_integer().
assert_non_negative_integer(Value) when is_integer(Value) andalso Value >= 0 ->
    Value;
assert_non_negative_integer(Value) when is_integer(Value) ->
    throw_negative_value();
assert_non_negative_integer(_Value) ->
    throw_illegal_type().

-spec assert_non_negative_integer(term(), term()) -> non_neg_integer().
assert_non_negative_integer(Value, _Name) when is_integer(Value) andalso Value >= 0 ->
    Value;
assert_non_negative_integer(Value, Name) when is_integer(Value) ->
    throw_negative_value(Name);
assert_non_negative_integer(_Value, Name) ->
    throw_illegal_type(Name).

-spec assert_not_greater_then(term() , term(), term(), term()) ->
    {integer(), integer()}.
assert_not_greater_then(Value1, Value2, _Name1, _Name2) when Value1 =< Value2 ->
    {Value1, Value2};
assert_not_greater_then(_Value1, _Value2, Name1, Name2) ->
    throw_greater_than(Name1, Name2).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec throw_illegal_type() -> no_return().
throw_illegal_type() ->
    throw(?ILLEGAL_TYPE_EXCEPTION).

-spec throw_illegal_type(term()) -> no_return().
throw_illegal_type(Name) ->
    throw({?ILLEGAL_TYPE_EXCEPTION, Name}).

-spec throw_negative_value() -> no_return().
throw_negative_value() ->
    throw(?NEGATIVE_VALUE_EXCEPTION).

-spec throw_negative_value(term()) -> no_return().
throw_negative_value(Name) ->
    throw({?NEGATIVE_VALUE_EXCEPTION, Name}).

-spec throw_greater_than(term(), term()) -> no_return().
throw_greater_than(Name1, Name2) ->
    throw({?VALUE_GREATER_THAN_EXCEPTION, Name1, Name2}).
