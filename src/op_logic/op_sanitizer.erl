%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements functions for parsing and sanitizing
%%% parameters of op_logic request.
%%% @end
%%%-------------------------------------------------------------------
-module(op_sanitizer).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

-type type_constraint() :: any | boolean | integer | binary | json.
-type value_constraint() ::
    any |
    non_empty |
    [term()] | % A list of accepted values
    {between, integer(), integer()} |
    {not_lower_than, integer()} | {not_greater_than, integer()} |
    fun((Val :: term()) -> true | {true, NewVal :: term()} | false).

-type param_spec() :: {type_constraint(), value_constraint()}.
% The 'aspect' key word allows to validate the data provided in aspect identifier.
-type params_spec() :: #{
    Key :: binary() | {aspect, binary()} => param_spec()
}.

-type data() :: #{Key :: aspect | binary() => term()}.
-type data_spec() :: #{
    required => params_spec(),
    at_least_one => params_spec(),
    optional => params_spec()
}.

-export_type([
    type_constraint/0, value_constraint/0,
    param_spec/0, params_spec/0,
    data/0, data_spec/0
]).

%% API
-export([sanitize_data/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sanitizes given data according to specified spec, throws on errors.
%% @end
%%--------------------------------------------------------------------
-spec sanitize_data(data(), data_spec()) -> data().
sanitize_data(Data0, DataSpec) ->
    RequiredParamsSpec = maps:get(required, DataSpec, #{}),
    OptionalParamsSpec = maps:get(optional, DataSpec, #{}),
    AtLeastOneParamsSpec = maps:get(at_least_one, DataSpec, #{}),

    Data1 = sanitize_required_params(Data0, RequiredParamsSpec),
    Data2 = sanitize_optional_params(Data1, OptionalParamsSpec),
    sanitize_at_least_one_params(Data2, AtLeastOneParamsSpec).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec sanitize_required_params(data(), params_spec()) -> data().
sanitize_required_params(Data, RequiredParamsSpec) ->
    lists:foldl(fun(Key, DataAcc) ->
        case transform_and_check_value(Key, DataAcc, RequiredParamsSpec) of
            false ->
                throw(?ERROR_MISSING_REQUIRED_VALUE(Key));
            {true, NewData} ->
                NewData
        end
    end, Data, maps:keys(RequiredParamsSpec)).


%% @private
-spec sanitize_optional_params(data(), params_spec()) -> data().
sanitize_optional_params(Data, OptionalParamsSpec) ->
    lists:foldl(fun(Key, DataAcc) ->
        case transform_and_check_value(Key, DataAcc, OptionalParamsSpec) of
            false ->
                DataAcc;
            {true, NewData} ->
                NewData
        end
    end, Data, maps:keys(OptionalParamsSpec)).


%% @private
-spec sanitize_at_least_one_params(data(), params_spec()) -> data().
sanitize_at_least_one_params(Data, AtLeastOneParamsSpec) ->
    {Data2, HasAtLeastOne} = lists:foldl(
        fun(Key, {DataAcc, HasAtLeastOneAcc}) ->
            case transform_and_check_value(Key, DataAcc, AtLeastOneParamsSpec) of
                false ->
                    {DataAcc, HasAtLeastOneAcc};
                {true, NewData} ->
                    {NewData, true}
            end
        end, {Data, false}, maps:keys(AtLeastOneParamsSpec)),
    case {length(maps:keys(AtLeastOneParamsSpec)), HasAtLeastOne} of
        {_, true} ->
            ok;
        {0, false} ->
            ok;
        {_, false} ->
            throw(?ERROR_MISSING_AT_LEAST_ONE_VALUE(maps:keys(AtLeastOneParamsSpec)))
    end,
    Data2.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs simple value conversion (if possible) and checks the type and value
%% of value for Key in Params. Takes into consideration special keys which are
%% in form {aspect, binary()}, that allows to validate data in aspect.
%% Params map must include 'aspect' key, that holds the aspect.
%% @end
%%--------------------------------------------------------------------
-spec transform_and_check_value(Key :: binary(), data(), params_spec()) ->
    {true, data()} | false.
transform_and_check_value({aspect, Key}, Data, ParamsSpec) ->
    {TypeConstraint, ValueConstraint} = maps:get({aspect, Key}, ParamsSpec),
    %% Aspect validator supports only aspects that are tuples
    {_, Value} = maps:get(aspect, Data),
    % Ignore the returned value - the check will throw in case the value is
    % not valid
    transform_and_check_value(TypeConstraint, ValueConstraint, Key, Value),
    {true, Data};
transform_and_check_value(Key, Data, ParamsSpec) ->
    case maps:get(Key, Data, undefined) of
        undefined ->
            false;
        Value ->
            {TypeConstraint, ValueConstraint} = maps:get(Key, ParamsSpec),
            NewValue = transform_and_check_value(
                TypeConstraint, ValueConstraint, Key, Value
            ),
            {true, Data#{Key => NewValue}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks the type and value for Key in Data and performs simple conversion
%% if required and possible.
%% @end
%%--------------------------------------------------------------------
-spec transform_and_check_value(type_constraint(), value_constraint(),
    Key :: binary(), Value :: term()) -> term().
transform_and_check_value(TypeConstraint, ValueConstraint, Key, Value0) ->
    try
        Value1 = check_type(TypeConstraint, Key, Value0),
        case check_value(TypeConstraint, ValueConstraint, Key, Value1) of
            ok ->
                Value1;
            {ok, Value2} ->
                Value2
        end
    catch
        throw:Error ->
            throw(Error);
        Type:Message ->
            ?error_stacktrace("Error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            throw(?ERROR_BAD_DATA(Key))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks the type of value for Key in Data and performs simple conversion
%% if required and possible.
%% @end
%%--------------------------------------------------------------------
-spec check_type(type_constraint(), Key :: binary(), Value :: term()) ->
    NewVal :: term() | no_return().
check_type(any, _Key, Term) ->
    Term;

check_type(binary, _Key, Binary) when is_binary(Binary) ->
    Binary;
check_type(binary, _Key, Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
check_type(binary, Key, _) ->
    throw(?ERROR_BAD_VALUE_BINARY(Key));

check_type(boolean, _Key, true) ->
    true;
check_type(boolean, _Key, <<"true">>) ->
    true;
check_type(boolean, _Key, false) ->
    false;
check_type(boolean, _Key, <<"false">>) ->
    false;
check_type(boolean, Key, _) ->
    throw(?ERROR_BAD_VALUE_BOOLEAN(Key));

check_type(integer, Key, Bin) when is_binary(Bin) ->
    try
        binary_to_integer(Bin)
    catch _:_ ->
        throw(?ERROR_BAD_VALUE_INTEGER(Key))
    end;
check_type(integer, _Key, Int) when is_integer(Int) ->
    Int;
check_type(integer, Key, _) ->
    throw(?ERROR_BAD_VALUE_INTEGER(Key));

check_type(gri, _Key, #gri{} = GRI) ->
    GRI;
check_type(gri, Key, EncodedGri) when is_binary(EncodedGri) ->
    try
        gri:deserialize(EncodedGri)
    catch _:_ ->
        throw(?ERROR_BAD_DATA(Key))
    end;
check_type(gri, Key, _) ->
    throw(?ERROR_BAD_DATA(Key));

check_type(json, _Key, JSON) when is_map(JSON) ->
    JSON;
check_type(json, Key, _) ->
    throw(?ERROR_BAD_VALUE_JSON(Key));

check_type(TypeConstraint, Key, _) ->
    ?error("Unknown type constraint: ~p for key: ~p", [TypeConstraint, Key]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts that specified value_constraint holds for Key in Params.
%% It is also possible to modify value by providing RectifyFun since
%% in some cases it may be desirable to perform specific transformations
%% on values.
%% @end
%%--------------------------------------------------------------------
-spec check_value(type_constraint(), value_constraint(), Key :: binary(),
    Value :: term()) -> ok | {ok, NewVal :: term()} | no_return().
check_value(_, any, _Key, _) ->
    ok;

check_value(binary, non_empty, Key, <<"">>) ->
    throw(?ERROR_BAD_VALUE_EMPTY(Key));
check_value(json, non_empty, Key, Map) when map_size(Map) == 0 ->
    throw(?ERROR_BAD_VALUE_EMPTY(Key));
check_value(_, non_empty, _Key, _) ->
    ok;

check_value(_, {not_lower_than, Threshold}, Key, Value) ->
    case Value >= Threshold of
        true ->
            ok;
        false ->
            throw(?ERROR_BAD_VALUE_TOO_LOW(Key, Threshold))
    end;
check_value(_, {between, Low, High}, Key, Value) ->
    case Value >= Low andalso Value =< High of
        true ->
            ok;
        false ->
            throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(Key, Low, High))
    end;

check_value(_, AllowedValues, Key, Val) when is_list(AllowedValues) ->
    case lists:member(Val, AllowedValues) of
        true ->
            ok;
        _ ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedValues))
    end;

check_value(_, RectifyFun, Key, Val) when is_function(RectifyFun, 1) ->
    case RectifyFun(Val) of
        true ->
            ok;
        {true, NewVal} ->
            {ok, NewVal};
        false ->
            throw(?ERROR_BAD_DATA(Key))
    end;

check_value(TypeConstraint, ValueConstraint, Key, _) ->
    ?error("Unknown {type, value} constraint: {~p, ~p} for key: ~p", [
        TypeConstraint, ValueConstraint, Key
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).
