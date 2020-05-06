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
%%% parameters of middleware request.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_sanitizer).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

-type type_constraint() ::
    any | boolean | integer | binary | list_of_binaries |
    json | gri | page_token | qos_expression.
-type value_constraint() ::
    any |
    non_empty |
    [term()] | % A list of accepted values
    {between, integer(), integer()} |
    {not_lower_than, integer()} | {not_greater_than, integer()} | custom_constraint().

-type custom_constraint() :: fun((Val :: term()) -> true | {true, NewVal :: term()} | false).

-type param_spec() :: {type_constraint(), value_constraint()}.
% The 'aspect' keyword allows to validate the data provided in aspect identifier.
-type params_spec() :: #{
    Param :: binary() | {aspect, binary()} => param_spec()
}.

-type data() :: #{Param :: aspect | binary() => term()}.
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
-spec sanitize_data(RawData :: data(), data_spec()) -> SanitizedData :: data().
sanitize_data(RawData, DataSpec) ->
    RequiredParamsSpec = maps:get(required, DataSpec, #{}),
    OptionalParamsSpec = maps:get(optional, DataSpec, #{}),
    AtLeastOneParamsSpec = maps:get(at_least_one, DataSpec, #{}),

    SanitizedData1 = lists:foldl(fun(Param, SanitizedDataAcc) ->
        case sanitize_param(Param, RawData, RequiredParamsSpec) of
            false ->
                throw(?ERROR_MISSING_REQUIRED_VALUE(Param));
            {true, Val} ->
                SanitizedDataAcc#{Param => Val}
        end
    end, #{}, maps:keys(RequiredParamsSpec)),

    SanitizedData2 = lists:foldl(fun(Param, SanitizedDataAcc) ->
        case sanitize_param(Param, RawData, OptionalParamsSpec) of
            false ->
                SanitizedDataAcc;
            {true, Val} ->
                SanitizedDataAcc#{Param => Val}
        end
    end, SanitizedData1, maps:keys(OptionalParamsSpec)),

    {SanitizedData3, HasAtLeastOne} = lists:foldl(
        fun(Param, {SanitizedDataAcc, HasAtLeastOneAcc}) ->
            case sanitize_param(Param, RawData, AtLeastOneParamsSpec) of
                false ->
                    {SanitizedDataAcc, HasAtLeastOneAcc};
                {true, Val} ->
                    {SanitizedDataAcc#{Param => Val}, true}
            end
        end, {SanitizedData2, false}, maps:keys(AtLeastOneParamsSpec)
    ),
    case {maps:size(AtLeastOneParamsSpec), HasAtLeastOne} of
        {_, true} ->
            ok;
        {0, false} ->
            ok;
        {_, false} ->
            throw(?ERROR_MISSING_AT_LEAST_ONE_VALUE(maps:keys(AtLeastOneParamsSpec)))
    end,

    SanitizedData3.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks the type and value of Param in RawData and performs simple conversion
%% if necessary and possible. Takes into consideration special params which are
%% in form {aspect, binary()}, that allows to validate data in aspect
%% (RawData must include 'aspect' key, that holds the aspect).
%% @end
%%--------------------------------------------------------------------
-spec sanitize_param(Param :: binary(), data(), params_spec()) ->
    {true, ParamValue :: term()} | false.
sanitize_param({aspect, Param}, RawData, ParamsSpec) ->
    {TypeConstraint, ValueConstraint} = maps:get({aspect, Param}, ParamsSpec),
    %% Aspect validator supports only aspects that are tuples
    {_, RawValue} = maps:get(aspect, RawData),
    % Ignore the returned value - the check will throw in case the value is
    % not valid
    {true, sanitize_param(TypeConstraint, ValueConstraint, Param, RawValue)};
sanitize_param(Param, RawData, ParamsSpec) ->
    case maps:get(Param, RawData, undefined) of
        undefined ->
            false;
        RawValue ->
            {TypeConstraint, ValueConstraint} = maps:get(Param, ParamsSpec),
            {true, sanitize_param(
                TypeConstraint, ValueConstraint, Param, RawValue
            )}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks the type and value for Param in RawData and performs simple
%% conversion if necessary and possible.
%% @end
%%--------------------------------------------------------------------
-spec sanitize_param(type_constraint(), value_constraint(),
    Param :: binary(), RawValue :: term()) -> term().
sanitize_param(TypeConstraint, ValueConstraint, Param, RawValue) ->
    try
        Value1 = check_type(TypeConstraint, Param, RawValue),
        case check_value(TypeConstraint, ValueConstraint, Param, Value1) of
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
            throw(?ERROR_BAD_DATA(Param))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks the type of value for Param and performs simple type
%% conversion if required and possible.
%% @end
%%--------------------------------------------------------------------
-spec check_type(type_constraint(), Param :: binary(), RawValue :: term()) ->
    NewVal :: term() | no_return().
check_type(any, _Param, Term) ->
    Term;

check_type(binary, _Param, Binary) when is_binary(Binary) ->
    Binary;
check_type(binary, _Param, Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
check_type(binary, Param, _) ->
    throw(?ERROR_BAD_VALUE_BINARY(Param));

check_type(list_of_binaries, Key, Values) ->
    try
        lists:map(fun
            (Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
            (Bin) when is_binary(Bin) -> Bin
        end, Values)
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_LIST_OF_BINARIES(Key))
    end;

check_type(boolean, _Param, true) ->
    true;
check_type(boolean, _Param, <<"true">>) ->
    true;
check_type(boolean, _Param, false) ->
    false;
check_type(boolean, _Param, <<"false">>) ->
    false;
check_type(boolean, Param, _) ->
    throw(?ERROR_BAD_VALUE_BOOLEAN(Param));

check_type(integer, Param, Bin) when is_binary(Bin) ->
    try
        binary_to_integer(Bin)
    catch _:_ ->
        throw(?ERROR_BAD_VALUE_INTEGER(Param))
    end;
check_type(integer, _Param, Int) when is_integer(Int) ->
    Int;
check_type(integer, Param, _) ->
    throw(?ERROR_BAD_VALUE_INTEGER(Param));

check_type(gri, _Param, #gri{} = GRI) ->
    GRI;
check_type(gri, Param, EncodedGri) when is_binary(EncodedGri) ->
    try
        gri:deserialize(EncodedGri)
    catch _:_ ->
        throw(?ERROR_BAD_DATA(Param))
    end;
check_type(gri, Param, _) ->
    throw(?ERROR_BAD_DATA(Param));

check_type(page_token, _Param, null) ->
    undefined;
check_type(page_token, _Param, <<"null">>) ->
    undefined;
check_type(page_token, _Param, undefined) ->
    undefined;
check_type(page_token, _Param, <<"undefined">>) ->
    undefined;
check_type(page_token, Param, <<>>) ->
    throw(?ERROR_BAD_VALUE_EMPTY(Param));
check_type(page_token, _Param, PageToken) when is_binary(PageToken) ->
    PageToken;
check_type(page_token, Param, _) ->
    throw(?ERROR_BAD_DATA(Param));

check_type(json, _Param, JSON) when is_map(JSON) ->
    JSON;
check_type(json, Param, _) ->
    throw(?ERROR_BAD_VALUE_JSON(Param));

check_type(TypeConstraint, Param, _) ->
    ?error("Unknown type constraint: ~p for param: ~p", [
        TypeConstraint, Param
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts that specified value_constraint holds for Param's Value.
%% It is also possible to modify value by providing RectifyFun since
%% in some cases it may be desirable to perform specific transformations
%% on values.
%% @end
%%--------------------------------------------------------------------
-spec check_value(type_constraint(), value_constraint(), Param :: binary(),
    Value :: term()) -> ok | {ok, NewVal :: term()} | no_return().
check_value(_, any, _Param, _) ->
    ok;

check_value(binary, non_empty, Param, <<"">>) ->
    throw(?ERROR_BAD_VALUE_EMPTY(Param));
check_value(json, non_empty, Param, Map) when map_size(Map) == 0 ->
    throw(?ERROR_BAD_VALUE_EMPTY(Param));
check_value(_, non_empty, _Param, _) ->
    ok;

check_value(_, {not_lower_than, Threshold}, Param, Value) ->
    case Value >= Threshold of
        true ->
            ok;
        false ->
            throw(?ERROR_BAD_VALUE_TOO_LOW(Param, Threshold))
    end;
check_value(_, {between, Low, High}, Param, Value) ->
    case Value >= Low andalso Value =< High of
        true ->
            ok;
        false ->
            throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(Param, Low, High))
    end;

check_value(_, AllowedValues, Param, Val) when is_list(AllowedValues) ->
    case lists:member(Val, AllowedValues) of
        true ->
            ok;
        _ ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(Param, AllowedValues))
    end;

check_value(_, RectifyFun, Param, Val) when is_function(RectifyFun, 1) ->
    case RectifyFun(Val) of
        true ->
            ok;
        {true, NewVal} ->
            {ok, NewVal};
        false ->
            throw(?ERROR_BAD_DATA(Param))
    end;

check_value(TypeConstraint, ValueConstraint, Param, _) ->
    ?error("Unknown {type, value} constraint: {~p, ~p} for param: ~p", [
        TypeConstraint, ValueConstraint, Param
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).
