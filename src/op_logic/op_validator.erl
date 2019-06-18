%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements some functions for parsing
%%% and processing parameters of op_logic request.
%%% @end
%%%-------------------------------------------------------------------
-module(op_validator).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

-type type_constraint() :: any | boolean | integer | binary | json.

-type value_constraint() ::
    any |
    name |
    non_empty |
    [term()] | % A list of accepted values
    {between, integer(), integer()} |
    {not_lower_than, integer()} | {not_greater_than, integer()}.

-type param_signature() :: {type_constraint(), value_constraint()}.
% The 'aspect' key word allows to validate the data provided in aspect identifier.
-type params_signature() :: #{
    Key :: binary() | {aspect, binary()} => param_signature()
}.

-type data() :: #{Key :: aspect | binary() => term()}.
-type data_signature() :: #{
    required => params_signature(),
    at_least_one => params_signature(),
    optional => params_signature()
}.

-export_type([
    type_constraint/0, value_constraint/0,
    param_signature/0, params_signature/0,
    data/0, data_signature/0
]).

-define(DEFAULT_ENTITY_NAME, <<"Unnamed">>).

%% API
-export([
    validate_data/2,
    validate_name/1, validate_name/5,
    normalize_name/1, normalize_name/9
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Ensures given data conform to specified signature.
%% Throws errors if it is not possible to sanitize them.
%% @end
%%--------------------------------------------------------------------
-spec validate_data(data(), data_signature()) -> data().
validate_data(Data0, Signature) ->
    RequiredParamsSignature = maps:get(required, Signature, #{}),
    OptionalParamsSignature = maps:get(optional, Signature, #{}),
    AtLeastOneParamsSignature = maps:get(at_least_one, Signature, #{}),

    Data1 = validate_required_params(Data0, RequiredParamsSignature),
    Data2 = validate_optional_params(Data1, OptionalParamsSignature),
    validate_at_least_one_params(Data2, AtLeastOneParamsSignature).


%%--------------------------------------------------------------------
%% @doc
%% Validates entity name against universal name format.
%% @end
%%--------------------------------------------------------------------
-spec validate_name(binary()) -> boolean().
validate_name(Name) ->
    validate_name(
        Name, ?NAME_FIRST_CHARS_ALLOWED, ?NAME_MIDDLE_CHARS_ALLOWED,
        ?NAME_LAST_CHARS_ALLOWED, ?NAME_MAXIMUM_LENGTH
    ).


%%--------------------------------------------------------------------
%% @doc
%% Validates entity name against given format.
%% @end
%%--------------------------------------------------------------------
-spec validate_name(Name :: binary(), FirstRgx :: binary(), MiddleRgx :: binary(),
    LastRgx :: binary(), MaxLength :: non_neg_integer()) -> boolean().
validate_name(Name, _, _, _, _) when not is_binary(Name) ->
    false;
validate_name(Name, FirstRgx, MiddleRgx, LastRgx, MaxLength) ->
    Regexp = <<
        "^[", FirstRgx/binary, "][", MiddleRgx/binary,
        "]{0,", (integer_to_binary(MaxLength - 2))/binary,
        "}[", LastRgx/binary, "]$"
    >>,
    try re:run(Name, Regexp, [{capture, none}, unicode, ucp]) of
        match -> true;
        _ -> false
    catch _:_ ->
        false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Trims disallowed characters from the beginning and the end of the string,
%% replaces disallowed characters in the middle with dashes('-').
%% If the name is too long, it is shortened to allowed size.
%% @end
%%--------------------------------------------------------------------
-spec normalize_name(binary()) -> binary().
normalize_name(Name) ->
    normalize_name(Name,
        ?NAME_FIRST_CHARS_ALLOWED, <<"">>,
        ?NAME_MIDDLE_CHARS_ALLOWED, <<"-">>,
        ?NAME_LAST_CHARS_ALLOWED, <<"">>,
        ?NAME_MAXIMUM_LENGTH, ?DEFAULT_ENTITY_NAME
    ).


%%--------------------------------------------------------------------
%% @doc
%% Normalizes given name according to Regexp for first, middle and last
%% characters (replaces disallowed characters with given).
%% If the name is too long, it is shortened to allowed size.
%% @end
%%--------------------------------------------------------------------
-spec normalize_name(Name :: binary(),
    FirstRgx :: binary(), FirstReplace :: binary(),
    MiddleRgx :: binary(), MiddleReplace :: binary(),
    LastRgx :: binary(), LastReplace :: binary(),
    MaxLength :: non_neg_integer(), DefaultName :: term()) -> term().
normalize_name(Name, FirstRgx, FirstReplace, MiddleRgx, MiddleReplace, LastRgx, LastReplace, MaxLength, DefaultName) ->
    TrimmedLeft = re:replace(Name,
        <<"^[^", FirstRgx/binary, "]*(?=[", FirstRgx/binary, "])">>, FirstReplace,
        [{return, binary}, unicode, ucp, global]
    ),
    TrimmedMiddle = re:replace(TrimmedLeft,
        <<"[^", MiddleRgx/binary, "]">>, MiddleReplace,
        [{return, binary}, unicode, ucp, global]
    ),
    % string module supports binaries in utf8
    Shortened = string:slice(TrimmedMiddle, 0, MaxLength),
    TrimmedRight = re:replace(Shortened,
        <<"(?<=[", LastRgx/binary, "])[^", LastRgx/binary, "]*$">>, LastReplace,
        [{return, binary}, unicode, ucp, global]
    ),
    case validate_name(TrimmedRight, FirstRgx, MiddleRgx, LastRgx, MaxLength) of
        false -> DefaultName;
        true -> TrimmedRight
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec validate_required_params(data(), params_signature()) -> data().
validate_required_params(Data, RequiredParamsSig) ->
    lists:foldl(fun(Key, DataAcc) ->
        case transform_and_check_value(Key, DataAcc, RequiredParamsSig) of
            false ->
                throw(?ERROR_MISSING_REQUIRED_VALUE(Key));
            {true, NewData} ->
                NewData
        end
    end, Data, maps:keys(RequiredParamsSig)).


%% @private
-spec validate_optional_params(data(), params_signature()) -> data().
validate_optional_params(Data, OptionalParamsSig) ->
    lists:foldl(fun(Key, DataAcc) ->
        case transform_and_check_value(Key, DataAcc, OptionalParamsSig) of
            false ->
                DataAcc;
            {true, NewData} ->
                NewData
        end
    end, Data, maps:keys(OptionalParamsSig)).


%% @private
-spec validate_at_least_one_params(data(), params_signature()) -> data().
validate_at_least_one_params(Data, AtLeastOneParamsSig) ->
    {Params2, HasAtLeastOne} = lists:foldl(
        fun(Key, {DataAcc, HasAtLeastOneAcc}) ->
            case transform_and_check_value(Key, DataAcc, AtLeastOneParamsSig) of
                false ->
                    {DataAcc, HasAtLeastOneAcc orelse false};
                {true, NewData} ->
                    {NewData, true}
            end
        end, {Data, false}, maps:keys(AtLeastOneParamsSig)),
    case {length(maps:keys(AtLeastOneParamsSig)), HasAtLeastOne} of
        {_, true} ->
            ok;
        {0, false} ->
            ok;
        {_, false} ->
            throw(?ERROR_MISSING_AT_LEAST_ONE_VALUE(maps:keys(AtLeastOneParamsSig)))
    end,
    Params2.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs simple value conversion (if possible) and checks the type and value
%% of value for Key in Params. Takes into consideration special keys which are
%% in form {aspect, binary()}, that allows to validate data in aspect.
%% Params map must include 'aspect' key, that holds the aspect.
%% @end
%%--------------------------------------------------------------------
-spec transform_and_check_value(Key :: binary(), data(), params_signature()) ->
    {true, data()} | false.
transform_and_check_value({aspect, Key}, Data, Signature) ->
    {TypeConstraint, ValueConstraint} = maps:get({aspect, Key}, Signature),
    %% Aspect validator supports only aspects that are tuples
    {_, Value} = maps:get(aspect, Data),
    % Ignore the returned value - the check will throw in case the value is
    % not valid
    transform_and_check_value(TypeConstraint, ValueConstraint, Key, Value),
    {true, Data};
transform_and_check_value(Key, Data, Signature) ->
    case maps:get(Key, Data, undefined) of
        undefined ->
            false;
        Value ->
            {TypeConstraint, ValueConstraint} = maps:get(Key, Signature),
            NewValue = transform_and_check_value(
                TypeConstraint, ValueConstraint, Key, Value
            ),
            {true, Data#{Key => NewValue}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs simple value conversion (if possible) and checks the type and value
%% of value.
%% @end
%%--------------------------------------------------------------------
-spec transform_and_check_value(type_constraint(), value_constraint(),
    Key :: binary(), Value :: term()) -> term().
transform_and_check_value(TypeConstraint, ValueConstraint, Key, Value) ->
    try
        NewValue = check_type(TypeConstraint, Key, Value),
        check_value(TypeConstraint, ValueConstraint, Key, NewValue),
        NewValue
    catch
        throw:Error ->
            throw(Error);
        Type:Message ->
            ?error_stacktrace(
                "Error in op_validator:transform_and_check_value - ~p:~p",
                [Type, Message]
            ),
            throw(?ERROR_BAD_DATA(Key))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs simple value conversion (if possible) and checks the type
%% of value for Key in Data.
%% @end
%%--------------------------------------------------------------------
-spec check_type(type_constraint(), Key :: binary(), Value :: term()) -> term().
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
%% @end
%%--------------------------------------------------------------------
-spec check_value(type_constraint(), value_constraint(), Key :: binary(),
    Value :: term()) -> ok | no_return().
check_value(_, any, _Key, _) ->
    ok;

check_value(binary, non_empty, Key, <<"">>) ->
    throw(?ERROR_BAD_VALUE_EMPTY(Key));
check_value(json, non_empty, Key, Map) when map_size(Map) == 0 ->
    throw(?ERROR_BAD_VALUE_EMPTY(Key));
check_value(_, non_empty, _Key, _) ->
    ok;

check_value(binary, name, _Key, Value) ->
    case validate_name(Value) of
        true -> ok;
        false -> throw(?ERROR_BAD_VALUE_NAME)
    end;

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

check_value(TypeConstraint, ValueConstraint, Key, _) ->
    ?error("Unknown {type, value} constraint: {~p, ~p} for key: ~p", [
        TypeConstraint, ValueConstraint, Key
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).
