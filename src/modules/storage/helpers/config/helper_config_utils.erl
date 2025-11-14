%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for storage helper configuration modules.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_config_utils).
-author("Bartosz Walkowicz").

-include("storage/s3.hrl").
-include("storage/common.hrl").
-include("modules/storage/helpers/helpers.hrl").

%% API
-export([
    add_optional_args_if_defined/2,
    build_args_diff_from_specs/2,
    set_optional_record_fields_if_defined/3,

    validate_user_ctx/2,
    validate_user_ctx/3,

    is_canonical/1,
    get_storage_path_type/1,

    storage_path_type_to_binary/1,
    storage_path_type_from_binary/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec add_optional_args_if_defined(helper_config:args(), Specs) -> helper_config:args() when
    Specs :: [{binary(), undefined | binary()} | {binary(), undefined | term(), fun((term()) -> binary())}].
add_optional_args_if_defined(RequiredArgs, Specs) ->
    lists:foldl(fun
        ({Name, Value}, Acc) when Value /= undefined ->
            Acc#{Name => Value};
        ({Name, Value, ConvertFun}, Acc) when Value /= undefined ->
            Acc#{Name => ConvertFun(Value)};
        (_, Acc) ->
            Acc
    end, RequiredArgs, Specs).


-spec build_args_diff_from_specs(helper_config:args(), Specs) -> helper_config:args() when
    Specs :: [{binary(), undefined | binary()} | {binary(), undefined | term(), fun((term()) -> binary())}].
build_args_diff_from_specs(CurrentArgs, Specs) ->
    lists:foldl(fun
        ({Name, Value}, Acc) when Value /= undefined ->
            case maps:find(Name, CurrentArgs) of
                {ok, Value} -> Acc;  % unchanged
                _ -> Acc#{Name => Value}  % changed or new
            end;
        ({Name, Value, ConvertFun}, Acc) when Value /= undefined ->
            NifValue = ConvertFun(Value),
            case maps:find(Name, CurrentArgs) of
                {ok, NifValue} -> Acc;  % unchanged
                _ -> Acc#{Name => NifValue}  % changed or new
            end;
        (_, Acc) ->
            Acc
    end, #{}, Specs).


-spec set_optional_record_fields_if_defined(
    BaseRecord :: tuple(),
    NifMap :: map(),
    FieldSpecs :: [{binary(), pos_integer()} | {binary(), pos_integer(), fun((binary()) -> term())}]
) ->
    tuple().
set_optional_record_fields_if_defined(BaseRecord, NifMap, FieldSpecs) ->
    lists:foldl(fun
        ({NifFieldName, RecordIndex}, Acc) ->
            case maps:find(NifFieldName, NifMap) of
                {ok, NifValue} ->
                    erlang:setelement(RecordIndex, Acc, NifValue);
                error ->
                    Acc
            end;
        ({NifFieldName, RecordIndex, ConvertFun}, Acc) ->
            case maps:find(NifFieldName, NifMap) of
                {ok, NifValue} ->
                    erlang:setelement(RecordIndex, Acc, ConvertFun(NifValue));
                error ->
                    Acc
            end
    end, BaseRecord, FieldSpecs).


-spec validate_user_ctx(helper_config:user_ctx(), [binary()]) -> ok | {error, term()}.
validate_user_ctx(UserCtx, RequiredFields) ->
    validate_user_ctx(UserCtx, RequiredFields, []).


%%--------------------------------------------------------------------
%% @doc
%% Validates user context map against field specifications.
%% Checks that all required fields are present, all values are binaries
%% (and not <<"null">>), and no unexpected fields exist.
%% @end
%%--------------------------------------------------------------------
-spec validate_user_ctx(helper_config:user_ctx(), [binary()], [binary()]) ->
    ok | {error, term()}.
validate_user_ctx(UserCtx, RequiredFields, OptionalFields) ->
    AllowedFields = RequiredFields ++ OptionalFields,
    case validate_all_fields_allowed(UserCtx, AllowedFields) of
        ok ->
            case validate_required_fields_present(UserCtx, RequiredFields) of
                ok -> validate_all_fields_are_valid_binaries(UserCtx);
                Error -> Error
            end;
        Error ->
            Error
    end.


-spec is_canonical(helper_config:t()) -> boolean().
is_canonical(HelperConfig) ->
    get_storage_path_type(HelperConfig) =:= ?CANONICAL_STORAGE_PATH.


-spec get_storage_path_type(helper_config:t()) -> binary().
get_storage_path_type(#helper_config{args = Args}) ->
    maps:get(<<"storagePathType">>, Args).


-spec storage_path_type_to_binary(flat | canonical) -> binary().
storage_path_type_to_binary(flat) -> <<"flat">>;
storage_path_type_to_binary(canonical) -> <<"canonical">>.


-spec storage_path_type_from_binary(binary()) -> flat | canonical.
storage_path_type_from_binary(<<"flat">>) -> flat;
storage_path_type_from_binary(<<"canonical">>) -> canonical.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec validate_all_fields_allowed(map(), [binary()]) -> ok | {error, term()}.
validate_all_fields_allowed(UserCtx, AllowedFields) ->
    UnexpectedFields = maps:keys(UserCtx) -- AllowedFields,
    case UnexpectedFields of
        [] -> ok;
        [Field | _] -> {error, {unexpected_field, Field}}
    end.


%% @private
-spec validate_required_fields_present(map(), [binary()]) -> ok | {error, term()}.
validate_required_fields_present(UserCtx, RequiredFields) ->
    case lists:filter(fun(Field) -> not maps:is_key(Field, UserCtx) end, RequiredFields) of
        [] -> ok;
        [MissingField | _] -> {error, {missing_field, MissingField}}
    end.


%% @private
-spec validate_all_fields_are_valid_binaries(map()) -> ok | {error, term()}.
validate_all_fields_are_valid_binaries(UserCtx) ->
    InvalidFields = maps:fold(fun
        (_Key, <<Value/binary>>, Acc) when Value /= <<"null">> -> Acc;
        (Key, Value, _Acc) -> [{Key, Value}]
    end, [], UserCtx),
    case InvalidFields of
        [] -> ok;
        [{Field, Value} | _] -> {error, {invalid_field_value, Field, Value}}
    end.
