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
-module(helper_spec_utils).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").

%% API
-export([
    add_optional_entries_if_defined/2,
    build_diff_from_specs/2,
    set_optional_record_fields_if_defined/3,
    redact_record_fields_if_defined/2,

    validate_credentials/2,
    validate_credentials/3,
    resolve_admin_id/1,

    is_canonical/1,
    get_storage_path_type/1,

    storage_path_type_to_binary/1,
    storage_path_type_from_binary/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec add_optional_entries_if_defined(helper_spec:configuration(), Specs) -> helper_spec:configuration() when
    Specs :: [{binary(), undefined | binary()} | {binary(), undefined | term(), fun((term()) -> binary())}].
add_optional_entries_if_defined(BaseParams, Specs) ->
    lists:foldl(fun
        ({Name, Value}, Acc) when Value /= undefined ->
            Acc#{Name => Value};
        ({Name, Value, ConvertFun}, Acc) when Value /= undefined ->
            Acc#{Name => ConvertFun(Value)};
        (_, Acc) ->
            Acc
    end, BaseParams, Specs).


-spec build_diff_from_specs(helper_spec:configuration(), Specs) -> helper_spec:configuration() when
    Specs :: [{binary(), undefined | binary()} | {binary(), undefined | term(), fun((term()) -> binary())}].
build_diff_from_specs(CurrentParams, Specs) ->
    lists:foldl(fun
        ({Name, Value}, Acc) when Value /= undefined ->
            case maps:find(Name, CurrentParams) of
                {ok, Value} -> Acc;  % unchanged
                _ -> Acc#{Name => Value}  % changed or new
            end;
        ({Name, Value, ConvertFun}, Acc) when Value /= undefined ->
            ConvertedValue = ConvertFun(Value),
            case maps:find(Name, CurrentParams) of
                {ok, ConvertedValue} -> Acc;  % unchanged
                _ -> Acc#{Name => ConvertedValue}  % changed or new
            end;
        (_, Acc) ->
            Acc
    end, #{}, Specs).


-spec set_optional_record_fields_if_defined(
    BaseRecord :: tuple(),
    Params :: map(),
    FieldSpecs :: [{binary(), pos_integer()} | {binary(), pos_integer(), fun((binary()) -> term())}]
) ->
    tuple().
set_optional_record_fields_if_defined(BaseRecord, Params, FieldSpecs) ->
    lists:foldl(fun
        ({Key, RecordIndex}, Acc) ->
            case maps:find(Key, Params) of
                {ok, Value} ->
                    erlang:setelement(RecordIndex, Acc, Value);
                error ->
                    Acc
            end;
        ({Key, RecordIndex, ConvertFun}, Acc) ->
            case maps:find(Key, Params) of
                {ok, Value} ->
                    erlang:setelement(RecordIndex, Acc, ConvertFun(Value));
                error ->
                    Acc
            end
    end, BaseRecord, FieldSpecs).


-spec redact_record_fields_if_defined(tuple(), [pos_integer()]) -> tuple().
redact_record_fields_if_defined(Record, FieldsToRedact) ->
    lists:foldl(fun(FieldNo, RecordAcc) ->
        case erlang:element(FieldNo, RecordAcc) of
            undefined ->
                RecordAcc;
            _ ->
                erlang:setelement(FieldNo, RecordAcc, ?CONFIDENTIAL_MASK)
        end
    end, Record, FieldsToRedact).


-spec validate_credentials(helper_spec:credentials(), [binary()]) -> ok | {error, term()}.
validate_credentials(CredentialsParams, RequiredFields) ->
    validate_credentials(CredentialsParams, RequiredFields, []).


%%--------------------------------------------------------------------
%% @doc
%% Validates user context map against field specifications.
%% Checks that all required fields are present, all values are binaries
%% (and not <<"null">>), and no unexpected fields exist.
%% @end
%%--------------------------------------------------------------------
-spec validate_credentials(helper_spec:credentials(), [binary()], [binary()]) ->
    ok | {error, term()}.
validate_credentials(CredentialsParams, RequiredFields, OptionalFields) ->
    AllowedFields = RequiredFields ++ OptionalFields,
    case validate_all_fields_allowed(CredentialsParams, AllowedFields) of
        ok ->
            case validate_required_fields_present(CredentialsParams, RequiredFields) of
                ok -> validate_all_fields_are_valid_binaries(CredentialsParams);
                Error -> Error
            end;
        Error ->
            Error
    end.


-spec resolve_admin_id(helper_spec:credentials()) -> helper_spec:credentials().
resolve_admin_id(Credentials = #{<<"onedataAccessToken">> := OnedataAccessToken}) ->
    TokenCredentials = auth_manager:build_token_credentials(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, ?USER(UserId), _} = auth_manager:verify_credentials(TokenCredentials),
    Credentials#{<<"adminId">> => UserId};

resolve_admin_id(Credentials) ->
    Credentials.


-spec is_canonical(helper_spec:t()) -> boolean().
is_canonical(HelperSpec) ->
    get_storage_path_type(HelperSpec) =:= ?CANONICAL_STORAGE_PATH.


-spec get_storage_path_type(helper_spec:t()) -> binary().
get_storage_path_type(#helper_spec{configuration = ConfigurationParams}) ->
    maps:get(<<"storagePathType">>, ConfigurationParams).


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
validate_all_fields_allowed(CredentialsParams, AllowedFields) ->
    UnexpectedFields = maps:keys(CredentialsParams) -- AllowedFields,
    case UnexpectedFields of
        [] -> ok;
        [Field | _] -> {error, {unexpected_field, Field}}
    end.


%% @private
-spec validate_required_fields_present(map(), [binary()]) -> ok | {error, term()}.
validate_required_fields_present(CredentialsParams, RequiredFields) ->
    case lists:filter(fun(Field) -> not maps:is_key(Field, CredentialsParams) end, RequiredFields) of
        [] -> ok;
        [MissingField | _] -> {error, {missing_field, MissingField}}
    end.


%% @private
-spec validate_all_fields_are_valid_binaries(map()) -> ok | {error, term()}.
validate_all_fields_are_valid_binaries(CredentialsParams) ->
    InvalidFields = maps:fold(fun
        (_Key, <<Value/binary>>, Acc) when Value /= <<"null">> -> Acc;
        (Key, Value, _Acc) -> [{Key, Value}]
    end, [], CredentialsParams),
    case InvalidFields of
        [] -> ok;
        [{Field, Value} | _] -> {error, {invalid_field_value, Field, Value}}
    end.
