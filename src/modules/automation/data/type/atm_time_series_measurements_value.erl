%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor`
%%% functionality for `atm_time_series_measurements_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_measurements_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


-define(DATA_TYPE, atm_time_series_measurements_type).

-define(FIELD_PATH(__INDEX, __KEY), str_utils:format_bin("[~B].~s", [__INDEX, __KEY])).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, Value, _ValueConstraints) ->
    try
        lists:foreach(fun({Index, Measurement}) ->
            check_implicit_measurement_constraints(Index - 1, Measurement)
        end, lists_utils:enumerate(Value))
    catch
        throw:{unverified_constraints, UnverifiedConstraints} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                Value, ?DATA_TYPE, UnverifiedConstraints
            ));
        throw:Error ->
            throw(Error);
        _:_ ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, ?DATA_TYPE))
    end.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_data_type:value_constraints()) ->
    json_utils:json_map().
compress(Value, _ValueConstraints) -> Value.


-spec expand(
    atm_workflow_execution_auth:record(),
    json_utils:json_map(),
    atm_data_type:value_constraints()
) ->
    {ok, atm_value:expanded()}.
expand(_AtmWorkflowExecutionAuth, Value, _ValueConstraints) ->
    {ok, Value}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_implicit_measurement_constraints(non_neg_integer(), atm_value:expanded()) ->
    ok | no_return().
check_implicit_measurement_constraints(Index, #{
    <<"tsName">> := TsName,
    <<"timestamp">> := Timestamp,
    <<"value">> := MeasurementValue
}) ->
    is_binary(TsName) orelse throw({unverified_constraints, #{
        ?FIELD_PATH(Index, <<"tsName">>) => <<"String">>
    }}),

    (is_integer(Timestamp) andalso Timestamp > 0) orelse throw({unverified_constraints, #{
        ?FIELD_PATH(Index, <<"timestamp">>) => <<"Non negative integer">>
    }}),

    is_number(MeasurementValue) orelse throw({unverified_constraints, #{
        ?FIELD_PATH(Index, <<"value">>) => <<"Number">>
    }}),

    ok.
