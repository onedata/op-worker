%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor`
%%% functionality for `atm_time_series_measurement_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_measurement_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


-define(DATA_TYPE, atm_time_series_measurement_type).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_time_series_measurement_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, Measurement, AtmDataSpec) ->
    try
        check_measurement_constraints(Measurement, AtmDataSpec)
    catch
        throw:{unverified_constraints, UnverifiedConstraints} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                Measurement, ?DATA_TYPE, UnverifiedConstraints
            ));
        throw:Error ->
            throw(Error);
        _:_ ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Measurement, ?DATA_TYPE))
    end.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_time_series_measurement_data_spec:record()) ->
    json_utils:json_map().
compress(Value, _AtmDataSpec) ->
    maps:with([<<"tsName">>, <<"timestamp">>, <<"value">>], Value).


-spec expand(
    atm_workflow_execution_auth:record(),
    json_utils:json_map(),
    atm_time_series_measurement_data_spec:record()
) ->
    {ok, atm_value:expanded()}.
expand(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_measurement_constraints(
    atm_value:expanded(),
    atm_time_series_measurement_data_spec:record()
) ->
    ok | no_return().
check_measurement_constraints(
    #{<<"tsName">> := TSName},
    #atm_time_series_measurement_data_spec{specs = AllowedMeasurementSpecs}
) ->
    case atm_time_series_names:find_matching_measurement_spec(TSName, AllowedMeasurementSpecs) of
        {ok, _} ->
            ok;
        error ->
            throw({unverified_constraints, #{<<"specs">> => jsonable_record:list_to_json(
                AllowedMeasurementSpecs, atm_time_series_measurement_spec
            )}})
    end.
