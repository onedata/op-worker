%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value`functionality for
%%% `atm_time_series_measurement_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_measurement_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_value callbacks
-export([
    validate_constraints/3,
    to_store_item/2,
    from_store_item/3,
    describe_store_item/3,
    transform_to_data_spec_conformant/3
]).

-define(DATA_TYPE, atm_time_series_measurement_type).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate_constraints(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_time_series_measurement_data_spec:record()
) ->
    ok | no_return().
validate_constraints(_AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    try
        check_measurement_constraints(Value, AtmDataSpec)
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


-spec to_store_item(automation:item(), atm_time_series_measurement_data_spec:record()) ->
    atm_store:item().
to_store_item(Value, _AtmDataSpec) when map_size(Value) > 3 ->
    maps:with([<<"tsName">>, <<"timestamp">>, <<"value">>], Value);
to_store_item(Value, _AtmDataSpec) ->
    Value.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_time_series_measurement_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    {ok, Value}.


-spec describe_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_time_series_measurement_data_spec:record()
) ->
    {ok, automation:item()}.
describe_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec transform_to_data_spec_conformant(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_time_series_measurement_data_spec:record()
) ->
    automation:item() | no_return().
transform_to_data_spec_conformant(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec) ->
    validate_constraints(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec),
    Value.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_measurement_constraints(
    automation:item(),
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
