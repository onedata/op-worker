%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS activity report used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").


%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_activity_report{}.
-export_type([record/0]).

-type type() :: atm_openfaas_function_pod_status_report | atm_openfaas_result_streamer_report.
-type body() :: atm_openfaas_function_pod_status_report:record() | atm_openfaas_result_streamer_report:record().
-export_type([type/0, body/0]).


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    Type = Record#atm_openfaas_activity_report.type,
    #{
        <<"activityReportType">> => type_to_json(Type),
        <<"activityReportBatch">> => lists:map(fun(Element) ->
            jsonable_record:to_json(Element, Type)
        end, Record#atm_openfaas_activity_report.batch)
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    Type = type_from_json(maps:get(<<"activityReportType">>, RecordJson)),
    #atm_openfaas_activity_report{
        type = Type,
        batch = lists:map(fun(ElementJson) ->
            jsonable_record:from_json(ElementJson, Type)
        end, maps:get(<<"activityReportBatch">>, RecordJson))
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec type_to_json(type()) -> json_utils:json_term().
type_to_json(atm_openfaas_function_pod_status_report) -> <<"podStatusReport">>;
type_to_json(atm_openfaas_result_streamer_report) -> <<"resultStreamerReport">>.


%% @private
-spec type_from_json(json_utils:json_term()) -> type().
type_from_json(<<"podStatusReport">>) -> atm_openfaas_function_pod_status_report;
type_from_json(<<"resultStreamerReport">>) -> atm_openfaas_result_streamer_report.
