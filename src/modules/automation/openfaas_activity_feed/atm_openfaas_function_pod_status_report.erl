%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS function status report used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_function_pod_status_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_function_pod_status_report{}.
-export_type([record/0]).

%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    #{
        <<"timestamp">> => Record#atm_openfaas_function_pod_status_report.timestamp,
        <<"functionName">> => Record#atm_openfaas_function_pod_status_report.function_name,
        <<"podId">> => Record#atm_openfaas_function_pod_status_report.pod_id,
        <<"currentPodStatus">> => Record#atm_openfaas_function_pod_status_report.current_pod_status,
        <<"podEvent">> => Record#atm_openfaas_function_pod_status_report.pod_event
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    #atm_openfaas_function_pod_status_report{
        timestamp = maps:get(<<"timestamp">>, RecordJson),
        function_name = maps:get(<<"functionName">>, RecordJson),
        pod_id = maps:get(<<"podId">>, RecordJson),
        current_pod_status = maps:get(<<"currentPodStatus">>, RecordJson),
        pod_event = maps:get(<<"podEvent">>, RecordJson)
    }.
