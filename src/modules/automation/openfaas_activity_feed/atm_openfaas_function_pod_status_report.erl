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

% readiness of the pod's containers, as reported by Kubernetes (e.g. <<"0/1">> or <<"2/2">>)
% this information is not a part of a k8s event, but is gathered when an event occurs
% and sent along in the report to indicate the current pod's containers readiness
-type containers_readiness() :: binary().

-type event_timestamp() :: time:millis().
% event type, as reported by Kubernetes (e.g. <<"Normal">> or <<"Error">>)
-type event_type() :: binary().
% event reason, as reported by Kubernetes (e.g. <<"Scheduled">> or <<"FailedCreate">>)
-type event_reason() :: binary().
% event message, as reported by Kubernetes (e.g. <<"Container created">>)
-type event_message() :: binary().

-export_type([containers_readiness/0]).
-export_type([event_timestamp/0, event_type/0, event_reason/0, event_message/0]).

-type record() :: #atm_openfaas_function_pod_status_report{}.
-export_type([record/0]).

%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    #{
        <<"functionName">> => Record#atm_openfaas_function_pod_status_report.function_name,
        <<"podId">> => Record#atm_openfaas_function_pod_status_report.pod_id,

        <<"containersReadiness">> => Record#atm_openfaas_function_pod_status_report.containers_readiness,

        <<"eventTimestamp">> => Record#atm_openfaas_function_pod_status_report.event_timestamp,
        <<"eventType">> => Record#atm_openfaas_function_pod_status_report.event_type,
        <<"eventReason">> => Record#atm_openfaas_function_pod_status_report.event_reason,
        <<"eventMessage">> => Record#atm_openfaas_function_pod_status_report.event_message
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    #atm_openfaas_function_pod_status_report{
        function_name = maps:get(<<"functionName">>, RecordJson),
        pod_id = maps:get(<<"podId">>, RecordJson),

        containers_readiness = maps:get(<<"containersReadiness">>, RecordJson),

        event_timestamp = maps:get(<<"eventTimestamp">>, RecordJson),
        event_type = maps:get(<<"eventType">>, RecordJson),
        event_reason = maps:get(<<"eventReason">>, RecordJson),
        event_message = maps:get(<<"eventMessage">>, RecordJson)
    }.
