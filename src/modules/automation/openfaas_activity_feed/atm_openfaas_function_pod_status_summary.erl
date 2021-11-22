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
-module(atm_openfaas_function_pod_status_summary).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type record() :: #atm_openfaas_function_pod_status_summary{}.
-export_type([record/0]).

%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    encode_with(Record, fun jsonable_record:to_json/2).


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    decode_with(RecordJson, fun jsonable_record:from_json/2).

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) -> json_utils:json_term().
db_encode(Record, NestedRecordEncoder) ->
    encode_with(Record, NestedRecordEncoder).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) -> record().
db_decode(RecordJson, NestedRecordDecoder) ->
    decode_with(RecordJson, NestedRecordDecoder).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec encode_with(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
encode_with(#atm_openfaas_function_pod_status_summary{
    current_status = CurrentStatus,
    current_status_observation_timestamp = CurrentStatusObservationTimestamp,
    event_log = EventLogId
}, _NestedRecordEncoder) ->
    #{
        <<"currentStatus">> => CurrentStatus,
        <<"currentStatusObservationTimestamp">> => CurrentStatusObservationTimestamp,
        <<"eventLogId">> => EventLogId
    }.


-spec decode_with(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
decode_with(RegistryJson, _NestedRecordDecoder) ->
    #atm_openfaas_function_pod_status_summary{
        current_status = maps:get(<<"currentStatus">>, RegistryJson),
        current_status_observation_timestamp = maps:get(<<"currentStatusObservationTimestamp">>, RegistryJson),
        event_log = maps:get(<<"eventLogId">>, RegistryJson)
    }.
