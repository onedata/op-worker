%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing a registry recording pod statuses and their changes in the
%%% context of a single OpenFaaS function (that can be executed by multiple pods).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_function_pod_status_registry).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([empty/0]).
-export([get_summary/2]).
-export([update_summary/4]).
-export([foreach_summary/2]).

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type record() :: #atm_openfaas_function_pod_status_registry{}.
-export_type([record/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec empty() -> record().
empty() ->
    #atm_openfaas_function_pod_status_registry{registry = #{}}.


-spec get_summary(atm_openfaas_function_activity_registry:pod_id(), record()) ->
    atm_openfaas_function_pod_status_summary:record().
get_summary(PodId, #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    maps:get(PodId, Registry).


-spec update_summary(
    atm_openfaas_function_activity_registry:pod_id(),
    fun((atm_openfaas_function_pod_status_summary:record()) -> atm_openfaas_function_pod_status_summary:record()),
    atm_openfaas_function_pod_status_summary:record(),
    record()
) -> record().
update_summary(PodId, Diff, Default, Record = #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    Record#atm_openfaas_function_pod_status_registry{
        registry = maps:update_with(PodId, Diff, Default, Registry)
    }.


-spec foreach_summary(
    fun((atm_openfaas_function_activity_registry:pod_id(), atm_openfaas_function_pod_status_summary:record()) -> term()),
    record()
) -> ok.
foreach_summary(Callback, #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    maps:foreach(Callback, Registry).

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
encode_with(#atm_openfaas_function_pod_status_registry{registry = Registry}, NestedRecordEncoder) ->
    maps:map(fun(_PodId, PodSummary) ->
        NestedRecordEncoder(PodSummary, atm_openfaas_function_pod_status_summary)
    end, Registry).


-spec decode_with(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
decode_with(RegistryJson, NestedRecordDecoder) ->
    #atm_openfaas_function_pod_status_registry{registry = maps:map(fun(_PodId, PodSummaryJson) ->
        NestedRecordDecoder(PodSummaryJson, atm_openfaas_function_pod_status_summary)
    end, RegistryJson)}.
