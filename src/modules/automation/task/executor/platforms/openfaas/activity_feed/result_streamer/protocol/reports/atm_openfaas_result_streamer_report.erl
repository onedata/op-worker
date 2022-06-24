%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS result streamer report used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%%
%%% These reports are sent by each lambda result streamer to report their appearance
%%% (registration), report a chunk of results that have relay method set to 'file_pipe'
%%% (they are read from files in the lambda pod as they appear), or report the end
%%% of streaming (deregistration).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_result_streamer_report{}.
% client-generated identifier of a specific report
-type id() :: binary().
-type body() :: atm_openfaas_result_streamer_registration_report:record()
| atm_openfaas_result_streamer_chunk_report:record()
| atm_openfaas_result_streamer_invalid_data_report:record()
| atm_openfaas_result_streamer_deregistration_report:record().

-export_type([record/0, id/0, body/0]).

-type type() :: atm_openfaas_result_streamer_registration_report
| atm_openfaas_result_streamer_chunk_report
| atm_openfaas_result_streamer_invalid_data_report
| atm_openfaas_result_streamer_deregistration_report.


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    Body = Record#atm_openfaas_result_streamer_report.body,
    BodyRecordType = utils:record_type(Body),
    maps:merge(
        #{
            <<"id">> => Record#atm_openfaas_result_streamer_report.id,
            <<"type">> => type_to_json(BodyRecordType)
        },
        jsonable_record:to_json(Body, BodyRecordType)
    ).


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    BodyRecordType = type_from_json(maps:get(<<"type">>, RecordJson)),
    #atm_openfaas_result_streamer_report{
        id = maps:get(<<"id">>, RecordJson),
        body = jsonable_record:from_json(RecordJson, BodyRecordType)
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec type_to_json(type()) -> json_utils:json_term().
type_to_json(atm_openfaas_result_streamer_registration_report) -> <<"registration">>;
type_to_json(atm_openfaas_result_streamer_chunk_report) -> <<"chunk">>;
type_to_json(atm_openfaas_result_streamer_invalid_data_report) -> <<"invalidData">>;
type_to_json(atm_openfaas_result_streamer_deregistration_report) -> <<"deregistration">>.


%% @private
-spec type_from_json(json_utils:json_term()) -> type().
type_from_json(<<"registration">>) -> atm_openfaas_result_streamer_registration_report;
type_from_json(<<"chunk">>) -> atm_openfaas_result_streamer_chunk_report;
type_from_json(<<"invalidData">>) -> atm_openfaas_result_streamer_invalid_data_report;
type_from_json(<<"deregistration">>) -> atm_openfaas_result_streamer_deregistration_report.
