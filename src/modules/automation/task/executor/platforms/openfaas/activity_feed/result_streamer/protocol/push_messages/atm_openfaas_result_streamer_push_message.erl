%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing a generic push message sent to lambda result streamers.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_push_message).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: atm_openfaas_result_streamer_finalization_signal:record()
| atm_openfaas_result_streamer_report_ack:record().

-export_type([record/0]).

-type type() :: atm_openfaas_result_streamer_finalization_signal
| atm_openfaas_result_streamer_report_ack.


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    RecordType = utils:record_type(Record),
    maps:merge(
        #{<<"type">> => type_to_json(RecordType)},
        jsonable_record:to_json(Record, RecordType)
    ).


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    RecordType = type_from_json(maps:get(<<"type">>, RecordJson)),
    jsonable_record:from_json(RecordJson, RecordType).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec type_to_json(type()) -> json_utils:json_term().
type_to_json(atm_openfaas_result_streamer_finalization_signal) -> <<"finalizationSignal">>;
type_to_json(atm_openfaas_result_streamer_report_ack) -> <<"reportAck">>.


%% @private
-spec type_from_json(json_utils:json_term()) -> type().
type_from_json(<<"finalizationSignal">>) -> atm_openfaas_result_streamer_finalization_signal;
type_from_json(<<"reportAck">>) -> atm_openfaas_result_streamer_report_ack.

