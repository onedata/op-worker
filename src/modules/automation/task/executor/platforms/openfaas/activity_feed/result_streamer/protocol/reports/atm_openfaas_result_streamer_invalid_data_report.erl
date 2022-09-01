%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS result streamer report of type "invalidData"
%%% used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_invalid_data_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_result_streamer_invalid_data_report{}.
-export_type([record/0]).


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    #{
        <<"resultName">> => Record#atm_openfaas_result_streamer_invalid_data_report.result_name,
        <<"base64EncodedData">> => Record#atm_openfaas_result_streamer_invalid_data_report.base_64_encoded_data
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    #atm_openfaas_result_streamer_invalid_data_report{
        result_name = maps:get(<<"resultName">>, RecordJson),
        base_64_encoded_data = maps:get(<<"base64EncodedData">>, RecordJson)
    }.
