%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS result streamer report of type "registration"
%%% used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_registration_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_result_streamer_registration_report{}.
-export_type([record/0]).


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(Record) ->
    #{
        <<"workflowExecutionId">> => Record#atm_openfaas_result_streamer_registration_report.workflow_execution_id,
        <<"taskExecutionId">> => Record#atm_openfaas_result_streamer_registration_report.task_execution_id,
        <<"resultStreamerId">> => Record#atm_openfaas_result_streamer_registration_report.result_streamer_id
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(RecordJson) ->
    #atm_openfaas_result_streamer_registration_report{
        workflow_execution_id = maps:get(<<"workflowExecutionId">>, RecordJson),
        task_execution_id = maps:get(<<"taskExecutionId">>, RecordJson),
        result_streamer_id = maps:get(<<"resultStreamerId">>, RecordJson)
    }.
