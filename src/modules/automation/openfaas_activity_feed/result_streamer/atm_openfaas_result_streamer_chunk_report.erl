%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing OpenFaaS result streamer report of type "chunk"
%%% used in openfaas activity feed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_chunk_report).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type chunk() :: #{ResultName :: automation:name() => [json_utils:json_term()]}.
-export_type([chunk/0]).

-type record() :: #atm_openfaas_result_streamer_chunk_report{}.
-export_type([record/0]).


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_openfaas_result_streamer_chunk_report{chunk = Chunk}) when is_map(Chunk) ->
    #{
        <<"chunk">> => Chunk
    }.


-spec from_json(json_utils:json_term()) -> record().
from_json(#{<<"chunk">> := Chunk}) when is_map(Chunk) ->
    #atm_openfaas_result_streamer_chunk_report{
        chunk = Chunk
    }.
