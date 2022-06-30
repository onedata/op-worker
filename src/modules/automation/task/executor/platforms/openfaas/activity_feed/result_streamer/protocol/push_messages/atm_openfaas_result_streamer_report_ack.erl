%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing a push message sent to lambda result streamers to
%%% acknowledge that a result streamer report has been processed.
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_report_ack).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_result_streamer_report_ack{}.
-export_type([record/0]).


%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_openfaas_result_streamer_report_ack{id = Id}) ->
    #{<<"id">> => Id}.


-spec from_json(json_utils:json_term()) -> record().
from_json(#{<<"id">> := Id}) ->
    #atm_openfaas_result_streamer_report_ack{id = Id}.
