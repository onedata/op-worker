%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing the push message sent to lambda result streamers to
%%% cue their finalization (flushing of all results and deregistering).
%%% The record is not persistable, but is encoded to JSON on the activity feed channel.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_finalization_signal).
-author("Lukasz Opiola").

-behaviour(jsonable_record).

-include("modules/automation/atm_execution.hrl").

%% jsonable_record callbacks
-export([to_json/1, from_json/1]).

-type record() :: #atm_openfaas_result_streamer_finalization_signal{}.
-export_type([record/0]).

%@TODO VFS-9388 "polymorphic" resultStreamerPushMessage with "type" discriminator

%%%===================================================================
%%% jsonable_record callbacks
%%%===================================================================

-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_openfaas_result_streamer_finalization_signal{}) ->
    #{<<"type">> => <<"finalizationSignal">>}.


-spec from_json(json_utils:json_term()) -> record().
from_json(#{<<"type">> := <<"finalizationSignal">>}) ->
    #atm_openfaas_result_streamer_finalization_signal{}.
