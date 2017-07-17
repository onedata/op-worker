%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains events utility functions.
%%% @end
%%%-------------------------------------------------------------------
-module(event_utils).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([aggregate_file_read_events/2, aggregate_file_written_events/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides default implementation for a file read events aggregation rule.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_file_read_events(OldEvt :: event:type(), NewEvt :: event:type()) ->
    NewEvt :: event:type().
aggregate_file_read_events(OldEvt, NewEvt) ->
    NewEvt#file_read_event{
        counter = OldEvt#file_read_event.counter + NewEvt#file_read_event.counter,
        size = OldEvt#file_read_event.size + NewEvt#file_read_event.size,
        blocks = fslogic_blocks:aggregate(
            OldEvt#file_read_event.blocks,
            NewEvt#file_read_event.blocks
        )
    }.

%%--------------------------------------------------------------------
%% @doc
%% Provides default implementation for a file written events aggregation rule.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_file_written_events(OldEvt :: event:type(), NewEvt :: event:type()) ->
    NewEvt :: event:type().
aggregate_file_written_events(OldEvt, NewEvt) ->
    NewEvt#file_written_event{
        counter = OldEvt#file_written_event.counter + NewEvt#file_written_event.counter,
        size = OldEvt#file_written_event.size + NewEvt#file_written_event.size,
        blocks = fslogic_blocks:aggregate(
            OldEvt#file_written_event.blocks,
            NewEvt#file_written_event.blocks
        )
    }.