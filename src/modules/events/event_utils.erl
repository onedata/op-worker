%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains utility functions for events management.
%%% @end
%%%-------------------------------------------------------------------
-module(event_utils).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/common_messages.hrl").

%% API
-export([aggregate_blocks/2, inject_event_stream_definition/1]).

-export_type([file_block/0]).

-type file_block() :: #file_block{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Aggregates lists of 'file_block' records.
%% IMPORTANT! Both list should contain disjoint file blocks sorted in ascending
%% order of block offset.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_blocks(Blocks1 :: [file_block()], Blocks2 :: [file_block()]) ->
    AggBlocks :: [file_block()].
aggregate_blocks([], Blocks) ->
    Blocks;

aggregate_blocks(Blocks, []) ->
    Blocks;

aggregate_blocks(Blocks1, Blocks2) ->
    aggregate_blocks(Blocks1, Blocks2, []).

%%--------------------------------------------------------------------
%% @doc
%% Injects event stream definition based on the subscription type.
%% @end
%%--------------------------------------------------------------------
-spec inject_event_stream_definition(Sub :: event:subscription()) ->
    NewSub :: event:subscription().
inject_event_stream_definition(#subscription{type = #file_attr_subscription{
    file_uuid = FileUuid, counter_threshold = CtrThr,
    time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_ATTR_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{type = #update_event{type = #file_attr{uuid = Uuid}}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        emission_rule = fun(Meta) -> Meta >= CtrThr end,
        emission_time = TimeThr,
        init_handler = open_sequencer_stream_handler(),
        terminate_handler = close_sequencer_stream_handler(),
        event_handler = send_events_handler()
    }};

inject_event_stream_definition(#subscription{type = #file_location_subscription{
    file_uuid = FileUuid, counter_threshold = CtrThr,
    time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_LOCATION_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{type = #update_event{type = #file_location{uuid = Uuid}}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        emission_rule = fun(Meta) -> Meta >= CtrThr end,
        emission_time = TimeThr,
        init_handler = open_sequencer_stream_handler(),
        terminate_handler = close_sequencer_stream_handler(),
        event_handler = send_events_handler()
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregates lists of 'file_block' records using acumulator AggBlocks.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_blocks(Blocks1 :: [file_block()], Blocks2 :: [file_block()],
    AggBlocks :: [file_block()]) -> NewAggBlocks :: [file_block()].
aggregate_blocks([], [], AggBlocks) ->
    lists:reverse(AggBlocks);

aggregate_blocks([], [Block | Blocks], AggBlocks) ->
    aggregate_blocks([], Blocks, aggregate_block(Block, AggBlocks));

aggregate_blocks([Block | Blocks], [], AggBlocks) ->
    aggregate_blocks(Blocks, [], aggregate_block(Block, AggBlocks));

aggregate_blocks([#file_block{offset = Offset1} = Block1 | Blocks1],
    [#file_block{offset = Offset2} | _] = Blocks2, AggBlocks)
    when Offset1 < Offset2 ->
    aggregate_blocks(Blocks1, Blocks2, aggregate_block(Block1, AggBlocks));

aggregate_blocks(Blocks1, [#file_block{} = Block2 | Blocks2], AggBlocks) ->
    aggregate_blocks(Blocks1, Blocks2, aggregate_block(Block2, AggBlocks)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregates 'file_block' record  with list of 'file_block' records.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_block(Block :: file_block(), Blocks :: [file_block()]) ->
    AggBlocks :: [file_block()].
aggregate_block(Block, []) ->
    [Block];

aggregate_block(#file_block{offset = Offset1, size = Size1} = Block1,
    [#file_block{offset = Offset2, size = Size2} | Blocks])
    when Offset1 =< Offset2 + Size2 ->
    [Block1#file_block{
        offset = Offset2,
        size = max(Offset1 + Size1, Offset2 + Size2) - Offset2
    } | Blocks];

aggregate_block(Block, Blocks) ->
    [Block | Blocks].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which opens sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec open_sequencer_stream_handler() -> Handler :: event_stream:init_handler().
open_sequencer_stream_handler() ->
    fun(_, SessId) ->
        {ok, StmId} = sequencer:open_stream(SessId),
        {StmId, SessId}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which closes sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec close_sequencer_stream_handler() -> Handler :: event_stream:terminate_handler().
close_sequencer_stream_handler() ->
    fun({StmId, SessId}) ->
        sequencer:close_stream(StmId, SessId)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which sends events to the remote subscriber via sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec send_events_handler() -> Handler :: event_stream:event_handler().
send_events_handler() ->
    fun(Evts, {StmId, SessId}) ->
        {ok, SeqMan} = session:get_sequencer_manager(SessId),
        lists:foreach(fun(Evt) ->
            sequencer:send_message(Evt, StmId, SeqMan)
        end, Evts)
    end.