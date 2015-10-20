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

-include("proto/oneclient/common_messages.hrl").

%% API
-export([aggregate_blocks/2]).

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
aggregate_block(#file_block{offset = Offset1, size = Size1, file_id = FID, storage_id = SID} = B,
    [#file_block{offset = Offset2, size = Size2, file_id = FID, storage_id = SID} | Blocks])
    when Offset1 =< Offset2 + Size2 ->
    [B#file_block{
        offset = Offset2,
        size = max(Offset1 + Size1, Offset2 + Size2) - Offset2
    } | Blocks];
aggregate_block(Block, Blocks) ->
    [Block | Blocks].