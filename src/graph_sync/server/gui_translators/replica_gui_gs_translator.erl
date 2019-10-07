%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of op logic results concerning
%%% replica entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([translate_resource/2]).

-type chunks_bar_entry() :: {BarNum :: non_neg_integer(), Fill :: non_neg_integer()}.
-type chunks_bar_data() :: [chunks_bar_entry()].
-type file_size() :: non_neg_integer().

-define(CHUNKS_BAR_WIDTH, 320).

-ifdef(TEST).
-export([interpolate_chunks/2]).
-endif.


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = distribution, scope = private}, Distribution) ->
    FileSize = lists:foldl(fun
        (#{<<"blocks">> := []}, Acc) ->
            Acc;
        (#{<<"blocks">> := Blocks}, Acc) ->
            [Offset, Size] = lists:last(Blocks),
            max(Acc, Offset + Size)
    end, 0, Distribution),

    DistributionMap = lists:foldl(fun(#{
        <<"providerId">> := ProviderId,
        <<"blocks">> := Blocks,
        <<"totalBlocksSize">> := TotalBlocksSize
    }, Acc) ->
        Data = lists:foldl(fun({BarNum, Fill}, DataAcc) ->
            DataAcc#{integer_to_binary(BarNum) => Fill}
        end, #{}, interpolate_chunks(Blocks, FileSize)),

        Acc#{ProviderId => #{
            <<"chunksBarData">> => Data,
            <<"blocksPercentage">> => case FileSize of
                0 -> 0;
                _ -> TotalBlocksSize * 100.0 / FileSize
            end
        }}
    end, #{}, Distribution),

    #{<<"distributionPerProvider">> => DistributionMap}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Interpolates file chunks to ?CHUNKS_BAR_WIDTH values between 0 and 100,
%% meaning how many percent of data in given block is held by a provider.
%% If the FileSize is below ?CHUNKS_BAR_WIDTH, it is scaled to a bigger
%% one before applying interpolation logic, together with the blocks.
%% Output is a proplist, where Key means the number of the bar and Value
%% means percentage of data in the segment held by the provider. Adjacent
%% bars with the same percentage are merged into one, i.e.
%%         instead of: [{0,33}, {1,33}, {2,33}, {3,33}, {4,50}, {5,0}, ...]
%% the output will be: [{0,33}, {4,50}, {5,0}, ...]
%% @end
%%--------------------------------------------------------------------
-spec interpolate_chunks(Blocks :: [[non_neg_integer()]], file_size()) ->
    chunks_bar_data().
interpolate_chunks([], _) ->
    [{0, 0}];
interpolate_chunks(_, 0) ->
    [{0, 0}];
interpolate_chunks(Blocks, FileSize) when FileSize < ?CHUNKS_BAR_WIDTH ->
    interpolate_chunks(
        [[O * ?CHUNKS_BAR_WIDTH, S * ?CHUNKS_BAR_WIDTH] || [O, S] <- Blocks],
        FileSize * ?CHUNKS_BAR_WIDTH
    );
interpolate_chunks(Blocks, FileSize) ->
    interpolate_chunks(
        lists:reverse(Blocks),
        FileSize,
        ?CHUNKS_BAR_WIDTH - 1,
        0,
        []
    ).


% Macros for more concise code, depending on variables in below functions.
-define(bar_start, floor(FileSize / ?CHUNKS_BAR_WIDTH * BarNum)).
-define(bar_end, floor(FileSize / ?CHUNKS_BAR_WIDTH * (BarNum + 1))).
-define(bar_size, (?bar_end - ?bar_start)).

%% @private
-spec interpolate_chunks(Blocks :: [[non_neg_integer()]], file_size(),
    BarNum :: non_neg_integer(), BytesAcc :: non_neg_integer(),
    chunks_bar_data()) -> chunks_bar_data().
interpolate_chunks([], _FileSize, -1, _BytesAcc, ResChunks) ->
    ResChunks;
interpolate_chunks([], _FileSize, _BarNum, 0, ResChunks) ->
    merge_chunks({0, 0}, ResChunks);
interpolate_chunks([], FileSize, BarNum, BytesAcc, ResChunks) ->
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks(
        [],
        ?bar_start,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    );
interpolate_chunks([[_Offset, 0] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) ->
    interpolate_chunks(PrevBlocks, FileSize, BarNum, BytesAcc, ResChunks);
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when ?bar_start < Offset ->
    interpolate_chunks(PrevBlocks, FileSize, BarNum, BytesAcc + Size, ResChunks);
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when Offset + Size > ?bar_start ->
    SizeInBar = Offset + Size - ?bar_start,
    Fill = round((BytesAcc + SizeInBar) * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size - SizeInBar] | PrevBlocks],
        FileSize,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    );
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) -> % Offset + Size =< ?bar_start
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size] | PrevBlocks],
        FileSize,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges adjacent chunks with the same fill value,
%% otherwise prepends the new chunk.
%% @end
%%--------------------------------------------------------------------
-spec merge_chunks(chunks_bar_entry(), chunks_bar_data()) ->
    chunks_bar_data().
merge_chunks({BarNum, Fill}, [{_, Fill} | Tail]) ->
    [{BarNum, Fill} | Tail];
merge_chunks({BarNum, Fill}, Result) ->
    [{BarNum, Fill} | Result].
