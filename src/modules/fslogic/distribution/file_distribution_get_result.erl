%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing operations on file distribution.
%%% @end
%%%--------------------------------------------------------------------
-module(file_distribution_get_result).
-author("Bartosz Walkowicz").

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([to_json/2]).


-type block() :: {Offset :: non_neg_integer(), Size :: integer()}.

% For below types description see interpolate_chunks fun doc
-type chunks_bar_entry() :: {BarNum :: non_neg_integer(), Fill :: non_neg_integer()}.
-type chunks_bar_data() :: [chunks_bar_entry()].


-define(CHUNKS_BAR_WIDTH, 320).

-ifdef(TEST).
-export([interpolate_chunks/2]).
-endif.


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(gs | rest, file_distribution:get_result()) -> json_utils:json_map().
to_json(gs, #file_distribution_get_result{distribution = #dir_distribution{
    logical_size = DirSize,
    physical_size_per_storage = PhysicalSizePerStorage
}}) ->
    #{
        <<"type">> => atom_to_binary(?DIRECTORY_TYPE),
        <<"size">> => DirSize,
        <<"distributionPerStorage">> => maps:map(fun(_StorageId, SizeOnStorage) ->
            case DirSize of
                0 -> 0;
                %% TODO can it be undefined? If no statistics are available probably yes?
                _ -> SizeOnStorage * 100.0 / DirSize
            end
        end, PhysicalSizePerStorage)
    };

to_json(gs, #file_distribution_get_result{distribution = #symlink_distribution{}}) ->
    %% TODO add any more info?
    #{
        <<"type">> => atom_to_binary(?SYMLINK_TYPE),
        <<"size">> => 0
    };

to_json(gs, #file_distribution_get_result{distribution = #reg_distribution{
    logical_size = FileSize,
    blocks_per_storage = FileBlocksPerStorage
}}) ->
    DistributionMap = maps:map(fun(_StorageId, FileBlocksOnStorage) ->
        {Blocks, TotalBlocksSize} = get_blocks_summary(FileBlocksOnStorage),

        Data = lists:foldl(fun({BarNum, Fill}, DataAcc) ->
            DataAcc#{integer_to_binary(BarNum) => Fill}
        end, #{}, interpolate_chunks(Blocks, FileSize)),

        #{
            <<"chunksBarData">> => Data,
            <<"blocksPercentage">> => case FileSize of
                0 -> 0;
                %% TODO MW can it be undefined?
                _ -> TotalBlocksSize * 100.0 / FileSize
            end
        }
    end, FileBlocksPerStorage),

    #{
        <<"type">> => atom_to_binary(?REGULAR_FILE_TYPE),
        <<"size">> => FileSize,
        <<"distributionPerStorage">> => DistributionMap
    };

to_json(rest, #file_distribution_get_result{distribution = #dir_distribution{
    logical_size = DirSize,
    physical_size_per_storage = PhysicalSizePerStorage
}}) ->
    #{
        <<"type">> => atom_to_binary(?DIRECTORY_TYPE),
        <<"size">> => DirSize,
        <<"sizePerStorage">> => PhysicalSizePerStorage
    };

to_json(rest, #file_distribution_get_result{distribution = #symlink_distribution{}}) ->
    %% TODO add any more info?
    #{
        <<"type">> => atom_to_binary(?SYMLINK_TYPE),
        <<"size">> => 0
    };

to_json(rest, #file_distribution_get_result{distribution = #reg_distribution{
    logical_size = FileSize,
    blocks_per_storage = FileBlocksPerStorage
}}) ->
    #{
        <<"type">> => atom_to_binary(?REGULAR_FILE_TYPE),
        <<"size">> => FileSize,
        <<"blocksPerStorage">> => maps:map(fun(_StorageId, FileBlocksOnStorage) ->
            {BlockList, TotalBlocksSize} = get_blocks_summary(FileBlocksOnStorage),

            #{
                <<"blocks">> => BlockList,
                <<"totalBlocksSize">> => TotalBlocksSize
            }
        end, FileBlocksPerStorage)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_blocks_summary(fslogic_blocks:blocks()) -> {[block()], TotalBlockSize :: integer()}.
get_blocks_summary(FileBlocks) ->
    lists:mapfoldl(
        fun(#file_block{offset = O, size = S}, SizeAcc) -> {[O, S], SizeAcc + S} end,
        0,
        FileBlocks
    ).


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
-spec interpolate_chunks([block()], file_meta:size() | undefined) ->
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
-spec interpolate_chunks(
    ReversedBlocks :: [[non_neg_integer()]], % File blocks passed to this fun should be in reverse order
    file_meta:size(),
    BarNum :: non_neg_integer(),
    BytesAcc :: non_neg_integer(),
    chunks_bar_data()
) ->
    chunks_bar_data().
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
