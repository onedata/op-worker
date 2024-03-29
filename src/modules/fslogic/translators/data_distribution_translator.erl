%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing translations of file distribution related objects.
%%% @end
%%%--------------------------------------------------------------------
-module(data_distribution_translator).
-author("Bartosz Walkowicz").

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    gather_result_to_json/3,
    storage_locations_to_json/1
]).


-type block() :: [integer()]. % 2 element list in format [Offset :: non_neg_integer(), Size :: integer()].

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


-spec gather_result_to_json(gs | rest, data_distribution:get_result(), file_id:file_guid()) ->
    json_utils:json_map().
gather_result_to_json(_, #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
    distribution_per_provider = DistributionPerProvider
}}, Guid) ->
    #{
        <<"type">> => atom_to_binary(?DIRECTORY_TYPE),
        <<"distributionPerProvider">> => maps:map(fun
            (ProviderId, {error, _} = Error) ->
                build_error_response(Error, Guid, ProviderId);

            (_ProviderId, #provider_dir_distribution_get_result{
                virtual_size = VirtualSize,
                logical_size = LogicalSize,
                physical_size_per_storage = PhysicalDirSizePerStorage
            }) ->
                #{
                    <<"success">> => true,
                    <<"virtualSize">> => utils:undefined_to_null(VirtualSize),
                    <<"logicalSize">> => utils:undefined_to_null(LogicalSize),
                    <<"distributionPerStorage">> => maps:map(fun(_StorageId, PhysicalSize) -> #{
                        <<"physicalSize">> => utils:undefined_to_null(PhysicalSize)
                    }
                    end, PhysicalDirSizePerStorage)
                }
        end, DistributionPerProvider)
    };

gather_result_to_json(_, #data_distribution_gather_result{distribution = #symlink_distribution_get_result{
    storages_per_provider = StoragesPerProvider
}}, _Guid) ->
    #{
        <<"type">> => atom_to_binary(?SYMLINK_TYPE),
            <<"distributionPerProvider">> => maps:map(fun(_ProviderId, StoragesList) -> #{ 
                <<"success">> => true,
               <<"virtualSize">> => 0,
               <<"distributionPerStorage">> =>
                   lists:foldl(fun(StorageId, Acc) ->   
                       Acc#{StorageId => #{<<"physicalSize">> => 0}}
                   end, #{}, StoragesList)
               } end, StoragesPerProvider)
    };

gather_result_to_json(gs, #data_distribution_gather_result{distribution = #reg_distribution_gather_result{
    distribution_per_provider = FileBlocksPerProvider
}}, Guid) ->
    DistributionMap = maps:map(fun
        (ProviderId, {error, _} = Error) ->
            build_error_response(Error, Guid, ProviderId);
    
        (_ProviderId, #provider_reg_distribution_get_result{
            virtual_size = VirtualSize,
            blocks_per_storage = BlocksPerStorage,
            locations_per_storage = LocationsPerStorage
        }) ->
            DistributionPerStorage = maps:fold(fun(StorageId, BlocksOnStorage, Acc) ->
                {Blocks, TotalBlocksSize} = get_blocks_summary(BlocksOnStorage),

                Data = lists:foldl(fun({BarNum, Fill}, DataAcc) ->
                    DataAcc#{integer_to_binary(BarNum) => Fill}
                end, #{}, interpolate_chunks(Blocks, VirtualSize)),

                Acc#{StorageId => #{
                    <<"physicalSize">> => TotalBlocksSize,
                    <<"chunksBarData">> => Data,
                    <<"blocksPercentage">> => case VirtualSize of
                        0 -> 0;
                        _ -> TotalBlocksSize * 100.0 / VirtualSize
                    end,
                    <<"blockCount">> => length(Blocks)
                }}
            end, #{}, BlocksPerStorage),
            #{
                <<"success">> => true,
                <<"virtualSize">> => VirtualSize,
                <<"distributionPerStorage">> => DistributionPerStorage,
                <<"locationsPerStorage">> => maps_utils:undefined_to_null(LocationsPerStorage)
            }
    end, FileBlocksPerProvider),

    #{
        <<"type">> => atom_to_binary(?REGULAR_FILE_TYPE),
        <<"distributionPerProvider">> => DistributionMap
    };

gather_result_to_json(rest, #data_distribution_gather_result{distribution = #reg_distribution_gather_result{
    distribution_per_provider = FileBlocksPerProvider
}}, Guid) ->
    #{
        <<"type">> => atom_to_binary(?REGULAR_FILE_TYPE),
        <<"distributionPerProvider">> => maps:map(fun
            (ProviderId, {error, _} = Error) ->
                build_error_response(Error, Guid, ProviderId);
            
            (_ProviderId, #provider_reg_distribution_get_result{
                virtual_size = VirtualSize,
                blocks_per_storage = BlocksPerStorage,
                locations_per_storage = LocationsPerStorage
            }) ->
                DistributionPerStorage = maps:fold(fun(StorageId, Blocks, Acc) ->
                    
                    {BlockList, TotalBlocksSize} = get_blocks_summary(Blocks),

                    Acc#{StorageId => #{
                        <<"physicalSize">> => TotalBlocksSize,
                        <<"blocks">> => BlockList
                    }}
                end, #{}, BlocksPerStorage),
                #{
                    <<"success">> => true,
                    <<"virtualSize">> => VirtualSize,
                    <<"distributionPerStorage">> => DistributionPerStorage,
                    <<"locationsPerStorage">> => maps_utils:undefined_to_null(LocationsPerStorage)
                }
            end, FileBlocksPerProvider)
    }.


-spec storage_locations_to_json(data_distribution:storage_locations_per_provider()) -> json_utils:json_term().
storage_locations_to_json(StorageLocations) ->
    #{
        <<"locationsPerProvider">> => maps:map(fun(_ProviderId, LocationsPerStorage) ->
            case LocationsPerStorage of
                {error, _} = Error ->
                    #{
                        <<"success">> => false,
                        <<"error">> => errors:to_json(Error)
                    };
                _ ->
                    #{
                        <<"success">> => true,
                        <<"locationsPerStorage">> => maps_utils:undefined_to_null(LocationsPerStorage)
                    }
            end
        end, StorageLocations)
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


-spec build_error_response({error, term()}, file_id:file_guid(), oneprovider:id()) ->
    json_utils:json_term().
build_error_response(Error, Guid, ProviderId) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    {ok, StoragesMap} = space_logic:get_provider_storages(SpaceId, ProviderId),
    ErrorJson = errors:to_json(Error),
    #{
        <<"success">> => false,
        <<"distributionPerStorage">> => maps:map(fun(_StorageId, _) -> #{
            <<"error">> => ErrorJson 
        }
        end, StoragesMap)
    }.

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
