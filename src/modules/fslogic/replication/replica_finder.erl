%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing functions for finding data in external replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_finder).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-type storage_details() :: {StorageId :: binary(), FileId :: binary()}.

%% API
-export([get_blocks_for_sync/2, get_unique_blocks/1,
    get_blocks_available_to_evict/2, get_all_blocks/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given blocks are synchronized in local file locations. If not,
%% returns list with tuples informing where to fetch data: {ProviderId, BlocksToFetch}
%% @end
%%--------------------------------------------------------------------
-spec get_blocks_for_sync([file_location:doc()], fslogic_blocks:blocks()) ->
    [{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}].
get_blocks_for_sync(_, []) ->
    [];
get_blocks_for_sync([], _) ->
    [];
get_blocks_for_sync(Locations, Blocks) ->
    LocalLocations = filter_local_locations(Locations),
    BlocksToSync = lists:foldl(fun(LocalLocation, BlocksToSync0) ->
        TruncatedBlocks = truncate_to_local_size(LocalLocation, BlocksToSync0),
        invalidate_local_blocks(LocalLocation, TruncatedBlocks)
    end, Blocks, LocalLocations),

    RemoteLocations = Locations -- LocalLocations,
    RemoteList = exclude_old_blocks(RemoteLocations),
    SortedRemoteList = lists:sort(RemoteList),
    AggregatedRemoteList = lists:foldl(fun
        ({ProviderId, ProviderBlocks, StorageDetails},
         [{ProviderId, BlocksAcc, StorageDetails} | Rest]) ->
            AggregatedBlocks = fslogic_blocks:merge(BlocksAcc, ProviderBlocks),
            [{ProviderId, AggregatedBlocks, StorageDetails} | Rest];
        (ProviderIdWithBlocks, Acc) ->
            [ProviderIdWithBlocks | Acc]
    end, [], SortedRemoteList),

    PresentBlocks = lists:map(fun({ProviderId, ProviderBlocks, StorageDetails}) ->
        AbsentBlocks = fslogic_blocks:invalidate(BlocksToSync, ProviderBlocks),
        PresentBlocks = fslogic_blocks:invalidate(BlocksToSync, AbsentBlocks),
        ConsolidatedPresentBlocks = fslogic_blocks:consolidate(PresentBlocks),
        {ProviderId, ConsolidatedPresentBlocks, StorageDetails}
    end, AggregatedRemoteList),

    minimize_present_blocks(PresentBlocks, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns lists of blocks that are unique in local locations (no other provider has them)
%% @end
%%--------------------------------------------------------------------
-spec get_unique_blocks(file_ctx:ctx()) -> {fslogic_blocks:blocks(), file_ctx:ctx()}.
get_unique_blocks(FileCtx) ->
    {LocationDocs, FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    LocalLocations = filter_local_locations(LocationDocs),
    RemoteLocations = LocationDocs -- LocalLocations,
    LocalBlocksList = get_all_blocks(LocalLocations),
    RemoteBlocksList = get_all_blocks(RemoteLocations),
    {fslogic_blocks:invalidate(LocalBlocksList, RemoteBlocksList), FileCtx2}.

%%-------------------------------------------------------------------
%% @doc
%% Finds block which can be evicted because they are replicated in
%% other provider.
%% NOTE: Currently this functions support only whole files.
%%       If remote provider doesn't have whole file, he is not included
%% in the response.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_available_to_evict(file_ctx:ctx(), version_vector:version_vector()) ->
    {undefined | [{od_provider:id(), fslogic_blocks:blocks()}] , file_ctx:ctx()}.
get_blocks_available_to_evict(FileCtx, LocalVV) ->
    {LocationDocs, FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    LocalLocations = filter_local_locations(LocationDocs),
    LocalBlocksList = get_all_blocks(LocalLocations),
    case LocalBlocksList of
        [] ->
            {undefined, FileCtx2};
        _ ->
            RemoteLocations = LocationDocs -- LocalLocations,
            Result = get_blocks_available_to_evict_per_provider(LocalBlocksList,
                LocalVV, RemoteLocations),
            %TODO maybe result should be sorted by version or by size?
            {Result, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns all blocks from given location list
%% @end
%%--------------------------------------------------------------------
-spec get_all_blocks([file_location:doc()]) -> fslogic_blocks:blocks().
get_all_blocks(LocationList) ->
    fslogic_blocks:consolidate(lists:sort([Block ||
        #document{value = #file_location{blocks = Blocks}} <- LocationList,
        Block <- Blocks
    ])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_blocks_available_to_evict_per_provider(fslogic_blocks:blocks(),
    version_vector:version_vector(), [file_location:doc()]) ->
    [{od_provider:id(), fslogic_block:blocks()}].
get_blocks_available_to_evict_per_provider(LocalBlocksList, LocalVV, RemoteLocations) ->
    lists:filtermap(fun(Doc = #document{
        value = #file_location{
            version_vector = VV,
            provider_id = ProviderId
        }
    }) ->
        case version_vector:compare(LocalVV, VV) of
            ComparisonResult when
                ComparisonResult =:= identical orelse
                ComparisonResult =:= lesser
            ->
                RemoteBlocksList = get_all_blocks([Doc]),
                % TODO currently we choose only providers who have all local blocks replicated
                case fslogic_blocks:invalidate(LocalBlocksList, RemoteBlocksList) of
                    [] ->
                        {true, {ProviderId, LocalBlocksList}};
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end, RemoteLocations).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters local location docs from location docs list.
%% @end
%%-------------------------------------------------------------------
-spec filter_local_locations([file_location:doc()]) -> [file_location:doc()].
filter_local_locations(LocationDocs) ->
    LocalProviderId = oneprovider:get_id(),
    lists:filter(fun(#document{value = #file_location{provider_id = Id}}) ->
        Id =:= LocalProviderId
    end, LocationDocs).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates LocalBlocks in Blocks.
%% @end
%%-------------------------------------------------------------------
-spec invalidate_local_blocks(file_location:doc(), fslogic_blocks:blocks()) -> fslogic_blocks:blocks().
invalidate_local_blocks(#document{value = #file_location{blocks = LocalBlocks}}, Blocks) ->
    fslogic_blocks:invalidate(Blocks, LocalBlocks).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates all blocks in Blocks that exceeds local file size
%% @end
%%-------------------------------------------------------------------
-spec truncate_to_local_size(file_location:doc(), fslogic_blocks:blocks()) -> fslogic_blocks:blocks().
truncate_to_local_size(#document{value = #file_location{size = LocalSize}}, Blocks) ->
    GlobalUpper = fslogic_blocks:upper(Blocks),
    case GlobalUpper > LocalSize of
        true ->
            fslogic_blocks:invalidate(Blocks, #file_block{offset = LocalSize, size = GlobalUpper});
        false ->
            Blocks
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given list of mappings between provider_id -> available_blocks,
%% returns minimized version suitable for data transfer, in which providers'
%% available blocks are disjoint.
%% @end
%%--------------------------------------------------------------------
-spec minimize_present_blocks([{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}],
                              fslogic_blocks:blocks()) ->
    [{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}].
minimize_present_blocks([], _) ->
    [];
minimize_present_blocks([{ProviderId, Blocks, StorageDetails} | Rest], AlreadyPresent) ->
    MinimizedBlocks = fslogic_blocks:invalidate(Blocks, AlreadyPresent),
    UpdatedAlreadyPresent = fslogic_blocks:merge(MinimizedBlocks, AlreadyPresent),
    case MinimizedBlocks of
        [] ->
            minimize_present_blocks(Rest, UpdatedAlreadyPresent);
        _ ->
            [{ProviderId, MinimizedBlocks, StorageDetails}
             | minimize_present_blocks(Rest, UpdatedAlreadyPresent)]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Excludes not up_to_date blocks.
%% @end
%%--------------------------------------------------------------------
-spec exclude_old_blocks([file_location:doc()]) ->
    [{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}].
exclude_old_blocks(RemoteLocations) ->
    RemoteList = lists:flatmap(fun(#document{
        value = #file_location{
            storage_id = StorageId,
            file_id = FileId,
            blocks = RemoteBlocks,
            provider_id = ProviderId,
            version_vector = VV
        }}) ->
        lists:map(fun(RB) ->
            {RB, {ProviderId, VV, {StorageId, FileId}}}
        end, RemoteBlocks)
    end, RemoteLocations),

    SortedRemoteList = lists:foldl(fun
        (Remote, []) ->
            [Remote];
        ({RemoteBlock, _} = Remote, [{LastBlock, _} = Last | AccTail] = Acc) ->
            U1 = fslogic_blocks:upper(LastBlock),
            L2 = fslogic_blocks:lower(RemoteBlock),
            case L2 >=  U1 of
                true ->
                    [Remote | Acc];
                _ ->
                    compare_blocks(Last, Remote) ++ AccTail
            end
    end, [], lists:sort(RemoteList)),

    [{ProviderId, [RemoteBlock], StorageDetails} ||
        {RemoteBlock, {ProviderId, _VV, StorageDetails}} <- SortedRemoteList].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Compares two blocks and excluded old parts of blocks.
%% @end
%%--------------------------------------------------------------------
-spec compare_blocks({fslogic_blocks:blocks(), {oneprovider:id(),
    version_vector:version_vector(), storage_details()}},
    {fslogic_blocks:blocks(), {oneprovider:id(),
        version_vector:version_vector(), storage_details()}}) ->
    [{fslogic_blocks:blocks(), {oneprovider:id(),
        version_vector:version_vector(), storage_details()}}].
compare_blocks({Block1, {_, VV1, _} = BlockInfo1} = B1,
    {Block2, {_, VV2, _} = BlockInfo2} = B2) ->
    case version_vector:compare(VV1, VV2) of
        lesser ->
            [Block1_2] = fslogic_blocks:invalidate([Block1], Block2),
            [B2, {Block1_2, BlockInfo1}];
        greater ->
            [Block2_2] = fslogic_blocks:invalidate([Block2], Block1),
            [{Block2_2, BlockInfo2}, B1];
        _ ->
            [B2, B1]
    end.