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

-type storage_details() :: {StorageId :: binary(), FileId :: binary()}.

%% API
-export([get_blocks_for_sync/2, get_unique_blocks/1]).

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
get_blocks_for_sync(Locations, Blocks) ->
    LocalProviderId = oneprovider:get_id(),
    LocalLocations = [Loc || Loc = #document{value = #file_location{provider_id = Id}} <- Locations, Id =:= LocalProviderId],
    RemoteLocations = Locations -- LocalLocations,
    LocalBlocksList = [LocalBlocks || #document{value = #file_location{blocks = LocalBlocks}} <- LocalLocations],

    BlocksToSync = lists:foldl(
        fun(LocalBlocks, Acc) ->
            fslogic_blocks:invalidate(Acc, LocalBlocks)
        end, Blocks, LocalBlocksList),

    RemoteList =
        [{ProviderId, RemoteBlocks, {StorageId, FileId}} ||
            #document{value = #file_location{storage_id = StorageId, file_id = FileId,
                                             blocks = RemoteBlocks, provider_id = ProviderId}}
                <- RemoteLocations],
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
    LocalProviderId = oneprovider:get_id(),
    LocalLocations = [Loc || Loc = #document{value = #file_location{provider_id = Id}} <- LocationDocs, Id =:= LocalProviderId],
    RemoteLocations = LocationDocs -- LocalLocations,
    LocalBlocksList = get_all_blocks(LocalLocations),
    RemoteBlocksList = get_all_blocks(RemoteLocations),
    {fslogic_blocks:invalidate(LocalBlocksList, RemoteBlocksList), FileCtx2}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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

%%--------------------------------------------------------------------
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
