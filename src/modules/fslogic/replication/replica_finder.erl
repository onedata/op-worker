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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([get_blocks_for_sync/2]).

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
    [{oneprovider:id(), fslogic_blocks:blocks()}].
get_blocks_for_sync(_, []) ->
    [];
get_blocks_for_sync(Locations, Blocks) ->
    LocalProviderId = oneprovider:get_provider_id(),
    LocalLocations = [Loc || Loc = #document{value = #file_location{provider_id = Id}} <- Locations, Id =:= LocalProviderId],
    RemoteLocations = Locations -- LocalLocations,
    LocalBlocksList = [LocalBlocks || #document{value = #file_location{blocks = LocalBlocks}} <- LocalLocations],

    BlocksToSync = lists:foldl(
        fun(LocalBlocks, Acc) ->
            fslogic_blocks:invalidate(Acc, LocalBlocks)
        end, Blocks, LocalBlocksList),

    RemoteList =
        [{ProviderId, RemoteBlocks} ||
            #document{value = #file_location{blocks = RemoteBlocks, provider_id = ProviderId}}
                <- RemoteLocations],
    SortedRemoteList = lists:sort(RemoteList),
    AggregatedRemoteList = lists:foldl(fun
        ({ProviderId, ProviderBlocks}, [{ProviderId, BlocksAcc} | Rest]) ->
            AggregatedBlocks = fslogic_blocks:merge(BlocksAcc, ProviderBlocks),
            [{ProviderId, AggregatedBlocks} | Rest];
        (ProviderIdWithBlocks, Acc) ->
            [ProviderIdWithBlocks | Acc]
    end, [], SortedRemoteList),

    PresentBlocks = lists:map(fun({ProviderId, ProviderBlocks}) ->
        AbsentBlocks = fslogic_blocks:invalidate(BlocksToSync, ProviderBlocks),
        PresentBlocks = fslogic_blocks:invalidate(BlocksToSync, AbsentBlocks),
        ConsolidatedPresentBlocks = fslogic_blocks:consolidate(PresentBlocks),
        {ProviderId, ConsolidatedPresentBlocks}
    end, AggregatedRemoteList),

    minimize_present_blocks(PresentBlocks, []).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For given list of mappings between provider_id -> available_blocks,
%% returns minimized version suitable for data transfer, in which providers'
%% available blocks are disjoint.
%% @end
%%--------------------------------------------------------------------
-spec minimize_present_blocks([{oneprovider:id(), fslogic_blocks:blocks()}], fslogic_blocks:blocks()) ->
    [{oneprovider:id(), fslogic_blocks:blocks()}].
minimize_present_blocks([], _) ->
    [];
minimize_present_blocks([{ProviderId, Blocks} | Rest], AlreadyPresent) ->
    MinimizedBlocks = fslogic_blocks:invalidate(Blocks, AlreadyPresent),
    UpdatedAlreadyPresent = fslogic_blocks:merge(MinimizedBlocks, AlreadyPresent),
    case MinimizedBlocks of
        [] ->
            minimize_present_blocks(Rest, UpdatedAlreadyPresent);
        _ ->
            [{ProviderId, MinimizedBlocks} | minimize_present_blocks(Rest, UpdatedAlreadyPresent)]
    end.
