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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-type storage_details() :: {StorageId :: binary(), FileId :: binary()}.
-type requests_list() :: [{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}].

%% API
-export([get_blocks_for_sync/2, get_remote_duplicated_blocks/1, get_all_blocks/1]).

% Enable/disable suiting of requested blocks` range to storage block size. If true,
% requested blocks will be enlarged to overlap with full storage system blocks to
% optimize data writing to storage system. Blocks are enlarged only if source provider
% posses data needed to enlarge block.
-define(BLOCK_SUITING_OPT, application:get_env(?APP_NAME, synchronizer_block_suiting, true)).
% Define minimal size of block that can be enlarged.
% This option can be used to prevent system from enlarging small blocks because such
% behaviour can result in significantly increased consumption of network bandwidth.
% By default the system tries to suite all requested blocks to storage block size
% (all blocks of size greater than 0).
-define(BLOCK_SUITING_MIN_SIZE, application:get_env(?APP_NAME, synchronizer_block_suiting_min_size, 0)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given blocks are synchronized in local file locations. If not,
%% returns list with tuples informing where to fetch data: {ProviderId, BlocksToFetch}
%% @end
%%--------------------------------------------------------------------
-spec get_blocks_for_sync([file_location:doc()], fslogic_blocks:blocks()) -> requests_list().
get_blocks_for_sync(_, []) ->
    [];
get_blocks_for_sync([], _) ->
    [];
get_blocks_for_sync(Locations, Blocks) ->
    [#document{value = #file_location{storage_id = LocalStorageId}} | _] =
        LocalLocations = filter_local_locations(Locations),
    BlocksToSync = lists:foldl(fun(LocalLocation, BlocksToSync0) ->
        TruncatedBlocks = truncate_to_local_size(LocalLocation, BlocksToSync0),
        invalidate_local_blocks(LocalLocation, TruncatedBlocks)
    end, Blocks, LocalLocations),

    RemoteLocations = Locations -- LocalLocations,
    RemoteList = exclude_old_blocks(RemoteLocations, BlocksToSync),
    SortedRemoteList = lists:sort(RemoteList),
    AggregatedRemoteList0 = lists:foldl(fun
        ({ProviderId, ProviderBlocks, StorageDetails},
         [{ProviderId, BlocksAcc, StorageDetails} | Rest]) ->
            AggregatedBlocks = fslogic_blocks:merge(BlocksAcc, ProviderBlocks),
            [{ProviderId, AggregatedBlocks, StorageDetails} | Rest];
        (ProviderIdWithBlocks, Acc) ->
            [ProviderIdWithBlocks | Acc]
    end, [], SortedRemoteList),
    AggregatedRemoteList = lists:reverse(AggregatedRemoteList0),

    PresentBlocks2 = lists:map(fun({ProviderId, ProviderBlocks, StorageDetails}) ->
        AbsentBlocks = fslogic_blocks:invalidate(BlocksToSync, ProviderBlocks),
        PresentBlocks = fslogic_blocks:invalidate(BlocksToSync, AbsentBlocks),
        ConsolidatedPresentBlocks = fslogic_blocks:consolidate(PresentBlocks),
        {ProviderId, ConsolidatedPresentBlocks, StorageDetails}
    end, AggregatedRemoteList),

    PresentBlocks3 = consolidate_requested_blocks(PresentBlocks2, AggregatedRemoteList),
    PresentBlocks4 = filter_small(PresentBlocks3),
    PresentBlocks5 = lists:filter(fun
        ({_, [], _}) -> false;
        (_) -> true
    end, PresentBlocks4),

    Requests = minimize_present_blocks(PresentBlocks5, []),
    suite_to_storage_block_size(Requests, AggregatedRemoteList, LocalStorageId).

-spec suite_to_storage_block_size(requests_list(), requests_list(), storage:id()) -> requests_list().
suite_to_storage_block_size(Requests, ProvidersAllBlocks, LocalStorageId) ->
    case ?BLOCK_SUITING_OPT of
        true ->
            case storage:get_block_size(LocalStorageId) of
                undefined ->
                    Requests;
                0 -> % Helper does not split files into blocks
                    Requests;
                StorageBlockSize ->
                    ProvidersAllBlocksMap = lists:foldl(fun({ProviderId, AllProviderBlocks, _StorageDetails}, Acc) ->
                        Acc#{ProviderId => AllProviderBlocks}
                    end, #{}, ProvidersAllBlocks),

                    MinSizeToSuite = ?BLOCK_SUITING_MIN_SIZE,
                    lists:map(fun({ProviderId, Blocks, StorageDetails}) ->
                        FinalBlocks = suite_blocks_sizes(Blocks, maps:get(ProviderId, ProvidersAllBlocksMap),
                            StorageBlockSize, MinSizeToSuite),
                        {ProviderId, FinalBlocks, StorageDetails}
                    end, Requests)
            end;
        _ ->
            Requests
    end.

-spec suite_blocks_sizes(fslogic_blocks:blocks(), fslogic_blocks:blocks(), non_neg_integer(), non_neg_integer()) ->
    fslogic_blocks:blocks().
suite_blocks_sizes([], _AllBlocks, _StorageBlockSize, _MinSizeToSuite) ->
    [];
suite_blocks_sizes([#file_block{size = Size} = Block | Blocks], AllBlocks, StorageBlockSize, MinSizeToSuite) when
    Size < MinSizeToSuite ->
    [Block | suite_blocks_sizes(Blocks, AllBlocks, StorageBlockSize, MinSizeToSuite)];
suite_blocks_sizes([#file_block{offset = Offset, size = Size} = Block | Blocks], 
    AllBlocks, StorageBlockSize, MinSizeToSuite) ->
    Block2 = case (Offset + Size) rem StorageBlockSize of
        0 ->
            Block;
        EndRem ->
            ExpectedSize = Size + (StorageBlockSize - EndRem),
            suite_block_end(Block, ExpectedSize, AllBlocks)
    end,

    Suited = case Offset rem StorageBlockSize of
        0 ->
            [Block2];
        OffsetRem ->
            ExpectedBeg = Offset - OffsetRem,
            PossibleSplitPoint = ExpectedBeg + StorageBlockSize,
            suite_block_offset_or_split(Block2, ExpectedBeg, PossibleSplitPoint, AllBlocks)
    end,

    Suited ++ suite_blocks_sizes(Blocks, AllBlocks, StorageBlockSize, MinSizeToSuite).

-spec suite_block_end(fslogic_blocks:block(), non_neg_integer(), fslogic_blocks:blocks()) -> fslogic_blocks:block().
suite_block_end(#file_block{offset = Offset, size = Size} = Block, ExpectedSize, AllBlocks) ->
    CanExtend = lists:any(fun(#file_block{offset = CheckO, size = CheckS}) ->
        % Verify if source provider posses data needed to extend ending of the block
        % (it has been verified before that it has whole block)
        CheckO =< Offset + Size andalso CheckO + CheckS >= Offset + ExpectedSize
    end, AllBlocks),

    case CanExtend of
        true -> Block#file_block{size = ExpectedSize};
        false -> Block
    end.

-spec suite_block_offset_or_split(fslogic_blocks:block(), non_neg_integer(), non_neg_integer(),
    fslogic_blocks:blocks()) -> fslogic_blocks:blocks().
suite_block_offset_or_split(#file_block{offset = Offset, size = Size} = Block,
    ExpectedBeg, PossibleSplitPoint, AllBlocks) ->
    CanExtend = lists:any(fun(#file_block{offset = CheckO, size = CheckS}) ->
        % Verify if source provider posses data needed to extend beginning of the block
        % (it has been verified before that it has whole block)
        CheckO =< ExpectedBeg andalso CheckO + CheckS >= Offset
    end, AllBlocks),

    case {CanExtend, PossibleSplitPoint < Size + Offset} of
        {true, _} ->
            [#file_block{offset = ExpectedBeg, size = Size + Offset - ExpectedBeg}];
        {false, true} ->
            % Split block as rtransfer_link requires it to split requests optimally
            [Block#file_block{size = PossibleSplitPoint - Offset},
                #file_block{offset = PossibleSplitPoint, size = Size + Offset - PossibleSplitPoint}];
        _ ->
            [Block]
    end.

%%-------------------------------------------------------------------
%% @doc
%% Finds block which can be deleted locally because they are
%% replicated in other provider.
%% NOTE: Currently this functions support only whole files.
%%       If remote provider doesn't have whole file, he is not included
%%       in the response.
%% NOTE: For better performance, pass location docs fetched without
%%       local blocks (option skip_local_blocks). In such case,
%%       duplicated blocks for local blocks won't be found.
%% @end
%%-------------------------------------------------------------------
-spec get_remote_duplicated_blocks([file_location:doc()]) ->
    undefined | [{od_provider:id(), fslogic_blocks:blocks()}].
get_remote_duplicated_blocks(LocationDocs) ->
    case filter_local_locations(LocationDocs) of
        [] ->
            undefined;
        [LocalLocation] ->
            LocalVV = file_location:get_version_vector(LocalLocation),
            case fslogic_location_cache:get_blocks(LocalLocation) of
                [] ->
                    [];
                LocalBlocks ->
                    RemoteLocations = LocationDocs -- [LocalLocation],
                    get_duplicated_blocks_per_provider(LocalBlocks, LocalVV, RemoteLocations)
                    %TODO VFS-4622 maybe result should be sorted by version or by size?
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns all blocks from given location list
%% @end
%%--------------------------------------------------------------------
-spec get_all_blocks([file_location:doc()]) -> fslogic_blocks:blocks().
get_all_blocks(LocationList) ->
    Blocks = lists:flatmap(fun(Location) ->
        fslogic_location_cache:get_blocks(Location, #{skip_local => true})
    end, LocationList),
    fslogic_blocks:consolidate(lists:sort(Blocks)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_duplicated_blocks_per_provider(fslogic_blocks:blocks(),
    version_vector:version_vector(), [file_location:doc()]) ->
    [{od_provider:id(), fslogic_blocks:blocks()}].
get_duplicated_blocks_per_provider(LocalBlocksList, LocalVV, RemoteLocations) ->
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
                % TODO VFS-3728 currently we choose only providers who have all local blocks replicated
                case fslogic_blocks:invalidate(LocalBlocksList, RemoteBlocksList) of
                    [] ->
                        {true, {ProviderId, RemoteBlocksList}};
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end, RemoteLocations).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Consolidates requested blocks filling small holes.
%% @end
%%--------------------------------------------------------------------
-spec consolidate_requested_blocks([{oneprovider:id(), fslogic_blocks:blocks(),
    storage_details()}], requests_list()) -> requests_list().
consolidate_requested_blocks(PresentBlocks, RemoteList) ->
    Zipped = lists:zip(PresentBlocks, RemoteList),
    MinSize = application:get_env(?APP_NAME, rtransfer_min_hole_size, 0),
    case MinSize =< 1 of
        true ->
            PresentBlocks;
        _ ->
            lists:map(fun
                ({{ProviderId, [], StorageDetails} = PB,
                    {ProviderId, _, StorageDetails}}) ->
                    PB;
                ({{ProviderId, [#file_block{}], StorageDetails} = PB,
                    {ProviderId, _, StorageDetails}}) ->
                    PB;
                ({{ProviderId, [First | Blocks], StorageDetails},
                    {ProviderId, AllBlocks, StorageDetails}}) ->
                    Blocks2 = lists:foldl(fun(#file_block{offset = O2, size = S2} = Block,
                        [#file_block{offset = O, size = S} | AccTail] = Acc) ->
                        case lists:any(fun(#file_block{offset = CheckO, size = CheckS}) ->
                            CheckO =< O andalso CheckO + CheckS >= O2 + S2
                        end, AllBlocks) of
                            true ->
                                case O2 - O - S < MinSize of
                                    true ->
                                        [#file_block{offset = O, size = S2 + O2 - O} | AccTail];
                                    _ ->
                                        [Block | Acc]
                                end;
                            _ ->
                                [Block | Acc]
                        end
                    end, [First], Blocks),
                    {ProviderId, lists:reverse(Blocks2), StorageDetails}
            end, Zipped)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Filter small blocks if they are present in bigger blocks from other providers.
%% @end
%%--------------------------------------------------------------------
-spec filter_small([{oneprovider:id(), fslogic_blocks:blocks(), storage_details()}]) -> requests_list().
filter_small([_] = PresentBlocks) ->
    PresentBlocks;
filter_small(PresentBlocks) ->
    AllBlocks = lists:foldl(fun({_ProviderId, Blocks, _StorageDetails}, Acc) ->
        Acc ++ Blocks
    end, [], PresentBlocks),
    lists:map(fun({ProviderId, Blocks, StorageDetails}) ->
        Blocks2 = lists:filter(fun(#file_block{offset = O, size = S}) ->
            not lists:any(fun(#file_block{offset = CheckO, size = CheckS}) ->
                (CheckO < O andalso CheckO + CheckS >= O + S) orelse
                    (CheckO =< O andalso CheckO + CheckS > O + S)
            end, AllBlocks)
        end, Blocks),
        {ProviderId, Blocks2, StorageDetails}
    end, PresentBlocks).


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
-spec invalidate_local_blocks(file_location:doc(), fslogic_blocks:blocks()) ->
    fslogic_blocks:blocks().
invalidate_local_blocks(FileLocation, Blocks) ->
    LocalBlocks = fslogic_location_cache:get_blocks(FileLocation,
        #{overlapping_blocks => Blocks}),
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
-spec minimize_present_blocks(requests_list(), fslogic_blocks:blocks()) -> requests_list().
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
-spec exclude_old_blocks([file_location:doc()], fslogic_blocks:blocks()) -> requests_list().
exclude_old_blocks(RemoteLocations, BlocksToSync) ->
    RemoteList = lists:flatmap(fun(#document{
        value = #file_location{
            storage_id = StorageId,
            file_id = FileId,
            provider_id = ProviderId,
            version_vector = VV
        }} = FL) ->
        RemoteBlocks = fslogic_location_cache:get_blocks(FL,
            #{overlapping_blocks => BlocksToSync}),
        lists:map(fun(RB) ->
            {RB, {ProviderId, VV, {StorageId, FileId}}}
        end, RemoteBlocks)
    end, RemoteLocations),

    RemoteList2 = exclude_old_blocks(lists:sort(RemoteList)),
    [{ProviderId, [RemoteBlock], StorageDetails} ||
        {RemoteBlock, {ProviderId, _VV, StorageDetails}} <- RemoteList2].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Excludes not up_to_date blocks from list.
%% @end
%%--------------------------------------------------------------------
-spec exclude_old_blocks(list()) -> list().
exclude_old_blocks(RemoteList) ->
    RemoteList2 = lists:foldl(fun
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
    end, [], RemoteList),
    RemoteList3 = lists:sort(RemoteList2),
    case RemoteList3 =:= RemoteList of
        true ->
            RemoteList;
        _ ->
            exclude_old_blocks(RemoteList3)
    end.

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
            B1List = fslogic_blocks:invalidate([Block1], Block2),
            [B2 | lists:map(fun(B) -> {B, BlockInfo1} end, B1List)];
        greater ->
            B2List = fslogic_blocks:invalidate([Block2], Block1),
            lists:map(fun(B) -> {B, BlockInfo2} end, B2List) ++ [B1];
        _ ->
            [B2, B1]
    end.
