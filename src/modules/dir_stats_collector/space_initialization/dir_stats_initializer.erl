%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for initialization of statistics collections
%%% for single directory. Initialization is process of creation of
%%% directory statistics after enabling statistics counting for
%%% existing space. If statistics counting is enabled from the
%%% begging (support), initialization is not required.
%%%
%%% NOTE: There is possible race between initialization and update of 
%%%       statistics. To handle it, initialization is repeated if 
%%%       any update appears in less than ?RACE_PREVENTING_TIME from 
%%%       initialization.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_initializer).
-author("Michal Wrzeszcz").


-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([new_initialization_data/0, are_stats_ready/1, is_race_reported/1, report_race/1, get_stats/2,
    update_stats_from_descendants/3, ensure_dir_initialized/2]).


-record(initialization_data, {
    status = race_possible :: initalized | race_possible,
    init_watch :: stopwatch:instance() | undefined, % watch used to handle initialization/update races
    dir_and_direct_children_stats :: dir_stats_collection:collection() | undefined,
    stats_from_descendants :: dir_stats_collection:collection() | undefined
}).


-type initialization_data() :: #initialization_data{}.
-type initialization_data_map() :: #{dir_stats_collection:type() => initialization_data()}.
-type collections_map() :: #{dir_stats_collection:type() => dir_stats_collection:collection()}.


-define(RACE_PREVENTING_TIME, 5000). % If update appears in less than ?RACE_PREVENTING_TIME from initialization
                                     % finish it is considered as possible race and initialization is repeated
-define(BATCH_SIZE, 100).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_initialization_data() -> initialization_data().
new_initialization_data() ->
    #initialization_data{}.


-spec are_stats_ready(initialization_data()) -> boolean().
are_stats_ready(#initialization_data{status = initalized, init_watch = Watch}) ->
    stopwatch:read_millis(Watch) >= ?RACE_PREVENTING_TIME;
are_stats_ready(_) ->
    false.


-spec is_race_reported(initialization_data()) -> boolean().
is_race_reported(#initialization_data{status = Status}) ->
    Status =:= race_possible.


-spec report_race(initialization_data()) -> initialization_data().
report_race(Data) ->
    Data#initialization_data{
        status = race_possible,
        init_watch = undefined,
        dir_and_direct_children_stats = undefined
    }.


-spec get_stats(initialization_data(), dir_stats_collection:type()) -> dir_stats_collection:collection().
get_stats(#initialization_data{
    dir_and_direct_children_stats = DirWithDirectChildrenStats,
    stats_from_descendants = StatsFromDescendants
}, CollectionType) ->
    dir_stats_collection:consolidate(CollectionType, DirWithDirectChildrenStats, StatsFromDescendants).


-spec update_stats_from_descendants(initialization_data(), dir_stats_collection:type(),
    dir_stats_collection:collection()) -> initialization_data().
update_stats_from_descendants(#initialization_data{
    stats_from_descendants = CurrentStats
} = Data, CollectionType, CollectionUpdate) ->
    Data#initialization_data{
        stats_from_descendants = dir_stats_collection:consolidate(CollectionType, CurrentStats, CollectionUpdate)
    }.


-spec ensure_dir_initialized(file_id:file_guid(), initialization_data_map()) -> initialization_data_map().
ensure_dir_initialized(Guid, DataMap) ->
    ToInit = maps:filter(fun(_CollectionType, #initialization_data{status = Status}) ->
        Status =:= race_possible
    end, DataMap),

    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    CollectionTypesToInit = maps:keys(ToInit),
    StatsMap = init_for_dir_children(
        FileUuid, SpaceId, CollectionTypesToInit, #{token => ?INITIAL_DATASTORE_LS_TOKEN, size => ?BATCH_SIZE}, #{}),
    FinalStatsMap = finish_dir_init(Guid, CollectionTypesToInit, StatsMap),

    maps:merge_with(fun(_CollectionType, Data, Stats) ->
        Data#initialization_data{
            status = initalized,
            dir_and_direct_children_stats = Stats
        }
    end, DataMap, FinalStatsMap).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_for_dir_children(file_meta:uuid(), file_id:space_id(), [dir_stats_collection:type()],
    file_meta:list_opts(), collections_map()) -> collections_map().
init_for_dir_children(_FileUuid, _SpaceId, [], _ListOpts, Acc) ->
    Acc;
init_for_dir_children(FileUuid, SpaceId, CollectionTypes, ListOpts, Acc) ->
    case file_meta:list_children(FileUuid, ListOpts) of
        {ok, Links, #{is_last := true} = _ListExtendedInfo} ->
            init_batch(SpaceId, Links, CollectionTypes, Acc);
        {ok, Links, ListExtendedInfo} ->
            UpdatedAcc = init_batch(SpaceId, Links, CollectionTypes, Acc),
            NewListOpts = maps:merge(ListOpts, maps:remove(is_last, ListExtendedInfo)),
            init_for_dir_children(FileUuid, SpaceId, CollectionTypes, NewListOpts, UpdatedAcc)
    end.


-spec init_batch(file_id:space_id(), [file_meta:link()], [dir_stats_collection:type()], collections_map()) ->
    collections_map().
init_batch(SpaceId, Links, CollectionTypes, InitialAcc) ->
    lists:foldl(fun(CollectionType, Acc) ->
        CollectionStats = maps:get(CollectionType, Acc, undefined),
        Acc#{CollectionType => init_batch_for_collection_type(SpaceId, Links, CollectionType, CollectionStats)}
    end, InitialAcc, CollectionTypes).


-spec init_batch_for_collection_type(file_id:space_id(), [file_meta:link()], [dir_stats_collection:type()],
    collections_map()) -> collections_map().
init_batch_for_collection_type(SpaceId, Links, CollectionType, InitialStats) ->
    lists:foldl(fun
        ({_, ChildUuid}, undefined) ->
            CollectionType:init_child(file_id:pack_guid(ChildUuid, SpaceId));
        ({_, ChildUuid}, Stats) ->
            dir_stats_collection:consolidate(CollectionType, Stats,
                CollectionType:init_child(file_id:pack_guid(ChildUuid, SpaceId)))
    end, InitialStats, Links).


-spec finish_dir_init(file_id:guid(), [dir_stats_collection:type()], collections_map()) -> collections_map().
finish_dir_init(Guid, CollectionTypes, ChildrenStats) ->
    StatsForGuid = lists:map(fun(CollectionType) -> CollectionType:init_dir(Guid) end, CollectionTypes),

    maps:merge_with(fun(CollectionType, Stats1, Stats2) ->
        dir_stats_collection:consolidate(CollectionType, Stats1, Stats2)
    end, StatsForGuid, ChildrenStats).