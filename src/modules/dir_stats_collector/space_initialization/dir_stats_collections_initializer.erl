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
%%% beginning (support), initialization is not required.
%%%
%%% Initialization is performed in batches and processing of each
%%% batch has to be triggered separately. It allows dir_stats_collector
%%% handling requests between batches processing not to
%%% block dir_stats_collector when initializing large directories.
%%%
%%% NOTE: There is possible race between initialization and update of 
%%%       statistics. To handle it, initialization is repeated if 
%%%       any update appears in less than ?RACE_PREVENTING_TIME
%%%       after finishing initialization.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collections_initializer).
-author("Michal Wrzeszcz").


-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([new_initialization_data/0, are_stats_ready/1, report_update/1, get_stats/2,
    update_stats_from_children_descendants/3,
    start_dir_initialization/2, continue_dir_initialization/1, finish_dir_initialization/3,
    abort_collection_initialization/2]).


-record(initialization_data, {
    status = preparing :: preparing | prepared,
    race_preventing_timer :: countdown_timer:instance() | undefined, % timer used to handle initialization/update races
    dir_and_direct_children_stats :: dir_stats_collection:collection() | undefined,
    stats_from_children_descendants :: dir_stats_collection:collection() | undefined
}).


-record(initialization_progress, {
    file_uuid :: file_meta:uuid(),
    space_id :: file_id:space_id(),
    collections_map :: collections_map(),
    list_opts :: file_listing:options()
}).


-type initialization_data() :: #initialization_data{}.
-type initialization_progress() :: #initialization_progress{}.
-type initialization_data_map() :: #{dir_stats_collection:type() => initialization_data()}.
-type collections_map() :: #{dir_stats_collection:type() => dir_stats_collection:collection()}.
-export_type([initialization_data/0, initialization_progress/0, initialization_data_map/0, collections_map/0]).


-define(RACE_PREVENTING_TIME, 30000). % If update appears in less than ?RACE_PREVENTING_TIME from initialization
                                      % finish it is considered as possible race and initialization is repeated
-define(BATCH_SIZE, 100).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_initialization_data() -> initialization_data().
new_initialization_data() ->
    #initialization_data{}.


-spec are_stats_ready(initialization_data()) -> boolean().
are_stats_ready(#initialization_data{status = prepared, race_preventing_timer = Timer}) when Timer =/= undefined ->
    countdown_timer:is_expired(Timer);
are_stats_ready(_) ->
    false.


-spec report_update(initialization_data()) -> initialization_data().
report_update(Data) ->
    % Update
    Data#initialization_data{
        status = preparing,
        race_preventing_timer = undefined,
        dir_and_direct_children_stats = undefined
    }.


-spec get_stats(initialization_data(), dir_stats_collection:type()) -> dir_stats_collection:collection().
get_stats(#initialization_data{
    dir_and_direct_children_stats = DirWithDirectChildrenStats,
    stats_from_children_descendants = undefined
}, _CollectionType) ->
    DirWithDirectChildrenStats;
get_stats(#initialization_data{
    dir_and_direct_children_stats = DirWithDirectChildrenStats,
    stats_from_children_descendants = StatsFromDescendants
}, CollectionType) ->
    dir_stats_collection:consolidate(CollectionType, DirWithDirectChildrenStats, StatsFromDescendants).


-spec update_stats_from_children_descendants(initialization_data(), dir_stats_collection:type(),
    dir_stats_collection:collection()) -> initialization_data().
update_stats_from_children_descendants(#initialization_data{
    stats_from_children_descendants = undefined
} = Data, _CollectionType, CollectionUpdate) ->
    Data#initialization_data{
        stats_from_children_descendants = CollectionUpdate
    };
update_stats_from_children_descendants(#initialization_data{
    stats_from_children_descendants = CurrentStats
} = Data, CollectionType, CollectionUpdate) ->
    Data#initialization_data{
        stats_from_children_descendants = dir_stats_collection:consolidate(
            CollectionType, CurrentStats, CollectionUpdate)
    }.


-spec start_dir_initialization(file_id:file_guid(), initialization_data_map()) -> initialization_progress().
start_dir_initialization(Guid, DataMap) ->
    ToInit = maps:filter(fun(_CollectionType, #initialization_data{status = Status}) ->
        Status =:= preparing
    end, DataMap),

    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    InitialCollectionsMap = maps:map(fun(_CollectionType, _Data) -> undefined end, ToInit),
    #initialization_progress{
        file_uuid = FileUuid,
        space_id = SpaceId,
        collections_map = InitialCollectionsMap,
        list_opts = #{tune_for_large_continuous_listing => true, limit => ?BATCH_SIZE}
    }.


-spec continue_dir_initialization(initialization_progress()) ->
    {finish, collections_map()} | {continue, initialization_progress()}.
continue_dir_initialization(#initialization_progress{
    file_uuid = FileUuid,
    space_id = SpaceId,
    collections_map = CollectionsMap,
    list_opts = ListOpts
} = InitializationProgress) ->
    {ok, Links, ListingPaginationToken} = file_listing:list(FileUuid, ListOpts),
    UpdatedCollectionsMap = init_batch(SpaceId, Links, CollectionsMap),
    case file_listing:is_finished(ListingPaginationToken) of
        true ->
            {finish, UpdatedCollectionsMap};
        _ ->
            NewListOpts = #{
                pagination_token => ListingPaginationToken,
                limit => ?BATCH_SIZE
            },
            {continue, InitializationProgress#initialization_progress{
                collections_map = UpdatedCollectionsMap,
                list_opts = NewListOpts
            }}
    end.


-spec finish_dir_initialization(file_id:file_guid(), initialization_data_map(), collections_map()) ->
    initialization_data_map().
finish_dir_initialization(Guid, DataMap, CollectionsMap) ->
    FinalCollectionsMap = finish_dir_init(Guid, CollectionsMap),

    Timer = countdown_timer:start_millis(?RACE_PREVENTING_TIME),
    maps:merge_with(fun(_CollectionType, Data, Stats) ->
        Data#initialization_data{
            status = prepared,
            race_preventing_timer = Timer,
            dir_and_direct_children_stats = Stats
        }
    end, DataMap, FinalCollectionsMap).


-spec abort_collection_initialization(initialization_progress(), dir_stats_collection:type()) ->
    {collections_left, initialization_progress()} | initialization_aborted_for_all_collections.
abort_collection_initialization(#initialization_progress{
    collections_map = CollectionsMap
} = InitializationProgress, CollectionType) ->
    UpdatedCollectionsMap = maps:remove(CollectionType, CollectionsMap),
    case maps:size(UpdatedCollectionsMap) of
        0 ->
            initialization_aborted_for_all_collections;
        _ ->
            {collections_left, InitializationProgress#initialization_progress{
                collections_map = UpdatedCollectionsMap
            }}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_batch(file_id:space_id(), [file_meta:link()], collections_map()) -> collections_map().
init_batch(_SpaceId, [], CollectionsMap) ->
    CollectionsMap;
init_batch(SpaceId, Links, CollectionsMap) ->
    maps:map(fun(CollectionType, CollectionStats) ->
        init_batch_for_collection_type(SpaceId, Links, CollectionType, CollectionStats)
    end, CollectionsMap).


-spec init_batch_for_collection_type(file_id:space_id(), [file_meta:link()], [dir_stats_collection:type()],
    collections_map()) -> collections_map().
init_batch_for_collection_type(SpaceId, Links, CollectionType, InitialStats) ->
    lists:foldl(fun
        ({_, ChildUuid}, undefined) ->
            CollectionType:init_child(file_id:pack_guid(ChildUuid, SpaceId), false);
        ({_, ChildUuid}, Stats) ->
            dir_stats_collection:consolidate(CollectionType, Stats,
                CollectionType:init_child(file_id:pack_guid(ChildUuid, SpaceId), false))
    end, InitialStats, Links).


-spec finish_dir_init(file_id:file_guid(), collections_map()) -> collections_map().
finish_dir_init(Guid, CollectionsMap) ->
    maps:map(fun
        (CollectionType, undefined) ->
            CollectionType, CollectionType:init_dir(Guid);
        (CollectionType, ChildrenStats) ->
            dir_stats_collection:consolidate(CollectionType, CollectionType:init_dir(Guid), ChildrenStats)
    end, CollectionsMap).