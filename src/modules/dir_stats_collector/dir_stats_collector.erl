%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin of PES framework used to collect statistics about directories.
%%% PES executors gather statistics for each directory and then update
%%% them also for directory parent. Thus, each directory possesses
%%% statistics gathered using all its nested directories and files.
%%% As a result, statistics for space directory are collected from all
%%% files and directories inside the space.
%%%
%%% Collector does not monitor any statistics itself. Collection
%%% modules (implementing dir_stats_collection_behaviour) are responsible
%%% for triggering update of statistics. Collection modules also provide
%%% persistence of statistics and methods of statistics' changes
%%% consolidation. Thus, collector does not understand the meaning of statistics.
%%% The role of collector is propagation of statistics' changes via files
%%% tree and caching of statistics' changes in process memory to prevent
%%% datastore overload. Collector does not understand the meaning of statistics.
%%%
%%% When any statistic is changed, executor for directory is started. It
%%% caches changes of statistics for directory, periodically flushing them.
%%% Flush has three steps:
%%%     1) saving statistics in datastore,
%%%     2) propagating accumulated updates to directory parent
%%%        (difference since the previous propagation),
%%%     3) pruning the cache of no longer needed entries, either those to be
%%%        deleted or those which were inactive (not updated or retrieved for
%%%        ?CACHED_DIR_STATS_INACTIVITY_PERIOD)
%%% If the executor has no statistics left in cache, it terminates after
%%% the idle timeout.
%%%
%%% Typically dir_stats_collector is used when collecting status is enabled
%%% (see dir_stats_collector_config). However, it is also used by
%%% dir_stats_collections_initialization_traverse which initializes
%%% collections for all directories. In such a case, it uses helper module
%%% dir_stats_collections_initializer as collecting initialization requires
%%% special treatment (races between initialization and updates can occur).
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API - single directory
-export([get_stats/3,
    update_stats_of_dir/3, update_stats_of_parent/3, update_stats_of_parent/4, update_stats_of_nearest_dir/3,
    flush_stats/2, delete_stats/2,
    initialize_collections/1]).
%% API - space
-export([stop_collecting/1]).

%% pes_plugin_behaviour callbacks
-export([get_root_supervisor_name/0]).
-export([get_mode/0,
    init/0, graceful_terminate/1, forced_terminate/2,
    handle_call/2, handle_cast/2]).


% Executor's state holds recently used (updated or read) statistics for different
% directories and collection types, which are cached in the state until flushed
% and ?CACHED_DIR_STATS_INACTIVITY_PERIOD passes since their last usage.
% A cached_dir_stats record is created for each guid/collection_type pair as statistics
% for each directory (guid) are stored in several collections.
-record(state, {
    dir_stats_cache = #{} :: #{cached_dir_stats_key() => cached_dir_stats()},
    has_unflushed_changes = false :: boolean(),
    flush_timer_ref :: reference() | undefined,
    initialization_timer_ref :: reference() | undefined,

    % Space status is initialized on first request for any dir in space
    space_collecting_statuses = #{} :: #{od_space:id() => dir_stats_collector_config:extended_active_collecting_status()}
}).

-record(cached_dir_stats, {
    % current statistics for the {guid, type} pair, i. e. previous stats acquired from db
    % with all the updates (that were reported during executor's lifecycle) applied
    current_stats :: dir_stats_collection:collection() | undefined, % undefined during collections initialization
    % accumulates all the updates that were reported since the last propagation to parent
    stat_updates_acc_for_parent = #{} :: dir_stats_collection:collection(),
    last_used :: stopwatch:instance() | undefined,
    parent :: file_id:file_guid() | root_dir | undefined, % resolved and stored upon first access to this field,

    % Field used to handle collections initialization when space collecting is changed to enabled for not empty space
    % (see dir_stats_collector_config)
    collecting_status :: dir_stats_collector_config:active_collecting_status(),
    initialization_data :: dir_stats_collections_initializer:initialization_data() | undefined

}).


%% Internal protocol - requests to dir_stats_collector.
-record(dsc_get_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type(),
    stat_names :: dir_stats_collection:stats_selector()
}).

-record(dsc_update_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type(),
    collection_update :: dir_stats_collection:collection(),
    update_type :: update_type()
}).

-record(dsc_flush_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type(),
    pruning_strategy :: pruning_strategy()
}).


-type update_type() :: internal | external. % internal update is update sent when collector is
                                            % flushing stat_updates_acc_for_parent
-type state() :: #state{}.

-type cached_dir_stats_key() :: {file_id:file_guid(), dir_stats_collection:type()}.
-type cached_dir_stats() :: #cached_dir_stats{}.

-type dir_status() :: flushed | unflushed | collections_initialization.
-type pruning_strategy() :: prune_flushed | prune_inactive.


-define(FLUSH_INTERVAL_MILLIS, 5000).
-define(CACHED_DIR_STATS_INACTIVITY_PERIOD, 10000). % stats that are already flushed and not used for this period
                                                    % are removed from the cache
-define(SUPERVISOR_NAME, dir_stats_collector_worker_sup).

-define(SCHEDULED_FLUSH, scheduled_flush).
-define(SCHEDULED_COLLECTIONS_INITIALIZATION, scheduled_stats_initialization).
-define(SCHEDULED_COLLECTIONS_INITIALIZATION_INTERVAL_MILLIS, 2000). % initialization is triggered not more often than
                                                                     % this time to decrease number of initialization/update
                                                                     % races when several updates of dir appear in short
                                                                     % period of time

-define(INITIALIZE_COLLECTIONS(Guid), {initialize_collections, Guid}).
-define(STOP_COLLECTING(SpaceId), {stop_collecting, SpaceId}).


%%%===================================================================
%%% API - single directory
%%%===================================================================

-spec get_stats(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:stats_selector()) ->
    {ok, dir_stats_collection:collection()} |
    ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN | ?ERROR_NOT_FOUND.
get_stats(Guid, CollectionType, StatNames) ->
    case dir_stats_collector_config:get_extended_collecting_status(file_id:guid_to_space_id(Guid)) of
        enabled ->
            Request = #dsc_get_request{
                guid = Guid,
                collection_type = CollectionType,
                stat_names = StatNames
            },
            call_designated_node(Guid, submit_and_await, [?MODULE, Guid, Request]);
        % TODO VFS-8837 - should we have 2 types of errors: ?ERROR_FORBIDDEN and ?ERROR_DIR_STATS_DISABLED_FOR_SPACE?
        % Maybe return single error ?ERROR_DIR_STATS_COLLECTING_NOT_ACTIVE_FOR_SPACE?
        {collections_initialization, _} ->
            ?ERROR_FORBIDDEN;
        _ ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec update_stats_of_dir(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir(Guid, CollectionType, CollectionUpdate) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            update_stats_of_dir(Guid, CollectionType, CollectionUpdate, external);
        false ->
            ok
    end.


-spec update_stats_of_parent(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent(Guid, CollectionType, CollectionUpdate) ->
    update_stats_of_parent(Guid, CollectionType, CollectionUpdate, add_hook).


-spec update_stats_of_parent(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection(),
    add_hook | return_error) -> ok | ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_NOT_FOUND.
update_stats_of_parent(Guid, CollectionType, CollectionUpdate, ParentErrorHandlingMethod) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case get_parent(Guid) of
                {ok, ParentGuid} ->
                    update_stats_of_parent_internal(ParentGuid, CollectionType, CollectionUpdate);
                ?ERROR_NOT_FOUND when ParentErrorHandlingMethod =:= add_hook ->
                    add_hook_for_missing_doc(Guid, CollectionType, CollectionUpdate);
                ?ERROR_NOT_FOUND ->
                    ?ERROR_NOT_FOUND
            end;
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks file type and executes update for directory identified by Guid or for parent if Guid represents regular
%% file or link.
%% @end
%%--------------------------------------------------------------------
-spec update_stats_of_nearest_dir(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_nearest_dir(Guid, CollectionType, CollectionUpdate) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
            case file_meta:get_including_deleted(FileUuid) of
                {ok, Doc} ->
                    case file_meta:get_type(Doc) of
                        ?DIRECTORY_TYPE ->
                            update_stats_of_dir(Guid, CollectionType, CollectionUpdate, external);
                        _ ->
                            update_stats_of_parent_internal(get_parent(Doc, SpaceId), CollectionType, CollectionUpdate)
                    end;
                ?ERROR_NOT_FOUND ->
                    file_meta_posthooks:add_hook(FileUuid, generator:gen_name(),
                        ?MODULE, ?FUNCTION_NAME, [Guid, CollectionType, CollectionUpdate])
            end;
        false ->
            ok
    end.


-spec flush_stats(file_id:file_guid(), dir_stats_collection:type()) ->
    ok | ?ERROR_FORBIDDEN | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_INTERNAL_SERVER_ERROR.
flush_stats(Guid, CollectionType) ->
    case dir_stats_collector_config:get_extended_collecting_status(file_id:guid_to_space_id(Guid)) of
        enabled -> request_flush(Guid, CollectionType, prune_inactive);
        {collections_initialization, _} -> ?ERROR_FORBIDDEN;
        _ -> ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec delete_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
delete_stats(Guid, CollectionType) ->
    % TODO VFS-8837 - delete only for directories
    % TODO VFS-8837 - delete collection when collecting was enabled in past
    % TODO VFS-8837 - delete from collector memory for collections_initialization status
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case request_flush(Guid, CollectionType, prune_flushed) of
                ok -> CollectionType:delete(Guid);
                ?ERROR_FORBIDDEN -> CollectionType:delete(Guid);
                ?ERROR_INTERNAL_SERVER_ERROR -> ?ERROR_INTERNAL_SERVER_ERROR
            end;
        false ->
            CollectionType:delete(Guid)
    end.


-spec initialize_collections(file_id:file_guid()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
initialize_collections(Guid) ->
    call_designated_node(Guid, submit_and_await, [?MODULE, Guid, ?INITIALIZE_COLLECTIONS(Guid)]).


%%%===================================================================
%%% API - space
%%%===================================================================

-spec stop_collecting(od_space:id()) -> ok.
stop_collecting(SpaceId) ->
    % Disabling stats collecting is async as it can take a lot of time
    % (it includes calls to all collector processes).
    spawn(fun() ->
        {Ans, BadNodes} = utils:rpc_multicall(consistent_hashing:get_all_nodes(),
            pes, multi_submit_and_await, [?MODULE, ?STOP_COLLECTING(SpaceId)]),
        FilteredAns = lists:filter(fun(NodeAns) -> NodeAns =/= ok end, lists:flatten(Ans)),

        case {FilteredAns, BadNodes} of
            {[], []} ->
                ok;
            _ ->
                ?error("Dir stats collector ~p error: not ok answers: ~p, bad nodes: ~p",
                    [?FUNCTION_NAME, FilteredAns, BadNodes])
        end,

        dir_stats_collector_config:report_collectors_stopped(SpaceId)
    end),
    ok.


%%%===================================================================
%%% Callback configuring plug-in
%%%===================================================================

-spec get_root_supervisor_name() -> pes_server_supervisor:name().
get_root_supervisor_name() ->
    ?SUPERVISOR_NAME.


%%%===================================================================
%%% Callbacks used by executor - setting mode.
%%%===================================================================

-spec get_mode() -> pes:mode().
get_mode() ->
    async.


%%%===================================================================
%%% Callbacks used by executor - initialization and termination
%%%===================================================================

-spec init() -> state().
init() ->
    #state{}.


-spec graceful_terminate(state()) -> {ok | defer, state()}.
graceful_terminate(#state{has_unflushed_changes = false} = State) ->
    {ok, State};

graceful_terminate(State) ->
    UpdatedState = flush_all(State),
    case UpdatedState#state.has_unflushed_changes of
        false -> {ok, UpdatedState};
        true -> {defer, UpdatedState}
    end.


-spec forced_terminate(pes:forced_termination_reason(), state()) -> ok.
forced_terminate(Reason, #state{has_unflushed_changes = false}) ->
    ?warning("Dir stats collector forced terminate, reason: ~p", [Reason]);

forced_terminate(Reason, State) ->
    UpdatedState = flush_all(State),
    case UpdatedState#state.has_unflushed_changes of
        false ->
            ?error("Dir stats collector emergency flush as a result of forced terminate, terminate reason: ~p",
                [Reason]);
        true ->
            NotFlushed = maps:keys(maps:filter(fun(_Key, CachedDirStats) ->
                get_dir_status(CachedDirStats) =/= flushed
            end, UpdatedState#state.dir_stats_cache)),
            ?critical("Dir stats collector terminate of process with unflushed changes, lost data for: ~p", [NotFlushed])
    end.


%%%===================================================================
%%% Callbacks used by executor - handling requests
%%%===================================================================

-spec handle_call(#dsc_get_request{} | #dsc_flush_request{} |
    ?INITIALIZE_COLLECTIONS(file_id:file_guid()) | ?STOP_COLLECTING(od_space:id()), state()) ->
    {
        ok | {ok, dir_stats_collection:collection()} |
            ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_FORBIDDEN | ?ERROR_NOT_FOUND,
        state()
    }.
handle_call(#dsc_get_request{
    guid = Guid,
    collection_type = CollectionType,
    stat_names = StatNames
}, State) ->
    case reset_last_used_timer(Guid, CollectionType, State) of
        {{ok, #cached_dir_stats{current_stats = CurrentStats, collecting_status = Status}}, UpdatedState} ->
            case Status of
                enabled -> {dir_stats_collection:with(StatNames, CurrentStats), UpdatedState};
                collections_initialization -> {?ERROR_FORBIDDEN, State}
            end;
        {?ERROR_FORBIDDEN, _UpdatedState} = Error ->
            Error
    end;


handle_call(#dsc_flush_request{guid = Guid, collection_type = CollectionType, pruning_strategy = PruningStrategy}, State) ->
    case flush_cached_dir_stats(gen_cached_dir_stats_key(Guid, CollectionType), PruningStrategy, State) of
        {flushed, UpdatedState} -> {ok, UpdatedState};
        {unflushed, UpdatedState} -> {?ERROR_INTERNAL_SERVER_ERROR, UpdatedState};
        {collections_initialization, UpdatedState} -> {?ERROR_FORBIDDEN, UpdatedState}
    end;

handle_call(?INITIALIZE_COLLECTIONS(Guid), State) ->
    {InitializationDataMap, UpdatedState} = lists:foldl(fun(CollectionType, {InitializationDataMapAcc, StateAcc}) ->
        case reset_last_used_timer(Guid, CollectionType, StateAcc) of
            {{ok, #cached_dir_stats{
                collecting_status = collections_initialization,
                initialization_data = InitializationData
            }}, UpdatedStateAcc} ->
                {InitializationDataMapAcc#{CollectionType => InitializationData}, UpdatedStateAcc};
            {_, UpdatedStateAcc} ->
                {InitializationDataMapAcc, UpdatedStateAcc}
        end
    end, {#{}, State}, dir_stats_collection:list_types()),

    case maps:size(InitializationDataMap) of
        0 ->
            {ok, UpdatedState};
        _ ->
            FinalState = initialize_collections(Guid, InitializationDataMap, UpdatedState),
            {ok, ensure_flush_scheduled(FinalState#state{has_unflushed_changes = true})}
    end;

handle_call(?STOP_COLLECTING(SpaceId), #state{
    dir_stats_cache = DirStatsCache,
    space_collecting_statuses = SpaceCollectingStatuses
} = State) ->
    UpdatedState = lists:foldl(fun({Guid, _} = CachedDirStatsKey, StateAcc) ->
        case file_id:guid_to_space_id(Guid) of
            SpaceId -> prune_cached_dir_stats(CachedDirStatsKey, StateAcc);
            _ -> StateAcc
        end
    end, State, maps:keys(DirStatsCache)),

    {ok, UpdatedState#state{space_collecting_statuses = maps:remove(SpaceId, SpaceCollectingStatuses)}};

handle_call(Request, State) ->
    ?log_bad_request(Request),
    {ok, State}.


-spec handle_cast(#dsc_update_request{} | ?SCHEDULED_FLUSH | ?SCHEDULED_COLLECTIONS_INITIALIZATION, state()) -> state().
handle_cast(#dsc_update_request{
    guid = Guid,
    collection_type = CollectionType,
    collection_update = CollectionUpdate,
    update_type = UpdateType
}, State) ->
    UpdateAns = update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
        update_collection_in_cache(CollectionType, UpdateType, CollectionUpdate, CachedDirStats)
    end, State),

    case UpdateAns of
        {?ERROR_FORBIDDEN, UpdatedState} ->
            UpdatedState;
        {_, UpdatedState} ->
            ensure_flush_scheduled(UpdatedState#state{has_unflushed_changes = true})
    end;

handle_cast(?SCHEDULED_FLUSH, State) ->
    flush_all(State);

handle_cast(?SCHEDULED_COLLECTIONS_INITIALIZATION, State) ->
    verify_space_collecting_statuses(
        initialize_all_cached_collections(State#state{initialization_timer_ref = undefined}));

handle_cast(Info, State) ->
    ?log_bad_request(Info),
    State.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update_stats_of_dir(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection(),
    update_type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir(Guid, CollectionType, CollectionUpdate, UpdateType) ->
    Request = #dsc_update_request{
        guid = Guid,
        collection_type = CollectionType,
        collection_update = CollectionUpdate,
        update_type = UpdateType
    },
    call_designated_node(Guid, acknowledged_cast, [?MODULE, Guid, Request]).


%% @private
-spec update_collection_in_cache(dir_stats_collection:type(), update_type(), dir_stats_collection:collection(),
    cached_dir_stats()) -> cached_dir_stats().
update_collection_in_cache(CollectionType, internal = _UpdateType, CollectionUpdate, #cached_dir_stats{
    collecting_status = collections_initialization,
    initialization_data = InitializationData
} = CachedDirStats) ->
    CachedDirStats#cached_dir_stats{
        initialization_data = dir_stats_collections_initializer:update_stats_from_descendants(
            InitializationData, CollectionType, CollectionUpdate)
    };

update_collection_in_cache(CollectionType, external = UpdateType, CollectionUpdate, #cached_dir_stats{
    collecting_status = collections_initialization,
    initialization_data = InitializationData
} = CachedDirStats) ->
    case dir_stats_collections_initializer:are_stats_ready(InitializationData) of
        true ->
            update_collection_in_cache(
                CollectionType, UpdateType, CollectionUpdate, set_collecting_enabled(CachedDirStats, CollectionType));
        false ->
            CachedDirStats#cached_dir_stats{
                initialization_data = dir_stats_collections_initializer:report_race(InitializationData)
            }
    end;

update_collection_in_cache(CollectionType, _UpdateType, CollectionUpdate, #cached_dir_stats{
    collecting_status = enabled,
    current_stats = CurrentStats,
    stat_updates_acc_for_parent = StatUpdatesAccForParent
} = CachedDirStats) ->
    CachedDirStats#cached_dir_stats{
        current_stats = dir_stats_collection:consolidate(CollectionType, CurrentStats, CollectionUpdate),
        stat_updates_acc_for_parent = dir_stats_collection:consolidate(
            CollectionType, StatUpdatesAccForParent, CollectionUpdate)
    }.



%% @private
-spec request_flush(file_id:file_guid(), dir_stats_collection:type(), PruningStrategy :: pruning_strategy()) ->
    ok | ?ERROR_FORBIDDEN | ?ERROR_INTERNAL_SERVER_ERROR.
request_flush(Guid, CollectionType, PruningStrategy) ->
    Request = #dsc_flush_request{
        guid = Guid,
        collection_type = CollectionType,
        pruning_strategy = PruningStrategy
    },
    case call_designated_node(
        Guid, submit_and_await, [?MODULE, Guid, Request, #{ensure_executor_alive => false}]
    ) of
        ignored -> ok;
        Other -> Other
    end.


%% @private
-spec flush_all(state()) -> state().
flush_all(#state{
    flush_timer_ref = undefined,
    dir_stats_cache = DirStatsCache
} = State) ->
    ensure_flush_scheduled(lists:foldl(fun(CachedDirStatsKey, StateAcc) ->
        {DirStatus, UpdatedStateAcc} = flush_cached_dir_stats(CachedDirStatsKey, prune_inactive, StateAcc),
        UpdatedStateAcc#state{
            has_unflushed_changes = StateAcc#state.has_unflushed_changes orelse DirStatus =/= flushed
        }
    end, State#state{has_unflushed_changes = false}, maps:keys(DirStatsCache)));

flush_all(#state{
    flush_timer_ref = Ref
} = State) ->
    erlang:cancel_timer(Ref),
    flush_all(State#state{flush_timer_ref = undefined}).


%% @private
-spec flush_cached_dir_stats(cached_dir_stats_key(), pruning_strategy(), state()) ->
    {dir_status(), state()}.
flush_cached_dir_stats(CachedDirStatsKey, _, State) when not is_map_key(CachedDirStatsKey, State#state.dir_stats_cache) ->
    {flushed, State}; % Key cannot be found - it has been flushed and pruned
flush_cached_dir_stats({_, CollectionType} = CachedDirStatsKey, PruningStrategy, State) ->
    #cached_dir_stats{initialization_data = InitializationData} = CachedDirStats =
        maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
    case get_dir_status(CachedDirStats) of
        unflushed ->
            {UpdatedCachedDirStats, UpdatedState} =
                save_and_propagate_cached_dir_stats(CachedDirStatsKey, CachedDirStats, State),
            UpdatedStatus = get_dir_status(UpdatedCachedDirStats),
            FinalState = case (UpdatedStatus =:= flushed) and (PruningStrategy =:= prune_flushed) of
                true ->
                    prune_cached_dir_stats(CachedDirStatsKey, UpdatedState);
                false ->
                    update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, UpdatedState)
            end,
            {UpdatedStatus, FinalState};
        flushed ->
            case (PruningStrategy =:= prune_flushed) orelse is_inactive(CachedDirStats) of
                true ->
                    {flushed, prune_cached_dir_stats(CachedDirStatsKey, State)};
                false ->
                    {flushed, State}
            end;
        collections_initialization ->
            case dir_stats_collections_initializer:are_stats_ready(InitializationData) of
                true ->
                    UpdatedState = update_cached_dir_stats(
                        CachedDirStatsKey, set_collecting_enabled(CachedDirStats, CollectionType), State),
                    flush_cached_dir_stats(CachedDirStatsKey, PruningStrategy, UpdatedState);
                false ->
                    {collections_initialization, State}
            end
    end.


%% @private
-spec initialize_all_cached_collections(state()) -> state().
initialize_all_cached_collections(#state{dir_stats_cache = DirStatsCache} = State) ->
    InitializationDataMaps = maps:fold(fun
        ({Guid, CollectionType} = _CachedDirStatsKey, #cached_dir_stats{
            collecting_status = collections_initialization,
            initialization_data = InitializationData
        }, Acc) ->
            InitializationDataMap = maps:get(Guid, Acc, #{}),
            Acc#{Guid => InitializationDataMap#{CollectionType => InitializationData}};
        (_CachedDirStatsKey, _CachedDirStats, Acc) ->
            Acc
    end, #{}, DirStatsCache),

    maps:fold(fun(Guid, InitializationDataMap, StateAcc) ->
        initialize_collections(Guid, InitializationDataMap, StateAcc)
    end, State, InitializationDataMaps).


%% @private
-spec initialize_collections(file_id:file_guid(), dir_stats_collections_initializer:initialization_data_map(), 
    state()) -> state().
initialize_collections(Guid, InitializationDataMap, State) ->
    try
        UpdatedInitializationDataMap = dir_stats_collections_initializer:ensure_dir_initialized(Guid, InitializationDataMap),
        maps:fold(fun(CollectionModule, InitializationData, StateAcc) ->
            CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionModule),
            CachedDirStats = maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
            UpdatedCachedDirStats = CachedDirStats#cached_dir_stats{
                initialization_data = InitializationData
            },
            update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, StateAcc)
        end, State, UpdatedInitializationDataMap)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Dir stats collector ~p error for ~p: ~p:~p",
                [?FUNCTION_NAME, Guid, Error, Reason], Stacktrace),
            State
    end.


-spec verify_space_collecting_statuses(state()) -> state().
verify_space_collecting_statuses(#state{
    space_collecting_statuses = SpaceCollectingStatuses,
    dir_stats_cache = DirStatsCache
} = State) ->
    IsAnyInitializingMap = maps:fold(fun({Guid, _}, #cached_dir_stats{collecting_status = Status}, Acc) ->
        SpaceId = file_id:guid_to_space_id(Guid),
        IsAnyInitializingInSpace = maps:get(SpaceId, Acc, false),
        Acc#{SpaceId => IsAnyInitializingInSpace orelse Status =:= collections_initialization}
    end, #{}, DirStatsCache),

    UpdatedSpaceCollectingStatuses = maps:fold(fun
        (SpaceId, collections_initialization, Acc) ->
            IsAnyInitializing = maps:get(SpaceId, IsAnyInitializingMap, false),
            case
                (not IsAnyInitializing) andalso
                dir_stats_collector_config:get_extended_collecting_status(SpaceId) =:= enabled
            of
                true -> Acc#{SpaceId => enabled};
                false -> Acc
            end;
        (_SpaceId, _SpaceCollectingStatus, Acc) ->
            Acc
    end, SpaceCollectingStatuses, SpaceCollectingStatuses),
    UpdatedState = State#state{space_collecting_statuses = UpdatedSpaceCollectingStatuses},

    AreAllEnabled = lists:all(fun(SpaceCollectingStatus) ->
        SpaceCollectingStatus =:= enabled
    end, maps:values(UpdatedSpaceCollectingStatuses)),
    case AreAllEnabled of
        true -> UpdatedState;
        false -> ensure_initialization_scheduled(UpdatedState)
    end.


%% @private
-spec get_dir_status(cached_dir_stats()) -> dir_status().
get_dir_status(#cached_dir_stats{
    collecting_status = enabled,
    stat_updates_acc_for_parent = StatUpdatesAccForParent
}) ->
    case maps:size(StatUpdatesAccForParent) of
        0 -> flushed;
        _ -> unflushed
    end;
get_dir_status(#cached_dir_stats{collecting_status = collections_initialization}) ->
    collections_initialization.


%% @private
-spec set_collecting_enabled(cached_dir_stats(), dir_stats_collection:type()) -> cached_dir_stats().
set_collecting_enabled(#cached_dir_stats{initialization_data = InitializationData} = CachedDirStats, CollectionType) ->
    CurrentStats = dir_stats_collections_initializer:get_stats(InitializationData, CollectionType),
    CachedDirStats#cached_dir_stats{
        current_stats = CurrentStats,
        stat_updates_acc_for_parent = CurrentStats,
        collecting_status = enabled,
        initialization_data = undefined
    }.


%% @private
-spec save_and_propagate_cached_dir_stats(cached_dir_stats_key(), cached_dir_stats(), state()) ->
    {UpdatedCachedDirStats :: cached_dir_stats(), state()}.
save_and_propagate_cached_dir_stats({Guid, CollectionType} = _CachedDirStatsKey,
    #cached_dir_stats{current_stats = CurrentStats} = CachedDirStats, State
) ->
    {CollectingStatus, State2} = acquire_space_collecting_status(file_id:guid_to_space_id(Guid), State),
    try
        case CollectingStatus of
            {collections_initialization, InitializationTraverseNum} ->
                CollectionType:save(Guid, CurrentStats, InitializationTraverseNum);
            _ ->
                CollectionType:save(Guid, CurrentStats, undefined)
        end,
        {propagate_to_parent(Guid, CollectionType, CachedDirStats), State2}
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Dir stats collector save and propagate error for collection type: ~p and guid ~p: ~p:~p",
                [CollectionType, Guid, Error, Reason], Stacktrace),
            {CachedDirStats, State2}
    end.


%% @private
-spec is_inactive(cached_dir_stats()) -> boolean().
is_inactive(#cached_dir_stats{last_used = LastUsed}) ->
    stopwatch:read_millis(LastUsed) >= ?CACHED_DIR_STATS_INACTIVITY_PERIOD.


%% @private
-spec ensure_flush_scheduled(state()) -> state().
ensure_flush_scheduled(State = #state{flush_timer_ref = undefined, has_unflushed_changes = true}) ->
    State#state{flush_timer_ref = pes:self_cast_after(?SCHEDULED_FLUSH, ?FLUSH_INTERVAL_MILLIS)};

ensure_flush_scheduled(State) ->
    State.


%% @private
-spec ensure_initialization_scheduled(state()) -> state().
ensure_initialization_scheduled(State = #state{initialization_timer_ref = undefined}) ->
    State#state{initialization_timer_ref = pes:self_cast_after(
        ?SCHEDULED_COLLECTIONS_INITIALIZATION, ?SCHEDULED_COLLECTIONS_INITIALIZATION_INTERVAL_MILLIS)};

ensure_initialization_scheduled(State) ->
    State.


%% @private
-spec reset_last_used_timer(file_id:file_guid(), dir_stats_collection:type(), state()) ->
    {{ok, UpdatedCachedDirStats :: cached_dir_stats()} | ?ERROR_FORBIDDEN, state()} | no_return().
reset_last_used_timer(Guid, CollectionType, State) ->
    update_in_cache(Guid, CollectionType, fun(CachedDirStats) -> CachedDirStats end, State).


%% @private
-spec update_in_cache(file_id:file_guid(), dir_stats_collection:type(),
    fun((cached_dir_stats()) -> cached_dir_stats()), state()) ->
    {{ok, UpdatedCachedDirStats :: cached_dir_stats()} | ?ERROR_FORBIDDEN, state()} | no_return().
update_in_cache(Guid, CollectionType, Diff, #state{dir_stats_cache = DirStatsCache} = State) ->
    CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
    FindAns = case maps:find(CachedDirStatsKey, DirStatsCache) of
        {ok, DirStatsFromCache} ->
            {DirStatsFromCache, State};
        error ->
            {Stats, InitializationTraverseNum} = CollectionType:acquire(Guid),
            case acquire_space_collecting_status(file_id:guid_to_space_id(Guid), State) of
                {enabled, State2} ->
                    {#cached_dir_stats{
                        collecting_status = enabled,
                        current_stats = Stats
                    }, State2};
                {{collections_initialization, InitializationTraverseNum}, State2} ->
                    {#cached_dir_stats{
                        collecting_status = enabled,
                        current_stats = Stats
                    }, State2};
                {{collections_initialization, _}, State2} ->
                    {
                        #cached_dir_stats{
                            collecting_status = collections_initialization,
                            initialization_data = dir_stats_collections_initializer:new_initialization_data()
                        },
                        ensure_initialization_scheduled(State2)
                    };
                {_, State2} ->
                    {?ERROR_FORBIDDEN, State2}
            end
    end,

    case FindAns of
        {?ERROR_FORBIDDEN, _UpdatedState} ->
            FindAns;
        {CachedDirStats, UpdatedState} ->
            UpdatedCachedDirStats = Diff(CachedDirStats#cached_dir_stats{last_used = stopwatch:start()}),
            {
                {ok, UpdatedCachedDirStats},
                update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, UpdatedState)
            }
    end.


%% @private
-spec prune_cached_dir_stats(cached_dir_stats_key(), state()) -> state().
prune_cached_dir_stats(CachedDirStatsKey, #state{dir_stats_cache = DirStatsCache} = State) ->
    State#state{dir_stats_cache = maps:remove(CachedDirStatsKey, DirStatsCache)}.


%% @private
-spec update_cached_dir_stats(cached_dir_stats_key(), cached_dir_stats(), state()) -> state().
update_cached_dir_stats(CachedDirStatsKey, CachedDirStats, #state{dir_stats_cache = DirStatsCache} = State) ->
    State#state{dir_stats_cache = maps:put(CachedDirStatsKey, CachedDirStats, DirStatsCache)}.


%% @private
-spec update_stats_of_parent_internal(file_id:file_guid() | root_dir, dir_stats_collection:type(),
    dir_stats_collection:collection()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent_internal(root_dir = _ParentGuid, _CollectionType, _CollectionUpdate) ->
    ok;
update_stats_of_parent_internal(ParentGuid, CollectionType, CollectionUpdate) ->
    update_stats_of_dir(ParentGuid, CollectionType, CollectionUpdate, external).


%% @private
-spec propagate_to_parent(file_id:file_guid(), dir_stats_collection:type(), cached_dir_stats()) -> cached_dir_stats().
propagate_to_parent(Guid, CollectionType, #cached_dir_stats{
    stat_updates_acc_for_parent = StatUpdatesAccForParent
} = CachedDirStats) ->
    {Parent, UpdatedCachedDirStats} = acquire_parent(Guid, CachedDirStats),

    Result = case Parent of
        root_dir -> ok;
        not_found -> add_hook_for_missing_doc(Guid, CollectionType, StatUpdatesAccForParent);
        _ -> update_stats_of_dir(Parent, CollectionType, StatUpdatesAccForParent, internal)
    end,

    case Result of
        ok ->
            UpdatedCachedDirStats#cached_dir_stats{stat_updates_acc_for_parent = #{}};
        {error, _} = Error ->
            ?error("Dir stats collector ~p error for collection type: ~p and guid ~p (parent ~p): ~p",
                [?FUNCTION_NAME, CollectionType, Guid, Parent, Error]),
            UpdatedCachedDirStats
    end.


%% @private
-spec acquire_parent(file_id:file_guid(), cached_dir_stats()) ->
    {file_id:file_guid() | root_dir | not_found, cached_dir_stats()}.
acquire_parent(Guid, #cached_dir_stats{
    parent = undefined
} = CachedDirStats) ->
    case get_parent(Guid) of
        {ok, ParentGuidToCache} -> {ParentGuidToCache, CachedDirStats#cached_dir_stats{parent = ParentGuidToCache}};
        ?ERROR_NOT_FOUND -> {not_found, CachedDirStats}
    end;

acquire_parent(_Guid, #cached_dir_stats{
    parent = CachedParent
} = CachedDirStats) ->
    {CachedParent, CachedDirStats}.


%% @private
-spec get_parent(file_id:file_guid()) -> {ok, file_id:file_guid() | root_dir} | ?ERROR_NOT_FOUND.
get_parent(Guid) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    case file_meta:get_including_deleted(FileUuid) of
        {ok, Doc} -> {ok, get_parent(Doc, SpaceId)};
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND
    end.


%% @private
-spec get_parent(file_meta:doc(), od_space:id()) -> file_id:file_guid() | root_dir.
get_parent(Doc, SpaceId) ->
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
    case fslogic_uuid:is_root_dir_uuid(ParentUuid) of
        true -> root_dir;
        false -> file_id:pack_guid(ParentUuid, SpaceId)
    end.


%% @private
-spec acquire_space_collecting_status(od_space:id(), state()) ->
    {dir_stats_collector_config:extended_collecting_status(), state()}.
acquire_space_collecting_status(SpaceId, #state{space_collecting_statuses = CollectingStatuses} = State) ->
    case maps:get(SpaceId, CollectingStatuses, undefined) of
        undefined ->
            case dir_stats_collector_config:get_extended_collecting_status(SpaceId) of
                enabled ->
                    {enabled, State#state{space_collecting_statuses = CollectingStatuses#{SpaceId => enabled}}};
                {collections_initialization, _} = Status ->
                    {Status, State#state{space_collecting_statuses = CollectingStatuses#{SpaceId => Status}}};
                Status ->
                    {Status, State} % race with status changing - do not cache
            end;
        CachedStatus ->
            {CachedStatus, State}
    end.


%% @private
-spec add_hook_for_missing_doc(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook_for_missing_doc(Guid, CollectionType, CollectionUpdate) ->
    Uuid = file_id:guid_to_uuid(Guid),
    file_meta_posthooks:add_hook(Uuid, generator:gen_name(),
        ?MODULE, update_stats_of_parent, [Guid, CollectionType, CollectionUpdate, return_error]).


%% @private
-spec call_designated_node(file_id:file_guid(), submit_and_await | acknowledged_cast, list()) ->
    ok | {ok, dir_stats_collection:collection()} | ignored | ?ERROR_FORBIDDEN | ?ERROR_INTERNAL_SERVER_ERROR.
call_designated_node(Guid, Function, Args) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    case erpc:call(Node, pes, Function, Args) of
        ?ERROR_FORBIDDEN -> ?ERROR_FORBIDDEN;
        {error, _} = Error ->
            ?error("Dir stats collector PES fun ~p error: ~p for guid ~p", [Function, Error, Guid]),
            ?ERROR_INTERNAL_SERVER_ERROR;
        Other ->
            Other
    end.


%% @private
-spec gen_cached_dir_stats_key(file_id:file_guid(), dir_stats_collection:type()) -> cached_dir_stats_key().
gen_cached_dir_stats_key(Guid, CollectionType) ->
    {Guid, CollectionType}.