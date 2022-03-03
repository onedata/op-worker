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
%%% If the executor has no statistics left in cache, it terminates after the idle timeout.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_stats/3,
    update_stats_of_dir/3, update_stats_of_parent/3, update_stats_of_parent/4, update_stats_of_nearest_dir/3,
    flush_stats/2, delete_stats/2,
    enable_stats_collecting/1, disable_stats_collecting/1]).


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

    % Space status is initialized on first dsc_update_request for any dir in space
    space_collecting_statuses = #{file_id:space_id() => dir_stats_collector_config:active_status()}
}).

-record(cached_dir_stats, {
    % current statistics for the {guid, type} pair, i. e. previous stats acquired from db
    % with all the updates (that were reported during executor's lifecycle) applied
    current_stats :: dir_stats_collection:collection(),
    % accumulates all the updates that were reported since the last propagation to parent
    stat_updates_acc_for_parent = #{} :: dir_stats_collection:collection(),
    last_used :: stopwatch:instance() | undefined,
    parent :: file_id:file_guid() | root_dir | undefined, % resolved and stored upon first access to this field,
    collecting_status :: active | initializing,
    initialization_data :: dir_stats_initializer:initialization_data() | undefined

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


-type update_type() :: internal | external.
-type state() :: #state{}.

-type cached_dir_stats_key() :: {file_id:file_guid(), dir_stats_collection:type()}.
-type cached_dir_stats() :: #cached_dir_stats{}.

-type pruning_strategy() :: prune_flushed | prune_inactive.


-define(FLUSH_INTERVAL_MILLIS, 5000).
-define(CACHED_DIR_STATS_INACTIVITY_PERIOD, 10000). % stats that are already flushed and not used for this period
                                                    % are removed from the cache
-define(SCHEDULED_STATS_INITIALIZATION_INTERVAL_MILLIS, 2000). % stats initialization is triggered not more often than
                                                               % this time to decrease number of initialization/update
                                                               % races when several updates of dir appear in short
                                                               % period of time
-define(SUPERVISOR_NAME, dir_stats_collector_worker_sup).

-define(SCHEDULED_FLUSH, scheduled_flush).
-define(SCHEDULED_STATS_INITIALIZATION, scheduled_stats_initialization).

-define(INITIALIZE(Guid), {initialize, Guid}).
-define(DISABLE(SpaceId), {disable, SpaceId}).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_stats(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:stats_selector()) ->
    {ok, dir_stats_collection:collection()} |
    ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN.
get_stats(Guid, CollectionType, StatNames) ->
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true ->
            Request = #dsc_get_request{
                guid = Guid,
                collection_type = CollectionType,
                stat_names = StatNames
            },
            call_designated_node(Guid, submit_and_await, [?MODULE, Guid, Request]);
        false ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec update_stats_of_dir(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir(Guid, CollectionType, CollectionUpdate) ->
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
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
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
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
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
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


-spec flush_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
flush_stats(Guid, CollectionType) ->
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true -> request_flush(Guid, CollectionType, prune_inactive);
        false -> ok
    end.


-spec delete_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
delete_stats(Guid, CollectionType) ->
    % TODO VFS-8837 - delete only for directories
    % TODO VFS-8837 - delete collection when collecting was enabled in past
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true ->
            case request_flush(Guid, CollectionType, prune_flushed) of
                ok -> CollectionType:delete(Guid);
                ?ERROR_INTERNAL_SERVER_ERROR -> ?ERROR_INTERNAL_SERVER_ERROR
            end;
        false ->
            ok
    end.


-spec enable_stats_collecting(file_id:file_guid()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
enable_stats_collecting(Guid) ->
    call_designated_node(Guid, submit_and_await, [?MODULE, Guid, ?INITIALIZE(Guid)]).


-spec disable_stats_collecting(file_id:space_id()) -> ok.
disable_stats_collecting(SpaceId) ->
    spawn(fun() ->
        {Ans, BadNodes} = utils:rpc_multicall(consistent_hashing:get_all_nodes(),
            pes, multi_submit_and_await, [?MODULE, ?DISABLE(SpaceId)]),
        FilteredAns = lists:filter(fun(NodeAns) -> NodeAns =/= ok end, Ans),

        case {FilteredAns, BadNodes} of
            {[], []} ->
                ok;
            _ ->
                ?error("Dir stats collector ~p error: not ok answers: ~p, bad nodes: ~p",
                    [?FUNCTION_NAME, FilteredAns, BadNodes])
        end,

        dir_stats_collector_config:report_disabling_finished(SpaceId)
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
                get_collection_status(CachedDirStats) =/= flushed
            end, UpdatedState#state.dir_stats_cache)),
            ?critical("Dir stats collector terminate of process with unflushed changes, lost data for: ~p", [NotFlushed])
    end.


%%%===================================================================
%%% Callbacks used by executor - handling requests
%%%===================================================================

-spec handle_call(#dsc_get_request{} | #dsc_flush_request{}, state()) ->
    {ok | {ok, dir_stats_collection:collection()} | ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_FORBIDDEN, state()}.
handle_call(#dsc_get_request{
    guid = Guid,
    collection_type = CollectionType,
    stat_names = StatNames
} = Request, #state{space_collecting_statuses = SpaceCollectingStatuses} = State) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case maps:get(SpaceId, SpaceCollectingStatuses, undefined) of
        undefined ->
            % TODO - czy mozemy jakos optymalizowac to (pobieramy stan zanim wyslemy wiadomosc)
            case dir_stats_collector_config:get_status_for_space(SpaceId) of
                disabled ->
                    {?ERROR_FORBIDDEN, State};
                Status ->
                    handle_call(Request, State#state{
                        space_collecting_statuses = SpaceCollectingStatuses#{SpaceId => Status}
                    })
            end;
        enabled ->
            {#cached_dir_stats{current_stats = CurrentStats}, UpdatedState} =
                reset_last_used_timer(Guid, CollectionType, acquire, State),
            {{ok, dir_stats_collection:with(StatNames, CurrentStats)}, UpdatedState};
        _ ->
            {?ERROR_FORBIDDEN, State}
    end;


handle_call(#dsc_flush_request{guid = Guid, collection_type = CollectionType, pruning_strategy = PruningStrategy}, State) ->
    case flush_cached_dir_stats(gen_cached_dir_stats_key(Guid, CollectionType), PruningStrategy, State) of
        {true, UpdatedState} -> {ok, UpdatedState};
        {false, UpdatedState} -> {?ERROR_INTERNAL_SERVER_ERROR, UpdatedState}
    end;

handle_call(?INITIALIZE(Guid), State) ->
    UpdatedState = lists:foldl(fun(CollectionType, StateAcc) ->
        reset_last_used_timer(Guid, CollectionType, prepare_initialization_data, StateAcc)
    end, State, dir_stats_collection:list_types()),

    {ok, initialize_dir_stats(UpdatedState)};

handle_call(?DISABLE(SpaceId), #state{dir_stats_cache = DirStatsCache} = State) ->
    UpdatedState = lists:foldl(fun({Guid, _} = CachedDirStatsKey, StateAcc) ->
        case file_id:guid_to_space_id(Guid) of
            SpaceId -> prune_cached_dir_stats(CachedDirStatsKey, StateAcc);
            _ -> StateAcc
        end
    end, State, maps:keys(DirStatsCache)),

    {ok, UpdatedState};

handle_call(Request, State) ->
    ?log_bad_request(Request),
    {ok, State}.


-spec handle_cast(#dsc_update_request{} | ?SCHEDULED_FLUSH, state()) -> state().
handle_cast(#dsc_update_request{
    guid = Guid,
    collection_type = CollectionType,
    collection_update = CollectionUpdate
} = Request, #state{space_collecting_statuses = SpaceCollectingStatuses} = State) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case maps:get(SpaceId, SpaceCollectingStatuses, undefined) of
        undefined ->
            case dir_stats_collector_config:get_status_for_space(SpaceId) of
                disabled ->
                    State;
                Status ->
                    handle_cast(Request, State#state{
                        space_collecting_statuses = SpaceCollectingStatuses#{SpaceId => Status}
                    })
            end;
        enabled ->
            {_, UpdatedState} = update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
                update_collection_in_cache(CollectionType, CollectionUpdate, CachedDirStats)
            end, acquire, State),

            ensure_flush_scheduled(UpdatedState#state{has_unflushed_changes = true});
        initializing ->
            handle_update_request_during_initialization(Request, State)
    end;

handle_cast(?SCHEDULED_FLUSH, #state{space_collecting_statuses = SpaceCollectingStatuses} = State) ->
    UpdatedState = flush_all(State#state{flush_timer_ref = undefined}),

    % TODO - moze to przeniesc do procedury inicjalizacji? tylko trzeba pamietac ze musi ona wystepowac w kolko tak dlugo jak jakis space ma stan initializing
    maps:fold(fun
        (SpaceId, initializing, StateAcc) ->
            case dir_stats_collector_config:get_status_for_space(SpaceId) of
                enabled -> StateAcc#state{space_collecting_statuses = SpaceCollectingStatuses#{SpaceId => enabled}};
                _ -> StateAcc
            end;
        (_SpaceId, _SpaceCollectingStatus, StateAcc) ->
            StateAcc
    end, UpdatedState, SpaceCollectingStatuses);

handle_cast(?SCHEDULED_STATS_INITIALIZATION, State) ->
    initialize_dir_stats(State);

handle_cast(Info, State) ->
    ?log_bad_request(Info),
    State.


set_collecting_active(#cached_dir_stats{initialization_data = InitializationData} = CachedDirStats, CollectionType) ->
    CurrentStats = dir_stats_initializer:get_stats(InitializationData, CollectionType),
    CachedDirStats#cached_dir_stats{
        current_stats = CurrentStats,
        stat_updates_acc_for_parent = CurrentStats,
        collecting_status = active,
        initialization_data = undefined
    }.


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


handle_update_request_during_initialization(#dsc_update_request{
    collection_update = internal,
    guid = Guid,
    collection_type = CollectionType,
    collection_update = CollectionUpdate
}, State) ->
    {_, UpdatedState} = update_in_cache(Guid, CollectionType, fun
        (#cached_dir_stats{collecting_status = active} = CachedDirStats) ->
            update_collection_in_cache(CollectionType, CollectionUpdate, CachedDirStats);
        (#cached_dir_stats{collecting_status = initializing, initialization_data = InitializationData} = CachedDirStats) ->
            CachedDirStats#cached_dir_stats{
                current_stats = dir_stats_initializer:update_stats_from_descendants(
                    InitializationData, CollectionType, CollectionUpdate)
            }
    end, prepare_initialization_data, State),

    ensure_flush_scheduled(UpdatedState#state{has_unflushed_changes = true});

handle_update_request_during_initialization(#dsc_update_request{
    collection_update = external,
    guid = Guid,
    collection_type = CollectionType,
    collection_update = CollectionUpdate
}, State) ->
    {#cached_dir_stats{collecting_status = UpdatedStatus, initialization_data = UpdatedInitializationData}, UpdatedState} =
        update_in_cache(Guid, CollectionType, fun
            (#cached_dir_stats{collecting_status = active} = CachedDirStats) ->
                update_collection_in_cache(CollectionType, CollectionUpdate, CachedDirStats);
            (#cached_dir_stats{
                collecting_status = initializing, initialization_data = InitializationData
            } = CachedDirStats) ->
                case dir_stats_initializer:are_stats_ready(InitializationData) of
                    true ->
                        update_collection_in_cache(CollectionType, CollectionUpdate, set_collecting_active(CachedDirStats));
                    false ->
                        CachedDirStats#cached_dir_stats{
                            current_stats = dir_stats_initializer:report_race(InitializationData)
                        }
                end
        end, prepare_initialization_data, State),

    FinalState = case
        UpdatedStatus =:= initializing andalso dir_stats_initializer:is_race_reported(UpdatedInitializationData)
    of
        true -> ensure_initialization_scheduled(UpdatedState);
        false -> UpdatedState
    end,
    ensure_flush_scheduled(FinalState#state{has_unflushed_changes = true}).


%% @private
-spec update_collection_in_cache(dir_stats_collection:type(), dir_stats_collection:collection(), cached_dir_stats()) ->
    cached_dir_stats().
update_collection_in_cache(CollectionType, CollectionUpdate, #cached_dir_stats{
    current_stats = CurrentStats, stat_updates_acc_for_parent = StatUpdatesAccForParent
} = CachedDirStats) ->
    CachedDirStats#cached_dir_stats{
        current_stats = dir_stats_collection:consolidate(CollectionType, CurrentStats, CollectionUpdate),
        stat_updates_acc_for_parent = dir_stats_collection:consolidate(
            CollectionType, StatUpdatesAccForParent, CollectionUpdate)
    }.



%% @private
-spec request_flush(file_id:file_guid(), dir_stats_collection:type(), PruningStrategy :: pruning_strategy()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
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
        {HasSucceeded, UpdatedStateAcc} = flush_cached_dir_stats(CachedDirStatsKey, prune_inactive, StateAcc),
        UpdatedStateAcc#state{
            has_unflushed_changes = StateAcc#state.has_unflushed_changes orelse not HasSucceeded
        }
    end, State#state{has_unflushed_changes = false}, maps:keys(DirStatsCache)));

flush_all(#state{
    flush_timer_ref = Ref
} = State) ->
    erlang:cancel_timer(Ref),
    flush_all(State#state{flush_timer_ref = undefined}).


%% @private
-spec flush_cached_dir_stats(cached_dir_stats_key(), pruning_strategy(), state()) ->
    {HasSucceeded :: boolean(), state()}.
flush_cached_dir_stats(CachedDirStatsKey, _, State) when not is_map_key(CachedDirStatsKey, State#state.dir_stats_cache) ->
    {true, State};
flush_cached_dir_stats({_, CollectionType} = CachedDirStatsKey, PruningStrategy, State) ->
    #cached_dir_stats{initialization_data = InitializationData} = CachedDirStats =
        maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
    case get_collection_status(CachedDirStats) of
        unflushed ->
            UpdatedCachedDirStats = save_and_propagate_cached_dir_stats(CachedDirStatsKey, CachedDirStats),
            SuccessfullyFlushed = get_collection_status(UpdatedCachedDirStats) =:= flushed,
            case SuccessfullyFlushed and (PruningStrategy =:= prune_flushed) of
                true ->
                    {SuccessfullyFlushed, prune_cached_dir_stats(CachedDirStatsKey, State)};
                false ->
                    {SuccessfullyFlushed, update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, State)}
            end;
        flushed ->
            case (PruningStrategy =:= prune_flushed) orelse is_inactive(CachedDirStats) of
                true ->
                    {true, prune_cached_dir_stats(CachedDirStatsKey, State)};
                false ->
                    {true, State}
            end;
        initializing ->
            case dir_stats_initializer:are_stats_ready(InitializationData) of
                true ->
                    UpdatedState = update_cached_dir_stats(
                        CachedDirStatsKey, set_collecting_active(CachedDirStats, CollectionType), State),
                    flush_cached_dir_stats(CachedDirStatsKey, PruningStrategy, UpdatedState);
                false ->
                    {false, State}
            end
    end.


%% @private
-spec initialize_dir_stats(state()) -> state().
initialize_dir_stats(#state{dir_stats_cache = DirStatsCache} = State) ->
    InitializationDataMaps = maps:fold(fun
        ({Guid, CollectionType} = _CachedDirStatsKey, #cached_dir_stats{
            collecting_status = initializing,
            initialization_data = InitializationData
        }, Acc) ->
            InitializationDataMap = maps:get(Guid, Acc, #{}),
            Acc#{Guid => InitializationDataMap#{CollectionType => InitializationData}};
        (_CachedDirStatsKey, _CachedDirStats, Acc) ->
            Acc
    end, #{}, DirStatsCache),

    maps:foldl(fun(Guid, InitializationDataMap, StateAcc) ->
        try
            UpdatedInitializationDataMap = dir_stats_initializer:ensure_dir_initialized(Guid, InitializationDataMap),
            maps:foldl(fun(CollectionModule, InitializationData, InternalStateAcc) ->
                CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionModule),
                CachedDirStats = maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
                UpdatedCachedDirStats = CachedDirStats#cached_dir_stats{
                    initialization_data = InitializationData
                },
                update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, InternalStateAcc)
            end, StateAcc, UpdatedInitializationDataMap)
        catch
            Error:Reason:Stacktrace ->
                ?error_stacktrace("Dir stats collector ~p error for ~p: ~p:~p",
                    [?FUNCTION_NAME, Guid, Error, Reason], Stacktrace),
                ensure_initialization_scheduled(StateAcc)
        end
    end, State, InitializationDataMaps).


%% @private
-spec get_collection_status(cached_dir_stats()) -> flushed | unflushed | initializing.
get_collection_status(#cached_dir_stats{
    collecting_status = active,
    stat_updates_acc_for_parent = StatUpdatesAccForParent
}) ->
    case maps:size(StatUpdatesAccForParent) of
        0 -> flushed;
        _ -> unflushed
    end;
get_collection_status(#cached_dir_stats{collecting_status = initializing}) ->
    initializing.


%% @private
-spec save_and_propagate_cached_dir_stats(cached_dir_stats_key(), cached_dir_stats()) -> UpdatedCachedDirStats :: cached_dir_stats().
save_and_propagate_cached_dir_stats({Guid, CollectionType} = _CachedDirStatsKey,
    #cached_dir_stats{current_stats = CurrentStats} = CachedDirStats) ->
    try
        CollectionType:save(Guid, CurrentStats),
        propagate_to_parent(Guid, CollectionType, CachedDirStats)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Dir stats collector save and propagate error for collection type: ~p and guid ~p: ~p:~p",
                [CollectionType, Guid, Error, Reason], Stacktrace),
            CachedDirStats
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
        ?SCHEDULED_STATS_INITIALIZATION_INTERVAL_MILLIS, ?SCHEDULED_STATS_INITIALIZATION)};

ensure_initialization_scheduled(State) ->
    State.


%% @private
-spec reset_last_used_timer(file_id:file_guid(), dir_stats_collection:type(), acquire | prepare_initialization_data,
    state()) -> {cached_dir_stats(), state()} | no_return(). % TODO - dac typ na missing record action
reset_last_used_timer(Guid, CollectionType, MissingRecordAction, State) ->
    update_in_cache(Guid, CollectionType, fun(CachedDirStats) -> CachedDirStats end, MissingRecordAction, State).


%% @private
-spec update_in_cache(file_id:file_guid(), dir_stats_collection:type(),
    fun((cached_dir_stats()) -> cached_dir_stats()), acquire | prepare_initialization_data, state()) ->
    {UpdatedCachedDirStats :: cached_dir_stats(), state()} | no_return().
update_in_cache(Guid, CollectionType, Diff, MissingRecordAction, #state{dir_stats_cache = DirStatsCache} = State) ->
    CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
    CachedDirStats = case {maps:find(CachedDirStatsKey, DirStatsCache), MissingRecordAction} of
        {{ok, DirStatsFromCache}, _} ->
            DirStatsFromCache;
        {error, acquire} ->
            #cached_dir_stats{current_stats = CollectionType:acquire(Guid)};
        _ ->
            #cached_dir_stats{initialization_data = dir_stats_initializer:new_initialization_data()}
    end,

    UpdatedCachedDirStats = Diff(CachedDirStats#cached_dir_stats{last_used = stopwatch:start()}),
    {UpdatedCachedDirStats, update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, State)}.


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
-spec add_hook_for_missing_doc(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook_for_missing_doc(Guid, CollectionType, CollectionUpdate) ->
    Uuid = file_id:guid_to_uuid(Guid),
    file_meta_posthooks:add_hook(Uuid, generator:gen_name(),
        ?MODULE, update_stats_of_parent, [Guid, CollectionType, CollectionUpdate, return_error]).


%% @private
-spec call_designated_node(file_id:file_guid(), submit_and_await | acknowledged_cast, list()) ->
    ok | {ok, dir_stats_collection:collection()} | ignored | ?ERROR_INTERNAL_SERVER_ERROR.
call_designated_node(Guid, Function, Args) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    case erpc:call(Node, pes, Function, Args) of
        {error, _} = Error ->
            ?error("Dir stats collector PES fun ~p error: ~p for Guid", [Function, Error, Guid]),
            ?ERROR_INTERNAL_SERVER_ERROR;
        Other ->
            Other
    end.


%% @private
-spec gen_cached_dir_stats_key(file_id:file_guid(), dir_stats_collection:type()) -> cached_dir_stats_key().
gen_cached_dir_stats_key(Guid, CollectionType) ->
    {Guid, CollectionType}.