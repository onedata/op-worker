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
%%% Flush has two steps: 1) saving statistics in datastore and 2) propagating them
%%% to directory parent. If statistics for a specific dir are not updated or
%%% retrieved for ?CACHED_DIR_STATS_INACTIVITY_PERIOD and have been
%%% successfully flushed, the executor clears them from cache. If the executor
%%% has no statistics left in cache, it terminates after the idle timeout.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_stats/3, update_stats_of_dir/3, update_stats_of_parent/3, update_stats_of_parent/4,
    update_stats_of_nearest_dir/3, flush_stats/2, delete_stats/2]).


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
    flush_timer_ref :: reference() | undefined
}).

-record(cached_dir_stats, {
    % current statistics for the {guid, type} pair, i. e. previous stats acquired from db
    % with all the updates (that were reported during executor's lifecycle) applied
    current_stats :: dir_stats_collection:collection(),
    % accumulates all the updates that were reported since the last propagation to parent
    stat_updates_acc_for_parent = #{} :: dir_stats_collection:collection(),
    last_used :: stopwatch:instance() | undefined,
    parent :: file_id:file_guid() | root_dir | undefined % resolved and stored upon first access to this field
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
    collection_update :: dir_stats_collection:collection()
}).

-record(dsc_flush_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type()
}).

-record(dsc_delete_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type()
}).


-type state() :: #state{}.

-type cached_dir_stats_key() :: {file_id:file_guid(), dir_stats_collection:type()}.
-type cached_dir_stats() :: #cached_dir_stats{}.


-define(FLUSH_INTERVAL_MILLIS, 5000).
-define(CACHED_DIR_STATS_INACTIVITY_PERIOD, 10000). % stats that are already flushed and not used for this period
                                                    % are removed from the cache
-define(SUPERVISOR_NAME, dir_stats_collector_worker_sup).

-define(SCHEDULED_FLUSH, scheduled_flush).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_stats(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:stats_selector()) ->
    {ok, dir_stats_collection:collection()} | ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE.
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
    % TODO VFS-8830 - consider usage of file_ctx() based API instead of guid() based one to optimize parent searching
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true ->
            Request = #dsc_update_request{
                guid = Guid,
                collection_type = CollectionType,
                collection_update = CollectionUpdate
            },
            call_designated_node(Guid, acknowledged_cast, [?MODULE, Guid, Request]);
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
                            update_stats_of_dir(Guid, CollectionType, CollectionUpdate);
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


-spec flush_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR .
flush_stats(Guid, CollectionType) ->
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true ->
            Request = #dsc_flush_request{guid = Guid, collection_type = CollectionType},
            case call_designated_node(
                Guid, submit_and_await, [?MODULE, Guid, Request, #{ensure_executor_alive => false}]
            ) of
                ignored -> ok;
                Other -> Other
            end;
        false ->
            ok
    end.


-spec delete_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
delete_stats(Guid, CollectionType) ->
    % TODO VFS-8837 - delete only for directories
    % TODO VFS-8837 - delete collection when collecting was enabled in past
    case dir_stats_collector_config:is_enabled_for_space(file_id:guid_to_space_id(Guid)) of
        true ->
            Request = #dsc_delete_request{guid = Guid, collection_type = CollectionType},
            case call_designated_node(
                Guid, submit_and_await, [?MODULE, Guid, Request, #{ensure_executor_alive => false}]
            ) of
                ignored -> CollectionType:delete(Guid);
                Other -> Other
            end;
        false ->
            ok
    end.


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
    UpdatedState = flush_internal(State),
    case UpdatedState#state.has_unflushed_changes of
        false -> {ok, UpdatedState};
        true -> {defer, UpdatedState}
    end.


-spec forced_terminate(pes:forced_termination_reason(), state()) -> ok.
forced_terminate(Reason, #state{has_unflushed_changes = false}) ->
    ?warning("Dir stats collector forced terminate, reason: ~p", [Reason]);

forced_terminate(Reason, State) ->
    UpdatedState = flush_internal(State),
    case UpdatedState#state.has_unflushed_changes of
        false ->
            ?error("Dir stats collector emergency flush as a result of forced terminate, terminate reason: ~p",
                [Reason]);
        true ->
            NotFlushed = maps:keys(maps:filter(fun(_Key, CachedDirStats) ->
                has_unflushed_changes(CachedDirStats)
            end, UpdatedState#state.dir_stats_cache)),
            ?critical("Dir stats collector terminate of process with unflushed changes, lost data for: ~p", [NotFlushed])
    end.


%%%===================================================================
%%% Callbacks used by executor - handling requests
%%%===================================================================

-spec handle_call(#dsc_get_request{} | #dsc_flush_request{} | #dsc_delete_request{}, state()) ->
    {ok | {ok, dir_stats_collection:collection()} | ?ERROR_INTERNAL_SERVER_ERROR, state()}.
handle_call(#dsc_get_request{
    guid = Guid,
    collection_type = CollectionType,
    stat_names = StatNames
}, State) ->
    {#cached_dir_stats{current_stats = CurrentStats}, UpdatedState} = reset_last_used_timer(State, Guid, CollectionType),
    {{ok, dir_stats_collection:with(StatNames, CurrentStats)}, UpdatedState};

handle_call(#dsc_flush_request{guid = Guid, collection_type = CollectionType}, State) ->
    CachedDirStatsKey = {Guid, CollectionType},
    case flush_keys([CachedDirStatsKey], false, State) of
        {true, UpdatedState} -> {ok, UpdatedState};
        {false, UpdatedState} -> {?ERROR_INTERNAL_SERVER_ERROR, UpdatedState}
    end;

handle_call(#dsc_delete_request{guid = Guid, collection_type = CollectionType}, State) ->
    % Flush data cached by executor as flush of this data after collection delete would result in recreation of collection
    CachedDirStatsKey = {Guid, CollectionType},
    case flush_keys([CachedDirStatsKey], true, State) of
        {true, UpdatedState} ->
            CollectionType:delete(Guid),
            {ok, UpdatedState};
        {false, UpdatedState} ->
            {?ERROR_INTERNAL_SERVER_ERROR, UpdatedState}
    end;

handle_call(Request, State) ->
    ?log_bad_request(Request),
    {ok, State}.


-spec handle_cast(#dsc_update_request{} | ?SCHEDULED_FLUSH, state()) -> state().
handle_cast(#dsc_update_request{
    guid = Guid,
    collection_type = CollectionType,
    collection_update = CollectionUpdate
}, State) ->
    {_, UpdatedState} = update_in_cache(State, Guid, CollectionType, fun(#cached_dir_stats{
        current_stats = CurrentStats, stat_updates_acc_for_parent = StatUpdatesAccForParent
    } = CachedDirStats) ->
        CachedDirStats#cached_dir_stats{
            current_stats = dir_stats_collection:consolidate(CollectionType, CurrentStats, CollectionUpdate),
            stat_updates_acc_for_parent = dir_stats_collection:consolidate(
                CollectionType, StatUpdatesAccForParent, CollectionUpdate)
        }
    end),
    
    ensure_flush_scheduled(UpdatedState#state{has_unflushed_changes = true});

handle_cast(?SCHEDULED_FLUSH, State) ->
    flush_internal(State#state{flush_timer_ref = undefined});

handle_cast(Info, State) ->
    ?log_bad_request(Info),
    State.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec flush_internal(state()) -> state().
flush_internal(#state{
    flush_timer_ref = undefined,
    dir_stats_cache = DirStatsCache
} = State) ->
    {HaveAllSucceeded, UpdatedState} = flush_keys(maps:keys(DirStatsCache), false, State),

    UpdatedState#state{has_unflushed_changes = not HaveAllSucceeded};

flush_internal(#state{
    flush_timer_ref = Ref
} = State) ->
    erlang:cancel_timer(Ref),
    flush_internal(State#state{flush_timer_ref = undefined}).


%% @private
-spec flush_keys([cached_dir_stats_key()], ForgetIfSuccessfullyFlushed :: boolean(), state()) ->
    {HaveAllSucceeded :: boolean(), state()}.
flush_keys(CachedDirStatsKeysToFlush, ForgetIfSuccessfullyFlushed, #state{dir_stats_cache = DirStatsCache} = State) ->
    {NewDirStatsCache, HaveAllSucceeded} = maps:fold(
        fun(
            CachedDirStatsKey, CachedDirStats, {DirStatsAcc, HasSucceededAcc} = Acc
        ) ->
            case has_unflushed_changes(CachedDirStats) of
                true ->
                    UpdatedCachedDirStats = flush_cached_dir_stats(CachedDirStatsKey, CachedDirStats),
                    KeySuccessfullyFlushed = not has_unflushed_changes(UpdatedCachedDirStats),
                    case KeySuccessfullyFlushed and ForgetIfSuccessfullyFlushed of
                        true ->
                            Acc;
                        false ->
                            {DirStatsAcc#{CachedDirStatsKey => UpdatedCachedDirStats},
                                KeySuccessfullyFlushed and HasSucceededAcc}
                    end;
                false ->
                    case ForgetIfSuccessfullyFlushed orelse should_forget_cached_dir_stats(CachedDirStats) of
                        true -> Acc;
                        false -> {DirStatsAcc#{CachedDirStatsKey => CachedDirStats}, HasSucceededAcc}
                    end
            end
        end,
        {maps:without(CachedDirStatsKeysToFlush, DirStatsCache), true},
        maps:with(CachedDirStatsKeysToFlush, DirStatsCache)
    ),

    {HaveAllSucceeded, State#state{dir_stats_cache = NewDirStatsCache}}.


%% @private
-spec has_unflushed_changes(cached_dir_stats()) -> boolean().
has_unflushed_changes(#cached_dir_stats{stat_updates_acc_for_parent = StatUpdatesAccForParent}) ->
    maps:size(StatUpdatesAccForParent) =/= 0.


%% @private
-spec flush_cached_dir_stats(cached_dir_stats_key(), cached_dir_stats()) -> UpdatedCachedDirStats :: cached_dir_stats().
flush_cached_dir_stats({Guid, CollectionType} = _CachedDirStatsKey,
    #cached_dir_stats{current_stats = CurrentStats} = CachedDirStats) ->
    try
        CollectionType:save(Guid, CurrentStats),
        propagate_to_parent(Guid, CollectionType, CachedDirStats)
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Dir stats collector save error for collection type: ~p and guid ~p: ~p:~p",
                [CollectionType, Guid, Error, Reason], Stacktrace),
            CachedDirStats
    end.


%% @private
-spec should_forget_cached_dir_stats(cached_dir_stats()) -> boolean().
should_forget_cached_dir_stats(#cached_dir_stats{last_used = LastUsed}) ->
    stopwatch:read_millis(LastUsed) >= ?CACHED_DIR_STATS_INACTIVITY_PERIOD.


%% @private
-spec ensure_flush_scheduled(state()) -> state().
ensure_flush_scheduled(State = #state{flush_timer_ref = undefined}) ->
    State#state{flush_timer_ref = pes:self_cast_after(?SCHEDULED_FLUSH, ?FLUSH_INTERVAL_MILLIS)};

ensure_flush_scheduled(State) ->
    State.


%% @private
-spec reset_last_used_timer(state(), file_id:file_guid(), dir_stats_collection:type()) ->
    {cached_dir_stats(), state()} | no_return().
reset_last_used_timer(State, Guid, CollectionType) ->
    update_in_cache(State, Guid, CollectionType, fun(CachedDirStats) -> CachedDirStats end).


%% @private
-spec update_in_cache(state(), file_id:file_guid(), dir_stats_collection:type(), 
    fun((cached_dir_stats()) -> cached_dir_stats())) ->
    {UpdatedCachedDirStats :: cached_dir_stats(), state()} | no_return().
update_in_cache(#state{dir_stats_cache = DirStatsCache} = State, Guid, CollectionType, Diff) ->
    CachedDirStatsKey = {Guid, CollectionType},
    CachedDirStats = case maps:find(CachedDirStatsKey, DirStatsCache) of
        {ok, DirStatsFromCache} ->
            DirStatsFromCache;
        error ->
            #cached_dir_stats{current_stats = CollectionType:acquire(Guid)}
    end,

    UpdatedCachedDirStats = Diff(CachedDirStats#cached_dir_stats{last_used = stopwatch:start()}),
    UpdatedDirStatsCache = DirStatsCache#{CachedDirStatsKey => UpdatedCachedDirStats},
    {UpdatedCachedDirStats, State#state{dir_stats_cache = UpdatedDirStatsCache}}.


%% @private
-spec update_stats_of_parent_internal(file_id:file_guid() | root_dir, dir_stats_collection:type(),
    dir_stats_collection:collection()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent_internal(root_dir = _ParentGuid, _CollectionType, _CollectionUpdate) ->
    ok;
update_stats_of_parent_internal(ParentGuid, CollectionType, CollectionUpdate) ->
    update_stats_of_dir(ParentGuid, CollectionType, CollectionUpdate).


%% @private
-spec propagate_to_parent(file_id:file_guid(), dir_stats_collection:type(), cached_dir_stats()) -> cached_dir_stats().
propagate_to_parent(Guid, CollectionType, #cached_dir_stats{
    stat_updates_acc_for_parent = StatUpdatesAccForParent
} = CachedDirStats) ->
    {Parent, UpdatedCachedDirStats} = acquire_parent(Guid, CachedDirStats),

    Result = case Parent of
        root_dir -> ok;
        not_found -> add_hook_for_missing_doc(Guid, CollectionType, StatUpdatesAccForParent);
        _ -> update_stats_of_dir(Parent, CollectionType, StatUpdatesAccForParent)
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
            ?error("Dir stats collector PES error: ~p", [Error]),
            ?ERROR_INTERNAL_SERVER_ERROR;
        Other ->
            Other
    end.