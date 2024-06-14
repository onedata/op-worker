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
%%% Typically dir_stats_collector is used when collecting is enabled
%%% (see dir_stats_service_state). However, it is also used by
%%% dir_stats_collections_initialization_traverse which initializes
%%% collections for all directories. In such a case, it uses helper module
%%% dir_stats_collections_initializer as collections initialization requires
%%% special treatment (races between initialization and updates can occur).
%%%
%%% NOTE: multiple collections can be initialized together to list directory
%%%       children only once. As a result, some of initialization data must
%%%       be stored per guid instead of per guid/collection_type pair.
%%%
%%% % TODO VFS-9204 Problems with dbsync can result in negative files count when link deletion is synced,
%%%        then stats for dir are initialized and only then deleted file_meta appears
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector).
-author("Michal Wrzeszcz").


-behavior(pes_plugin_behaviour).
-behaviour(file_meta_posthooks_behaviour).


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API - single directory
-export([get_stats/3,
    update_stats_of_dir/3, update_stats_of_dir_without_state_check/3,
    update_stats_of_parent/3, update_stats_of_parent/4, update_stats_of_nearest_dir/3,
    flush_stats/2, delete_stats/2,
    initialize_collections/1,
    report_file_moved/4,
    is_uuid_counted/1]).
%% API - space
-export([stop_collecting/1]).

%% pes_plugin_behaviour callbacks
-export([get_root_supervisor_name/0]).
-export([get_mode/0,
    init/0, graceful_terminate/1, forced_terminate/2,
    handle_call/2, handle_cast/2]).

%% file_meta_posthooks related functions
-export([
    add_missing_file_meta_on_update_posthook/3,
    encode_file_meta_posthook_args/2,
    decode_file_meta_posthook_args/2
]).

% Executor's state holds recently used (updated or read) statistics for different
% directories and collection types, which are cached in the state until flushed
% and ?CACHED_DIR_STATS_INACTIVITY_PERIOD passes since their last usage.
% A cached_dir_stats record is created for each guid/collection_type pair as statistics
% for each directory (guid) are stored in several collections.
-record(state, {
    dir_stats_cache = #{} :: #{cached_dir_stats_key() => cached_dir_stats()},
    has_unflushed_changes = false :: boolean(),

    % Field used during collections initialization - multiple collection types for dir are initialized together so
    % progress data cannot be stored in dir_stats_cache and must be stored in separate map
    % NOTE: values of statistics are kept in dir_stats_cache during initialization - this field contains only
    %       information about progress (children of directory already used and children to be used)
    initialization_progress_map = #{} ::
        #{file_id:file_guid() => dir_stats_collections_initializer:initialization_progress()},

    flush_timer_ref :: reference() | undefined,
    initialization_timer_ref :: reference() | undefined,

    % Space status is initialized on first request for any dir in space
    space_collecting_statuses = #{} :: #{od_space:id() => dir_stats_service_state:extended_active_status()}
}).

-record(cached_dir_stats, {
    % current statistics for the {guid, type} pair, i. e. previous stats acquired from db
    % with all the updates (that were reported during executor's lifecycle) applied
    current_stats :: dir_stats_collection:collection() | undefined, % undefined during collections initialization
    % accumulates all the updates that were reported since the last propagation to parent
    stat_updates_acc_for_parent = #{} :: dir_stats_collection:collection(),
    last_used :: stopwatch:instance() | undefined,
    parent :: file_id:file_guid() | undefined, % resolved and stored upon first access to this field,
                                               % value <<"root_dir">> is used when dir has no parent to send updates

    % Field used to store information about collecting status to enable special requests handling during collection
    % initialization (when space collecting is changed to enabled for not empty space - see dir_stats_service_state)
    collecting_status :: dir_stats_service_state:active_status(),
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
    update_type :: update_type(),
    collection_type :: dir_stats_collection:type(),
    collection_update :: dir_stats_collection:collection()
}).

-record(dsc_flush_request, {
    guid :: file_id:file_guid(),
    collection_type :: dir_stats_collection:type(),
    pruning_strategy :: pruning_strategy()
}).


-type update_type() :: internal | external. % internal update is update sent when collector is flushing
                                            % stat_updates_acc_for_parent; external update is update sent
                                            % by other processes than collectors
-type state() :: #state{}.

-type cached_dir_stats_key() :: {file_id:file_guid(), dir_stats_collection:type()}.
-type cached_dir_stats() :: #cached_dir_stats{}.

-type dir_status() :: flushed | unflushed | initializing.
-type pruning_strategy() :: prune_flushed | prune_inactive.

-type collecting_status_error() :: ?ERROR_DIR_STATS_NOT_READY | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE.
-type error() :: collecting_status_error() | ?ERROR_NOT_FOUND | ?ERROR_INTERNAL_SERVER_ERROR.
-export_type([collecting_status_error/0, error/0]).

-type pid_to_notify() :: pid() | undefined.

-define(FLUSH_INTERVAL_MILLIS, 5000).
-define(CACHED_DIR_STATS_INACTIVITY_PERIOD, 10000). % stats that are already flushed and not used for this period
                                                    % are removed from the cache
-define(SUPERVISOR_NAME, dir_stats_service_worker_sup).

-define(SCHEDULED_FLUSH, scheduled_flush).
% When initialization of directory fails (e.g. as a result of race) or request to not initialized collection appears,
% next initialization is scheduled. ?SCHEDULED_COLLECTIONS_INITIALIZATION message is used to trigger all scheduled
% initializations. Scheduled initializations are executed every ?SCHEDULED_COLLECTIONS_INITIALIZATION_INTERVAL_MILLIS.
-define(SCHEDULED_COLLECTIONS_INITIALIZATION, scheduled_stats_initialization).
-define(SCHEDULED_COLLECTIONS_INITIALIZATION_INTERVAL_MILLIS, 2000).

-define(INIT_FIRST_RETRY_INTERVAL, 30000). % 30 sek
-define(MAX_INIT_RETRY_INTERVAL, timer:minutes(30)).
-define(INITIALIZE_COLLECTIONS(Guid, PidToNotify), {initialize_collections, Guid, PidToNotify}).
-define(CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify),
    ?CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify, ?INIT_FIRST_RETRY_INTERVAL)).
-define(CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify, RetryInterval),
    {continue_collections_initialization, Guid, PidToNotify, RetryInterval}).
-define(STOP_COLLECTING(SpaceId), {stop_collecting, SpaceId}).
-define(FILE_MOVED(Guid, TargetParentGuid), {file_moved, Guid, TargetParentGuid}).

% Log not more often than once every 5 min for every unique space
-define(THROTTLE_LOG(SpaceId, Log), utils:throttle({?MODULE, ?FUNCTION_NAME, SpaceId}, 300, fun() -> Log end)).

%%%===================================================================
%%% API - single directory
%%%===================================================================

-spec get_stats(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:stats_selector()) ->
    {ok, dir_stats_collection:collection()} | error().
get_stats(Guid, CollectionType, StatNames) ->
    case dir_stats_service_state:get_extended_status(file_id:guid_to_space_id(Guid)) of
        enabled ->
            Request = #dsc_get_request{
                guid = Guid,
                collection_type = CollectionType,
                stat_names = StatNames
            },
            call_designated_node(Guid, submit_and_await, [?MODULE, Guid, Request]);
        {initializing, _} ->
            ?ERROR_DIR_STATS_NOT_READY;
        _ ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec update_stats_of_dir(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir(Guid, CollectionType, CollectionUpdate) ->
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(Guid)) of
        true ->
            update_stats_of_dir(Guid, external, CollectionType, CollectionUpdate);
        false ->
            ok
    end.


-spec update_stats_of_dir_without_state_check(file_id:file_guid(), dir_stats_collection:type(),
    dir_stats_collection:collection()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir_without_state_check(Guid, CollectionType, CollectionUpdate) ->
    update_stats_of_dir(Guid, external, CollectionType, CollectionUpdate).


-spec update_stats_of_parent(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent(Guid, CollectionType, CollectionUpdate) ->
    update_stats_of_parent(Guid, CollectionType, CollectionUpdate, add_hook).


-spec update_stats_of_parent(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection(),
    add_hook | return_error) -> ok | ?ERROR_NOT_FOUND | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent(Guid, CollectionType, CollectionUpdate, ParentErrorHandlingMethod) ->
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(Guid)) of
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
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    case dir_stats_service_state:is_active(SpaceId) of
        true ->
            case file_meta:get_including_deleted_local_or_remote(FileUuid, SpaceId) of
                {ok, Doc} ->
                    case file_meta:get_type(Doc) of
                        ?DIRECTORY_TYPE ->
                            update_stats_of_dir(Guid, external, CollectionType, CollectionUpdate);
                        _ ->
                            update_stats_of_parent_internal(get_parent(Doc, SpaceId), CollectionType, CollectionUpdate)
                    end;
                ?ERROR_NOT_FOUND ->
                    add_missing_file_meta_on_update_posthook(Guid, CollectionType, CollectionUpdate)
            end;
        false ->
            ok
    end.


-spec flush_stats(file_id:file_guid(), dir_stats_collection:type()) ->
    ok | collecting_status_error() | ?ERROR_INTERNAL_SERVER_ERROR.
flush_stats(Guid, CollectionType) ->
    case dir_stats_service_state:get_extended_status(file_id:guid_to_space_id(Guid)) of
        enabled -> request_flush(Guid, CollectionType, prune_inactive);
        {initializing, _} -> ?ERROR_DIR_STATS_NOT_READY;
        _ -> ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec delete_stats(file_id:file_guid(), dir_stats_collection:type()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
delete_stats(Guid, CollectionType) ->
    % TODO VFS-9204 - delete only for directories
    % TODO VFS-9204 - delete collection when collecting was enabled in past
    % TODO VFS-9204 - delete from collector memory for collections_initialization status
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(Guid)) of
        true ->
            % TODO VFS-9204 - deletion of stat docs results in races when file is deleted via trash in
            % multiprovider environment (collection is deleted before doc move to trash handling)
            ok;
%%            case request_flush(Guid, CollectionType, prune_flushed) of
%%                ok -> CollectionType:delete(Guid);
%%                ?ERROR_DIR_STATS_NOT_READY -> CollectionType:delete(Guid);
%%                ?ERROR_INTERNAL_SERVER_ERROR -> ?ERROR_INTERNAL_SERVER_ERROR
%%            end;
        false ->
            CollectionType:delete(Guid)
    end.


-spec initialize_collections(file_id:file_guid()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
initialize_collections(Guid) ->
    call_designated_node(Guid, submit_and_await, [?MODULE, Guid, ?INITIALIZE_COLLECTIONS(Guid, self())]).


-spec report_file_moved(onedata_file:type(), file_id:file_guid(), file_id:file_guid(), file_id:file_guid()) -> ok.
report_file_moved(?DIRECTORY_TYPE, FileGuid, _SourceParentGuid, TargetParentGuid) ->
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(FileGuid)) of
        true ->
            Message = ?FILE_MOVED(FileGuid, TargetParentGuid),
            call_designated_node(FileGuid, acknowledged_cast, [?MODULE, FileGuid, Message]);
        false ->
            ok
    end;
report_file_moved(_, FileGuid, SourceParentGuid, TargetParentGuid) ->
    try
        % Warning: rename is not protected from races with write so stats could be incorrect if such race appears
        lists:foreach(fun(CollectionType) ->
            ChildStats = CollectionType:init_child(FileGuid, true),
            case dir_stats_collection:on_collection_move(CollectionType, ChildStats) of
                {update_source_parent, CollectionUpdate} ->
                    update_stats_of_dir(SourceParentGuid, external, CollectionType, CollectionUpdate);
                ignore ->
                    ok
            end,
            update_stats_of_dir(TargetParentGuid, external, CollectionType, ChildStats)
        end, dir_stats_collection:list_types())
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Error handling file ~tp move from ~tp to ~tp: ~tp:~tp",
                [FileGuid, SourceParentGuid, TargetParentGuid, Error, Reason], Stacktrace)
    end.


-spec is_uuid_counted(file_meta:uuid()) -> boolean().
is_uuid_counted(Uuid) ->
    not (fslogic_file_id:is_trash_dir_uuid(Uuid) orelse
        archivisation_tree:is_special_uuid(Uuid) orelse
        archivisation_tree:is_archive_dir_uuid(Uuid)
    ).


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
        ErrorAns = lists:filter(fun(NodeAns) -> NodeAns =/= ok end, lists:flatten(Ans)),

        case {ErrorAns, BadNodes} of
            {[], []} ->
                ok;
            _ ->
                ?error("Dir stats collector ~tp error: not ok answers: ~tp, bad nodes: ~tp",
                    [?FUNCTION_NAME, ErrorAns, BadNodes])
        end,

        dir_stats_service_state:report_collectors_stopped(SpaceId)
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
    ?warning("Dir stats collector forced terminate, reason: ~tp", [Reason]);

forced_terminate(Reason, State) ->
    UpdatedState = flush_all(State),
    case UpdatedState#state.has_unflushed_changes of
        false ->
            ?error("Dir stats collector emergency flush as a result of forced terminate, terminate reason: ~tp",
                [Reason]);
        true ->
            NotFlushed = maps:keys(maps:filter(fun(_Key, CachedDirStats) ->
                get_dir_status(CachedDirStats) =/= flushed
            end, UpdatedState#state.dir_stats_cache)),
            ?critical("Dir stats collector terminate of process with unflushed changes, lost data for: ~tp", [NotFlushed])
    end.


%%%===================================================================
%%% Callbacks used by executor - handling requests
%%%===================================================================

-spec handle_call(#dsc_get_request{} | #dsc_flush_request{} |
    ?INITIALIZE_COLLECTIONS(file_id:file_guid(), pid()) | ?STOP_COLLECTING(od_space:id()), state()) ->
    {
        ok | {ok, dir_stats_collection:collection()} | error(),
        state()
    }.
handle_call(#dsc_get_request{
    guid = Guid,
    collection_type = CollectionType,
    stat_names = StatNames
}, State) ->
    case reset_last_used_timer(Guid, CollectionType, State) of
        {
            {ok, #cached_dir_stats{current_stats = CurrentStats, collecting_status = enabled}},
            UpdatedState
        } ->
            {dir_stats_collection:with_all(StatNames, CurrentStats), UpdatedState};
        {
            {ok, #cached_dir_stats{current_stats = _CurrentStats, collecting_status = initializing}},
            UpdatedState
        } ->
            {?ERROR_DIR_STATS_NOT_READY, UpdatedState};
        {
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE,
            _UpdatedState
        } = Error ->
            Error
    end;


handle_call(#dsc_flush_request{guid = Guid, collection_type = CollectionType, pruning_strategy = PruningStrategy}, State) ->
    flush_cached_dir_stats(gen_cached_dir_stats_key(Guid, CollectionType), PruningStrategy, State);

handle_call(?INITIALIZE_COLLECTIONS(Guid, PidToNotify), State) ->
    {InitializationDataMap, UpdatedState} = lists:foldl(fun(CollectionType, {InitializationDataMapAcc, StateAcc}) ->
        case reset_last_used_timer(Guid, CollectionType, StateAcc) of
            {{ok, #cached_dir_stats{
                collecting_status = initializing,
                initialization_data = InitializationData
            }}, UpdatedStateAcc} ->
                {InitializationDataMapAcc#{CollectionType => InitializationData}, UpdatedStateAcc};
            {_, UpdatedStateAcc} ->
                {InitializationDataMapAcc, UpdatedStateAcc}
        end
    end, {#{}, State}, dir_stats_collection:list_types()),

    case maps:size(InitializationDataMap) of
        0 ->
            PidToNotify ! initialization_finished,
            {ok, UpdatedState};
        _ ->
            FinalState = start_collections_initialization_for_dir(Guid, PidToNotify, InitializationDataMap, UpdatedState),
            {ok, FinalState}
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


-spec handle_cast(#dsc_update_request{} | ?SCHEDULED_FLUSH | ?SCHEDULED_COLLECTIONS_INITIALIZATION |
    ?CONTINUE_COLLECTIONS_INITIALIZATION(file_id:file_guid(), pid_to_notify(), time:millis()) |
    ?FILE_MOVED(file_id:file_guid(), file_id:file_guid()), state()) -> state().
handle_cast(#dsc_update_request{
    guid = Guid,
    update_type = UpdateType,
    collection_type = CollectionType,
    collection_update = CollectionUpdate
}, State) ->
    UpdateAns = update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
        update_collection_in_cache(CollectionType, UpdateType, CollectionUpdate, CachedDirStats)
    end, State),

    case UpdateAns of
        {?ERROR_DIR_STATS_DISABLED_FOR_SPACE, UpdatedState} ->
            UpdatedState;
        {{ok, #cached_dir_stats{collecting_status = initializing}}, UpdatedState} ->
            abort_collection_initialization(Guid, CollectionType, UpdatedState);
        {_, UpdatedState} ->
            ensure_flush_scheduled(UpdatedState#state{has_unflushed_changes = true})
    end;

handle_cast(?SCHEDULED_FLUSH, State) ->
    flush_all(State);

handle_cast(?SCHEDULED_COLLECTIONS_INITIALIZATION, State) ->
    ensure_space_collecting_statuses_up_to_date(
        start_collections_initialization_for_all_cached_dirs(State#state{initialization_timer_ref = undefined}));

handle_cast(?CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify, RetryInterval), State) ->
    continue_collections_initialization_for_dir(Guid, PidToNotify, RetryInterval, State);

handle_cast(?FILE_MOVED(Guid, TargetParentGuid), State) ->
    lists:foldl(fun(CollectionType, StateAcc) ->
        collection_moved(Guid, CollectionType, TargetParentGuid, StateAcc)
    end, State, dir_stats_collection:list_types());

handle_cast(Info, State) ->
    ?log_bad_request(Info),
    State.


%%%===================================================================
%%% file_meta_posthooks related functions
%%%===================================================================

-spec add_missing_file_meta_on_update_posthook(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_missing_file_meta_on_update_posthook(Guid, CollectionType, CollectionUpdate) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    file_meta_posthooks:add_hook({file_meta_missing, FileUuid}, generator:gen_name(),
        SpaceId, ?MODULE, update_stats_of_nearest_dir, [Guid, CollectionType, CollectionUpdate]).


-spec encode_file_meta_posthook_args(file_meta_posthooks:function_name(), [term()]) ->
    file_meta_posthooks:encoded_args().
encode_file_meta_posthook_args(update_stats_of_parent, [Guid, CollectionType, CollectionUpdate, return_error]) ->
    encode_collection_details(Guid, CollectionType, CollectionUpdate);
encode_file_meta_posthook_args(update_stats_of_nearest_dir, [Guid, CollectionType, CollectionUpdate]) ->
    encode_collection_details(Guid, CollectionType, CollectionUpdate).


-spec decode_file_meta_posthook_args(file_meta_posthooks:function_name(), file_meta_posthooks:encoded_args()) ->
    [term()].
decode_file_meta_posthook_args(update_stats_of_parent, EncodedArgs) ->
    decode_collection_details(EncodedArgs) ++ [return_error];
decode_file_meta_posthook_args(update_stats_of_nearest_dir, EncodedArgs) ->
    decode_collection_details(EncodedArgs).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update_stats_of_dir(file_id:file_guid(), update_type(), dir_stats_collection:type(),
    dir_stats_collection:collection()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_dir(Guid, UpdateType, CollectionType, CollectionUpdate) ->
    Request = #dsc_update_request{
        guid = Guid,
        update_type = UpdateType,
        collection_type = CollectionType,
        collection_update = CollectionUpdate
    },
    call_designated_node(Guid, acknowledged_cast, [?MODULE, Guid, Request]).


%% @private
-spec update_collection_in_cache(dir_stats_collection:type(), update_type(), dir_stats_collection:collection(),
    cached_dir_stats()) -> cached_dir_stats().
update_collection_in_cache(CollectionType, internal = _UpdateType, CollectionUpdate, #cached_dir_stats{
    collecting_status = initializing,
    initialization_data = InitializationData
} = CachedDirStats) ->
    CachedDirStats#cached_dir_stats{
        initialization_data = dir_stats_collections_initializer:update_stats_from_children_descendants(
            InitializationData, CollectionType, CollectionUpdate)
    };

update_collection_in_cache(CollectionType, external = UpdateType, CollectionUpdate, #cached_dir_stats{
    collecting_status = initializing,
    initialization_data = InitializationData
} = CachedDirStats) ->
    case dir_stats_collections_initializer:are_stats_ready(InitializationData) of
        true ->
            update_collection_in_cache(
                CollectionType, UpdateType, CollectionUpdate, set_collecting_enabled(CachedDirStats, CollectionType));
        false ->
            CachedDirStats#cached_dir_stats{
                initialization_data = dir_stats_collections_initializer:report_update(InitializationData)
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
    ok | ?ERROR_DIR_STATS_NOT_READY | ?ERROR_INTERNAL_SERVER_ERROR.
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
        {FlushAns, UpdatedStateAcc} = flush_cached_dir_stats(CachedDirStatsKey, prune_inactive, StateAcc),
        UpdatedStateAcc#state{
            has_unflushed_changes = StateAcc#state.has_unflushed_changes orelse FlushAns =/= ok
        }
    end, State#state{has_unflushed_changes = false}, maps:keys(DirStatsCache)));

flush_all(#state{
    flush_timer_ref = Ref
} = State) ->
    erlang:cancel_timer(Ref),
    flush_all(State#state{flush_timer_ref = undefined}).


%% @private
-spec flush_cached_dir_stats(cached_dir_stats_key(), pruning_strategy(), state()) ->
    {ok | ?ERROR_DIR_STATS_NOT_READY | ?ERROR_INTERNAL_SERVER_ERROR, state()}.
flush_cached_dir_stats(CachedDirStatsKey, _, State) when not is_map_key(CachedDirStatsKey, State#state.dir_stats_cache) ->
    {ok, State}; % Key cannot be found - it has been flushed and pruned
flush_cached_dir_stats({_, CollectionType} = CachedDirStatsKey, PruningStrategy, State) ->
    #cached_dir_stats{initialization_data = InitializationData} = CachedDirStats =
        maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
    case get_dir_status(CachedDirStats) of
        unflushed ->
            {UpdatedCachedDirStats, UpdatedState} =
                save_and_propagate_cached_dir_stats(CachedDirStatsKey, CachedDirStats, State),
            UpdatedStatus = get_dir_status(UpdatedCachedDirStats),
            case {UpdatedStatus, PruningStrategy} of
                {flushed, prune_flushed} ->
                    {ok, prune_cached_dir_stats(CachedDirStatsKey, UpdatedState)};
                {flushed, prune_inactive} ->
                    {ok, update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, UpdatedState)};
                {unflushed, _} ->
                    {?ERROR_INTERNAL_SERVER_ERROR,
                        update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, UpdatedState)}
            end;
        flushed ->
            case (PruningStrategy =:= prune_flushed) orelse is_inactive(CachedDirStats) of
                true ->
                    {ok, prune_cached_dir_stats(CachedDirStatsKey, State)};
                false ->
                    {ok, State}
            end;
        initializing ->
            case dir_stats_collections_initializer:are_stats_ready(InitializationData) of
                true ->
                    UpdatedState = update_cached_dir_stats(
                        CachedDirStatsKey, set_collecting_enabled(CachedDirStats, CollectionType), State),
                    flush_cached_dir_stats(CachedDirStatsKey, PruningStrategy, UpdatedState);
                false ->
                    {?ERROR_DIR_STATS_NOT_READY, State}
            end
    end.


%% @private
-spec start_collections_initialization_for_all_cached_dirs(state()) -> state().
start_collections_initialization_for_all_cached_dirs(#state{dir_stats_cache = DirStatsCache} = State) ->
    InitializationDataMaps = maps:fold(fun
        ({Guid, CollectionType} = _CachedDirStatsKey, #cached_dir_stats{
            collecting_status = initializing,
            initialization_data = InitializationData
        }, Acc) ->
            case dir_stats_collections_initializer:is_initialization_pending(InitializationData) of
                true ->
                    Acc;
                false ->
                    InitializationDataMap = maps:get(Guid, Acc, #{}),
                    Acc#{Guid => InitializationDataMap#{CollectionType => InitializationData}}
            end;
        (_CachedDirStatsKey, _CachedDirStats, Acc) ->
            Acc
    end, #{}, DirStatsCache),

    maps:fold(fun(Guid, InitializationDataMap, StateAcc) ->
        start_collections_initialization_for_dir(Guid, undefined, InitializationDataMap, StateAcc)
    end, State, InitializationDataMaps).


%% @private
-spec start_collections_initialization_for_dir(file_id:file_guid(), pid_to_notify(),
    dir_stats_collections_initializer:initialization_data_map(), state()) -> state().
start_collections_initialization_for_dir(Guid, PidToNotify, InitializationDataMap, #state{
    initialization_progress_map = ProgressMap
} = State) ->
    case maps:is_key(Guid, ProgressMap) of
        true ->
            State;
        false ->
            DirInitializationProgress =
                dir_stats_collections_initializer:start_dir_initialization(Guid, InitializationDataMap),
            continue_collections_initialization_for_dir(Guid, PidToNotify, ?INIT_FIRST_RETRY_INTERVAL,
                State#state{initialization_progress_map = ProgressMap#{Guid => DirInitializationProgress}})
    end.


%% @private
-spec continue_collections_initialization_for_dir(file_id:file_guid(), pid_to_notify(), time:millis(), state()) -> state().
continue_collections_initialization_for_dir(
    Guid, PidToNotify, RetryInterval,
    #state{initialization_progress_map = ProgressMap} = State
) ->
    try
        case maps:get(Guid, ProgressMap, undefined) of
            undefined ->
                (PidToNotify =/= undefined) andalso (PidToNotify ! initialization_finished),
                State; % Progress can be deleted from map as a result of race with stats update
            DirInitializationProgress ->
                case dir_stats_collections_initializer:continue_dir_initialization(DirInitializationProgress) of
                    {continue, UpdatedDirInitializationProgress} ->
                        pes:self_cast(?CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify)),
                        State#state{initialization_progress_map = ProgressMap#{Guid => UpdatedDirInitializationProgress}};
                    {finish, CollectionsMap} ->
                        (PidToNotify =/= undefined) andalso (PidToNotify ! initialization_finished),
                        finish_collections_initialization_for_dir(Guid, CollectionsMap,
                            State#state{initialization_progress_map = maps:remove(Guid, ProgressMap)})
                end
        end
    catch
        Error:Reason:Stacktrace ->
            case datastore_runner:normalize_error(Reason) of
                no_connection_to_onezone ->
                    ok;
                dir_size_stats_init_error ->
                    ok; % Error has been logged by dir_size_stats module
                _ ->
                    ?error_stacktrace("Dir stats collector ~tp error for ~tp: ~tp:~tp",
                        [?FUNCTION_NAME, Guid, Error, Reason], Stacktrace)
            end,
            NewRetryInterval = min(2 * RetryInterval, ?MAX_INIT_RETRY_INTERVAL),
            pes:self_cast_after(?CONTINUE_COLLECTIONS_INITIALIZATION(Guid, PidToNotify, NewRetryInterval), RetryInterval),
            State
    end.


%% @private
-spec finish_collections_initialization_for_dir(file_id:file_guid(),
    dir_stats_collections_initializer:collections_map(), state()) -> state().
finish_collections_initialization_for_dir(Guid, CollectionsMap, State) ->
    InitializationDataMap = maps:map(fun(CollectionType, _InitializedCollection) ->
        CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
        #cached_dir_stats{initialization_data = InitializationData} =
            maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
        InitializationData
    end, CollectionsMap),

    UpdatedInitializationDataMap =
        dir_stats_collections_initializer:finish_dir_initialization(Guid, InitializationDataMap, CollectionsMap),
    maps:fold(fun(CollectionType, UpdatedInitializationData, StateAcc) ->
        CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
        CachedDirStats = maps:get(CachedDirStatsKey, State#state.dir_stats_cache),
        UpdatedCachedDirStats = CachedDirStats#cached_dir_stats{
            initialization_data = UpdatedInitializationData
        },
        update_cached_dir_stats(CachedDirStatsKey, UpdatedCachedDirStats, StateAcc)
    end, State, UpdatedInitializationDataMap).


%% @private
-spec abort_collection_initialization(file_id:file_guid(), dir_stats_collection:type(), state()) -> state().
abort_collection_initialization(Guid, CollectionType, #state{initialization_progress_map = ProgressMap} = State) ->
    case maps:get(Guid, ProgressMap, undefined) of
        undefined ->
            State;
        DirInitializationProgress ->
            case dir_stats_collections_initializer:abort_collection_initialization(
                DirInitializationProgress, CollectionType
            ) of
                {collections_left, UpdatedDirInitializationProgress} ->
                    State#state{initialization_progress_map = ProgressMap#{Guid => UpdatedDirInitializationProgress}};
                initialization_aborted_for_all_collections ->
                    State#state{initialization_progress_map = maps:remove(Guid, ProgressMap)}
            end
    end.


%% @private
-spec ensure_space_collecting_statuses_up_to_date(state()) -> state().
ensure_space_collecting_statuses_up_to_date(#state{
    space_collecting_statuses = SpaceCollectingStatuses,
    dir_stats_cache = DirStatsCache
} = State) ->
    IsAnyInitializingInSpaceMap = maps:fold(fun({Guid, _}, #cached_dir_stats{collecting_status = Status}, Acc) ->
        SpaceId = file_id:guid_to_space_id(Guid),
        IsAnyInitializingInSpace = maps:get(SpaceId, Acc, false),
        Acc#{SpaceId => IsAnyInitializingInSpace orelse Status =:= initializing}
    end, #{}, DirStatsCache),

    UpdatedSpaceCollectingStatuses = maps:map(fun
        (SpaceId, initializing) ->
            IsAnyInitializing = maps:get(SpaceId, IsAnyInitializingInSpaceMap, false),
            case
                (not IsAnyInitializing) andalso
                dir_stats_service_state:get_extended_status(SpaceId) =:= enabled
            of
                true -> enabled;
                false -> initializing
            end;
        (_SpaceId, SpaceCollectingStatus) ->
            SpaceCollectingStatus
    end, SpaceCollectingStatuses),
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
get_dir_status(#cached_dir_stats{collecting_status = initializing}) ->
    initializing.


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
    {_, CollectingStatus, State2} = acquire_space_collecting_status(file_id:guid_to_space_id(Guid), State),
    try
        case CollectingStatus of
            {initializing, Incarnation} ->
                CollectionType:save(Guid, CurrentStats, Incarnation);
            _ ->
                CollectionType:save(Guid, CurrentStats, current)
        end,
        {propagate_to_parent(Guid, CollectionType, CachedDirStats), State2}
    catch
        throw:{error, space_unsupported} ->
            % There can be a lot of files to save if space has been incorrectly unsupported - log must be throttled
            SpaceId = file_id:guid_to_space_id(Guid),
            ?THROTTLE_LOG(SpaceId, ?warning("Cannot save or propagate cache dir stats for collection type:"
                " ~tp and guid ~tp due to space ~tp unsupport", [CollectionType, Guid, SpaceId])),
            {CachedDirStats#cached_dir_stats{stat_updates_acc_for_parent = #{}}, State2};
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Dir stats collector save and propagate error for collection type: ~tp and guid ~tp:~n~tp:~tp",
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
    {{ok, UpdatedCachedDirStats :: cached_dir_stats()} | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE, state()} | no_return().
reset_last_used_timer(Guid, CollectionType, State) ->
    update_in_cache(Guid, CollectionType, fun(CachedDirStats) -> CachedDirStats end, State).


%% @private
-spec update_in_cache(file_id:file_guid(), dir_stats_collection:type(),
    fun((cached_dir_stats()) -> cached_dir_stats()), state()) ->
    {{ok, UpdatedCachedDirStats :: cached_dir_stats()} | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE, state()} | no_return().
update_in_cache(Guid, CollectionType, Diff, #state{dir_stats_cache = DirStatsCache} = State) ->
    CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
    FindAns = case maps:find(CachedDirStatsKey, DirStatsCache) of
        {ok, DirStatsFromCache} ->
            {DirStatsFromCache, State};
        error ->
            {Stats, Incarnation} = CollectionType:acquire(Guid),
            case acquire_space_collecting_status(file_id:guid_to_space_id(Guid), State) of
                {enabled, _, State2} ->
                    {#cached_dir_stats{
                        collecting_status = enabled,
                        current_stats = Stats
                    }, State2};
                {{initializing, Incarnation}, _, State2} ->
                    {#cached_dir_stats{
                        collecting_status = enabled,
                        current_stats = Stats
                    }, State2};
                % Collection incarnation is not equal to current incarnation - collection
                % is outdated - initialize it once more
                {{initializing, _}, _, State2} ->
                    {
                        #cached_dir_stats{
                            collecting_status = initializing,
                            initialization_data = dir_stats_collections_initializer:new_initialization_data()
                        },
                        ensure_flush_scheduled(ensure_initialization_scheduled(
                            State2#state{has_unflushed_changes = true}
                        ))
                    };
                {_, _, State2} ->
                    {?ERROR_DIR_STATS_DISABLED_FOR_SPACE, State2}
            end
    end,

    case FindAns of
        {?ERROR_DIR_STATS_DISABLED_FOR_SPACE, _UpdatedState} ->
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
-spec update_stats_of_parent_internal(file_id:file_guid(), dir_stats_collection:type(),
    dir_stats_collection:collection()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
update_stats_of_parent_internal(<<"root_dir">> = _ParentGuid, _CollectionType, _CollectionUpdate) ->
    ok;
update_stats_of_parent_internal(ParentGuid, CollectionType, CollectionUpdate) ->
    update_stats_of_dir(ParentGuid, external, CollectionType, CollectionUpdate).


%% @private
-spec propagate_to_parent(file_id:file_guid(), dir_stats_collection:type(), cached_dir_stats()) -> cached_dir_stats().
propagate_to_parent(Guid, CollectionType, #cached_dir_stats{
    stat_updates_acc_for_parent = StatUpdatesAccForParent
} = CachedDirStats) ->
    case fslogic_file_id:is_tmp_dir_guid(Guid) of
        true ->
            CachedDirStats#cached_dir_stats{stat_updates_acc_for_parent = #{}};
        false ->
            {Parent, UpdatedCachedDirStats} = acquire_parent(Guid, CachedDirStats),

            Result = case Parent of
                <<"root_dir">> -> ok;
                undefined -> add_hook_for_missing_doc(Guid, CollectionType, StatUpdatesAccForParent);
                _ -> update_stats_of_dir(Parent, internal, CollectionType, StatUpdatesAccForParent)
            end,

            case Result of
                ok ->
                    UpdatedCachedDirStats#cached_dir_stats{stat_updates_acc_for_parent = #{}};
                {error, _} = Error ->
                    ?error("Dir stats collector ~tp error for collection type: ~tp and guid ~tp (parent ~tp): ~tp",
                        [?FUNCTION_NAME, CollectionType, Guid, Parent, Error]),
                    UpdatedCachedDirStats
            end
    end.


%% @private
-spec acquire_parent(file_id:file_guid(), cached_dir_stats()) ->
    {file_id:file_guid() | undefined, cached_dir_stats()}.
acquire_parent(Guid, CachedDirStats) ->
    UpdatedCachedDirStats = cache_parent(Guid, CachedDirStats),
    {UpdatedCachedDirStats#cached_dir_stats.parent, UpdatedCachedDirStats}.


%% @private
-spec cache_parent(file_id:file_guid(), cached_dir_stats()) -> cached_dir_stats().
cache_parent(Guid, #cached_dir_stats{
    parent = undefined
} = CachedDirStats) ->
    case get_parent(Guid) of
        {ok, ParentGuidToCache} ->
            case dir_stats_collector_metadata:get_parent(Guid) of
                undefined ->
                    dir_stats_collector_metadata:update_parent(Guid, ParentGuidToCache),
                    CachedDirStats#cached_dir_stats{parent = ParentGuidToCache};
                ParentGuidToCache ->
                    CachedDirStats#cached_dir_stats{parent = ParentGuidToCache};
                OldParentGuid ->
                    pes:self_cast(?FILE_MOVED(Guid, ParentGuidToCache)),
                    CachedDirStats#cached_dir_stats{parent = OldParentGuid}
            end;
        ?ERROR_NOT_FOUND ->
            CachedDirStats
    end;

cache_parent(_Guid, CachedDirStats) ->
    CachedDirStats.


%% @private
-spec get_parent(file_id:file_guid()) -> {ok, file_id:file_guid()} | ?ERROR_NOT_FOUND.
get_parent(Guid) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    case file_meta:get_including_deleted_local_or_remote(FileUuid, SpaceId) of
        {ok, Doc} -> {ok, get_parent(Doc, SpaceId)};
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND
    end.


%% @private
-spec get_parent(file_meta:doc(), od_space:id()) -> file_id:file_guid().
get_parent(Doc, SpaceId) ->
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
    case fslogic_file_id:is_root_dir_uuid(ParentUuid) of
        true -> <<"root_dir">>;
        false -> file_id:pack_guid(ParentUuid, SpaceId)
    end.


%% @private
-spec acquire_space_collecting_status(od_space:id(), state()) -> {
    NewCollectionStatus :: dir_stats_service_state:extended_status(),
    ExistingCollectionStatus :: dir_stats_service_state:extended_status(),
    state()
}.
acquire_space_collecting_status(SpaceId, #state{space_collecting_statuses = CollectingStatuses} = State) ->
    case maps:get(SpaceId, CollectingStatuses, undefined) of
        undefined ->
            case dir_stats_service_state:get_extended_status(SpaceId) of
                enabled ->
                    {enabled, enabled, State#state{space_collecting_statuses = CollectingStatuses#{SpaceId => enabled}}};
                {initializing, _} = Status ->
                    {Status, Status, State#state{space_collecting_statuses = CollectingStatuses#{SpaceId => Status}}};
                Status ->
                    {Status, Status, State} % race with status changing - do not cache
            end;
        enabled ->
            {enabled, enabled, State};
        {initializing, _} = CachedStatus ->
            {dir_stats_service_state:get_extended_status(SpaceId), CachedStatus, State}
    end.


%% @private
-spec add_hook_for_missing_doc(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook_for_missing_doc(Guid, CollectionType, CollectionUpdate) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(Guid),
    file_meta_posthooks:add_hook({file_meta_missing, FileUuid}, generator:gen_name(), SpaceId,
        ?MODULE, update_stats_of_parent, [Guid, CollectionType, CollectionUpdate, return_error]).


%% @private
-spec call_designated_node(file_id:file_guid(), submit_and_await | acknowledged_cast, list()) ->
    ok | {ok, dir_stats_collection:collection()} | ignored | error().
call_designated_node(Guid, Function, Args) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    case erpc:call(Node, pes, Function, Args) of
        ?ERROR_DIR_STATS_NOT_READY -> ?ERROR_DIR_STATS_NOT_READY;
        ?ERROR_DIR_STATS_DISABLED_FOR_SPACE -> ?ERROR_DIR_STATS_DISABLED_FOR_SPACE;
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND;
        {error, _} = Error ->
            ?error("Dir stats collector PES fun ~tp error: ~tp for guid ~tp", [Function, Error, Guid]),
            ?ERROR_INTERNAL_SERVER_ERROR;
        Other ->
            Other
    end.


%% @private
-spec gen_cached_dir_stats_key(file_id:file_guid(), dir_stats_collection:type()) -> cached_dir_stats_key().
gen_cached_dir_stats_key(Guid, CollectionType) ->
    {Guid, CollectionType}.


%% @private
-spec collection_moved(file_id:file_guid(), dir_stats_collection:type(), file_id:file_guid(), state()) -> state().
collection_moved(Guid, CollectionType, TargetParentGuid, State) ->
    case update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
        cache_parent(Guid, CachedDirStats)
    end, State) of
        {
            {ok, #cached_dir_stats{parent = undefined}},
            UpdatedState
        } ->
            UpdatedState; % Parent cannot be acquired so no data could be propagated to it
        {
            {ok, #cached_dir_stats{parent = TargetParentGuid}},
            UpdatedState
        } ->
            UpdatedState;
        {
            {ok, #cached_dir_stats{
                parent = PrevParentGuid,
                current_stats = CurrentStats,
                collecting_status = enabled
            }},
            UpdatedState
        } ->
            CachedDirStatsKey = gen_cached_dir_stats_key(Guid, CollectionType),
            % TODO VFS-9204 - handle flush errors
            {_FlushAns, UpdatedState2} = flush_cached_dir_stats(CachedDirStatsKey, prune_inactive, UpdatedState),
            ConsolidatedStats = case is_uuid_counted(file_id:guid_to_uuid(Guid)) of
                true ->
                    InitialDirStats = CollectionType:init_child(Guid, true),
                    dir_stats_collection:consolidate(CollectionType, InitialDirStats, CurrentStats);
                false ->
                    CurrentStats
            end,

            case dir_stats_collection:on_collection_move(CollectionType, ConsolidatedStats) of
                {update_source_parent, CollectionUpdate} ->
                    update_stats_of_dir(PrevParentGuid, internal, CollectionType, CollectionUpdate);
                ignore ->
                    ok
            end,

            {_, UpdatedState3} = update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
                CachedDirStats#cached_dir_stats{parent = TargetParentGuid}
            end, UpdatedState2),
            update_stats_of_dir(TargetParentGuid, internal, CollectionType, ConsolidatedStats),
            dir_stats_collector_metadata:update_parent(Guid, TargetParentGuid),
            UpdatedState3;

        {
            {ok, #cached_dir_stats{current_stats = _CurrentStats, collecting_status = initializing}},
            UpdatedState
        } ->
            {_, UpdatedState2} = update_in_cache(Guid, CollectionType, fun(CachedDirStats) ->
                CachedDirStats#cached_dir_stats{parent = TargetParentGuid}
            end, UpdatedState),
            dir_stats_collector_metadata:update_parent(Guid, TargetParentGuid),
            UpdatedState2;
        {
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE,
            UpdatedState
        } ->
            UpdatedState
    end.


%% @private
-spec encode_collection_details(file_id:file_guid(), dir_stats_collection:type(), dir_stats_collection:collection()) ->
    binary().
encode_collection_details(Guid, CollectionType, Collection) ->
    EncodedCollectionType = dir_stats_collection:encode_type(CollectionType),
    term_to_binary([Guid, EncodedCollectionType, CollectionType:compress(Collection)]).


%% @private
-spec decode_collection_details(binary()) -> [term()].
decode_collection_details(EncodedCollectionDetails) ->
    [Guid, EncodedCollectionType, EncodedCollection] = binary_to_term(EncodedCollectionDetails),
    CollectionType = dir_stats_collection:decode_type(EncodedCollectionType),
    [Guid, CollectionType, CollectionType:decompress(EncodedCollection)].
