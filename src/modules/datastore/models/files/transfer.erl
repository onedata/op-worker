%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about transfers. Creation of doc works
%%% as a trigger for starting a transfer.
%%% We distinguish 3 types of transfers:
%%%     - replication
%%%     - replica_eviction
%%%     - migration (replica_eviction preceded by replication)
%%% @end
%%%-------------------------------------------------------------------
-module(transfer).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init/0, cleanup/0,
    start/6, get/1, update/2, update_and_run/3, delete/1,
    cancel/1, rerun/2
]).

-export([
    mark_dequeued/1, set_controller_process/1,

    is_replication/1, is_eviction/1, is_migration/1,
    is_ongoing/1, is_replication_ongoing/1, is_eviction_ongoing/1,
    is_ended/1, is_replication_ended/1, is_eviction_ended/1,

    increment_files_to_process_counter/2, increment_files_processed_counter/1,
    increment_files_failed_and_processed_counters/1, increment_files_replicated_counter/1,
    mark_data_replication_finished/3,
    increment_files_evicted_and_processed_counters/1,
    rerun_not_ended_transfers/1,
    restart_pools/0
]).

% list functions
-export([
    list_waiting_transfers/1, list_waiting_transfers/3, list_waiting_transfers/4,
    list_ongoing_transfers/1, list_ongoing_transfers/3, list_ongoing_transfers/4,
    list_ended_transfers/1, list_ended_transfers/3, list_ended_transfers/4
]).

-export([get_link_key/2, get_link_key_by_state/2, get_replication_status/1]).

%% datastore_model callbacks
-export([
    get_ctx/0, get_record_struct/1, get_posthooks/0, get_record_version/0,
    upgrade_record/2, resolve_conflict/3
]).

-type id() :: binary().
-type diff() :: datastore:diff(transfer()).
-type status() :: scheduled | enqueued | active | completed | aborting |
    failed | cancelled | skipped.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type doc() :: datastore_doc:doc(transfer()).
-type timestamp() :: non_neg_integer().
-type list_limit() :: non_neg_integer() | all.

-export_type([
    id/0, transfer/0, status/0, callback/0, doc/0,
    timestamp/0, list_limit/0
]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Initialize resources required by transfers.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    start_pools().

%%-------------------------------------------------------------------
%% @doc
%% Cleanup resources required by transfers.
%% @end
%%-------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    stop_pools().

%%--------------------------------------------------------------------
%% @doc
%% Starts the transfer and records it for specified session.
%% @end
%%--------------------------------------------------------------------
-spec start(session:id(), fslogic_worker:file_guid(), file_meta:path(),
    undefined | od_provider:id(), undefined | od_provider:id(), binary()) ->
    {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, SourceProviderId, TargetProviderId, Callback) ->
    {ok, UserId} = session:get_user_id(SessionId),
    start_for_user(UserId, FileGuid, FilePath, SourceProviderId,
        TargetProviderId, Callback
    ).

%%--------------------------------------------------------------------
%% @doc
%% Starts the transfer for specified user id.
%% @end
%%--------------------------------------------------------------------
-spec start_for_user(od_user:id(), fslogic_worker:file_uuid(),
    file_meta:path(), undefined | od_provider:id(), undefined | od_provider:id(),
    binary()) -> {ok, id()} | ignore | {error, Reason :: term()}.
start_for_user(UserId, FileGuid, FilePath, EvictingProviderId,
    ReplicatingProviderId, Callback
) ->
    ReplicationStatus = case ReplicatingProviderId of
        undefined -> skipped;
        _ -> scheduled
    end,
    EvictionStatus = case EvictingProviderId of
        undefined -> skipped;
        _ -> scheduled
    end,
    ScheduleTime = provider_logic:zone_time_seconds(),
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),
    ToCreate = #document{
        scope = SpaceId,
        value = #transfer{
            file_uuid = fslogic_uuid:guid_to_uuid(FileGuid),
            space_id = SpaceId,
            user_id = UserId,
            path = FilePath,
            callback = Callback,
            replication_status = ReplicationStatus,
            eviction_status = EvictionStatus,
            scheduling_provider_id = oneprovider:get_id(),
            evicting_provider = EvictingProviderId,
            replicating_provider = ReplicatingProviderId,
            schedule_time = ScheduleTime,
            start_time = 0,
            finish_time = 0,
            last_update = #{},
            min_hist = #{},
            hr_hist = #{},
            dy_hist = #{},
            mth_hist = #{}
        }},

    {ok, Doc = #document{key = TransferId}} = create(ToCreate),
    transfer_links:add_waiting_transfer_link(Doc),
    transfer_changes:handle(Doc),
    {ok, TransferId}.


-spec rerun_not_ended_transfers(od_space:id()) -> [id()].
rerun_not_ended_transfers(SpaceId) ->
    {ok, WaitingTransferIds} = list_waiting_transfers(SpaceId),
    {ok, OngoingTransferIds} = list_ongoing_transfers(SpaceId),

    Reruns = lists:foldl(fun(TransferId, CurrReruns) ->
        case maybe_rerun(TransferId) of
            {error, non_participating_provider} ->
                CurrReruns;
            {ok, moved_to_ended} ->
                CurrReruns;
            {ok, marked_failed} ->
                CurrReruns;
            {ok, NewTransferId} ->
                CurrReruns#{TransferId => NewTransferId}
        end
    end, #{}, WaitingTransferIds ++ OngoingTransferIds),

    case map_size(Reruns) of
        0 ->
            ok;
        RerunsNum ->
            ?info("Space ~p - ~p unfinished transfers has been rerun:~n~p", [
                SpaceId, RerunsNum, Reruns
            ])
    end,
    maps:values(Reruns).


-spec rerun(undefined | od_user:id(), doc() | id()) ->
    {ok, id()} | {error, term()}.
rerun(UserId, #document{key = TransferId, value = Transfer}) ->
    case is_ended(Transfer) of
        false ->
            {error, not_ended};
        true ->
            #transfer{
                file_uuid = FileUuid,
                space_id = SpaceId,
                user_id = OldUserId,
                path = FilePath,
                evicting_provider = EvictingProviderId,
                replicating_provider = ReplicatingProviderId,
                callback = Callback
            } = Transfer,

            NewUserId = utils:ensure_defined(UserId, undefined, OldUserId),
            FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),

            {ok, NewTransferId} = start_for_user(NewUserId, FileGuid, FilePath,
                EvictingProviderId, ReplicatingProviderId, Callback
            ),
            update(TransferId, fun(OldTransfer) ->
                {ok, OldTransfer#transfer{rerun_id = NewTransferId}}
            end),
            {ok, NewTransferId}
    end;
rerun(UserId, TransferId) ->
    case ?MODULE:get(TransferId) of
        {ok, Doc} -> rerun(UserId, Doc);
        {error, Error} -> {error, Error}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns transfer document.
%% @end
%%-------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(TransferId) ->
    datastore_model:get(?CTX, TransferId).

%%-------------------------------------------------------------------
%% @doc
%% Deletes transfer document.
%% @end
%%-------------------------------------------------------------------
-spec delete(id()) -> ok.
delete(TransferId) ->
    {ok, Doc} = ?MODULE:get(TransferId),
    ok = transfer_links:delete_waiting_transfer_link(Doc),
    ok = transfer_links:delete_ongoing_transfer_link(Doc),
    ok = transfer_links:delete_ended_transfer_link(Doc),
    ok = transferred_file:report_transfer_deletion(Doc),
    ok = datastore_model:delete(?CTX, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Schedule cancellation of transfer. It is not possible for ended transfers.
%% @end
%%--------------------------------------------------------------------
-spec cancel(id()) -> ok | {error, term()}.
cancel(TransferId) ->
    Result = update(TransferId, fun(Transfer) ->
        case is_ended(Transfer) of
            true ->
                {error, already_ended};
            false ->
                {ok, Transfer#transfer{cancel = true}}
        end
    end),

    case Result of
        {ok, _} -> ok;
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Unset enqueued flag and delete transfer from waiting links tree.
%% @end
%%--------------------------------------------------------------------
-spec mark_dequeued(id()) -> {ok, doc()} | {error, term()}.
mark_dequeued(TransferId) ->
    %todo move to utils???
    update_and_run(
        TransferId,
        fun(Transfer) -> {ok, Transfer#transfer{enqueued = false}} end,
        fun transfer_links:delete_waiting_transfer_link/1
    ).


-spec set_controller_process(id()) -> {ok, doc()} | {error, term()}.
set_controller_process(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{pid = EncodedPid}}
    end).


-spec is_replication(transfer()) -> boolean().
is_replication(#transfer{evicting_provider = undefined} = Transfer) ->
    is_binary(Transfer#transfer.replicating_provider);
is_replication(#transfer{}) ->
    false.


-spec is_eviction(transfer()) -> boolean().
is_eviction(#transfer{replicating_provider = undefined} = Transfer) ->
    is_binary(Transfer#transfer.evicting_provider);
is_eviction(#transfer{}) ->
    false.


-spec is_migration(transfer()) -> boolean().
is_migration(#transfer{
    replicating_provider = ReplicatingProvider,
    evicting_provider = EvictingProviderId
}) ->
    is_binary(ReplicatingProvider) andalso is_binary(EvictingProviderId).


-spec is_ongoing(doc() | transfer() | id() | undefined) -> boolean().
is_ongoing(undefined) ->
    true;
is_ongoing(#document{value = Transfer}) ->
    is_ongoing(Transfer);
is_ongoing(Transfer = #transfer{}) ->
    is_replication_ongoing(Transfer) orelse is_eviction_ongoing(Transfer);
is_ongoing(TransferId) ->
    {ok, #document{value = Transfer}} = transfer:get(TransferId),
    is_ongoing(Transfer).


-spec is_replication_ongoing(transfer()) -> boolean().
is_replication_ongoing(#transfer{replication_status = scheduled}) -> true;
is_replication_ongoing(#transfer{replication_status = enqueued}) -> true;
is_replication_ongoing(#transfer{replication_status = active}) -> true;
is_replication_ongoing(#transfer{replication_status = _}) -> false.


-spec is_eviction_ongoing(transfer()) -> boolean().
is_eviction_ongoing(#transfer{eviction_status = scheduled}) -> true;
is_eviction_ongoing(#transfer{eviction_status = enqueued}) -> true;
is_eviction_ongoing(#transfer{eviction_status = active}) -> true;
is_eviction_ongoing(#transfer{eviction_status = _}) -> false.


-spec is_ended(transfer()) -> boolean().
is_ended(Transfer) ->
    is_replication_ended(Transfer) and is_eviction_ended(Transfer).


-spec is_replication_ended(transfer()) -> boolean().
is_replication_ended(#transfer{replication_status = completed}) -> true;
is_replication_ended(#transfer{replication_status = cancelled}) -> true;
is_replication_ended(#transfer{replication_status = skipped}) -> true;
is_replication_ended(#transfer{replication_status = failed}) -> true;
is_replication_ended(#transfer{replication_status = _}) -> false.


-spec is_eviction_ended(transfer()) -> boolean().
is_eviction_ended(#transfer{eviction_status = completed}) -> true;
is_eviction_ended(#transfer{eviction_status = cancelled}) -> true;
is_eviction_ended(#transfer{eviction_status = skipped}) -> true;
is_eviction_ended(#transfer{eviction_status = failed}) -> true;
is_eviction_ended(#transfer{eviction_status = _}) -> false.


-spec increment_files_to_process_counter(undefined | id(), non_neg_integer()) ->
    {ok, undefined | doc()} | {error, term()}.
increment_files_to_process_counter(undefined, _FilesNum) ->
    {ok, undefined};
increment_files_to_process_counter(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_to_process = Transfer#transfer.files_to_process + FilesNum
        }}
    end).


-spec increment_files_processed_counter(undefined | id()) ->
    {ok, undefined | doc()} | {error, term()}.
increment_files_processed_counter(undefined) ->
    {ok, undefined};
increment_files_processed_counter(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_processed = Transfer#transfer.files_processed + 1
        }}
    end).


-spec increment_files_evicted_and_processed_counters(undefined | id()) ->
    {ok, undefined | doc()} | {error, term()}.
increment_files_evicted_and_processed_counters(undefined) ->
    {ok, undefined};
increment_files_evicted_and_processed_counters(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_evicted = Transfer#transfer.files_evicted + 1,
            files_processed = Transfer#transfer.files_processed + 1
        }}
    end).


-spec increment_files_failed_and_processed_counters(id()) ->
    {ok, doc()} | {error, term()}.
increment_files_failed_and_processed_counters(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_processed = Transfer#transfer.files_processed + 1,
            failed_files = Transfer#transfer.failed_files + 1
        }}
    end).


-spec increment_files_replicated_counter(undefined | id()) ->
    {ok, undefined | doc()} | {error, term()}.
increment_files_replicated_counter(undefined) ->
    {ok, undefined};
increment_files_replicated_counter(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_replicated = Transfer#transfer.files_replicated + 1
        }}
    end).


%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'Bytes' bytes per provider.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_replication_finished(TransferId :: undefined | id(), od_space:id(),
    BytesPerProvider :: #{od_provider:id() => non_neg_integer()}
) ->
    {ok, undefined | doc()} | {error, term()}.
mark_data_replication_finished(undefined, SpaceId, BytesPerProvider) ->
    ok = space_transfer_stats:update_with_cache(
        ?ON_THE_FLY_TRANSFERS_TYPE, SpaceId, BytesPerProvider
    ),
    {ok, undefined};
mark_data_replication_finished(TransferId, SpaceId, BytesPerProvider) ->
    CurrentTime = provider_logic:zone_time_seconds(),
    ok = space_transfer_stats:update(
        ?JOB_TRANSFERS_TYPE, SpaceId, BytesPerProvider, CurrentTime
    ),

    BytesTransferred = maps:fold(
        fun(_, Bytes, Acc) -> Acc + Bytes end, 0, BytesPerProvider
    ),
    UpdateFun = fun(Transfer = #transfer{
        bytes_replicated = OldBytes,
        start_time = StartTime,
        last_update = LastUpdateMap,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        LastUpdates = lists:map(fun(ProviderId) ->
            maps:get(ProviderId, LastUpdateMap, StartTime)
        end, maps:keys(BytesPerProvider)),
        LatestLastUpdate = lists:max(LastUpdates),
        % Due to race between processes updating stats it is possible
        % for LatestLastUpdate to be larger than CurrentTime, also because
        % provider_logic:zone_time_seconds() caches zone time locally it is
        % possible for time of various provider nodes to differ by several
        % seconds.
        % So if the CurrentTime is less than LatestLastUpdate by no more than
        % 5 sec accept it and update latest slot, otherwise silently reject it
        case CurrentTime - LatestLastUpdate > -5 of
            false ->
                {ok, Transfer};
            true ->
                ApproxCurrentTime = max(CurrentTime, LatestLastUpdate),
                NewTimestamps = maps:map(
                    fun(_, _) -> ApproxCurrentTime end, BytesPerProvider),
                {ok, Transfer#transfer{
                    bytes_replicated = OldBytes + BytesTransferred,
                    last_update = maps:merge(LastUpdateMap, NewTimestamps),
                    min_hist = transfer_histograms:update(
                        BytesPerProvider, MinHistograms, ?MINUTE_STAT_TYPE,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    hr_hist = transfer_histograms:update(
                        BytesPerProvider, HrHistograms, ?HOUR_STAT_TYPE,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    dy_hist = transfer_histograms:update(
                        BytesPerProvider, DyHistograms, ?DAY_STAT_TYPE,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    mth_hist = transfer_histograms:update(
                        BytesPerProvider, MthHistograms, ?MONTH_STAT_TYPE,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    )
                }}
        end
    end,

    update(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_scheduled_transfers(SpaceId, 0, all).
%% @end
%%-------------------------------------------------------------------
-spec list_waiting_transfers(od_space:id()) ->
    {ok, [id()]}.
list_waiting_transfers(SpaceId) ->
    list_waiting_transfers(SpaceId, 0, all).

%%--------------------------------------------------------------------
%% @doc
%% list_scheduled_transfers(SpaceId, undefined, Offset, Limit).
%% @end
%%-------------------------------------------------------------------
-spec list_waiting_transfers(od_space:id(), integer(), list_limit()) ->
    {ok, [id()]}.
list_waiting_transfers(SpaceId, Offset, Limit) ->
    list_waiting_transfers(SpaceId, undefined, Offset, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Returns all transfers for given space that are scheduled.
%% @end
%%-------------------------------------------------------------------
-spec list_waiting_transfers(od_space:id(), undefined | id(),
    integer(), list_limit()) -> {ok, [id()]}.
list_waiting_transfers(SpaceId, StartId, Offset, Limit) ->
    {ok, transfer_links:list_links(SpaceId, ?WAITING_TRANSFERS_KEY,
        StartId, Offset, Limit)}.

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_active_transfers(SpaceId, 0, all).
%% @end
%%-------------------------------------------------------------------
-spec list_ongoing_transfers(od_space:id()) -> {ok, [id()]}.
list_ongoing_transfers(SpaceId) ->
    list_ongoing_transfers(SpaceId, 0, all).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_active_transfers(SpaceId, undefined, Offset, Limit).
%% @end
%%-------------------------------------------------------------------
-spec list_ongoing_transfers(od_space:id(), integer(), list_limit()) ->
    {ok, [id()]}.
list_ongoing_transfers(SpaceId, Offset, Limit) ->
    list_ongoing_transfers(SpaceId, undefined, Offset, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Returns all transfers for given space that are active.
%% @end
%%-------------------------------------------------------------------
-spec list_ongoing_transfers(od_space:id(), undefined | id(),
    integer(), list_limit()) -> {ok, [id()]}.
list_ongoing_transfers(SpaceId, StartId, Offset, Limit) ->
    {ok, transfer_links:list_links(SpaceId, ?ONGOING_TRANSFERS_KEY,
        StartId, Offset, Limit)}.

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_past_transfers(SpaceId, 0, all).
%% @end
%%-------------------------------------------------------------------
-spec list_ended_transfers(od_space:id()) -> {ok, [id()]}.
list_ended_transfers(SpaceId) ->
    list_ended_transfers(SpaceId, 0, all).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_past_transfers(SpaceId, undefined, Offset, Limit).
%% @end
%%-------------------------------------------------------------------
-spec list_ended_transfers(od_space:id(), integer(), list_limit()) ->
    {ok, [id()]}.
list_ended_transfers(SpaceId, Offset, Limit) ->
    list_ended_transfers(SpaceId, undefined, Offset, Limit).

%%--------------------------------------------------------------------
%% @doc
%% Returns all transfers for given space that are past.
%% @end
%%-------------------------------------------------------------------
-spec list_ended_transfers(od_space:id(), undefined | id(),
    integer(), list_limit()) -> {ok, [id()]}.
list_ended_transfers(SpaceId, StartId, Offset, Limit) ->
    {ok, transfer_links:list_links(SpaceId, ?ENDED_TRANSFERS_KEY,
        StartId, Offset, Limit)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns the link key for given transfer.
%% @end
%%-------------------------------------------------------------------
-spec get_link_key_by_state(id() | doc(), binary()) ->
    {ok, transfer_links:link_key()} | {error, term()}.
get_link_key_by_state(TransferId, TransferState) when is_binary(TransferId) ->
    case ?MODULE:get(TransferId) of
        {ok, Transfer} -> get_link_key_by_state(Transfer, TransferState);
        {error, Reason} -> {error, Reason}
    end;
get_link_key_by_state(#document{key = TransferId, value = Transfer}, TransferState) ->
    Timestamp = case TransferState of
        ?ENDED_TRANSFERS_STATE -> Transfer#transfer.finish_time;
        _ -> Transfer#transfer.schedule_time
    end,
    get_link_key(TransferId, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Returns the link key based on transfer id and schedule time.
%% @end
%%-------------------------------------------------------------------
-spec get_link_key(id(), timestamp()) ->
    {ok, transfer_links:link_key()} | {error, term()}.
get_link_key(TransferId, Timestamp) ->
    {ok, transfer_links:link_key(TransferId, Timestamp)}.

%%-------------------------------------------------------------------
%% @doc
%% Restarts worker pools used by replication and replica_eviction mechanisms.
%% @end
%%-------------------------------------------------------------------
-spec restart_pools() -> ok.
restart_pools() ->
    ok = stop_pools(),
    ok = start_pools().

-spec get_replication_status(doc() | transfer()) -> status().
get_replication_status(#transfer{replication_status = Status}) ->
    Status;
get_replication_status(#document{value = Transfer}) ->
    get_replication_status(Transfer).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates transfer.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    datastore_model:create(?CTX, Doc).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates transfer.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(TransferId, Diff) ->
    datastore_model:update(?CTX, TransferId, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Updates transfer doc and evaluates given code upon success.
%% @end
%%--------------------------------------------------------------------
-spec update_and_run(id(), diff(),
    fun((transfer()) -> ok)) -> {ok, doc()} | {error, term()}.
update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate) ->
    case transfer:update(TransferId, UpdateFun) of
        {ok, Doc} ->
            ok = OnSuccessfulUpdate(Doc),
            {ok, Doc};
        {error, Error} ->
            {error, Error}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function reruns given transfer (replication, migration or replica_eviction)
%% if possible.
%% @end
%%-------------------------------------------------------------------
maybe_rerun(Doc = #document{key = TransferId, value = Transfer}) ->
    SourceProviderId = Transfer#transfer.evicting_provider,
    TargetProviderId = Transfer#transfer.replicating_provider,
    SelfId = oneprovider:get_id(),

    IsReplicationAborting = Transfer#transfer.replication_status =:= aborting,
    IsEvictionAborting = Transfer#transfer.eviction_status =:= aborting,
    IsReplicationOngoing = is_replication_ongoing(Transfer),
    IsEvictionOngoing = is_eviction_ongoing(Transfer),

    case {
        IsReplicationOngoing, IsReplicationAborting,
        IsEvictionOngoing, IsEvictionAborting, SelfId
    } of
        {true, _, _, _, TargetProviderId} ->
            replication_status:handle_failed(TransferId, true),
            rerun(undefined, TransferId);
        {_, true, _, _, TargetProviderId} ->
            replication_status:handle_failed(TransferId, true),
            {ok, marked_failed};
        {_, _, true, _, SourceProviderId} ->
            replica_eviction_status:handle_failed(TransferId, true),
            rerun(undefined, TransferId);
        {_, _, _, true, SourceProviderId} ->
            replica_eviction_status:handle_failed(TransferId, true),
            {ok, marked_failed};
        {false, false, false, false, _} ->
            IsEviction = is_eviction(Transfer),
            IsReplication = is_replication(Transfer),
            IsMigration = is_migration(Transfer),
            case {IsEviction, IsReplication, IsMigration, SelfId} of
                % replica_eviction
                {true, false, false, SourceProviderId} ->
                    transfer_links:move_transfer_link_from_ongoing_to_ended(Doc),
                    {ok, moved_to_ended};
                % replication
                {false, true, false, TargetProviderId} ->
                    transfer_links:move_transfer_link_from_ongoing_to_ended(Doc),
                    {ok, moved_to_ended};
                % migration
                {false, false, true, TargetProviderId} ->
                    transfer_links:move_transfer_link_from_ongoing_to_ended(Doc),
                    {ok, moved_to_ended};
                {_, _, _, _} ->
                    {error, non_participating_provider}
            end;
        {_, _, _, _, _} ->
            {error, non_participating_provider}
    end;
maybe_rerun(TransferId) ->
    case ?MODULE:get(TransferId) of
        {ok, Doc} -> maybe_rerun(Doc);
        {error, Error} -> {error, Error}
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pools responsible for evicting and replicating
%% files and directories.
%% @end
%%-------------------------------------------------------------------
-spec start_pools() -> ok.
start_pools() ->
    {ok, _} = worker_pool:start_sup_pool(?REPLICATION_WORKERS_POOL, [
        {workers, ?REPLICATION_WORKERS_NUM},
        {worker, {?REPLICATION_WORKER, []}},
        {queue_type, lifo}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?REPLICATION_CONTROLLERS_POOL, [
        {workers, ?REPLICATION_CONTROLLERS_NUM},
        {worker, {?REPLICATION_CONTROLLER, []}}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?REPLICA_EVICTION_WORKERS_POOL, [
        {workers, ?REPLICA_EVICTION_WORKERS_NUM},
        {worker, {?REPLICA_EVICTION_WORKER, []}},
        {queue_type, lifo}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?REPLICA_DELETION_WORKERS_POOL, [
        {workers, ?REPLICA_DELETION_WORKERS_NUM},
        {worker, {?REPLICA_DELETION_WORKER, []}}
    ]),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops worker pools responsible for evicting or replicating
%% files and directories.
%% @end
%%-------------------------------------------------------------------
-spec stop_pools() -> ok.
stop_pools() ->
    ok = wpool:stop_sup_pool(?REPLICATION_WORKERS_POOL),
    ok = wpool:stop_sup_pool(?REPLICATION_CONTROLLERS_POOL),
    ok = wpool:stop_sup_pool(?REPLICA_EVICTION_WORKERS_POOL),
    ok = wpool:stop_sup_pool(?REPLICA_DELETION_WORKERS_POOL),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Posthook responsible for calling transfer_changes:handle_function
%% for locally updated document.
%% @end
%%-------------------------------------------------------------------
-spec run_on_transfer_doc_change(atom(), list(), term()) -> {ok, doc()}.
run_on_transfer_doc_change(update, [_, _, _], Result = {ok, Doc}) ->
    transfer_changes:handle(Doc),
    Result;
run_on_transfer_doc_change(_, _, Result) ->
    Result.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    8.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [
        fun run_on_transfer_doc_change/3
    ].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {path, string},
        {callback, string},
        {transfer_status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {start_time, integer},
        {last_update, integer},
        {min_hist, [integer]},
        {hr_hist, [integer]},
        {dy_hist, [integer]}
    ]};
get_record_struct(2) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {path, string},
        {callback, string},
        {transfer_status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {start_time, integer},
        {last_update, integer},
        {min_hist, [integer]},
        {hr_hist, [integer]},
        {dy_hist, [integer]}
    ]};
get_record_struct(3) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(4) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_transfer, integer},
        {files_transferred, integer},
        {failed_files, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(5) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(6) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_invalidated, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(7) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {status, atom},
        {invalidation_status, atom},
        {schedule_provider_id, string},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_transferred, integer},
        {bytes_transferred, integer},
        {files_invalidated, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]};
get_record_struct(8) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {user_id, string},
        {rerun_id, string},
        {path, string},
        {callback, string},
        {enqueued, atom},
        {cancel, atom},
        {replication_status, atom},
        {eviction_status, atom},
        {schedule_provider_id, string},
        {replicating_provider, string},
        {evicting_provider, string},
        {pid, string}, %todo VFS-3657
        {files_to_process, integer},
        {files_processed, integer},
        {failed_files, integer},
        {files_replicated, integer},
        {bytes_replicated, integer},
        {files_evicted, integer},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, StartTime, LastUpdate,
    MinHist, HrHist, DyHist}
) ->
    {2, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        0, 0, BytesToTransfer, BytesTransferred, StartTime, LastUpdate,
        MinHist, HrHist, DyHist
    }};
upgrade_record(2, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    FilesToInvalidate, FilesInvalidated, BytesToTransfer, BytesTransferred,
    StartTime, LastUpdate, MinHist, HrHist, DyHist}
) ->
    {3, {?MODULE, FileUuid, SpaceId, undefined, Path, CallBack, TransferStatus,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
        StartTime, LastUpdate,
        % There are three changes in histograms:
        %   1) They are now maps #{ProviderId => Histogram}, where ProviderId is
        %       the provider FROM which the amount of data expressed in the
        %       histogram was transferred.
        %   2) Histogram naming convention - minute histogram is now a histogram
        %       that SPANS OVER one minute, here with 5 seconds window.
        %       Other histograms are renamed analogically.
        %   3) LastUpdate must be remembered per provider to correctly keep
        %       track in histograms.
        % As there is no way to deduce source providers, older transfers will
        % only have one histogram accessible under target provider id.
        % last_update
        #{TargetProviderId => LastUpdate},
        % min_hist
        #{TargetProviderId => lists:duplicate(60 div ?FIVE_SEC_TIME_WINDOW, 0)},
        %hr_hist
        #{TargetProviderId => MinHist},
        % dy_hist
        #{TargetProviderId => HrHist},
        % mth_hist
        #{TargetProviderId => DyHist}
    }};
upgrade_record(3, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {4, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred, 0,
        BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
        StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(4, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred, FailedFiles,
    _BytesToTransfer, BytesTransferred, _FilesToInvalidate, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {5, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SourceProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
    }};
upgrade_record(5, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
    FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {6, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
        InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        StartTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
        MthHist
    }};
upgrade_record(6, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
    FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
    ScheduleTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
    MthHist
}) ->
    {7, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, true, Status,
        InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        ScheduleTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
        MthHist
    }};
upgrade_record(7, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Enqueued,
    Status, InvalidationStatus, SchedulingProviderId, SourceProviderId,
    TargetProviderId, _InvalidateSourceReplica, Pid, FilesToProcess,
    FilesProcessed, FailedFiles, FilesTransferred, BytesTransferred,
    FilesInvalidated, ScheduleTime, StartTime, FinishTime,
    LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {8, {?MODULE, FileUuid, SpaceId, UserId, undefined, Path, CallBack, Enqueued,
        false, Status, InvalidationStatus, SchedulingProviderId,
        TargetProviderId, SourceProviderId, Pid, FilesToProcess,
        FilesProcessed, FailedFiles, FilesTransferred, BytesTransferred,
        FilesInvalidated, ScheduleTime, StartTime, FinishTime,
        LastUpdate, MinHist, HrHist, DyHist, MthHist
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Provides custom resolution of remote, concurrent modification conflicts.
%% Should return 'default' if default conflict resolution should be applied.
%% Should return 'ignore' if new change is obsolete.
%% Should return '{Modified, Doc}' when custom conflict resolution has been
%% applied, where Modified defines whether next revision should be generated.
%% If Modified is set to 'false' conflict resolution outcome will be saved as
%% it is.
%% =============
%% This conflict resolution promotes the field enqueued = false in all cases
%% in order to avoid marking a transfer dequeued multiple times.
%% Also if transfer is still ongoing promotes the field cancel = true.
%% In case of finished and rerun transfer promotes rerun_id of newer doc.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, NewDoc, PreviousDoc) ->
    #document{value = PrevTransfer} = PreviousDoc,
    #document{value = NewTransfer} = NewDoc,

    PrevDocVec = {
        PrevTransfer#transfer.cancel,
        PrevTransfer#transfer.enqueued,
        PrevTransfer#transfer.rerun_id
    },
    NewDocVec = {
        NewTransfer#transfer.cancel,
        NewTransfer#transfer.enqueued,
        NewTransfer#transfer.rerun_id
    },
    {D1, D2} = order_transfers(PreviousDoc, NewDoc),

    case PrevDocVec == NewDocVec of
        true ->
            {false, D1};
        false ->
            #document{value = T1} = D1,
            #document{value = T2} = D2,

            EmergingTransfer = T1#transfer{
                enqueued = T1#transfer.enqueued and T2#transfer.enqueued,
                cancel = case is_ended(T1) of
                    true -> T1#transfer.cancel;
                    false -> T1#transfer.cancel or T2#transfer.cancel
                end,
                rerun_id = utils:ensure_defined(
                    T1#transfer.rerun_id, undefined, T2#transfer.rerun_id
                )
            },
            {true , D1#document{value = EmergingTransfer}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Compares 2 transfers given as args and returns them as tuple with first
%% element being the greater/newer one.
%% Fields being compared are in order: status, replica_eviction_status,
%% files_to_process, files_processed, files_replicated, bytes_replicated
%% and files_evicted.
%% Since only provider performing replication/replica_eviction modifies those
%% fields, all of them must be greater or equal when comparing one transfer to
%% the other.
%% @end
%%--------------------------------------------------------------------
-spec order_transfers(doc(), doc()) -> {doc(), doc()}.
order_transfers(D1, D2) ->
    #document{revs = [Rev1 | _], value = T1} = D1,
    #document{revs = [Rev2 | _], value = T2} = D2,
    IsGreaterRev = datastore_utils:is_greater_rev(Rev1, Rev2),

    Vec1 = {
        status_to_int(T1#transfer.replication_status),
        status_to_int(T1#transfer.eviction_status),
        T1#transfer.files_to_process, T1#transfer.files_processed,
        T1#transfer.failed_files, T1#transfer.files_replicated,
        T1#transfer.bytes_replicated, T1#transfer.files_evicted,
        T1#transfer.start_time, T1#transfer.finish_time, IsGreaterRev
    },

    Vec2 = {
        status_to_int(T2#transfer.replication_status),
        status_to_int(T2#transfer.eviction_status),
        T2#transfer.files_to_process, T2#transfer.files_processed,
        T2#transfer.failed_files, T2#transfer.files_replicated,
        T2#transfer.bytes_replicated, T2#transfer.files_evicted,
        T2#transfer.start_time, T2#transfer.finish_time, not IsGreaterRev
    },

    case Vec1 >= Vec2 of
        true -> {D1, D2};
        false -> {D2, D1}
    end.


-spec status_to_int(status()) -> integer().
status_to_int(scheduled) -> 0;
status_to_int(enqueued) -> 1;
status_to_int(active) -> 2;
status_to_int(completed) -> 3;
status_to_int(aborting) -> 4;
status_to_int(cancelled) -> 5;
status_to_int(failed) -> 6;
status_to_int(skipped) -> 7.
