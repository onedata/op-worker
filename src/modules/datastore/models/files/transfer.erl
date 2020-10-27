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
    start/8, get/1, get_effective/1, update/2, update_and_run/3, delete/1,
    cancel/1, rerun_ended/2
]).

-export([
    mark_dequeued/1, set_controller_process/1, set_rerun_id/2,

    is_replication/1, is_eviction/1, is_migration/1,
    type/1, data_source_type/1,
    is_ongoing/1, is_replication_ongoing/1, is_eviction_ongoing/1,
    is_ended/1, is_replication_ended/1, is_eviction_ended/1,

    replication_status/1, eviction_status/1, status/1,

    increment_files_to_process_counter/2, increment_files_processed_counter/1,
    increment_files_evicted_and_processed_counters/1,
    increment_files_failed_and_processed_counters/1,
    increment_files_replicated_counter/1, mark_data_replication_finished/3,

    rerun_not_ended_transfers/1
]).

% list functions
-export([
    list_waiting_transfers/1, list_waiting_transfers/3, list_waiting_transfers/4,
    list_ongoing_transfers/1, list_ongoing_transfers/3, list_ongoing_transfers/4,
    list_ended_transfers/1, list_ended_transfers/3, list_ended_transfers/4
]).

-export([get_link_key/2, get_link_key_by_state/2]).

%% datastore_model callbacks
-export([
    get_ctx/0, get_record_struct/1, get_posthooks/0, get_record_version/0,
    upgrade_record/2, resolve_conflict/3
]).

-type id() :: binary().
-type diff() :: datastore_doc:diff(transfer()).
% Status of transfer subtask - 'replication' or 'eviction' of data source
% (replication and eviction consist of only 1 subtask while migration has 2).
-type subtask_status() ::
    ?SCHEDULED_STATUS | ?ENQUEUED_STATUS | ?ACTIVE_STATUS | ?COMPLETED_STATUS |
    ?ABORTING_STATUS | ?FAILED_STATUS | ?CANCELLED_STATUS | ?SKIPPED_STATUS.
% Summarized transfer status calculated taking into account all subtask statuses.
-type transfer_status() ::
    ?SCHEDULED_STATUS | ?ENQUEUED_STATUS | ?REPLICATING_STATUS | ?EVICTING_STATUS |
    ?COMPLETED_STATUS | ?ABORTING_STATUS | ?FAILED_STATUS | ?CANCELLED_STATUS |
    ?SKIPPED_STATUS.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type type() :: replication | eviction | migration.
-type data_source_type() :: file | view.
-type doc() :: datastore_doc:doc(transfer()).
-type timestamp() :: non_neg_integer().
-type list_limit() :: non_neg_integer() | all.
-type view_name() :: undefined | index:key().
-type query_view_params() :: undefined | index:query_options() .

-export_type([
    id/0, transfer/0, type/0, data_source_type/0, subtask_status/0, callback/0, doc/0,
    timestamp/0, list_limit/0, view_name/0, query_view_params/0
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
    undefined | od_provider:id(), undefined | od_provider:id(), binary(),
    view_name(), query_view_params()) -> {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, SourceProviderId, TargetProviderId,
    Callback, IndexName, QueryViewParams
) ->
    {ok, UserId} = session:get_user_id(SessionId),
    start_for_user(UserId, FileGuid, FilePath, SourceProviderId,
        TargetProviderId, Callback, IndexName, QueryViewParams
    ).

%%--------------------------------------------------------------------
%% @doc
%% Starts the transfer for specified user id.
%% @end
%%--------------------------------------------------------------------
-spec start_for_user(od_user:id(), fslogic_worker:file_guid(),
    file_meta:path(), undefined | od_provider:id(), undefined | od_provider:id(),
    callback(), view_name(), query_view_params()) ->
    {ok, id()} | ignore | {error, Reason :: term()}.
start_for_user(UserId, FileGuid, FilePath, EvictingProviderId,
    ReplicatingProviderId, Callback, IndexName, QueryViewParams
) ->
    ReplicationStatus = case ReplicatingProviderId of
        undefined -> ?SKIPPED_STATUS;
        _ -> ?SCHEDULED_STATUS
    end,
    EvictionStatus = case EvictingProviderId of
        undefined -> ?SKIPPED_STATUS;
        _ -> ?SCHEDULED_STATUS
    end,
    ScheduleTime = time_utils:timestamp_seconds(),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    ToCreate = #document{
        scope = SpaceId,
        value = #transfer{
            file_uuid = file_id:guid_to_uuid(FileGuid),
            space_id = SpaceId,
            user_id = UserId,
            path = FilePath,
            callback = Callback,
            replication_status = ReplicationStatus,
            eviction_status = EvictionStatus,
            scheduling_provider = oneprovider:get_id(),
            evicting_provider = EvictingProviderId,
            replicating_provider = ReplicatingProviderId,
            schedule_time = ScheduleTime,
            start_time = 0,
            finish_time = 0,
            last_update = #{},
            min_hist = #{},
            hr_hist = #{},
            dy_hist = #{},
            mth_hist = #{},
            index_name = IndexName,
            query_view_params = QueryViewParams
        }},

    {ok, Doc = #document{key = TransferId}} = create(ToCreate),
    transfer_links:add_waiting(Doc),
    transfer_changes:handle(Doc),
    {ok, TransferId}.


%%-------------------------------------------------------------------
%% @doc
%% Traverses waiting and ongoing links in search of transfers targeting local
%% provider. If they were still ongoing marks them as failed and reruns.
%% Otherwise does not rerun them but move to ended links and mark as failed
%% if necessary.
%% This function should be called only once after provider restart.
%% @end
%%-------------------------------------------------------------------
-spec rerun_not_ended_transfers(od_space:id()) -> [id()].
rerun_not_ended_transfers(SpaceId) ->
    {ok, WaitingTransferIds} = list_waiting_transfers(SpaceId),
    {ok, OngoingTransferIds} = list_ongoing_transfers(SpaceId),

    Reruns = lists:foldl(fun(TransferId, CurrReruns) ->
        case maybe_rerun(TransferId) of
            skip ->
                CurrReruns;
            {ok, NewTransferId} ->
                CurrReruns#{TransferId => NewTransferId};
            {error, Reason} ->
                ?error("Failed to rerun transfer ~p due to: ~p", [
                    TransferId, Reason
                ]),
                CurrReruns
        end
    end, #{}, lists:usort(WaitingTransferIds ++ OngoingTransferIds)),

    case map_size(Reruns) of
        0 ->
            ok;
        RerunsNum ->
            ?info("Space ~p - ~p unfinished transfers has been rerun:~n~p", [
                SpaceId, RerunsNum, Reruns
            ])
    end,
    maps:values(Reruns).


-spec rerun_ended(undefined | od_user:id(), doc() | id()) ->
    {ok, id()} | {error, term()}.
rerun_ended(UserId, TransferDoc) ->
    rerun_ended(UserId, TransferDoc, false).

-spec rerun_ended(undefined | od_user:id(), doc() | id(), boolean()) ->
    {ok, id()} | {error, term()}.
rerun_ended(UserId, #document{key = TransferId, value = Transfer}, MarkTransferFailed) ->
    case is_ended(Transfer) orelse MarkTransferFailed of
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
                callback = Callback,
                index_name = IndexName,
                query_view_params = QueryViewParams
            } = Transfer,

            NewUserId = utils:ensure_defined(UserId, OldUserId),
            FileGuid = file_id:pack_guid(FileUuid, SpaceId),

            {ok, NewTransferId} = start_for_user(NewUserId, FileGuid, FilePath,
                EvictingProviderId, ReplicatingProviderId, Callback, IndexName,
                QueryViewParams
            ),

            IsReplicationOngoing = is_replication_ongoing(Transfer),
            IsEvictionOngoing = is_eviction_ongoing(Transfer),
            case {IsReplicationOngoing, IsEvictionOngoing} of
                {true, _} ->    % replication or first phase of migration
                    replication_status:handle_restart(TransferId, NewTransferId, MarkTransferFailed);
                {false, true} -> % eviction or second phase of migration
                    replica_eviction_status:handle_restart(TransferId, NewTransferId, MarkTransferFailed);
                {false, false} ->
                    set_rerun_id(TransferId, NewTransferId)
            end,

            {ok, NewTransferId}
    end;
rerun_ended(UserId, TransferId, MarkTransferFailed) ->
    case ?MODULE:get(TransferId) of
        {ok, Doc} -> rerun_ended(UserId, Doc, MarkTransferFailed);
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
%% Returns effective transfer document, that is document of transfer after
%% following all `rerun_id` links (it is filled if transfer was rerun).
%% @end
%%-------------------------------------------------------------------
-spec get_effective(id()) -> {ok, doc()} | {error, term()}.
get_effective(TransferId) ->
    case datastore_model:get(?CTX, TransferId) of
        {ok, #document{value = #transfer{rerun_id = undefined}}} = Res ->
            Res;
        {ok, #document{value = #transfer{rerun_id = NextJobTransferId}}} ->
            get_effective(NextJobTransferId);
        {error, _} = Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Deletes transfer document.
%% @end
%%-------------------------------------------------------------------
-spec delete(id()) -> ok.
delete(TransferId) ->
    {ok, Doc} = ?MODULE:get(TransferId),
    ok = transfer_links:delete_waiting(Doc),
    ok = transfer_links:delete_ongoing(Doc),
    ok = transfer_links:delete_ended(Doc),
    ok = transferred_file:report_transfer_deletion(Doc),
    ok = datastore_model:delete(?CTX, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Schedules cancellation of transfer. It is not possible for ended transfers.
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
    update_and_run(
        TransferId,
        fun(Transfer) -> {ok, Transfer#transfer{enqueued = false}} end,
        fun transfer_links:delete_waiting/1
    ).


-spec set_controller_process(id()) -> {ok, doc()} | {error, term()}.
set_controller_process(TransferId) ->
    EncodedPid = transfer_utils:encode_pid(self()),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{pid = EncodedPid}}
    end).


-spec set_rerun_id(transfer:id(), transfer:id()) -> {ok, transfer:doc()} | {error, term()}.
set_rerun_id(TransferId, NewTransferId) ->
    transfer:update(TransferId, fun(OldTransfer) ->
        {ok, OldTransfer#transfer{rerun_id = NewTransferId}}
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
is_migration(#transfer{replicating_provider = undefined}) -> false;
is_migration(#transfer{evicting_provider = undefined}) -> false;
is_migration(_) -> true.


-spec type(transfer()) -> type().
type(#transfer{replicating_provider = <<_/binary>>, evicting_provider = undefined}) ->
    replication;
type(#transfer{replicating_provider = undefined, evicting_provider = <<_/binary>>}) ->
    eviction;
type(#transfer{replicating_provider = <<_/binary>>, evicting_provider = <<_/binary>>}) ->
    migration.


-spec data_source_type(transfer()) -> data_source_type().
data_source_type(#transfer{index_name = undefined}) ->
    file;
data_source_type(_) ->
    view.


-spec is_ongoing(doc() | transfer() | id() | undefined) -> boolean().
is_ongoing(undefined) ->
    true;
is_ongoing(Transfer = #transfer{}) ->
    is_replication_ongoing(Transfer) orelse is_eviction_ongoing(Transfer);
is_ongoing(TransferId) ->
    {ok, #document{value = Transfer}} = transfer:get(TransferId),
    is_ongoing(Transfer).


-spec is_replication_ongoing(transfer()) -> boolean().
is_replication_ongoing(#transfer{replication_status = ?SCHEDULED_STATUS}) -> true;
is_replication_ongoing(#transfer{replication_status = ?ENQUEUED_STATUS}) -> true;
is_replication_ongoing(#transfer{replication_status = ?ACTIVE_STATUS}) -> true;
is_replication_ongoing(#transfer{replication_status = _}) -> false.

-spec is_eviction_ongoing(transfer()) -> boolean().
is_eviction_ongoing(#transfer{eviction_status = ?SCHEDULED_STATUS}) -> true;
is_eviction_ongoing(#transfer{eviction_status = ?ENQUEUED_STATUS}) -> true;
is_eviction_ongoing(#transfer{eviction_status = ?ACTIVE_STATUS}) -> true;
is_eviction_ongoing(#transfer{eviction_status = _}) -> false.


-spec is_ended(transfer()) -> boolean().
is_ended(Transfer) ->
    is_replication_ended(Transfer) and is_eviction_ended(Transfer).


-spec is_replication_ended(transfer()) -> boolean().
is_replication_ended(#transfer{replication_status = ?COMPLETED_STATUS}) -> true;
is_replication_ended(#transfer{replication_status = ?CANCELLED_STATUS}) -> true;
is_replication_ended(#transfer{replication_status = ?SKIPPED_STATUS}) -> true;
is_replication_ended(#transfer{replication_status = ?FAILED_STATUS}) -> true;
is_replication_ended(#transfer{replication_status = _}) -> false.


-spec is_eviction_ended(transfer()) -> boolean().
is_eviction_ended(#transfer{eviction_status = ?COMPLETED_STATUS}) -> true;
is_eviction_ended(#transfer{eviction_status = ?CANCELLED_STATUS}) -> true;
is_eviction_ended(#transfer{eviction_status = ?SKIPPED_STATUS}) -> true;
is_eviction_ended(#transfer{eviction_status = ?FAILED_STATUS}) -> true;
is_eviction_ended(#transfer{eviction_status = _}) -> false.


-spec replication_status(transfer()) -> subtask_status().
replication_status(#transfer{replication_status = Status}) -> Status.


-spec eviction_status(transfer()) -> subtask_status().
eviction_status(#transfer{eviction_status = Status}) -> Status.


%%--------------------------------------------------------------------
%% @doc
%% Returns status of given transfer. Replaces 'active' subtask status with
%% 'replicating' for replication and 'evicting' for eviction.
%% In case of migration 'evicting' indicates that the replication itself has
%% finished, but source replica eviction is still in progress.
%% @end
%%--------------------------------------------------------------------
-spec status(transfer()) -> transfer_status().
status(T = #transfer{
    replication_status = ?COMPLETED_STATUS,
    replicating_provider = P1,
    evicting_provider = P2
}) when is_binary(P1) andalso is_binary(P2) ->
    case T#transfer.eviction_status of
        ?SCHEDULED_STATUS -> ?EVICTING_STATUS;
        ?ENQUEUED_STATUS -> ?EVICTING_STATUS;
        ?ACTIVE_STATUS -> ?EVICTING_STATUS;
        Status -> Status
    end;
status(T = #transfer{replication_status = ?SKIPPED_STATUS}) ->
    case T#transfer.eviction_status of
        ?ACTIVE_STATUS -> ?EVICTING_STATUS;
        Status -> Status
    end;
status(#transfer{replication_status = ?ACTIVE_STATUS}) ->
    ?REPLICATING_STATUS;
status(#transfer{replication_status = Status}) ->
    Status.


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
    CurrentTime = time_utils:timestamp_seconds(),
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
        % time_utils:timestamp_seconds() caches zone time locally it is
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
                        BytesPerProvider, MinHistograms, ?MINUTE_PERIOD,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    hr_hist = transfer_histograms:update(
                        BytesPerProvider, HrHistograms, ?HOUR_PERIOD,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    dy_hist = transfer_histograms:update(
                        BytesPerProvider, DyHistograms, ?DAY_PERIOD,
                        LastUpdateMap, StartTime, ApproxCurrentTime
                    ),
                    mth_hist = transfer_histograms:update(
                        BytesPerProvider, MthHistograms, ?MONTH_PERIOD,
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
    {ok, transfer_links:list(SpaceId, ?WAITING_TRANSFERS_KEY,
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
    {ok, transfer_links:list(SpaceId, ?ONGOING_TRANSFERS_KEY,
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
    {ok, transfer_links:list(SpaceId, ?ENDED_TRANSFERS_KEY,
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
%% This function reruns given transfer (replication, migration or
%% replica_eviction) if possible. Otherwise marks it as failed (transfers which
%% were being aborted) or moves to ended (transfers already ended but kept in
%% ongoing link tree).
%% @end
%%-------------------------------------------------------------------
-spec maybe_rerun(doc()) ->
    skip | {ok, id()} | {error, term()}.
maybe_rerun(Doc = #document{key = TransferId, value = Transfer}) ->
    SourceProviderId = Transfer#transfer.evicting_provider,
    TargetProviderId = Transfer#transfer.replicating_provider,
    SchedulingProviderId = Transfer#transfer.scheduling_provider,
    SelfId = oneprovider:get_id(),

    IsReplicationAborting = Transfer#transfer.replication_status =:= ?ABORTING_STATUS,
    IsEvictionAborting = Transfer#transfer.eviction_status =:= ?ABORTING_STATUS,
    IsReplicationOngoing = is_replication_ongoing(Transfer),
    IsEvictionOngoing = is_eviction_ongoing(Transfer),

    case {
        IsReplicationOngoing, IsReplicationAborting,
        IsEvictionOngoing, IsEvictionAborting, SelfId
    } of
        {true, _, _, _, TargetProviderId} ->
            % it is replication of first step of migration
            rerun_ended(undefined, Doc, true);
        {_, true, _, _, TargetProviderId} ->
            replication_status:handle_failed(TransferId, true),
            skip;
        {true, _, true, _, SourceProviderId} ->
            % it is migration and first step hasn't been finished yet
            skip;
        {_, true, true, _, SourceProviderId} ->
            % it is migration and first step hasn't been finished yet
            skip;
        {_, _, true, _, SourceProviderId} ->
            rerun_ended(undefined, Doc, true);
        {_, _, _, true, SourceProviderId} ->
            replica_eviction_status:handle_failed(TransferId, true),
            skip;
        {false, false, false, false, _} ->
            IsEviction = is_eviction(Transfer),
            IsReplication = is_replication(Transfer),
            IsMigration = is_migration(Transfer),
            case {IsEviction, IsReplication, IsMigration, SelfId} of
                % replica_eviction
                {true, false, false, SourceProviderId} ->
                    transfer_links:move_from_ongoing_to_ended(Doc),
                    skip;
                % replication
                {false, true, false, TargetProviderId} ->
                    transfer_links:move_from_ongoing_to_ended(Doc),
                    skip;
                % migration
                {false, false, true, TargetProviderId} ->
                    transfer_links:move_from_ongoing_to_ended(Doc),
                    skip;
                {_, _, _, SchedulingProviderId} ->
                    % ensure that ended transfer, that was scheduled by this provider is deleted from scheduled tree
                    transfer_links:delete_waiting(Doc),
                    skip;
                {_, _, _, _} ->
                    skip
            end;
        {_, _, _, _, _} ->
            {ok, non_participating_provider}
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
        {worker, {gen_transfer_worker, [?REPLICATION_WORKER]}},
        {queue_type, lifo}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?REPLICATION_CONTROLLERS_POOL, [
        {workers, ?REPLICATION_CONTROLLERS_NUM},
        {worker, {?REPLICATION_CONTROLLER, []}}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?REPLICA_EVICTION_WORKERS_POOL, [
        {workers, ?REPLICA_EVICTION_WORKERS_NUM},
        {worker, {gen_transfer_worker, [?REPLICA_EVICTION_WORKER]}},
        {queue_type, lifo}
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
    ok = wpool:stop_sup_pool(?REPLICA_EVICTION_WORKERS_POOL).

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
    10.

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
get_record_struct(Version) ->
    transfer_model:get_record_struct(Version).

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(Version, Record) ->
    transfer_model:upgrade_record(Version, Record).

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
            {true, D1#document{value = EmergingTransfer}}
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
    IsGreaterRev = datastore_rev:is_greater(Rev1, Rev2),

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


-spec status_to_int(subtask_status()) -> integer().
status_to_int(?SCHEDULED_STATUS) -> 0;
status_to_int(?ENQUEUED_STATUS) -> 1;
status_to_int(?ACTIVE_STATUS) -> 2;
status_to_int(?COMPLETED_STATUS) -> 3;
status_to_int(?ABORTING_STATUS) -> 4;
status_to_int(?CANCELLED_STATUS) -> 5;
status_to_int(?FAILED_STATUS) -> 6;
status_to_int(?SKIPPED_STATUS) -> 7.
