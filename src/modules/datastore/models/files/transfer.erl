%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about ongoing transfer. Creation of doc works as a
%%% trigger for starting a transfer or replica invalidation.
%%% We distinguish 3 types of transfers:
%%%     - replication
%%%     - invalidation
%%%     - migration (invalidation preceded by replication)
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
-export([start/7, cancel/1, get_info/1, get/1, init/0, cleanup/0, restart/1,
    delete/1, update/2]).

-export([mark_enqueued/1, mark_dequeued/1, mark_completed/1, mark_failed/1, mark_cancelled/1,
    mark_active_invalidation/1, mark_completed_invalidation/1,
    mark_failed_invalidation/1, mark_cancelled_invalidation/1,
    increase_files_to_process_counter/2, increase_files_processed_counter/1,
    mark_failed_file_processing/1, increase_files_transferred_counter/1,
    mark_data_transfer_finished/3, increase_files_invalidated_counter/1,
    restart_unfinished_transfers/1]).

% list functions
-export([
    list_waiting_transfers/1, list_waiting_transfers/3, list_waiting_transfers/4,
    list_ongoing_transfers/1, list_ongoing_transfers/3, list_ongoing_transfers/4,
    list_ended_transfers/1, list_ended_transfers/3, list_ended_transfers/4]).

-export([get_link_key/1, get_link_key/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_posthooks/0, get_record_version/0,
    upgrade_record/2, resolve_conflict/3]).

-type id() :: binary().
-type diff() :: datastore:diff(transfer()).
-type status() :: scheduled | enqueued | active | completed | failed | cancelled | skipped.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type doc() :: datastore_doc:doc(transfer()).
-type timestamp() :: non_neg_integer().
-type list_limit() :: non_neg_integer() | all.

-export_type([id/0, transfer/0, status/0, callback/0, doc/0, timestamp/0,
    list_limit/0]).

-define(MAX_FILE_TRANSFER_FAILURES_PER_TRANSFER,
    application:get_env(?APP_NAME, max_file_transfer_failures_per_transfer, 10)).

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
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start(session:id(), fslogic_worker:file_guid(), file_meta:path(),
    undefined | od_provider:id(), undefined | od_provider:id(), binary(),
    boolean()) -> {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, SourceProviderId, TargetProviderId,
    Callback, InvalidateSourceReplica
) ->
    TransferStatus = case TargetProviderId of
        undefined ->
            skipped;
        _ ->
            scheduled
    end,
    InvalidationStatus = case InvalidateSourceReplica of
        true ->
            scheduled;
        false ->
            skipped
    end,
    ScheduleTime = provider_logic:zone_time_seconds(),
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),
    {ok, UserId} = session:get_user_id(SessionId),
    ToCreate = #document{
        scope = fslogic_uuid:guid_to_space_id(FileGuid),
        value = #transfer{
            file_uuid = fslogic_uuid:guid_to_uuid(FileGuid),
            space_id = SpaceId,
            user_id = UserId,
            path = FilePath,
            callback = Callback,
            status = TransferStatus,
            invalidation_status = InvalidationStatus,
            scheduling_provider_id = oneprovider:get_id(),
            source_provider_id = SourceProviderId,
            target_provider_id = TargetProviderId,
            invalidate_source_replica = InvalidateSourceReplica,
            schedule_time = ScheduleTime,
            start_time = 0,
            finish_time = 0,
            last_update = #{},
            min_hist = #{},
            hr_hist = #{},
            dy_hist = #{},
            mth_hist = #{}

        }},
    {ok, #document{key = TransferId}} = create(ToCreate),
    ok = transfer_links:add_waiting_transfer_link(TransferId, SpaceId, ScheduleTime),
    transfer_changes:handle(ToCreate#document{key = TransferId}),
    transferred_file:report_transfer_start(FileGuid, TransferId, ScheduleTime),
    {ok, TransferId}.

%%-------------------------------------------------------------------
%% @doc
%% Restarts all unfinished transfers.
%% @end
%%-------------------------------------------------------------------
-spec restart_unfinished_transfers(od_space:id()) -> [id()].
restart_unfinished_transfers(SpaceId) ->
    {ok, OngoingTransferIds} = list_ongoing_transfers(SpaceId),
    {Restarted, Failed} = lists:foldl(
        fun(TransferId, {Restarted0, Failed0}) ->
            case restart(TransferId) of
                {ok, TransferId} ->
                    {[TransferId | Restarted0], Failed0};
                {error, not_target_provider} ->
                    {Restarted0, Failed0};
                {error, not_source_provider} ->
                    {Restarted0, Failed0};
                {error, not_found} ->
                    {Restarted0, [TransferId | Failed0]}
            end
        end, {[], []}, OngoingTransferIds),

    case Restarted of
        [] -> ok;
        _ ->
            ?info("Restarted transfers ~p in space ~p", [Restarted, SpaceId])
    end,
    remove_unfinished_transfers_links(Failed, SpaceId),
    Restarted.

%%-------------------------------------------------------------------
%% @doc
%% Restarts transfer referenced by given TransferId.
%% @end
%%-------------------------------------------------------------------
-spec restart(id()) -> {ok, id()} | {error, term()}.
restart(TransferId) ->
    FinishTime = transfer_utils:get_finish_time(TransferId),
    case update(TransferId, fun maybe_restart/1) of
        {ok, #document{value = #transfer{
            file_uuid = FileUuid,
            space_id = SpaceId,
            schedule_time = NewScheduleTime
        }}} ->
            move_from_past_to_current_links_tree(TransferId, SpaceId,
                FinishTime, NewScheduleTime),
            transferred_file:report_transfer_start(
                fslogic_uuid:uuid_to_guid(FileUuid, SpaceId), TransferId, NewScheduleTime
            ),
            {ok, TransferId};
        {error, not_target_provider} ->
            {error, not_target_provider};
        {error, not_source_provider} ->
            {error, not_source_provider};
        Error ->
            ?error_stacktrace("Restarting transfer ~p failed due to ~p",
                [TransferId, Error]),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets transfer info
%% @end
%%--------------------------------------------------------------------
-spec get_info(TransferId :: id()) -> maps:map().
get_info(TransferId) ->
    transfer_utils:get_info(TransferId).

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
%% Returns transfer document.
%% @end
%%-------------------------------------------------------------------
-spec delete(id()) -> ok.
delete(TransferId) ->
    {ok, #document{value = #transfer{space_id = SpaceId}}} = ?MODULE:get(TransferId),
    {ok, #document{value = #transfer{
        space_id = SpaceId,
        schedule_time = ScheduleTime,
        finish_time = FinishTime
    }}} = ?MODULE:get(TransferId),
    ok = transfer_links:delete_waiting_transfer_link(TransferId, SpaceId, ScheduleTime),
    ok = transfer_links:delete_ongoing_transfer_link(TransferId, SpaceId, ScheduleTime),
    ok = transfer_links:delete_ended_transfer_link(TransferId, SpaceId, FinishTime),
    ok = datastore_model:delete(?CTX, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec cancel(id()) -> ok | {error, term()}.
cancel(TransferId) ->
    %todo mark_cancelling VFS-3990
    {ok, _} = mark_cancelled(TransferId),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as enqueued and sets number of files to transfer to 1.
%% @end
%%--------------------------------------------------------------------
-spec mark_enqueued(id()) -> {ok, id()} | {error, term()}.
mark_enqueued(TransferId) ->
    Pid = transfer_utils:encode_pid(self()),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            status = enqueued,
            start_time = provider_logic:zone_time_seconds(),
            files_to_process = 1,
            pid = Pid
        }}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Marks transfer as active and adds it to active transfers list.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(id(), od_space:id(), transfer:timestamp()) -> {ok, id()} | {error, term()}.
mark_active(TransferId, SpaceId, ScheduleTime) ->
    ok = transfer_links:add_ongoing_transfer_link(TransferId, SpaceId, ScheduleTime),
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{status = active}}
    end,
    case update(TransferId, UpdateFun) of
        {ok, _} ->
            {ok, TransferId};
        Error ->
            ok = transfer_links:delete_ongoing_transfer_link(TransferId, SpaceId, ScheduleTime),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as dequeued and removes it from scheduled transfers list.
%% @end
%%--------------------------------------------------------------------
-spec mark_dequeued(id()) -> {ok, id()} | {error, term()}.
mark_dequeued(TransferId) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{enqueued = false}}
    end,
    OnSuccessfulUpdate = fun(#transfer{space_id = SpaceId, schedule_time = ScheduleTime}) ->
        ok = transfer_links:delete_waiting_transfer_link(TransferId,
            SpaceId, ScheduleTime)
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed(id()) -> {ok, id()} | {error, term()}.
mark_completed(TransferId) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{
            status = completed,
            finish_time = case transfer_utils:is_migration(Transfer) of
                true -> 0;
                false -> provider_logic:zone_time_seconds()
            end
        }}
    end,
    OnSuccessfulUpdate = fun(Transfer) ->
        case transfer_utils:is_migration(Transfer) of
            false ->
                move_to_finished(TransferId, Transfer);
            true ->
                ok
        end
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).

%%--------------------------------------------------------------------
%% @doc
%% Marks status in transfer (replication, migration, invalidation)
%% document as failed.
%% If given document describes migration transfer,
%% invalidation_status is also marked as failed.
%% @end
%%--------------------------------------------------------------------
-spec mark_failed(id()) -> {ok, id()} | {error, term()}.
mark_failed(TransferId) ->
    UpdateFun = fun(T = #transfer{invalidation_status = InvalidationStatus}) ->
        {ok, T#transfer{
            status = failed,
            finish_time = provider_logic:zone_time_seconds(),
            invalidation_status = case transfer_utils:is_invalidation(T) of
                true ->
                    failed;
                _ ->
                    InvalidationStatus
            end}}
    end,
    update_and_move_to_finished(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as cancelled.
%% @end
%%--------------------------------------------------------------------
-spec mark_cancelled(id()) -> {ok, id()} | {error, term()}.
mark_cancelled(TransferId) ->
    UpdateFun = fun(T = #transfer{invalidation_status = InvalidationStatus}) ->
        {ok, T#transfer{
            status = cancelled,
            finish_time = provider_logic:zone_time_seconds(),
            invalidation_status = case transfer_utils:is_invalidation(T) of
                true ->
                    cancelled;
                _ ->
                    InvalidationStatus
            end}
        }
    end,
    update_and_move_to_finished(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as active.
%% @end
%%--------------------------------------------------------------------
-spec mark_active_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_active_invalidation(TransferId) ->
    Pid = transfer_utils:encode_pid(self()),
    {ok, _} = ?extract_key(update(TransferId, fun(Transfer) ->
        StartTime = case Transfer#transfer.start_time of
            0 -> provider_logic:zone_time_seconds();
            Timestamp -> Timestamp
        end,
        {ok, Transfer#transfer{
            invalidation_status = active,
            files_to_process = Transfer#transfer.files_to_process + 1,
            pid = Pid,
            start_time = StartTime
        }}
    end)).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_completed_invalidation(TransferId) ->
    UpdateFun = fun(T) ->
        {ok, T#transfer{
            invalidation_status = completed,
            finish_time = provider_logic:zone_time_seconds()
        }}
    end,
    update_and_move_to_finished(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as failed
%% @end
%%--------------------------------------------------------------------
-spec mark_failed_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_failed_invalidation(TransferId) ->
    UpdateFun = fun(T) ->
        {ok, T#transfer{
            invalidation_status = failed,
            finish_time = provider_logic:zone_time_seconds()
        }}
    end,
    update_and_move_to_finished(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as cancelled
%% @end
%%--------------------------------------------------------------------
-spec mark_cancelled_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_cancelled_invalidation(TransferId) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{
            invalidation_status = cancelled,
            finish_time = provider_logic:zone_time_seconds()}
        }
    end,
    update_and_move_to_finished(TransferId, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'FilesNum' files are scheduled to be processed.
%% @end
%%--------------------------------------------------------------------
-spec increase_files_to_process_counter(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
increase_files_to_process_counter(undefined, _FilesNum) ->
    {ok, undefined};
increase_files_to_process_counter(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_to_process = Transfer#transfer.files_to_process + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'FilesNum' files.
%% @end
%%--------------------------------------------------------------------
-spec increase_files_processed_counter(undefined | id()) ->
    {ok, undefined | id()} | {error, term()}.
increase_files_processed_counter(undefined) ->
    {ok, undefined};
increase_files_processed_counter(TransferId) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{
            files_processed = Transfer#transfer.files_processed + 1
        }}
    end,
    OnSuccessfulUpdate = fun(Transfer) ->
        case Transfer of
            #transfer{status = enqueued, files_processed = 1, bytes_transferred = 0,
                space_id = SpaceId, schedule_time = ScheduleTime
            } ->
                % Done only once per transfer, when first bytes are sent or when the
                % first file is processed.
                mark_active(TransferId, SpaceId, ScheduleTime),
                ok;
            _ ->
                ok
        end
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).

%%--------------------------------------------------------------------
%% @doc
%% Increase failed_files counter
%% @end
%%--------------------------------------------------------------------
-spec mark_failed_file_processing(id()) -> {ok, id()} | {error, term()}.
mark_failed_file_processing(TransferId) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{
            files_processed = Transfer#transfer.files_processed + 1,
            failed_files = Transfer#transfer.failed_files + 1
        }}
    end,
    OnSuccessfulUpdate = fun(Transfer) ->
        case Transfer of
            #transfer{
                status = enqueued, files_processed = 1, bytes_transferred = 0,
                space_id = SpaceId, schedule_time = ScheduleTime
            } ->
                % Done only once per transfer, when first bytes are sent or when the
                % first file is processed.
                mark_active(TransferId, SpaceId, ScheduleTime),
                ok;
            _ ->
                ok
        end
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).


%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'FilesNum' files.
%% @end
%%--------------------------------------------------------------------
-spec increase_files_transferred_counter(undefined | id()) ->
    {ok, undefined | id()} | {error, term()}.
increase_files_transferred_counter(undefined) ->
    {ok, undefined};
increase_files_transferred_counter(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_transferred = Transfer#transfer.files_transferred + 1
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful invalidation of 'FilesNum' files.
%% If files_to_invalidate counter equals files_invalidated, invalidation
%% transfer is marked as finished.
%% @end
%%--------------------------------------------------------------------
-spec increase_files_invalidated_counter(undefined | id()) ->
    {ok, undefined | id()} | {error, term()}.
increase_files_invalidated_counter(undefined) ->
    {ok, undefined};
increase_files_invalidated_counter(TransferId) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_invalidated = Transfer#transfer.files_invalidated + 1
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'Bytes' bytes per provider.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_finished(TransferId :: undefined | id(), od_space:id(),
    BytesPerProvider :: #{od_provider:id() => non_neg_integer()}
) ->
    {ok, undefined | id()} | {error, term()}.
mark_data_transfer_finished(undefined, SpaceId, BytesPerProvider) ->
    ok = space_transfer_stats:update_with_cache(
        ?ON_THE_FLY_TRANSFERS_TYPE, SpaceId, BytesPerProvider
    ),
    {ok, undefined};
mark_data_transfer_finished(TransferId, SpaceId, BytesPerProvider) ->
    CurrentTime = provider_logic:zone_time_seconds(),
    ok = space_transfer_stats:update(
        ?JOB_TRANSFERS_TYPE, SpaceId, BytesPerProvider, CurrentTime
    ),

    BytesTransferred = maps:fold(
        fun(_, Bytes, Acc) -> Acc + Bytes end, 0, BytesPerProvider
    ),
    UpdateFun = fun(Transfer = #transfer{
        bytes_transferred = OldBytes,
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
                    bytes_transferred = OldBytes + BytesTransferred,
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

    OnSuccessfulUpdate = fun(Transfer) ->
        case Transfer of
            #transfer{
                status = enqueued,
                files_processed = 0, bytes_transferred = BytesTransferred,
                space_id = SpaceId, schedule_time = ScheduleTime
            } ->
                % Done only once per transfer, when first bytes are sent or when the
                % first file is processed.
                mark_active(TransferId, SpaceId, ScheduleTime),
                ok;
            _ ->
                ok
        end
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).

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
    {ok, transfer_links:list_transfers(SpaceId, ?WAITING_TRANSFERS_KEY,
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
    {ok, transfer_links:list_transfers(SpaceId, ?ONGOING_TRANSFERS_KEY,
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
    {ok, transfer_links:list_transfers(SpaceId, ?ENDED_TRANSFERS_KEY,
        StartId, Offset, Limit)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns the link key for given transfer.
%% @end
%%-------------------------------------------------------------------
-spec get_link_key(id() | doc()) ->
    {ok, transfer_links:link_key()} | {error, term()}.
get_link_key(TransferId) when is_binary(TransferId) ->
    case ?MODULE:get(TransferId) of
        {ok, Transfer} -> get_link_key(Transfer);
        {error, Reason} -> {error, Reason}
    end;
get_link_key(#document{key = TransferId, value = Transfer}) ->
    Timestamp = case transfer_utils:is_ongoing(Transfer) of
        true -> Transfer#transfer.schedule_time;
        false -> Transfer#transfer.finish_time
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
%% @private
%% @doc
%% Updates transfer.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(TransferId, Diff) ->
    datastore_model:update(?CTX, TransferId, Diff).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates transfer doc and evaluates given code upon success.
%% @end
%%--------------------------------------------------------------------
-spec update_and_run(id(), diff(), fun((transfer()) -> ok)) ->
    {ok, id()} | {error, term()}.
update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate) ->
    case update(TransferId, UpdateFun) of
        {ok, #document{value = Transfer}} ->
            ok = OnSuccessfulUpdate(Transfer),
            {ok, TransferId};
        {error, Error} ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates transfer doc and moves the transfer to finished.
%% @end
%%--------------------------------------------------------------------
-spec update_and_move_to_finished(id(), diff()) ->
    {ok, id()} | {error, term()}.
update_and_move_to_finished(TransferId, UpdateFun) ->
    OnSuccessfulUpdate = fun(Transfer) ->
        move_to_finished(TransferId, Transfer)
    end,
    update_and_run(TransferId, UpdateFun, OnSuccessfulUpdate).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Moves the transfer link from current to past tree and reports transfer finish
%% for the root file.
%% @end
%%--------------------------------------------------------------------
-spec move_to_finished(id(), transfer()) -> ok.
move_to_finished(TransferId, #transfer{file_uuid = FileUuid, space_id = SpaceId,
    schedule_time = ScheduleTime, finish_time = FinishTime
}) ->
    transferred_file:report_transfer_finish(
        fslogic_uuid:uuid_to_guid(FileUuid, SpaceId), TransferId, ScheduleTime
    ),
    move_from_current_to_past_links_tree(TransferId, SpaceId, ScheduleTime, FinishTime).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether calling provider can reset given
%% transfer (replication, migration or invalidation).
%% If true, it resets transfer document.
%% @end
%%-------------------------------------------------------------------
-spec maybe_restart(transfer()) -> {ok, transfer()} | {error, term()}.
maybe_restart(Transfer) ->
    case {transfer_utils:is_invalidation(Transfer), transfer_utils:is_migration(Transfer)} of
        {false, false} ->
            % transfer
            maybe_reset_replication_record(Transfer);
        {true, false} ->
            % invalidation
            maybe_reset_invalidation_record(Transfer);
        {true, true} ->
            % migration
            maybe_reset_migration_record(Transfer)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether calling provider can reset given
%% replication transfer. If true, it resets transfer document.
%% @end
%%-------------------------------------------------------------------
-spec maybe_reset_replication_record(transfer()) -> {ok, transfer()} | {error, term()}.
maybe_reset_replication_record(Transfer = #transfer{
    status = Status,
    invalidation_status = InvalidationStatus,
    target_provider_id = TargetProviderId
}) ->
    case oneprovider:get_id() =:= TargetProviderId of
        true ->
            {ok, Transfer#transfer{
                    status = reset_status(Status),
                    invalidation_status = reset_status(InvalidationStatus),
                    files_to_process = 0,
                    files_processed = 0,
                    failed_files = 0,
                    files_transferred = 0,
                    bytes_transferred = 0,
                    pid = undefined,
                    schedule_time = provider_logic:zone_time_seconds(),
                    last_update = #{},
                    min_hist = #{},
                    hr_hist = #{},
                    dy_hist = #{},
                    mth_hist = #{}
                }};
        false ->
            {error, not_target_provider}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether calling provider can reset given
%% invalidation transfer. If true, it resets transfer document.
%% @end
%%-------------------------------------------------------------------
-spec maybe_reset_invalidation_record(transfer()) -> {ok, transfer()} | {error, term()}.
maybe_reset_invalidation_record(Transfer = #transfer{
    source_provider_id = SourceProviderId
}) ->
    case oneprovider:get_id() =:= SourceProviderId of
        true ->
            {ok, Transfer#transfer{
                invalidation_status = scheduled,
                files_to_process = 0,
                files_processed = 0,
                failed_files = 0,
                files_invalidated = 0,
                pid = undefined,
                schedule_time = provider_logic:zone_time_seconds()
            }};
        false ->
            {error, not_source_provider}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether calling provider can reset given
%% migration transfer. If true, it resets transfer document.
%% @end
%%-------------------------------------------------------------------
-spec maybe_reset_migration_record(transfer()) -> {ok, transfer()} | {error, term()}.
maybe_reset_migration_record(Transfer = #transfer{
    source_provider_id = SourceProviderId
}) ->
    case {transfer_utils:is_transfer_ongoing(Transfer),
        transfer_utils:is_invalidation_ongoing(Transfer)}
    of
        {true, _} ->
            maybe_reset_replication_record(Transfer);
        {_, true} ->
            case SourceProviderId =:= oneprovider:get_id() of
                true ->
                    {ok, Transfer#transfer{
                        status = scheduled,
                        invalidation_status = scheduled,
                        files_to_process = 0,
                        files_processed = 0,
                        failed_files = 0,
                        files_transferred = 0,
                        bytes_transferred = 0,
                        files_invalidated = 0,
                        pid = undefined,
                        schedule_time = provider_logic:zone_time_seconds(),
                        last_update = #{},
                        min_hist = #{},
                        hr_hist = #{},
                        dy_hist = #{},
                        mth_hist = #{}
                    }};
                false ->
                    {error, not_source_provider}
            end;
        {false, false} ->
            maybe_reset_replication_record(Transfer)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pools responsible for replicating files and directories.
%% @end
%%-------------------------------------------------------------------
-spec start_pools() -> ok.
start_pools() ->
    {ok, _} = worker_pool:start_sup_pool(?TRANSFER_WORKERS_POOL, [
        {workers, ?TRANSFER_WORKERS_NUM},
        {worker, {transfer_worker, []}},
        {queue_type, lifo}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?TRANSFER_CONTROLLERS_POOL, [
        {workers, ?TRANSFER_CONTROLLERS_NUM},
        {worker, {transfer_controller, []}}
    ]),
    {ok, _} = worker_pool:start_sup_pool(?INVALIDATION_WORKERS_POOL, [
        {workers, ?INVALIDATION_WORKERS_NUM}
    ]),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops worker pools responsible for replicating files and directories.
%% @end
%%-------------------------------------------------------------------
-spec stop_pools() -> ok.
stop_pools() ->
    true = worker_pool:stop_pool(?TRANSFER_WORKERS_POOL),
    true = worker_pool:stop_pool(?TRANSFER_CONTROLLERS_POOL),
    true = worker_pool:stop_pool(?INVALIDATION_WORKERS_POOL),
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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Removes all TransferIds from unfinished_transfer
%% @end
%%-------------------------------------------------------------------
-spec remove_unfinished_transfers_links([id()], od_space:id()) -> ok.
remove_unfinished_transfers_links(TransferIds, SpaceId) ->
    lists:foreach(fun(TransferId) ->
        ok = transfer_links:delete_ongoing_transfer_link(TransferId, SpaceId)
    end, TransferIds).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Moves given TransferId from past to current transfers links tree.
%% @end
%%-------------------------------------------------------------------
-spec move_from_past_to_current_links_tree(id(), od_space:id(),
    non_neg_integer(), non_neg_integer()) -> ok.
move_from_past_to_current_links_tree(TransferId, SpaceId, FinishTime, NewScheduleTime) ->
    ok = transfer_links:add_ongoing_transfer_link(TransferId, SpaceId, NewScheduleTime),
    ok = transfer_links:delete_ended_transfer_link(TransferId, SpaceId, FinishTime).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Moves given TransferId from current to past transfers links tree.
%% @end
%%-------------------------------------------------------------------
-spec move_from_current_to_past_links_tree(id(), od_space:id(),
    non_neg_integer(), non_neg_integer()) -> ok.
move_from_current_to_past_links_tree(TransferId, SpaceId, ScheduleTime, FinishTime) ->
    ok = transfer_links:add_ended_transfer_link(TransferId, SpaceId, FinishTime),
    ok = transfer_links:delete_ongoing_transfer_link(TransferId, SpaceId, ScheduleTime).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resets transfer status if it's different than skipped.
%% @end
%%-------------------------------------------------------------------
-spec reset_status(status()) -> status().
reset_status(skipped) -> skipped;
reset_status(_) -> scheduled.


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
    7.

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
    StartTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
    MthHist
}) ->
    {7, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, true, Status,
        InvalidationStatus, SchedulingProviderId, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToProcess, FilesProcessed,
        FailedFiles, FilesTransferred, BytesTransferred, FilesInvalidated,
        StartTime, StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist,
        MthHist
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
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, NewDoc, PreviousDoc) ->
    #document{value = PrevTransfer} = PreviousDoc,
    #document{revs = [NewRev], value = NewTransfer} = NewDoc,

    {D1, D2} = order_transfers(PreviousDoc, NewDoc),

    case PrevTransfer#transfer.enqueued == NewTransfer#transfer.enqueued of
        true ->
            {false, D1};
        false ->
            #document{revs = [Rev], value = T1} = D1,
            #document{value = T2} = D2,

            EmergingTransfer = T1#transfer{
                enqueued = T1#transfer.enqueued and T2#transfer.enqueued
            },
            {Rev == NewRev, D1#document{value = EmergingTransfer}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Compares 2 transfers given as args and returns them as tuple with first
%% element being the greater/newer one.
%% Fields being compared are in order: files_to_process, files_processed,
%% files_transferred, bytes_transferred and files_invalidated, revision.
%% Since only provider performing replication/invalidation modifies those
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
        T1#transfer.files_to_process, T1#transfer.files_processed,
        T1#transfer.files_transferred, T1#transfer.bytes_transferred,
        T1#transfer.files_invalidated, IsGreaterRev
    },

    Vec2 = {
        T2#transfer.files_to_process, T2#transfer.files_processed,
        T2#transfer.files_transferred, T2#transfer.bytes_transferred,
        T2#transfer.files_invalidated, not IsGreaterRev
    },

    case Vec1 >= Vec2 of
        true -> {D1, D2};
        false -> {D2, D1}
    end.
