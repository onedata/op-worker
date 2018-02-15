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
%%% @end
%%%-------------------------------------------------------------------
-module(transfer).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/transfer.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/7, cancel/1, get_status/1, get_info/1, get/1, init/0, cleanup/0,
    decode_pid/1, encode_pid/1, get_controller/1, remove_links/3, restart/1, add_link/3]).
-export([mark_active/2, mark_completed/3, mark_failed/2,
    mark_active_invalidation/1, mark_completed_invalidation/2, mark_failed_invalidation/2,
    mark_file_transfer_scheduled/2, mark_file_transfer_finished/2,
    mark_data_transfer_scheduled/2, mark_data_transfer_finished/3,
    for_each_past_transfer/3, for_each_current_transfer/3, restart_unfinished_transfers/1,
    mark_file_invalidation_finished/2, mark_file_invalidation_scheduled/2,
    mark_cancelled/1, increase_failed_file_transfers/1, mark_cancelled_invalidation/1]).
-export([list_transfers/2, is_ongoing/1, is_migrating/1]).

%% model_behaviour callbacks
-export([save/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4, delete/2]).
-export([record_struct/1, record_upgrade/2]).

-type id() :: binary().
-type status() :: scheduled | skipped | active | completed | cancelled | failed.
-type callback() :: undefined | binary().
-type record() :: #transfer{}.
-type doc() :: #document{value :: record()}.
-type virtual_list_id() :: binary(). % ?(SUCCESSFUL|FAILED|UNFINISHED)_TRANSFERS_KEY

-export_type([id/0, record/0, status/0, callback/0, doc/0]).

-define(MAX_FILE_TRANSFER_RETRIES_PER_TRANSFER,
    application:get_env(?APP_NAME, max_file_transfer_retries_per_transfer, 10)).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
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
        {pid, string},
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
record_struct(2) ->
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
        {pid, string},
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer},
        {files_to_invalidate, integer},
        {files_invalidated, integer},
        {start_time, integer},
        {last_update, integer},
        {min_hist, [integer]},
        {hr_hist, [integer]},
        {dy_hist, [integer]}
    ]};
record_struct(3) ->
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
        {pid, string},
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
record_struct(4) ->
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
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
record_upgrade(1, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, StartTime, LastUpdate,
    MinHist, HrHist, DyHist}
) ->
    {2, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
        InvalidationStatus, SourceProviderId, TargetProviderId,
        InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
        BytesToTransfer, BytesTransferred, 0, 0, StartTime, LastUpdate,
        MinHist, HrHist, DyHist
    }};
record_upgrade(2, {?MODULE, FileUuid, SpaceId, Path, CallBack, TransferStatus,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
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
        % hr_hist
        #{TargetProviderId => MinHist},
        % dy_hist
        #{TargetProviderId => HrHist},
        % mth_hist
        #{TargetProviderId => DyHist}
    }};
record_upgrade(3, {?MODULE, FileUuid, SpaceId, UserId, Path, CallBack, Status,
    InvalidationStatus, SourceProviderId, TargetProviderId,
    InvalidateSourceReplica, Pid, FilesToTransfer, FilesTransferred,
    BytesToTransfer, BytesTransferred, FilesToInvalidate, FilesInvalidated,
    StartTime, FinishTime, LastUpdate, MinHist, HrHist, DyHist, MthHist
}) ->
    {4, #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        user_id = UserId,
        path = Path,
        callback = CallBack,
        status = Status,
        invalidation_status = InvalidationStatus,
        source_provider_id = SourceProviderId,
        target_provider_id = TargetProviderId,
        invalidate_source_replica = InvalidateSourceReplica,
        pid = Pid,
        files_to_transfer = FilesToTransfer,
        files_transferred = FilesTransferred,
        failed_files = 0,
        bytes_to_transfer = BytesToTransfer,
        bytes_transferred = BytesTransferred,
        files_to_invalidate = FilesToInvalidate,
        files_invalidated = FilesInvalidated,
        start_time = StartTime,
        finish_time = FinishTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist,
        mth_hist = MthHist
    }}.


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
    start_pools(),
    ok.

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
    undefined | od_provider:id(), undefined | od_provider:id(), binary(), boolean()) ->
    {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, SourceProviderId, TargetProviderId, Callback, InvalidateSourceReplica) ->
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
    TimeSeconds = time_utils:zone_time_seconds(),
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
            source_provider_id = SourceProviderId,
            target_provider_id = TargetProviderId,
            invalidate_source_replica = InvalidateSourceReplica,
            start_time = TimeSeconds,
            finish_time = 0,
            last_update = #{},
            min_hist = #{},
            hr_hist = #{},
            dy_hist = #{},
            mth_hist = #{}

        }},
    {ok, TransferId} = create(ToCreate),
    session:add_transfer(SessionId, TransferId),
    ok = add_link(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId),
    transfer_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    invalidation_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    {ok, TransferId}.

%%-------------------------------------------------------------------
%% @doc
%% Restarts all unfinished transfers.
%% @end
%%-------------------------------------------------------------------
-spec restart_unfinished_transfers(od_space:id()) -> [id()].
restart_unfinished_transfers(SpaceId) ->
    {ok, {Restarted, Failed}} = for_each_current_transfer(fun(TransferId, {Restarted0, Failed0}) ->
        case restart(TransferId) of
            {ok, TransferId} ->
                {[TransferId | Restarted0], Failed0};
            {error, not_target_provider} ->
                {Restarted0, Failed0};
            {error, not_source_provider} ->
                {Restarted0, Failed0};
            {error, {not_found, transfer}} ->
                {Restarted0, [TransferId | Failed0]}
        end
    end, {[], []}, SpaceId),

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
    case update(TransferId, fun maybe_restart/1) of
        {ok, TransferId} ->
            {ok, TransferDoc = #document{value = #transfer{space_id = SpaceId}}} = get(TransferId),
            move_from_past_to_current_links_tree(TransferId, SpaceId),
            transfer_controller:on_new_transfer_doc(TransferDoc),
            invalidation_controller:on_new_transfer_doc(TransferDoc),
            {ok, TransferId};
        {error, not_target_provider} ->
            {error, not_target_provider};
        {error, not_source_provdier} ->
            {error, not_source_provdier};
        Error ->
            ?error_stacktrace("Restarting transfer ~p failed due to ~p", [TransferId, Error]),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: id()) -> status().
get_status(TransferId) ->
    {ok, #document{value = #transfer{status = Status}}} = get(TransferId),
    Status.

%%--------------------------------------------------------------------
%% @doc
%% Gets transfer info
%% @end
%%--------------------------------------------------------------------
-spec get_info(TransferId :: id()) -> maps:map().
get_info(TransferId) ->
    {ok, #document{value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        user_id = UserId,
        path = Path,
        status = TransferStatus,
        invalidation_status = InvalidationStatus,
        target_provider_id = TargetProviderId,
        callback = Callback,
        files_to_transfer = FilesToTransfer,
        files_transferred = FilesTransferred,
        failed_files = FailedFiles,
        bytes_to_transfer = BytesToTransfer,
        bytes_transferred = BytesTransferred,
        files_to_invalidate = FilesToInvalidate,
        files_invalidated = FilesInvalidated,
        start_time = StartTime,
        finish_time = FinishTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist,
        mth_hist = MthHist
    }}} = get(TransferId),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    #{
        <<"fileId">> => FileObjectId,
        <<"userId">> => UserId,
        <<"path">> => Path,
        <<"transferStatus">> => atom_to_binary(TransferStatus, utf8),
        <<"invalidationStatus">> => atom_to_binary(InvalidationStatus, utf8),
        <<"targetProviderId">> => utils:ensure_defined(TargetProviderId, undefined, null),
        <<"callback">> => NullableCallback,
        <<"filesToTransfer">> => FilesToTransfer,
        <<"filesTransferred">> => FilesTransferred,
        <<"failedFiles">> => FailedFiles,
        <<"filesToInvalidate">> => FilesToInvalidate,
        <<"filesInvalidated">> => FilesInvalidated,
        <<"bytesToTransfer">> => BytesToTransfer,
        <<"bytesTransferred">> => BytesTransferred,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime,
        % It is possible that there is no last update, if 0 bytes were
        % transferred, in this case take the start time.
        <<"lastUpdate">> => lists:max([StartTime | maps:values(LastUpdate)]),
        <<"minHist">> => MinHist,
        <<"hrHist">> => HrHist,
        <<"dyHist">> => DyHist,
        <<"mthHist">> => MthHist
    }.

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec cancel(id()) -> ok | {error, term()}.
cancel(TransferId) ->
    {ok, _} = transfer:mark_cancelled(TransferId),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as active and sets number of files to transfer to 1.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(id(), pid()) -> {ok, id()} | {error, term()}.
mark_active(TransferId, TransferControllerPid) ->
    Pid = encode_pid(TransferControllerPid),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            status = active,
            files_to_transfer = 1,
            pid = Pid
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed(id(), od_space:id(), boolean()) -> {ok, id()} | {error, term()}.
mark_completed(TransferId, SpaceId, InvalidateSourceReplica) ->
    UpdateFun = fun(Transfer) ->
        {ok, Transfer#transfer{
            status = completed,
            finish_time = time_utils:zone_time_seconds()
        }}
    end,
    case transfer:update(TransferId, UpdateFun) of
        {ok, _} ->
            case InvalidateSourceReplica of
                false ->
                    move_from_current_to_past_links_tree(TransferId, SpaceId),
                    {ok, TransferId};
                true ->
                    {ok, TransferId}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks status in transfer (replication, migration, invalidation)
%% document as failed.
%% If given document describes migration transfer,
%% invalidation_status is also marked as failed.
%% @end
%%--------------------------------------------------------------------
-spec mark_failed(id(), od_space:id()) -> {ok, id()} | {error, term()}.
mark_failed(TransferId, SpaceId) ->
    UpdateFun =  fun(T = #transfer{invalidation_status = InvalidationStatus}) ->
        {ok, T#transfer{
            status = failed,
            finish_time = time_utils:zone_time_seconds(),
            invalidation_status = case is_migrating(T) of
                true ->
                    failed;
                _ ->
                    InvalidationStatus
            end}}
    end,
    case transfer:update(TransferId, UpdateFun) of
        {ok, _}  ->
            move_from_current_to_past_links_tree(TransferId, SpaceId),
            {ok, TransferId};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Increase failed_files transfer
%% @end
%%--------------------------------------------------------------------
-spec increase_failed_file_transfers(id()) -> {ok, id()} | {error, term()}.
increase_failed_file_transfers(TransferId) ->
    {ok, _} = transfer:update(TransferId, fun(T = #transfer{failed_files = FailedFiles}) ->
        {ok, T#transfer{failed_files = FailedFiles + 1}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as cancelled.
%% @end
%%--------------------------------------------------------------------
-spec mark_cancelled(id()) -> {ok, id()} | {error, term()}.
mark_cancelled(TransferId) ->
    case transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{status = cancelled}}
    end) of
        {ok, _} ->
            {ok, #document{value = #transfer{space_id = SpaceId}}} = get(TransferId),
            move_from_current_to_past_links_tree(TransferId, SpaceId),
            {ok, TransferId};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as active.
%% @end
%%--------------------------------------------------------------------
-spec mark_active_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_active_invalidation(TransferId) ->
    Pid = encode_pid(self()),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            invalidation_status = active,
            files_to_invalidate = 1,
            pid = Pid
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed_invalidation(id(), od_space:id()) -> {ok, id()} | {error, term()}.
mark_completed_invalidation(TransferId, SpaceId) ->
    case update(TransferId, fun(T) ->
        {ok, T#transfer{invalidation_status = completed}}
    end) of
        {ok, _} ->
            move_from_current_to_past_links_tree(TransferId, SpaceId),
            {ok, TransferId};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as failed
%% @end
%%--------------------------------------------------------------------
-spec mark_failed_invalidation(id(), od_space:id()) -> {ok, id()} | {error, term()}.
mark_failed_invalidation(TransferId, SpaceId) ->
    case transfer:update(TransferId, fun(T) ->
        T#transfer{invalidation_status = failed}
    end) of
        {ok, _} ->
            move_from_current_to_past_links_tree(TransferId, SpaceId),
            {ok, TransferId};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as cancelled
%% @end
%%--------------------------------------------------------------------
-spec mark_cancelled_invalidation(id()) -> {ok, id()} | {error, term()}.
mark_cancelled_invalidation(TransferId) ->
    transfer:update(TransferId, fun(T = #transfer{space_id = SpaceId}) ->
        ok = add_link(?PAST_TRANSFERS_KEY, TransferId, SpaceId),
        ok = remove_links(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId),
        T#transfer{invalidation_status = cancelled}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'FilesNum' files are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_transfer_scheduled(undefined | id(), non_neg_integer()) ->
    {ok, id()} | {error, term()}.
mark_file_transfer_scheduled(undefined, _FilesNum) ->
    {ok, undefined};
mark_file_transfer_scheduled(TransferId, FilesNum) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_to_transfer = Transfer#transfer.files_to_transfer + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'FilesNum' files.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_transfer_finished(undefined | id(), non_neg_integer()) ->
    {ok, id()} | {error, term()}.
mark_file_transfer_finished(undefined, _FilesNum) ->
    {ok, undefined};
mark_file_transfer_finished(TransferId, FilesNum) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_transferred = Transfer#transfer.files_transferred + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'FilesNum' files are scheduled to be invalidated.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_invalidation_scheduled(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
mark_file_invalidation_scheduled(undefined, _) ->
    {ok, undefined};
mark_file_invalidation_scheduled(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_to_invalidate = Transfer#transfer.files_to_invalidate + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful invalidation of 'FilesNum' files.
%% If files_to_invalidate counter equals files_invalidated, invalidation
%% transfer is marked as finished.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_invalidation_finished(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
mark_file_invalidation_finished(undefined, _FilesNum) ->
    {ok, undefined};
mark_file_invalidation_finished(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
        CurrentTime = time_utils:zone_time_seconds(),
        {ok, Transfer#transfer{
            finish_time = CurrentTime,
            files_invalidated = Transfer#transfer.files_invalidated + FilesNum
        }}
    end).


%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'Bytes' bytes are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_scheduled(undefined | id(), non_neg_integer()) ->
    {ok, id()} | {error, term()}.
mark_data_transfer_scheduled(undefined, _Bytes) ->
    {ok, undefined};
mark_data_transfer_scheduled(TransferId, Bytes) ->
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            bytes_to_transfer = Transfer#transfer.bytes_to_transfer + Bytes
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'Bytes' bytes.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_finished(undefined | id(), od_provider:id(),
    non_neg_integer()) -> {ok, id()} | {error, term()}.
mark_data_transfer_finished(undefined, _ProviderId, _Bytes) ->
    {ok, undefined};
mark_data_transfer_finished(TransferId, ProviderId, Bytes) ->
    transfer:update(TransferId, fun(Transfer = #transfer{
        bytes_transferred = OldBytes,
        start_time = StartTime,
        last_update = LastUpdateMap,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        LastUpdate = maps:get(ProviderId, LastUpdateMap, StartTime),
        CurrentTime = time_utils:zone_time_seconds(),
        {ok, Transfer#transfer{
            bytes_transferred = OldBytes + Bytes,
            last_update = maps:put(ProviderId, CurrentTime, LastUpdateMap),
            min_hist = update_histogram(
                ProviderId, Bytes, MinHistograms,
                ?FIVE_SEC_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            hr_hist = update_histogram(
                ProviderId, Bytes, HrHistograms,
                ?MIN_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            dy_hist = update_histogram(
                ProviderId, Bytes, DyHistograms,
                ?HOUR_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            mth_hist = update_histogram(
                ProviderId, Bytes, MthHistograms,
                ?DAY_TIME_WINDOW, LastUpdate, CurrentTime
            )
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each transfer that has been finished.
%% Its status can be one of: completed, cancelled,failed.
%% @end
%%--------------------------------------------------------------------
-spec for_each_past_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_past_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?PAST_TRANSFERS_KEY, Callback, Acc0, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each ongoing transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_current_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_current_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?CURRENT_TRANSFERS_KEY, Callback, Acc0, SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Encodes Pid to binary.
%% @end
%%-------------------------------------------------------------------
-spec encode_pid(pid()) -> binary().
encode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_binary(pid_to_list(Pid)).

%%-------------------------------------------------------------------
%% @doc
%% Decodes Pid from binary.
%% @end
%%-------------------------------------------------------------------
-spec decode_pid(binary()) -> pid().
decode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_pid(binary_to_list(Pid)).

%%-------------------------------------------------------------------
%% @doc
%% Returns pid of transfer_controller for given Transfer.
%% @end
%%-------------------------------------------------------------------
-spec get_controller(id()) -> pid().
get_controller(TransferId) ->
    {ok, #document{value = #transfer{pid = ControllerPid}}} = get(TransferId),
    decode_pid(ControllerPid).

%%-------------------------------------------------------------------
%% @doc
%% Returns all transfers for given space that are ongoing or finished.
%% @end
%%-------------------------------------------------------------------
-spec list_transfers(od_space:id(), Ongoing :: boolean()) -> {ok, [id()]}.
list_transfers(SpaceId, Ongoing) ->
    Transfers = case Ongoing of
        true ->
            list_transfers_internal(SpaceId, ?CURRENT_TRANSFERS_KEY);
        false ->
            list_transfers_internal(SpaceId, ?PAST_TRANSFERS_KEY)
    end,
    {ok, Transfers}.

%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if given transfer is ongoing.
%%  * Replication is considered ongoing when data transfer hasn't finished.
%%  * Migration is considered ongoing when data transfer or replica
%%      invalidation hasn't finished.
%% @end
%%-------------------------------------------------------------------
-spec is_ongoing(record() | id() | undefined) -> boolean().
is_ongoing(undefined) ->
    true;
is_ongoing(Transfer = #transfer{}) ->
    is_transfer_ongoing(Transfer) orelse is_invalidation_ongoing(Transfer);
is_ongoing(TransferId) ->
    {ok, #document{value = Transfer}} = get(TransferId),
    is_ongoing(Transfer).

%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if given transfer is migrating a replica.
%% @end
%%-------------------------------------------------------------------
-spec is_migrating(record()) -> boolean().
is_migrating(#transfer{invalidate_source_replica = Flag}) -> Flag.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document = #document{scope = Scope}) ->
    model:execute_with_default_context(?MODULE, save, [Document], [{scope, Scope}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document = #document{scope = Scope}) ->
    model:execute_with_default_context(?MODULE, create, [Document], [{scope, Scope}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/2.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key(), od_space:id()) -> ok | datastore:generic_error().
delete(Key, SpaceId) ->
    ok = remove_links(?CURRENT_TRANSFERS_KEY, Key, SpaceId),
    ok = remove_links(?PAST_TRANSFERS_KEY, Key, SpaceId),
    model:execute_with_default_context(?MODULE, delete, [Key], [{scope, SpaceId}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:generic_error().
create_or_update(Doc = #document{scope = Scope}, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff], [{scope, Scope}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(transfer_bucket, [{transfer, update}], ?GLOBALLY_CACHED_LEVEL,
        ?GLOBALLY_CACHED_LEVEL, true, false, oneprovider:get_provider_id()),
    Config#model_config{
        version = 4,
        sync_enabled = true
    }.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(?MODULE, update, _, _, Result = {ok, TransferId}) ->
    {ok, #document{value = Transfer}} = get(TransferId),
    handle_updated(Transfer),
    Result;
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link to transfer. Links are added to link tree associated with
%% given space.
%% Real link source_id will be obtained from link_root/2 function.
%% @end
%%--------------------------------------------------------------------
-spec add_link(SourceId :: virtual_list_id(), TransferId :: id(),
    SpaceId :: od_space:id()) -> ok.
add_link(SourceId, TransferId, SpaceId) ->
    model:execute_with_default_context(?MODULE, add_links, [
        link_root(SourceId, SpaceId), {TransferId, {TransferId, ?MODEL_NAME}}
    ], [{scope, SpaceId}]).

%%--------------------------------------------------------------------
%% @doc
%% Removes link/links to transfer/transfers
%% Real link source_id will be obtained from link_root/2 function.
%% @end
%%--------------------------------------------------------------------
-spec remove_links(SourceId :: virtual_list_id(), TransferId :: id() | [id()],
    SpaceId :: od_space:id()) -> ok.
remove_links(SourceId, TransferIds, SpaceId) ->
    model:execute_with_default_context(?MODULE, delete_links, [
        link_root(SourceId, SpaceId), TransferIds],
        [{scope, SpaceId}]
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each successfully completed transfer
%% @end
%%--------------------------------------------------------------------
-spec list_transfers_internal(SpaceId :: od_space:id(), virtual_list_id()) ->
    [transfer:id()].
list_transfers_internal(SpaceId, ListDocId) ->
    Callback = fun(TransferId, Acc) ->
        [TransferId | Acc]
    end,
    {ok, Transfers} = for_each_transfer(ListDocId, Callback, [], SpaceId),
    Transfers.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each successfully completed transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_transfer(
    virtual_list_id(), Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_transfer(ListDocId, Callback, Acc0, SpaceId) ->
    model:execute_with_default_context(?MODULE, foreach_link, [
        link_root(ListDocId, SpaceId),
        fun(LinkName, _LinkTarget, Acc) ->
            Callback(LinkName, Acc)
        end, Acc0
    ]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether calling provider can reset given
%% transfer (replication, migration or invalidation).
%% If true, it resets transfer document.
%% @end
%%-------------------------------------------------------------------
-spec maybe_restart(record()) -> {ok, id()} | {error, term()}.
maybe_restart(Transfer) ->
    case {is_migrating(Transfer), is_invalidating(Transfer)} of
        {false, false} ->
            % transfer
            maybe_reset_replication_record(Transfer);
        {true, true} ->
            % invalidation
            maybe_reset_invalidation_record(Transfer);
        {true, false} ->
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
-spec maybe_reset_replication_record(record()) -> {ok, id()} | {error, term()}.
maybe_reset_replication_record(Transfer = #transfer{
    status = Status,
    invalidation_status = InvalidationStatus,
    target_provider_id = TargetProviderId
}) ->
    case oneprovider:get_provider_id() =:= TargetProviderId of
        true ->
            {ok, Transfer#transfer{
                status = reset_status(Status),
                invalidation_status = reset_status(InvalidationStatus),
                files_to_transfer = 0,
                files_transferred = 0,
                failed_files = 0,
                bytes_to_transfer = 0,
                bytes_transferred = 0,
                start_time = time_utils:zone_time_seconds(),
                finish_time = 0,
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
-spec maybe_reset_invalidation_record(record()) -> {ok, id()} | {error, term()}.
maybe_reset_invalidation_record(Transfer = #transfer{
    source_provider_id = SourceProviderId
}) ->
    case oneprovider:get_provider_id() =:= SourceProviderId of
        true ->
            {ok, Transfer#transfer{
                invalidation_status = scheduled,
                files_invalidated = 0,
                files_to_invalidate = 0,
                start_time = time_utils:zone_time_seconds(),
                finish_time = 0
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
-spec maybe_reset_migration_record(record()) -> {ok, id()} | {error, term()}.
maybe_reset_migration_record(Transfer = #transfer{
    source_provider_id = SourceProviderId
}) ->
    case {is_transfer_ongoing(Transfer), is_invalidation_ongoing(Transfer)} of
        {true, _} ->
            maybe_reset_replication_record(Transfer);
        {_, true} ->
            case SourceProviderId =:= oneprovider:get_provider_id() of
                true ->
                    {ok, Transfer#transfer{
                        status = scheduled,
                        invalidation_status = scheduled,
                        files_to_transfer = 0,
                        files_transferred = 0,
                        bytes_to_transfer = 0,
                        bytes_transferred = 0,
                        files_invalidated = 0,
                        files_to_invalidate = 0,
                        start_time = time_utils:zone_time_seconds(),
                        finish_time = 0,
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
%% Posthook responsible for stopping transfer or invalidation controller.
%% @end
%%-------------------------------------------------------------------
-spec handle_updated(record()) -> ok.
handle_updated(#transfer{
    status = active,
    files_to_transfer = FilesToTransfer,
    files_transferred = FilesToTransfer,
    failed_files = 0,
    bytes_to_transfer = BytesToTransfer,
    bytes_transferred = BytesToTransfer,
    pid = Pid
}) ->
    transfer_controller:mark_finished(decode_pid(Pid));
handle_updated(#transfer{
    status = active,
    files_to_transfer = FilesToTransfer,
    files_transferred = FilesTransferred,
    failed_files = FailedFiles,
    pid = Pid
}) ->
    case FailedFiles > ?MAX_FILE_TRANSFER_RETRIES_PER_TRANSFER of
        true ->
            transfer_controller:mark_failed(decode_pid(Pid), exceeded_number_of_retries);
        false ->
            case FailedFiles + FilesTransferred =:= FilesToTransfer of
                true ->
                    transfer_controller:mark_failed(decode_pid(Pid), file_transfer_failures);
                _ ->
                    ok
            end
    end;
handle_updated(#transfer{
    status = TransferStatus,
    files_to_invalidate = FilesToInvalidate,
    files_invalidated = FilesToInvalidate,
    invalidation_status = active,
    pid = Pid
}) when TransferStatus =:= completed orelse TransferStatus =:= skipped ->
    invalidation_controller:finish_invalidation(decode_pid(Pid));
handle_updated(_) ->
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Removes all TransferIds from unfinished_transfer
%% @end
%%-------------------------------------------------------------------
-spec remove_unfinished_transfers_links([id()], od_space:id()) -> ok.
remove_unfinished_transfers_links(TransferIds, SpaceId) ->
    remove_links(?CURRENT_TRANSFERS_KEY, TransferIds, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns links tree root for given space.
%% @end
%%-------------------------------------------------------------------
-spec link_root(binary(), od_space:id()) -> binary().
link_root(Prefix, SpaceId) ->
    <<Prefix/binary, "_", SpaceId/binary>>.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time and Window.
%% The length of created histogram is based on the Window.
%% @end
%%-------------------------------------------------------------------
-spec update_histogram(oneprovider:id(), Bytes :: non_neg_integer(),
    Histograms, Window :: non_neg_integer(), LastUpdate :: non_neg_integer(),
    CurrentTime :: non_neg_integer()) -> Histograms
    when Histograms :: maps:map(od_provider:id(), histogram:histogram()).
update_histogram(ProviderId, Bytes, Histograms, Window, LastUpdate, CurrentTime) ->
    Histogram = case maps:find(ProviderId, Histograms) of
        error ->
            new_time_slot_histogram(LastUpdate, Window);
        {ok, Values} ->
            new_time_slot_histogram(LastUpdate, Window, Values)
    end,
    UpdatedHistogram = time_slot_histogram:increment(Histogram, CurrentTime, Bytes),
    UpdatedValues = time_slot_histogram:get_histogram_values(UpdatedHistogram),
    maps:put(ProviderId, UpdatedValues, Histograms).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time and Window.
%% The length of created histogram is based on the Window.
%% @end
%%-------------------------------------------------------------------
-spec new_time_slot_histogram(LastUpdate :: non_neg_integer(),
    Window :: non_neg_integer()) -> time_slot_histogram:histogram().
new_time_slot_histogram(LastUpdate, ?FIVE_SEC_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?FIVE_SEC_TIME_WINDOW, histogram:new(?MIN_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?MIN_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?MIN_TIME_WINDOW, histogram:new(?HOUR_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?HOUR_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?HOUR_TIME_WINDOW, histogram:new(?DAY_HIST_LENGTH));
new_time_slot_histogram(LastUpdate, ?DAY_TIME_WINDOW) ->
    new_time_slot_histogram(LastUpdate, ?DAY_TIME_WINDOW, histogram:new(?MONTH_HIST_LENGTH)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new time_slot_histogram based on LastUpdate time, Window and values.
%% @end
%%-------------------------------------------------------------------
-spec new_time_slot_histogram(LastUpdate :: non_neg_integer(),
    Window :: non_neg_integer(), histogram:histogram()) ->
    time_slot_histogram:histogram().
new_time_slot_histogram(LastUpdate, Window, Values) ->
    time_slot_histogram:new(LastUpdate, Window, Values).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Predicate saying if given transfer is ongoing. Checks only if data transfer
%% is finished, no matter if that is a replication or migration.
%% @end
%%-------------------------------------------------------------------
-spec is_transfer_ongoing(record()) -> boolean().
is_transfer_ongoing(#transfer{status = scheduled}) -> true;
is_transfer_ongoing(#transfer{status = skipped}) -> false;
is_transfer_ongoing(#transfer{status = active}) -> true;
is_transfer_ongoing(#transfer{status = completed}) -> false;
is_transfer_ongoing(#transfer{status = cancelled}) -> false;
is_transfer_ongoing(#transfer{status = failed}) -> false.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Predicate saying if invalidation within given transfer is ongoing. Returns
%% false for transfers that are not a migration.
%% @end
%%-------------------------------------------------------------------
-spec is_invalidation_ongoing(record()) -> boolean().
is_invalidation_ongoing(#transfer{invalidate_source_replica = false}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = completed}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = skipped}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = cancelled}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = failed}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = scheduled}) -> true;
is_invalidation_ongoing(#transfer{invalidation_status = active}) -> true.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Moves given TransferId from past to current transfers links tree.
%% @end
%%-------------------------------------------------------------------
-spec move_from_past_to_current_links_tree(id(), od_space:id()) -> ok.
move_from_past_to_current_links_tree(TransferId, SpaceId) ->
    ok = add_link(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?PAST_TRANSFERS_KEY, TransferId, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Moves given TransferId from current to past transfers links tree.
%% @end
%%-------------------------------------------------------------------
-spec move_from_current_to_past_links_tree(id(), od_space:id()) -> ok.
move_from_current_to_past_links_tree(TransferId, SpaceId) ->
    ok = add_link(?PAST_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Predicate saying if given transfer is only invalidating replica.
%% @end
%%-------------------------------------------------------------------
-spec is_invalidating(record()) -> boolean().
is_invalidating(#transfer{
    invalidate_source_replica = Flag,
    target_provider_id = undefined
}) ->
    Flag;
is_invalidating(_) ->
    false.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resets transfer status if it's different than skipped.
%% @end
%%-------------------------------------------------------------------
-spec reset_status(status()) -> status().
reset_status(skipped) -> skipped;
reset_status(_) -> scheduled.
