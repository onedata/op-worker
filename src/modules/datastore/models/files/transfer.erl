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

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/6, stop/1, get_status/1, get_info/1, get/1, init/0, cleanup/0, decode_pid/1, encode_pid/1]).
-export([mark_active/1, mark_completed/1, mark_failed/2,
    mark_active_invalidation/1, mark_completed_invalidation/2, mark_failed_invalidation/2,
    mark_file_transfer_scheduled/2, mark_file_transfer_finished/2,
    mark_data_transfer_scheduled/2, mark_data_transfer_finished/2,
    for_each_successful_transfer/2, for_each_failed_transfer/2,
    for_each_unfinished_transfer/2, restart_unfinished_and_failed_transfers/0,
    mark_file_invalidation_finished/2, mark_file_invalidation_scheduled/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_posthooks/0, get_record_version/0,
    upgrade_record/2]).

-type id() :: binary().
-type diff() :: datastore:diff(transfer()).
-type status() :: scheduled | skipped | active | completed | cancelled | failed.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type doc() :: datastore_doc:doc(transfer()).
-type virtual_list_id() :: binary(). % ?(SUCCESSFUL|FAILED|UNFINISHED)_TRANSFERS_KEY

-export_type([id/0, status/0, callback/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_provider_id(),
    local_links_tree_id => oneprovider:get_provider_id()
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
    oneprovider:id(), binary(), boolean()) -> {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, ProviderId, Callback, InvalidateSourceReplica) ->
    TransferStatus = case ProviderId of
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
    TimeSeconds = utils:system_time_seconds(),
    MinHist = time_slot_histogram:new(?MIN_TIME_WINDOW, 60),
    HrHist = time_slot_histogram:new(?HR_TIME_WINDOW, 24),
    DyHist = time_slot_histogram:new(?DY_TIME_WINDOW, 30),
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),
    ToCreate = #document{
        scope = fslogic_uuid:guid_to_space_id(FileGuid),
        value = #transfer{
            file_uuid = fslogic_uuid:guid_to_uuid(FileGuid),
            space_id = SpaceId,
            path = FilePath,
            callback = Callback,
            transfer_status = TransferStatus,
            invalidation_status = InvalidationStatus,
            source_provider_id = oneprovider:get_provider_id(),
            target_provider_id = ProviderId,
            invalidate_source_replica = InvalidateSourceReplica,
            start_time = TimeSeconds,
            last_update = 0,
            min_hist = time_slot_histogram:get_histogram_values(MinHist),
            hr_hist = time_slot_histogram:get_histogram_values(HrHist),
            dy_hist = time_slot_histogram:get_histogram_values(DyHist)

        }},
    {ok, #document{key = TransferId}} = create(ToCreate),
    session:add_transfer(SessionId, TransferId),
    add_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    transfer_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    invalidation_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    {ok, TransferId}.

%%-------------------------------------------------------------------
%% @doc
%% Restarts all unfinished and failed transfers.
%% @end
%%-------------------------------------------------------------------
-spec restart_unfinished_and_failed_transfers() -> ok.
restart_unfinished_and_failed_transfers() ->
    restart_unfinished_transfers(),
    restart_failed_transfers(),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: id()) -> status().
get_status(TransferId) ->
    {ok, #document{value = #transfer{transfer_status = Status}}} =
        datastore_model:get(?CTX, TransferId),
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
        path = Path,
        transfer_status = TransferStatus,
        invalidation_status = InvalidationStatus,
        target_provider_id = TargetProviderId,
        callback = Callback,
        files_to_transfer = FilesToTransfer,
        files_transferred = FilesTransferred,
        bytes_to_transfer = BytesToTransfer,
        bytes_transferred = BytesTransferred,
        start_time = StartTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist
    }}} = datastore_model:get(?CTX, TransferId),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    #{
        <<"fileId">> => FileObjectId,
        <<"path">> => Path,
        <<"transferStatus">> => atom_to_binary(TransferStatus, utf8),
        <<"invalidationStatus">> => atom_to_binary(InvalidationStatus, utf8),
        <<"targetProviderId">> => TargetProviderId,
        <<"callback">> => NullableCallback,
        <<"filesToTransfer">> => FilesToTransfer,
        <<"filesTransferred">> => FilesTransferred,
        <<"bytesToTransfer">> => BytesToTransfer,
        <<"bytesTransferred">> => BytesTransferred,
        <<"startTime">> => StartTime,
        <<"lastUpdate">> => LastUpdate,
        <<"minHist">> => MinHist,
        <<"hrHist">> => HrHist,
        <<"dyHist">> => DyHist
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns transfer document.
%% @end
%%-------------------------------------------------------------------
-spec get(id()) -> {ok, doc()}.
get(TransferId) ->
    datastore_model:get(?CTX, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec stop(id()) -> ok | {error, term()}.
stop(TransferId) ->
    {ok, #document{value = #transfer{space_id = SpaceId}}} = datastore_model:get(?CTX, TransferId),
    remove_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    remove_link(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
    remove_link(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
    datastore_model:delete(?CTX, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as active and sets number of files to transfer to 1.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(id()) -> {ok, id()} | {error, term()}.
mark_active(TransferId) ->
    Pid = encode_pid(self()),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            transfer_status = active,
            files_to_transfer = 1,
            pid = Pid
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed(id()) -> {ok, id()} | {error, term()}.
mark_completed(TransferId) ->
    update(TransferId, fun(Transfer) ->
        case Transfer#transfer.invalidation_status of
            skipped ->
                SpaceId = Transfer#transfer.space_id,
                add_link(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
                remove_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
                {ok, Transfer#transfer{transfer_status = completed}};
            _ ->
                {ok, Transfer#transfer{transfer_status = completed}}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as failed
%% @end
%%--------------------------------------------------------------------
-spec mark_failed(id(), od_space:id()) -> {ok, id()} | {error, term()}.
mark_failed(TransferId, SpaceId) ->
    add_link(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
    remove_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{transfer_status = failed}}
    end).

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
    add_link(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
    remove_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{invalidation_status = completed}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks replica invalidation as failed
%% @end
%%--------------------------------------------------------------------
-spec mark_failed_invalidation(id(), od_space:id()) -> {ok, id()} | {error, term()}.
mark_failed_invalidation(TransferId, SpaceId) ->
    add_link(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
    remove_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{invalidation_status = failed}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'FilesNum' files are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_transfer_scheduled(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
mark_file_transfer_scheduled(undefined, _FilesNum) ->
    {ok, undefined};
mark_file_transfer_scheduled(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
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
    {ok, undefined | id()} | {error, term()}.
mark_file_transfer_finished(undefined, _FilesNum) ->
    {ok, undefined};
mark_file_transfer_finished(TransferId, FilesNum) ->
    update(TransferId, fun(Transfer) ->
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
        {ok, Transfer#transfer{
            files_invalidated = Transfer#transfer.files_invalidated + FilesNum
        }}
    end).


%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'Bytes' bytes are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_scheduled(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
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
-spec mark_data_transfer_finished(undefined | id(), non_neg_integer()) ->
    {ok, undefined | id()} | {error, term()}.
mark_data_transfer_finished(undefined, _Bytes) ->
    {ok, undefined};
mark_data_transfer_finished(TransferId, Bytes) ->
    update(TransferId, fun(Transfer = #transfer{
        bytes_transferred = OldBytes,
        last_update = LastUpdate,
        min_hist = MinHistValues,
        hr_hist = HrHistValues,
        dy_hist = DyHistValues
    }) ->
        MinHist = time_slot_histogram:new(LastUpdate, ?MIN_TIME_WINDOW, MinHistValues),
        HrHist = time_slot_histogram:new(LastUpdate, ?HR_TIME_WINDOW, HrHistValues),
        DyHist = time_slot_histogram:new(LastUpdate, ?DY_TIME_WINDOW, DyHistValues),
        ActualTimestamp = utils:system_time_seconds(),
        {ok, Transfer#transfer{
            bytes_transferred = OldBytes + Bytes,
            last_update = ActualTimestamp,
            min_hist = time_slot_histogram:get_histogram_values(
                time_slot_histogram:increment(MinHist, ActualTimestamp, Bytes)
            ),
            hr_hist = time_slot_histogram:get_histogram_values(
                time_slot_histogram:increment(HrHist, ActualTimestamp, Bytes)
            ),
            dy_hist = time_slot_histogram:get_histogram_values(
                time_slot_histogram:increment(DyHist, ActualTimestamp, Bytes)
            )
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each successfully completed transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_successful_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each_successful_transfer(Callback, Acc0) ->
    for_each_transfer(?SUCCESSFUL_TRANSFERS_KEY, Callback, Acc0).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each failed transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_failed_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each_failed_transfer(Callback, Acc0) ->
    for_each_transfer(?FAILED_TRANSFERS_KEY, Callback, Acc0).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each unfinished transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_unfinished_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each_unfinished_transfer(Callback, Acc0) ->
    for_each_transfer(?UNFINISHED_TRANSFERS_KEY, Callback, Acc0).

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
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(TransferId, Diff) ->
    ?extract_key(datastore_model:update(?CTX, TransferId, Diff)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link to transfer.
%% @end
%%--------------------------------------------------------------------
-spec add_link(SourceId :: virtual_list_id(), TransferId :: id(),
    od_space:id()) -> ok.
add_link(SourceId, TransferId, SpaceId) ->
    TreeId = oneprovider:get_provider_id(),
    Ctx = ?CTX#{scope => SpaceId},
    {ok, _} = datastore_model:add_links(Ctx, SourceId, TreeId, {TransferId, <<>>}),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes link to transfer.
%% @end
%%--------------------------------------------------------------------
-spec remove_link(SourceId :: virtual_list_id(), TransferId :: id(),
    od_space:id()) -> ok.
remove_link(SourceId, TransferId, SpaceId) ->
    TreeId = oneprovider:get_provider_id(),
    Ctx = ?CTX#{scope => SpaceId},
    ok = datastore_model:delete_links(Ctx, SourceId, TreeId, TransferId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each successfully completed transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_transfer(
    virtual_list_id(), Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each_transfer(ListDocId, Callback, Acc0) ->
    datastore_model:fold_links(?CTX, ListDocId, all, fun
        (#link{name = Name}, Acc) -> {ok, Callback(Name, Acc)}
    end, Acc0, #{}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Restarts transfer referenced by given TransferId.
%% @end
%%-------------------------------------------------------------------
-spec restart(id()) -> {ok, id()}.
restart(TransferId) ->
    TimeSeconds = utils:system_time_seconds(),
    MinHist = time_slot_histogram:new(?MIN_TIME_WINDOW, 60),
    HrHist = time_slot_histogram:new(?HR_TIME_WINDOW, 24),
    DyHist = time_slot_histogram:new(?DY_TIME_WINDOW, 30),
    {ok, TransferId} = update(TransferId, fun(Transfer) ->

        TransferStatus = case Transfer#transfer.transfer_status of
            completed -> completed;
            skipped -> skipped;
            cancelled -> cancelled;
            _ -> scheduled
        end,

        InvalidationStatus = case Transfer#transfer.invalidation_status of
            completed -> completed;
            skipped -> skipped;
            cancelled -> cancelled;
            _ -> scheduled
        end,

        {ok, Transfer#transfer{
            transfer_status = TransferStatus,
            invalidation_status = InvalidationStatus,
            files_to_transfer = 0,
            files_transferred = 0,
            bytes_to_transfer = 0,
            bytes_transferred = 0,
            files_invalidated = 0,
            files_to_invalidate = 0,
            start_time = TimeSeconds,
            last_update = 0,
            min_hist = time_slot_histogram:get_histogram_values(MinHist),
            hr_hist = time_slot_histogram:get_histogram_values(HrHist),
            dy_hist = time_slot_histogram:get_histogram_values(DyHist)
        }}
    end),
    {ok, TransferDoc} = datastore_model:get(?CTX, TransferId),
    transfer_controller:on_new_transfer_doc(TransferDoc),
    invalidation_controller:on_new_transfer_doc(TransferDoc),
    {ok, TransferId}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Restarts all unfinished transfers.
%% @end
%%-------------------------------------------------------------------
-spec restart_unfinished_transfers() -> term().
restart_unfinished_transfers() ->
    for_each_unfinished_transfer(fun(TransferId, _AccIn) ->
        {ok, TransferId} = restart(TransferId)
    end, []).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Restarts all failed transfers.
%% @end
%%-------------------------------------------------------------------
-spec restart_failed_transfers() -> term().
restart_failed_transfers() ->
    for_each_failed_transfer(fun(TransferId, _AccIn) ->
        {ok, TransferId} = restart(TransferId)
    end, []).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pools responsible for replicating files and directories.
%% @end
%%-------------------------------------------------------------------
-spec start_pools() -> ok.
start_pools() ->
    {ok, _} = worker_pool:start_sup_pool(?REPLICATION_POOL, [
        {workers, ?REPLICATION_POOL_SIZE}, {queue_type, lifo}
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
    true = worker_pool:stop_pool(?REPLICATION_POOL),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Posthook responsible for stopping transfer or invalidation controller.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_completed(atom(), list(), term()) -> term().
maybe_mark_completed(update, [_, _, _], Result = {ok, #document{value = Transfer}}) ->
    handle_updated(Transfer),
    Result;
maybe_mark_completed(_, _, Result) ->
    Result.

handle_updated(#transfer{
    transfer_status = active,
    files_to_transfer = FilesToTransfer,
    files_transferred = FilesToTransfer,
    bytes_to_transfer = BytesToTransfer,
    bytes_transferred = BytesToTransfer,
    pid = Pid
}) ->
    transfer_controller:finish_transfer(decode_pid(Pid));
handle_updated(#transfer{
    transfer_status = TransferStatus,
    files_to_invalidate = FilesToInvalidate,
    files_invalidated = FilesToInvalidate,
    invalidation_status = active,
    pid = Pid
}) when TransferStatus =:= completed orelse TransferStatus =:= skipped ->
    invalidation_controller:finish_transfer(decode_pid(Pid));
handle_updated(_) ->
    ok.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
    2.

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
    BytesToTransfer, BytesTransferred, StartTime, LastUpdate, MinHist, HrHist,
    DyHist}
) ->
    {2, #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        path = Path,
        callback = CallBack,
        transfer_status = TransferStatus,
        invalidation_status = InvalidationStatus,
        source_provider_id = SourceProviderId,
        target_provider_id = TargetProviderId,
        invalidate_source_replica = InvalidateSourceReplica,
        pid = Pid,
        files_to_transfer = FilesToTransfer,
        files_transferred = FilesTransferred,
        bytes_to_transfer = BytesToTransfer,
        bytes_transferred = BytesTransferred,
        files_to_invalidate = 0,
        files_invalidated = 0,
        start_time = StartTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun maybe_mark_completed/3].