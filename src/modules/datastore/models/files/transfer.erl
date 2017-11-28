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
-export([start/6, stop/1, get_status/1, get_info/1, get/1, init/0, cleanup/0, decode_pid/1, encode_pid/1]).
-export([mark_active/1, mark_completed/1, mark_failed/2,
    mark_active_invalidation/1, mark_completed_invalidation/2, mark_failed_invalidation/2,
    mark_file_transfer_scheduled/2, mark_file_transfer_finished/2,
    mark_data_transfer_scheduled/2, mark_data_transfer_finished/2,
    for_each_successful_transfer/3, for_each_failed_transfer/3,
    for_each_unfinished_transfer/3, restart_unfinished_transfers/1,
    mark_file_invalidation_finished/2, mark_file_invalidation_scheduled/2]).

%% model_behaviour callbacks
-export([save/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).

-type id() :: binary().
-type status() :: scheduled | skipped | active | completed | cancelled | failed.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type doc() :: #document{value :: transfer()}.
-type virtual_list_id() :: binary(). % ?(SUCCESSFUL|FAILED|UNFINISHED)_TRANSFERS_KEY

-export_type([id/0, status/0, callback/0, doc/0]).

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
        {pid, string}, %todo VFS-3657
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
    {ok, TransferId} = create(ToCreate),
    session:add_transfer(SessionId, TransferId),
    ok = add_link(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
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
    {Restarted, Failed} = for_each_unfinished_transfer(fun(TransferId, {Restarted0, Failed0}) ->
        case restart(TransferId) of
            {ok, TransferId} ->
                {[TransferId | Restarted0], Failed0};
            {error, {not_found, transfer}} ->
                {Restarted0, [TransferId | Failed0]}
        end
    end, {[], []}, SpaceId),
    remove_unfinished_transfers_links(Failed, SpaceId),
    Restarted.

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: id()) -> status().
get_status(TransferId) ->
    {ok, #document{value = #transfer{transfer_status = Status}}} = get(TransferId),
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
    }}} = get(TransferId),
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

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec stop(id()) -> ok | {error, term()}.
stop(TransferId) ->
    {ok, #document{value = #transfer{space_id = SpaceId}}} = get(TransferId),
    ok = remove_links(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
    delete(TransferId).

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
    transfer:update(TransferId, fun(Transfer) ->
        case Transfer#transfer.invalidation_status of
            skipped ->
                SpaceId = Transfer#transfer.space_id,
                ok = add_link(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
                ok = remove_links(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
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
    ok = add_link(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
    {ok, _} = transfer:update(TransferId, #{transfer_status => failed}).

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
    ok = add_link(?SUCCESSFUL_TRANSFERS_KEY, TransferId, SpaceId),
    ok = remove_links(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
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
    transfer:update(TransferId, fun(Transfer) ->
        ok = add_link(?FAILED_TRANSFERS_KEY, TransferId, SpaceId),
        ok = remove_links(?UNFINISHED_TRANSFERS_KEY, TransferId, SpaceId),
        Transfer#transfer{invalidation_status = failed}
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
    {ok, id()} | {error, term()}.
mark_data_transfer_scheduled(undefined, _Bytes) ->
    {ok, undefined};
mark_data_transfer_scheduled(TransferId, Bytes) ->
    transfer:update(TransferId, fun(Transfer) ->
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
    {ok, id()} | {error, term()}.
mark_data_transfer_finished(undefined, _Bytes) ->
    {ok, undefined};
mark_data_transfer_finished(TransferId, Bytes) ->
    transfer:update(TransferId, fun(Transfer = #transfer{
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
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_successful_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?SUCCESSFUL_TRANSFERS_KEY, Callback, Acc0, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each failed transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_failed_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_failed_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?FAILED_TRANSFERS_KEY, Callback, Acc0, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each unfinished transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_unfinished_transfer(
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_unfinished_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?UNFINISHED_TRANSFERS_KEY, Callback, Acc0, SpaceId).

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
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

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
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

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
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(transfer_bucket, [{transfer, update}], ?GLOBALLY_CACHED_LEVEL,
        ?GLOBALLY_CACHED_LEVEL, true, false, oneprovider:get_provider_id()),
    Config#model_config{version = 2, sync_enabled = true}.

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
%% Adds link to transfer
%% @end
%%--------------------------------------------------------------------
-spec add_link(SourceId :: virtual_list_id(), TransferId :: id(),
    SpaceId :: od_space:id()) -> ok.
add_link(SourceId, TransferId, SpaceId) ->
    model:execute_with_default_context(?MODULE, add_links, [
        link_root(SourceId, SpaceId), {TransferId, {TransferId, ?MODEL_NAME}}
    ], [{scope, SpaceId}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes link to transfer
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
-spec for_each_transfer(
    virtual_list_id(), Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_transfer(ListDocId, Callback, Acc0, SpaceId) ->
    model:execute_with_default_context(?MODULE, foreach_link, [link_root(ListDocId, SpaceId),
        fun(LinkName, _LinkTarget, Acc) ->
            Callback(LinkName, Acc)
        end, Acc0]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Restarts transfer referenced by given TransferId.
%% @end
%%-------------------------------------------------------------------
-spec restart(id()) -> {ok, id()} | {error, term()}.
restart(TransferId) ->
    TimeSeconds = utils:system_time_seconds(),
    MinHist = time_slot_histogram:new(?MIN_TIME_WINDOW, 60),
    HrHist = time_slot_histogram:new(?HR_TIME_WINDOW, 24),
    DyHist = time_slot_histogram:new(?DY_TIME_WINDOW, 30),
    UpdateFun = fun(Transfer) ->
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
    end,
    case update(TransferId, UpdateFun) of
        {ok, TransferId} ->
            {ok, TransferDoc} = get(TransferId),
            transfer_controller:on_new_transfer_doc(TransferDoc),
            invalidation_controller:on_new_transfer_doc(TransferDoc),
            {ok, TransferId};
        Error ->
            ?error_stacktrace("Restarting transfer ~p faile due to ~p", [TransferId, Error]),
            Error
    end.

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
-spec handle_updated(transfer()) -> ok.
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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Removes all TransferIds from unfinished_transfer
%% @end
%%-------------------------------------------------------------------
-spec remove_unfinished_transfers_links([id()], od_space:id()) -> ok.
remove_unfinished_transfers_links(TransferIds, SpaceId) ->
    remove_links(?UNFINISHED_TRANSFERS_KEY, TransferIds, SpaceId).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns links tree root for given space.
%% @end
%%-------------------------------------------------------------------
-spec link_root(binary(), od_space:id()) -> binary().
link_root(Prefix, SpaceId) ->
    <<Prefix/binary, "_", SpaceId/binary>>.