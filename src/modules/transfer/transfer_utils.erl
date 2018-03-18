%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Utils functions fon operating on transfer record.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_utils).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([encode_pid/1, decode_pid/1,
    is_active/1, is_finished/1, is_invalidation/1, is_migration/1,
    get_status/1, is_ongoing/1, is_transfer_ongoing/1, is_invalidation_ongoing/1,
    get_start_time/1, get_finish_time/1, get_info/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This functions checks whether given transfer is active.
%% Transfer is considered active when:
%%  * it's status is active
%%  * it's controller is alive (to avoid stalled transfers after restart)
%% @end
%%-------------------------------------------------------------------
-spec is_active(transfer:transfer()) -> boolean().
is_active(#transfer{
    pid = ControllerPid,
    status = active
}) ->
    erlang:process_info(decode_pid(ControllerPid)) =/= undefined;
is_active(#transfer{
    pid = ControllerPid,
    invalidation_status = active
}) ->
    erlang:process_info(decode_pid(ControllerPid)) =/= undefined;
is_active(_) ->
    false.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given transfer has been finished.
%% @end
%%-------------------------------------------------------------------
-spec is_finished(transfer:transfer()) -> boolean().
is_finished(#transfer{
    status = ReplicationStatus,
    invalidation_status = InvalidationStatus
}) ->
    lists:member(ReplicationStatus, [completed, skipped, failed, cancelled]) and
    lists:member(InvalidationStatus, [completed, skipped, failed, cancelled]).

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
%% Predicate saying if given transfer is invalidating a replica.
%% @end
%%-------------------------------------------------------------------
-spec is_invalidation(transfer:transfer()) -> boolean().
is_invalidation(#transfer{invalidate_source_replica = Flag}) -> Flag.

%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if given transfer is migrating replica.
%% (If target_provider_id is defined)
%% @end
%%-------------------------------------------------------------------
-spec is_migration(transfer:transfer()) -> boolean().
is_migration(#transfer{target_provider_id = undefined}) ->
    false;
is_migration(Transfer) ->
    is_invalidation(Transfer).

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: transfer:id()) -> transfer:status().
get_status(TransferId) ->
    {ok, #document{value = #transfer{status = Status}}} = transfer:get(TransferId),
    Status.

%%------------------------------------------------------------------
%% @doc
%% Getter for start_time field of transfer record.
%% @end
%%-------------------------------------------------------------------
-spec get_start_time(transfer:id()) -> transfer:timestamp().
get_start_time(TransferId) ->
    {ok,  #document{value = #transfer{start_time = StartTime}}} = transfer:get(TransferId),
    StartTime.

%%-------------------------------------------------------------------
%% @doc
%% Getter for finish_time field of transfer record.
%% @end
%%-------------------------------------------------------------------
-spec get_finish_time(transfer:id()) -> transfer:timestamp().
get_finish_time(TransferId) ->
    {ok,  #document{value = #transfer{finish_time = FinishTime}}} = transfer:get(TransferId),
    FinishTime.

%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if given transfer is ongoing.
%%  * Replication is considered ongoing when data transfer hasn't finished.
%%  * Migration is considered ongoing when data transfer or replica
%%      invalidation hasn't finished.
%% @end
%%-------------------------------------------------------------------
-spec is_ongoing(transfer:transfer() | transfer:id() | undefined) -> boolean().
is_ongoing(undefined) ->
    true;
is_ongoing(Transfer = #transfer{}) ->
    is_transfer_ongoing(Transfer) orelse is_invalidation_ongoing(Transfer);
is_ongoing(TransferId) ->
    {ok, #document{value = Transfer}} = transfer:get(TransferId),
    is_ongoing(Transfer).


%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if given transfer is ongoing. Checks only if data transfer
%% is finished, no matter if that is a replication or migration.
%% @end
%%-------------------------------------------------------------------
-spec is_transfer_ongoing(transfer:transfer()) -> boolean().
is_transfer_ongoing(#transfer{status = scheduled}) -> true;
is_transfer_ongoing(#transfer{status = skipped}) -> false;
is_transfer_ongoing(#transfer{status = active}) -> true;
is_transfer_ongoing(#transfer{status = completed}) -> false;
is_transfer_ongoing(#transfer{status = cancelled}) -> false;
is_transfer_ongoing(#transfer{status = failed}) -> false.

%%-------------------------------------------------------------------
%% @doc
%% Predicate saying if invalidation within given transfer is ongoing. Returns
%% false for transfers that are not a migration.
%% @end
%%-------------------------------------------------------------------
-spec is_invalidation_ongoing(transfer:transfer()) -> boolean().
is_invalidation_ongoing(#transfer{invalidate_source_replica = false}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = completed}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = skipped}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = cancelled}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = failed}) -> false;
is_invalidation_ongoing(#transfer{invalidation_status = scheduled}) -> true;
is_invalidation_ongoing(#transfer{invalidation_status = active}) -> true.

%%--------------------------------------------------------------------
%% @doc
%% Gets transfer info
%% @end
%%--------------------------------------------------------------------
-spec get_info(TransferId :: transfer:id()) -> maps:map().
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
        files_to_process = FilesToProcess,
        files_processed = FilesProcessed,
        failed_files = FailedFiles,
        files_transferred = FilesTransferred,
        bytes_transferred = BytesTransferred,
        files_invalidated = FilesInvalidated,
        start_time = StartTime,
        finish_time = FinishTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist,
        mth_hist = MthHist
    }}} = transfer:get(TransferId),
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
        <<"filesToProcess">> => FilesToProcess,
        <<"filesProcessed">> => FilesProcessed,
        <<"filesTransferred">> => FilesTransferred,
        <<"failedFiles">> => FailedFiles,
        <<"filesInvalidated">> => FilesInvalidated,
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
