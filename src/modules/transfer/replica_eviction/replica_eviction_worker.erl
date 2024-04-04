%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of gen_transfer_worker for replica_eviction_workers_pool.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_worker).
-author("Jakub Kudzia").
-author("Bartosz Walkowicz").

-behaviour(transfer_traverse_worker).
-behaviour(replica_deletion_behaviour).

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([process_replica_deletion_result/4]).

%% gen_transfer_worker callbacks
-export([
    transfer_regular_file/2,
    required_permissions/0
]).

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_deletion_worker after deleting file
%% replica.
%% @end
%%-------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), od_space:id(), file_meta:uuid(),
    transfer:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~tp in transfer ~tp in space ~tp released ~tp bytes.", [
        FileUuid, TransferId, SpaceId, ReleasedBytes
    ]),
    {ok, _} = transfer:increment_files_evicted_and_processed_counters(TransferId),
    ok;
process_replica_deletion_result({error, canceled}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~tp in transfer ~tp in space ~tp was canceled.", [FileUuid, TransferId, SpaceId]),
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok;
process_replica_deletion_result({error, file_opened}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~tp in transfer ~tp in space ~tp skipped because the file is opened.",
        [FileUuid, TransferId, SpaceId]),
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok;
process_replica_deletion_result(Error, SpaceId, FileUuid, TransferId) ->
    ?error("Error during replica eviction ~ts", [?autoformat([
        Error, FileUuid, TransferId, SpaceId
    ])]),
    {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId),
    ok.


%%%===================================================================
%%% transfer_traverse_worker callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_traverse_worker} callback required_permissions/0.
%% @end
%%--------------------------------------------------------------------
-spec required_permissions() -> [data_access_control:requirement()].
required_permissions() ->
    % TODO VFS-10259 use offline session in transfer
    [?TRAVERSE_ANCESTORS].


%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_traverse_worker} callback transfer_regular_file/2.
%%
%% Schedules safe file_replica_deletion via replica_deletion mechanism.
%% If SupportingProviderId is undefined, it will be chosen from
%% providers who have given file replicated.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_traverse_worker:traverse_info()) ->
    ok | {error, term()}.
transfer_regular_file(FileCtx, #{
    transfer_id := TransferId,
    replica_holder_provider_id := undefined
}) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case replica_deletion_master:find_supporter_and_prepare_deletion_request(FileCtx) of
        undefined ->
            transfer:increment_files_processed_counter(TransferId);
        DeletionRequest ->
            replica_deletion_master:request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB)
    end,
    ok;

transfer_regular_file(FileCtx, #{
    transfer_id := TransferId,
    replica_holder_provider_id := ReplicaHolderProviderId
}) ->
    {LocalFileLocationDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, _FileCtx3} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    DeletionRequest = replica_deletion_master:prepare_deletion_request(FileUuid, ReplicaHolderProviderId, Blocks, VV),
    replica_deletion_master:request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB).
