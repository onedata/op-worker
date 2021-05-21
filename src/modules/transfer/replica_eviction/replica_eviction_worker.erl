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

-behaviour(gen_transfer_worker).
-behaviour(replica_deletion_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    enqueue_data_transfer/2,
    process_replica_deletion_result/4
]).

%% gen_transfer_worker callbacks
-export([
    enqueue_data_transfer/4,
    transfer_regular_file/2,
    view_querying_chunk_size/0,
    max_transfer_retries/0,
    required_permissions/0
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined).
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams) ->
    enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined).

%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_deletion_worker after deleting file
%% replica.
%% @end
%%-------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), od_space:id(), file_meta:uuid(),
    transfer:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p in space ~p released ~p bytes.", [
        FileUuid, TransferId, SpaceId, ReleasedBytes
    ]),
    {ok, _} = transfer:increment_files_evicted_and_processed_counters(TransferId),
    ok;
process_replica_deletion_result({error, canceled}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p in space ~p was canceled.", [FileUuid, TransferId, SpaceId]),
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok;
process_replica_deletion_result({error, file_opened}, SpaceId, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p in space ~p skipped because the file is opened.",
        [FileUuid, TransferId, SpaceId]),
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok;
process_replica_deletion_result(Error, SpaceId, FileUuid, TransferId) ->
    ?error("Error ~p occurred during replica eviction of file ~p in transfer ~p in space ~p", [
        Error, FileUuid, TransferId, SpaceId
    ]),
    {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId),
    ok.

%%%===================================================================
%%% gen_transfer_worker callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback required_permissions/0.
%% @end
%%--------------------------------------------------------------------
-spec required_permissions() -> [data_access_control:requirement()].
required_permissions() ->
    []. % todo VFS-4844

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback max_transfer_retries/0.
%% @end
%%--------------------------------------------------------------------
-spec max_transfer_retries() -> non_neg_integer().
max_transfer_retries() ->
    op_worker:get_env(max_eviction_retries_per_file_replica, 5).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback view_querying_chunk_size/0.
%% @end
%%--------------------------------------------------------------------
-spec view_querying_chunk_size() -> non_neg_integer().
view_querying_chunk_size() ->
    op_worker:get_env(replica_eviction_by_view_batch, 1000).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback enqueue_data_transfer/4.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams = #transfer_params{transfer_id = TransferId},
    RetriesLeft, NextRetry
) ->
    case file_ctx:is_trash_dir_const(FileCtx) andalso is_migration(TransferParams) of
        true ->
            % ignore trash directory in case of migration
            transfer:increment_files_processed_counter(TransferId);
        false ->
            RetriesLeft2 = utils:ensure_defined(RetriesLeft, max_transfer_retries()),
            worker_pool:cast(?REPLICA_EVICTION_WORKERS_POOL, ?TRANSFER_DATA_REQ(
                FileCtx, TransferParams, RetriesLeft2, NextRetry
            ))
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback transfer_regular_file/2.
%%
%% Schedules safe file_replica_deletion via replica_deletion mechanism.
%% If SupportingProviderId is undefined, it will be chosen from
%% providers who have given file replicated.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, Params = #transfer_params{supporting_provider = undefined}) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TransferId = Params#transfer_params.transfer_id,
    case replica_deletion_master:find_supporter_and_prepare_deletion_request(FileCtx) of
        undefined ->
            transfer:increment_files_processed_counter(TransferId);
        DeletionRequest ->
            replica_deletion_master:request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB)
    end,
    ok;
transfer_regular_file(FileCtx, #transfer_params{
    transfer_id = TransferId,
    supporting_provider = SupportingProvider
}) ->
    {LocalFileLocationDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, _FileCtx3} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    DeletionRequest = replica_deletion_master:prepare_deletion_request(FileUuid, SupportingProvider, Blocks, VV),
    replica_deletion_master:request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB).


-spec is_migration(transfer_params()) -> boolean().
is_migration(#transfer_params{supporting_provider = undefined}) ->
    false;
is_migration(#transfer_params{supporting_provider = SupportingProvider}) when is_binary(SupportingProvider) ->
    true.