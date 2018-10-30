%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of gen_transfer_worker for replica_eviction_workers_pool.
%%% Worker is responsible for replica eviction of one file.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_worker).
-author("Jakub Kudzia").
-author("Bartosz Walkowicz").

-behaviour(gen_transfer_worker).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    enqueue_data_transfer/2,
    process_replica_deletion_result/3
]).

%% gen_transfer_worker callbacks
-export([
    enqueue_data_transfer/4,
    transfer_regular_file/2,
    index_querying_chunk_size/0,
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
-spec process_replica_deletion_result(replica_deletion:result(), file_meta:uuid(),
    transfer:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p released ~p bytes.", [
        FileUuid, TransferId, ReleasedBytes
    ]),
    {ok, _} = transfer:increment_files_evicted_and_processed_counters(TransferId),
    ok;
process_replica_deletion_result(Error, FileUuid, TransferId) ->
    ?error("Error ~p occured during replica eviction of file ~p in procedure ~p", [
        Error, FileUuid, TransferId
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
-spec required_permissions() -> list().
required_permissions() ->
    []. % todo VFS-4844

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback max_transfer_retries/0.
%% @end
%%--------------------------------------------------------------------
-spec max_transfer_retries() -> non_neg_integer().
max_transfer_retries() ->
    application:get_env(?APP_NAME, max_eviction_retries_per_file_replica, 5).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback index_querying_chunk_size/0.
%% @end
%%--------------------------------------------------------------------
-spec index_querying_chunk_size() -> non_neg_integer().
index_querying_chunk_size() ->
    application:get_env(?APP_NAME, replica_eviction_by_index_batch, 1000).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback enqueue_data_transfer/4.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams, RetriesLeft, NextRetry) ->
    RetriesLeft2 = utils:ensure_defined(RetriesLeft, undefined, max_transfer_retries()),
    worker_pool:cast(?REPLICA_EVICTION_WORKERS_POOL, ?TRANSFER_DATA_REQ(
        FileCtx, TransferParams, RetriesLeft2, NextRetry
    )),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback transfer_regular_file/2.
%%
%% Schedules safe file_replica_deletion via replica_deletion mechanism.
%% If SupportingProviderId is undefined, it will bo chosen from
%% providers who have given file replicated.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, Params = #transfer_params{supporting_provider = undefined}) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TransferId = Params#transfer_params.transfer_id,
    case replica_deletion_master:get_setting_for_deletion_task(FileCtx) of
        undefined ->
            transfer:increment_files_processed_counter(TransferId);
        {FileUuid, ProviderId, Blocks, VV} ->
            schedule_replica_deletion_task(
                FileUuid, ProviderId, Blocks, VV, TransferId, SpaceId
            )
    end,
    ok;
transfer_regular_file(FileCtx, #transfer_params{
    transfer_id = TransferId,
    supporting_provider = SupportingProvider
}) ->
    {LocalFileLocationDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, FileCtx} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    schedule_replica_deletion_task(
        FileUuid, SupportingProvider, Blocks, VV, TransferId, SpaceId
    ),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Adds task of replica deletion to replica_deletion_master queue.
%% @end
%%-------------------------------------------------------------------
-spec schedule_replica_deletion_task(file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(), transfer:id(),
    od_space:id()) -> ok.
schedule_replica_deletion_task(FileUuid, Provider, Blocks, VV, TransferId, SpaceId) ->
    replica_deletion_master:enqueue_task(
        FileUuid, Provider, Blocks, VV, TransferId, eviction, SpaceId
    ).
