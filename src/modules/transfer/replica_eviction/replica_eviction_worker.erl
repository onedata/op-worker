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

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([
    enqueue_data_transfer/2,
    process_replica_deletion_result/3
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
-spec process_replica_deletion_result(replica_deletion:result(), file_meta:uuid(),
    transfer:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p released ~p bytes.", [
        FileUuid, TransferId, ReleasedBytes
    ]),
    {ok, _} = transfer:increment_files_evicted_and_processed_counters(TransferId),
    ok;
process_replica_deletion_result(Error, FileUuid, TransferId) ->
    ?error("Error ~p occurred during replica eviction of file ~p in procedure ~p", [
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
-spec required_permissions() -> [check_permissions:raw_access_definition()].
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
%% {@link transfer_worker_behaviour} callback view_querying_chunk_size/0.
%% @end
%%--------------------------------------------------------------------
-spec view_querying_chunk_size() -> non_neg_integer().
view_querying_chunk_size() ->
    application:get_env(?APP_NAME, replica_eviction_by_view_batch, 1000).

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
%% Checks whether file replica is protected by QoS and schedules eviction appropriately.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, #transfer_params{
    transfer_id = TransferId,
    supporting_provider = SupportingProvider
}) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    % TODO: use actual storage id
    EvictingStorage = oneprovider:get_id_or_undefined(),
    SupportingStorage = SupportingProvider,
    QosStorages = case file_qos:get_effective(Uuid) of
        undefined -> #{};
        #file_qos{target_storages = TS} -> TS
    end,

    EquivalentStorages =
        providers_qos:get_provider_qos(EvictingStorage)
            ==
        providers_qos:get_provider_qos(SupportingStorage),

    case {maps:is_key(EvictingStorage, QosStorages), EquivalentStorages andalso
        not maps:is_key(SupportingStorage, QosStorages)} of
        {true, true} ->
            Diff = fun(#file_qos{target_storages = TS}) ->
                PrevStorageQosList = maps:get(EvictingStorage, TS, []),
                NewStorageQosList = maps:get(SupportingStorage, TS, []),
                {ok, #file_qos{target_storages = (maps:remove(EvictingStorage, TS))#{
                    SupportingStorage => NewStorageQosList ++ PrevStorageQosList}
                }}
            end,
            QosList = maps:get(EvictingStorage, QosStorages),
            NewFileDoc = #document{
                key = Uuid,
                scope = file_ctx:get_space_id_const(FileCtx),
                value = #file_qos{
                    qos_list = QosList,
                    target_storages = #{SupportingStorage => QosList}
                }
            },
            file_qos:create_or_update(NewFileDoc, Diff),
            schedule_regular_file_eviction(FileCtx, TransferId, SupportingProvider);
        {true, false} ->
            transfer:increment_files_processed_counter(TransferId),
            ok;
        _ ->
            schedule_regular_file_eviction(FileCtx, TransferId, SupportingProvider)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Schedules safe file_replica_deletion via replica_deletion mechanism.
%% If SupportingProviderId is undefined, it will be chosen from
%% providers who have given file replicated.
%% @end
%%-------------------------------------------------------------------
-spec schedule_regular_file_eviction(file_ctx:ctx(), transfer:id(), od_provider:id() | undefined) -> ok.
schedule_regular_file_eviction(FileCtx, TransferId, undefined) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case replica_deletion_master:get_setting_for_deletion_task(FileCtx) of
        undefined ->
            transfer:increment_files_processed_counter(TransferId);
        {FileUuid, ProviderId, Blocks, VV} ->
            schedule_replica_deletion_task(
                FileUuid, ProviderId, Blocks, VV, TransferId, SpaceId
            )
    end;
schedule_regular_file_eviction(FileCtx, TransferId, SupportingProvider) ->
    {LocalFileLocationDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, _FileCtx3} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    schedule_replica_deletion_task(
        FileUuid, SupportingProvider, Blocks, VV, TransferId, SpaceId
    ).

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
