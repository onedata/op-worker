%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests synchronizing and getting
%%% synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(sync_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([
    synchronize_block/5,
    synchronize_block_and_compute_checksum/3,
    get_file_distribution/2,
    enqueue_file_replication/4,
    start_transfer/5,
    enqueue_transfer/3
]).
-export([
    schedule_file_replication/4, schedule_file_replication/5, replicate_file/4,
    schedule_replica_invalidation/4, schedule_replica_invalidation/5, invalidate_file_replica/5
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), fslogic_blocks:block(),
    Prefetch :: boolean(), undefined | transfer:id()) ->
    fslogic_worker:fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch, TransferId) ->
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx), % trigger file_location creation
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    synchronize_block(UserCtx, FileCtx3, #file_block{offset = 0, size = Size}, Prefetch, TransferId);
synchronize_block(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
    ok = replica_synchronizer:synchronize(UserCtx, FileCtx, Block, Prefetch, TransferId),
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(),
    file_ctx:ctx(), fslogic_blocks:block()) -> fslogic_worker:fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx, Range = #file_block{offset = Offset, size = Size}) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, Handle} = lfm_files:open(SessId, {guid, FileGuid}, read), %todo do not use lfm, operate on fslogic directly
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally
    lfm_files:release(Handle),

    Checksum = crypto:hash(md4, Data),
    {LocationToSend, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #sync_response{
            checksum = Checksum,
            file_location = LocationToSend
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_distribution(_UserCtx, FileCtx) ->
    {Locations, _FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    ProviderDistributions = lists:map(fun(#document{
        value = #file_location{
            provider_id = ProviderId,
            blocks = Blocks
        }
    }) ->
        #provider_file_distribution{
            provider_id = ProviderId,
            blocks = Blocks
        }
    end, Locations),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_distribution{
            provider_file_distributions = ProviderDistributions
        }
    }.

%%-------------------------------------------------------------------
%% @doc
%% Adds task for starting given transfer to worker_pool
%% @end
%%-------------------------------------------------------------------
-spec enqueue_transfer(user_ctx:ctx(), file_ctx:ctx(), undefined | transfer:id()) -> ok.
enqueue_transfer(UserCtx, FileCtx, TransferId) ->
    worker_pool:cast(?TRANSFER_WORKERS_POOL,
        {start_transfer, UserCtx, FileCtx, undefined, TransferId, self()}).

%%-------------------------------------------------------------------
%% @doc
%% Marks transfer as active and adds task for replication to worker_pool.
%% @end
%%-------------------------------------------------------------------
-spec start_transfer(user_ctx:ctx(), file_ctx:ctx(),
    undefined | fslogic_blocks:block(), undefined | transfer:id(), pid()) -> ok.
start_transfer(UserCtx, FileCtx, Block, TransferId, TransferControllerPid) ->
    {ok, _} = transfer:mark_active(TransferId, TransferControllerPid),
    enqueue_file_replication(UserCtx, FileCtx, Block, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    od_provider:id(), transfer:callback()) -> fslogic_worker:provider_response().
schedule_file_replication(UserCtx, FileCtx, TargetProviderId, Callback) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    schedule_file_replication(UserCtx, FileCtx, FilePath, TargetProviderId, Callback).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:path(), od_provider:id(), transfer:callback()) ->
    fslogic_worker:provider_response().
schedule_file_replication(UserCtx, FileCtx, FilePath, TargerProviderId, Callback) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(
        SessionId, FileGuid, FilePath, undefined, TargerProviderId, Callback, false
    ),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Schedules invalidation of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    SourceProviderId :: od_provider:id(), MigrationProviderId :: undefined | od_provider:id()) ->
    fslogic_worker:provider_response().
schedule_replica_invalidation(UserCtx, FileCtx, SourceProviderId, MigrationProviderId) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    schedule_replica_invalidation(UserCtx, FileCtx, FilePath, SourceProviderId, MigrationProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Schedules invalidation of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:path(), od_provider:id(), od_provider:id()) ->
    fslogic_worker:provider_response().
schedule_replica_invalidation(UserCtx, FileCtx, FilePath, SourceProviderId, MigrationProviderId) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(
        SessionId, FileGuid, FilePath, SourceProviderId, MigrationProviderId, undefined, true
    ),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.


%%--------------------------------------------------------------------
%% @doc
%% @equiv replicate_file_insecure/5 with permission check
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(user_ctx:ctx(), file_ctx:ctx(),
    undefined | fslogic_blocks:block(), undefined | transfer:id()) ->
    fslogic_worker:provider_response().
replicate_file(UserCtx, FileCtx, Block, TransferId) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, Block, 0, TransferId],
        fun replicate_file_insecure/5).

%%--------------------------------------------------------------------
%% @doc
%% @equiv invalidate_file_replica_internal/4 but catches exception and
%% notifies invalidation_controller
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica(user_ctx:ctx(), file_ctx:ctx(),
    undefined | fslogic_blocks:block(), undefined | transfer:id(),
    autocleaning:id() | undefined) -> fslogic_worker:provider_response().
invalidate_file_replica(UserCtx, FileCtx, MigrationProviderId, undefined, AutoCleaningId) ->
    invalidate_file_replica_internal(UserCtx, FileCtx, MigrationProviderId, undefined, AutoCleaningId);
invalidate_file_replica(UserCtx, FileCtx, MigrationProviderId, TransferId, AutoCleaningId) ->
    try
        invalidate_file_replica_internal(UserCtx, FileCtx, MigrationProviderId, TransferId, AutoCleaningId)
    catch
        _:Error ->
            {ok, #document{value = #transfer{pid = Pid}}} = transfer:get(TransferId),
            invalidation_controller:failed_transfer(transfer:decode_pid(Pid), Error)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file replication to worker from ?TRANSFER_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    undefined | fslogic_blocks:block(), transfer:id() | undefined) -> ok.
enqueue_file_replication(UserCtx, FileCtx, Block, TransferId) ->
    worker_pool:cast(?TRANSFER_WORKERS_POOL,
        {start_file_replication, UserCtx, FileCtx, Block, TransferId}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv invalidate_file_replica_insecure/3 with permission check
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica_internal(user_ctx:ctx(), file_ctx:ctx(),
    undefined | oneprovider:id(), undefined | transfer:id(),
    autocleaning:id() | undefined) -> fslogic_worker:provider_response().
invalidate_file_replica_internal(UserCtx, FileCtx, MigrationProviderId, TransferId, AutocleaningId) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, MigrationProviderId, 0, TransferId, AutocleaningId],
        fun invalidate_file_replica_insecure/6).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file_insecure(user_ctx:ctx(), file_ctx:ctx(),
    fslogic_blocks:block(), non_neg_integer(), undefined | transfer:id()) ->
    fslogic_worker:provider_response().
replicate_file_insecure(UserCtx, FileCtx, Block, Offset, TransferId) ->
    case transfer:should_continue(TransferId) of
        true ->
            {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
            case file_ctx:is_dir(FileCtx) of
                {true, FileCtx2} ->
                    case file_ctx:get_file_children(FileCtx2, UserCtx, Offset, Chunk) of
                        {Children, _FileCtx3} when length(Children) < Chunk ->
                            transfer:mark_file_transfer_scheduled(TransferId, length(Children)),
                            replicate_children(UserCtx, Children, Block, TransferId),
                            transfer:mark_file_transfer_finished(TransferId, 1),
                            #provider_response{status = #status{code = ?OK}};
                        {Children, FileCtx3} ->
                            transfer:mark_file_transfer_scheduled(TransferId, Chunk),
                            replicate_children(UserCtx, Children, Block, TransferId),
                            replicate_file_insecure(UserCtx, FileCtx3, Block, Offset + Chunk, TransferId)
                    end;
                {false, FileCtx2} ->
                    #fuse_response{status = Status} =
                        synchronize_block(UserCtx, FileCtx2, Block, false, TransferId),
                    transfer:mark_file_transfer_finished(TransferId, 1),
                    #provider_response{status = Status}
            end;
        false ->
            throw({transfer_cancelled, TransferId})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates children list.
%% @end
%%--------------------------------------------------------------------
-spec replicate_children(user_ctx:ctx(), [file_ctx:ctx()],
    fslogic_blocks:block(), undefined | transfer:id()) -> ok.
replicate_children(UserCtx, Children, Block, TransferId) ->
    lists:foreach(fun(ChildCtx) ->
        enqueue_file_replication(UserCtx, ChildCtx, Block, TransferId)
    end, Children).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replica of given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica_insecure(user_ctx:ctx(), file_ctx:ctx(),
    oneprovider:id() | undefined, non_neg_integer(), undefined | transfer:id(),
    autocleaning:id() | undefined) -> fslogic_worker:provider_response().
invalidate_file_replica_insecure(UserCtx, FileCtx, MigrationProviderId, Offset,
    TransferId, AutoCleaningId
) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            case file_ctx:get_file_children(FileCtx2, UserCtx, Offset, Chunk) of
                {Children, _FileCtx3} when length(Children) < Chunk ->
                    transfer:mark_file_invalidation_scheduled(TransferId, length(Children)),
                    invalidate_children_replicas(UserCtx, Children, MigrationProviderId, TransferId, AutoCleaningId),
                    transfer:mark_file_invalidation_finished(TransferId, 1),
                    #provider_response{status = #status{code = ?OK}};
                {Children, FileCtx3} ->
                    transfer:mark_file_invalidation_scheduled(TransferId, Chunk),
                    invalidate_children_replicas(UserCtx, Children, MigrationProviderId, TransferId, AutoCleaningId),
                    invalidate_file_replica_insecure(UserCtx, FileCtx3,
                        MigrationProviderId, Offset + Chunk, TransferId, AutoCleaningId)
            end;
        {false, FileCtx2} ->
            case replica_finder:get_unique_blocks(FileCtx2) of
                {[], FileCtx3} ->
                    {ReleasedSize, _FileCtx4} = file_ctx:get_local_storage_file_size(FileCtx3),
                    Response = #provider_response{status = #status{code = ?OK}}
                        = invalidate_fully_redundant_file_replica(UserCtx, FileCtx3),
                    autocleaning:mark_released_file(AutoCleaningId, ReleasedSize),
                    transfer:mark_file_invalidation_finished(TransferId, 1),
                    Response;
                {_Other, FileCtx3} ->
                    Response = invalidate_partially_unique_file_replica(UserCtx,
                        FileCtx3, MigrationProviderId
                    ),
                    % todo VFS-3795
                    transfer:mark_file_invalidation_finished(TransferId, 1),
                    Response
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replica of file whose data is partially unique
%% (stored locally in one copy)
%% @end
%%--------------------------------------------------------------------
-spec invalidate_partially_unique_file_replica(user_ctx:ctx(), file_ctx:ctx(),
    oneprovider:id() | undefined) -> fslogic_worker:provider_response().
invalidate_partially_unique_file_replica(_UserCtx, _FileCtx, undefined) ->
    #provider_response{status = #status{code = ?OK}};
invalidate_partially_unique_file_replica(_UserCtx, _FileCtx, _MigrationProviderId) ->
    %% @TODO VFS-3728
%%    SessionId = user_ctx:get_session_id(UserCtx),
%%    FileGuid = file_ctx:get_guid_const(FileCtx),
%%    ok = logical_file_manager:schedule_file_replication(SessionId, {guid, FileGuid}, MigrationProviderId),
%%    invalidate_fully_redundant_file_replica(UserCtx, FileCtx).
    #provider_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replica of file whose data is not unique
%% (it is stored also on other providers)
%% @end
%%--------------------------------------------------------------------
-spec invalidate_fully_redundant_file_replica(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
invalidate_fully_redundant_file_replica(UserCtx, FileCtx) ->
    #fuse_response{status = #status{code = ?OK}} =
        truncate_req:truncate_insecure(UserCtx, FileCtx, 0, false),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    LocalFileId = file_location:local_id(FileUuid),
    case file_location:delete(LocalFileId) of
        ok -> ok;
        {error, {not_found, _}} -> ok
    end,
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replicas of children list.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_children_replicas(user_ctx:ctx(), [file_ctx:ctx()],
    oneprovider:id(), undefined | transfer:id(), undefined | autocleaning:id()) -> ok.
invalidate_children_replicas(UserCtx, Children, MigrationProviderId, TransferId, AutoCleaningId) ->
    lists:foreach(fun(ChildCtx) ->
        %todo possible parallelization on a process pool, cannot parallelize as
        %todo in replication because you must be sure that all children are invalidated before parent
        invalidate_file_replica(UserCtx, ChildCtx, MigrationProviderId, TransferId, AutoCleaningId)
    end, Children).

