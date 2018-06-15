%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests invalidating file
%%% replicas (including whole file trees).
%%% @end
%%%-------------------------------------------------------------------
-module(invalidation_req).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start_invalidation/4,
    schedule_replica_invalidation/4,
    invalidate_file_replica/4,
    enqueue_file_invalidation/6
]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Marks transfer as active and adds task for replication to worker_pool.
%% @end
%%-------------------------------------------------------------------
-spec start_invalidation(user_ctx:ctx(), file_ctx:ctx(), sync_req:provider_id(),
    sync_req:transfer_id()) -> ok.
start_invalidation(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    {ok, _} = transfer:mark_active_invalidation(TransferId),
    enqueue_file_invalidation(UserCtx, FileCtx, MigrationProviderId, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Schedules invalidation of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    SourceProviderId :: sync_req:provider_id(),
    MigrationProviderId :: sync_req:provider_id()) -> sync_req:provider_response().
schedule_replica_invalidation(UserCtx, FileCtx, SourceProviderId,
    MigrationProviderId
) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    schedule_replica_invalidation(UserCtx, FileCtx, FilePath, SourceProviderId,
        MigrationProviderId).

%%--------------------------------------------------------------------
%% @doc
%% @equiv invalidate_file_replica_internal/4 but catches exception and
%% notifies invalidation_controller
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica(user_ctx:ctx(), file_ctx:ctx(), sync_req:block(),
    sync_req:transfer_id()) -> sync_req:provider_response().
invalidate_file_replica(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, MigrationProviderId, TransferId],
        fun invalidate_file_replica_insecure/4).

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file invalidation to worker from ?INVALIDATION_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_file_invalidation(UserCtx, FileCtx, MigrationProviderId, TransferId,
    Retries, NextRetry) ->
    worker_pool:cast(?INVALIDATION_WORKERS_POOL,
        {start_file_invalidation, UserCtx, FileCtx, MigrationProviderId,
            TransferId, Retries, NextRetry
        }
    ).

%%--------------------------------------------------------------------
%% @doc
%% Enqueues invalidation of children.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_children_invalidation(user_ctx:ctx(), [file_ctx:ctx()],
    oneprovider:id(), sync_req:transfer_id()) -> ok.
enqueue_children_invalidation(UserCtx, Children, MigrationProviderId,
    TransferId) ->
    lists:foreach(fun(ChildCtx) ->
        enqueue_file_invalidation(UserCtx, ChildCtx, MigrationProviderId,
            TransferId)
    end, Children).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv enqueue_file_invalidation(UserCtx, FileCtx,
%% MigrationProviderId, TransferId, undefined, undefined).
%% @end
%%--------------------------------------------------------------------
-spec enqueue_file_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id()) -> ok.
enqueue_file_invalidation(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    enqueue_file_invalidation(UserCtx, FileCtx, MigrationProviderId,
        TransferId, undefined, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules invalidation of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_invalidation(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:path(), sync_req:provider_id(), sync_req:provider_id()) ->
    sync_req:provider_response().
schedule_replica_invalidation(UserCtx, FileCtx, FilePath, SourceProviderId,
    MigrationProviderId
) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath,
        SourceProviderId, MigrationProviderId, undefined, true),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replica of given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica_insecure(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id()) ->
    sync_req:provider_response().
invalidate_file_replica_insecure(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    case transfer_utils:is_ongoing(TransferId) of
        true ->
            case file_ctx:is_dir(FileCtx) of
                {true, FileCtx2} ->
                    invalidate_dir(UserCtx, FileCtx2, MigrationProviderId, 0,
                        TransferId);
                {false, FileCtx2} ->
                    evict_file(FileCtx2, MigrationProviderId, TransferId)
            end;
        false ->
            #provider_response{status = #status{code = ?OK}}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively schedules invalidation of directory children.
%% @end
%%-------------------------------------------------------------------
-spec invalidate_dir(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), non_neg_integer(), sync_req:transfer_id()) ->
    sync_req:provider_response().
invalidate_dir(UserCtx, FileCtx, MigrationProviderId, Offset, TransferId) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    {Children, FileCtx3} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Chunk),
    Length = length(Children),
    case Length < Chunk of
        true ->
            transfer:increase_files_to_process_counter(TransferId, Length),
            enqueue_children_invalidation(UserCtx, Children, MigrationProviderId,
                TransferId),
            transfer:increase_files_processed_counter(TransferId),
            #provider_response{status = #status{code = ?OK}};
        false ->
            transfer:increase_files_to_process_counter(TransferId, Chunk),
            enqueue_children_invalidation(UserCtx, Children, MigrationProviderId,
                TransferId),
            invalidate_dir(UserCtx, FileCtx3,
                MigrationProviderId, Offset + Chunk, TransferId)
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Evicts regular file.
%% @end
%%-------------------------------------------------------------------
-spec evict_file(file_ctx:ctx(), sync_req:provider_id(), sync_req:transfer_id())
        -> sync_req:provider_response().
evict_file(FileCtx, undefined, TransferId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case  replica_evictor:get_setting_for_eviction_task(FileCtx) of
        undefined ->
            transfer:increase_files_processed_counter(TransferId);
        {FileUuid, ProviderId, Blocks, VV} ->
            schedule_eviction_task(FileUuid, ProviderId, Blocks, VV, TransferId, SpaceId)
    end,
    #provider_response{status = #status{code = ?OK}};
evict_file(FileCtx, SupportingProviderId, TransferId) ->
    {LocalFileLocationDoc, FileCtx2} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, FileCtx} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    schedule_eviction_task(FileUuid, SupportingProviderId, Blocks, VV, TransferId,
        SpaceId),
    #provider_response{status = #status{code = ?OK}}.


-spec schedule_eviction_task(file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), transfer:id(), od_space:id()) -> ok.
schedule_eviction_task(FileUuid, Provider, Blocks, VV, TransferId, SpaceId) ->
    replica_evictor:evict(FileUuid, Provider, Blocks, VV, TransferId, invalidation, SpaceId).

