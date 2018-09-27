%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests evicting file
%%% replicas (including whole file trees).
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_req).
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
    schedule_replica_eviction/4]).

%% internal API
-export([
    enqueue_replica_eviction/4,
    enqueue_replica_eviction/6,
    evict_file_replica/4
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Schedules eviction of replica by creating transfer doc.
%% Returns the id of the created transfer doc wrapped in
%% 'scheduled_transfer' provider response. Resolves file path
%% based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction(user_ctx:ctx(), file_ctx:ctx(),
    SourceProviderId :: sync_req:provider_id(),
    MigrationProviderId :: sync_req:provider_id()) -> sync_req:provider_response().
schedule_replica_eviction(UserCtx, FileCtx, SourceProviderId,
    MigrationProviderId
) ->
    check_permissions:execute(
        [], %todo VFS-4844
        [UserCtx, FileCtx, SourceProviderId, MigrationProviderId],
        fun schedule_replica_eviction_insecure/4).

%%--------------------------------------------------------------------
%% @doc
%% @equiv evict_file_replica_insecure/4 but checks permissions
%% @end
%%--------------------------------------------------------------------
-spec evict_file_replica(user_ctx:ctx(), file_ctx:ctx(), sync_req:block(),
    sync_req:transfer_id()) -> sync_req:provider_response().
evict_file_replica(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    check_permissions:execute(
        [], %todo VFS-4844
        [UserCtx, FileCtx, MigrationProviderId, TransferId],
        fun evict_file_replica_insecure/4).


%%%===================================================================
%%% Internal API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file eviction to worker from
%% ?eviction_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_replica_eviction(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_replica_eviction(UserCtx, FileCtx, MigrationProviderId, TransferId,
    Retries, NextRetry) ->
    worker_pool:cast(?REPLICA_EVICTION_WORKERS_POOL,
        {start_replica_eviction, UserCtx, FileCtx, MigrationProviderId,
            TransferId, Retries, NextRetry
        }
    ).

%%--------------------------------------------------------------------
%% @doc
%% Adds task of file eviction to worker from
%% ?eviction_WORKERS_POOL.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_children_eviction(user_ctx:ctx(), [file_ctx:ctx()],
    oneprovider:id(), sync_req:transfer_id()) -> ok.
enqueue_children_eviction(UserCtx, Children, MigrationProviderId,
    TransferId) ->
    lists:foreach(fun(ChildCtx) ->
        enqueue_replica_eviction(UserCtx, ChildCtx, MigrationProviderId,
            TransferId)
    end, Children).

%%--------------------------------------------------------------------
%% @doc
%% @equiv enqueue_file_eviction(UserCtx, FileCtx,
%% MigrationProviderId, TransferId, undefined, undefined).
%% @end
%%--------------------------------------------------------------------
-spec enqueue_replica_eviction(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id()) -> ok.
enqueue_replica_eviction(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    enqueue_replica_eviction(UserCtx, FileCtx, MigrationProviderId,
        TransferId, undefined, undefined).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules eviction of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction_insecure(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:provider_id()) -> sync_req:provider_response().
schedule_replica_eviction_insecure(UserCtx, FileCtx, SourceProviderId,
    MigrationProviderId
) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath,
        SourceProviderId, MigrationProviderId, undefined),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Evicts replica of given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec evict_file_replica_insecure(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:transfer_id()) ->
    sync_req:provider_response().
evict_file_replica_insecure(UserCtx, FileCtx, MigrationProviderId, TransferId) ->
    case transfer:is_ongoing(TransferId) of
        true ->
            case file_ctx:is_dir(FileCtx) of
                {true, FileCtx2} ->
                    evict_dir(UserCtx, FileCtx2, MigrationProviderId, 0,
                        TransferId);
                {false, FileCtx2} ->
                    schedule_file_replica_deletion(FileCtx2, MigrationProviderId, TransferId)
            end;
        false ->
            #provider_response{status = #status{code = ?OK}}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively schedules eviction of directory children.
%% @end
%%-------------------------------------------------------------------
-spec evict_dir(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), non_neg_integer(), sync_req:transfer_id()) ->
    sync_req:provider_response().
evict_dir(UserCtx, FileCtx, MigrationProviderId, Offset, TransferId) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    {Children, FileCtx3} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Chunk),
    Length = length(Children),
    case Length < Chunk of
        true ->
            transfer:increment_files_to_process_counter(TransferId, Length),
            enqueue_children_eviction(UserCtx, Children, MigrationProviderId,
                TransferId),
            transfer:increment_files_processed_counter(TransferId),
            #provider_response{status = #status{code = ?OK}};
        false ->
            transfer:increment_files_to_process_counter(TransferId, Chunk),
            enqueue_children_eviction(UserCtx, Children, MigrationProviderId,
                TransferId),
            evict_dir(UserCtx, FileCtx3,
                MigrationProviderId, Offset + Chunk, TransferId)
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Schedules safe file_replica_deletion via replica_deletion mechanism.
%% If SupportingProviderId is undefined, it will bo chosen from
%% providers who have given file replicated.
%% @end
%%-------------------------------------------------------------------
-spec schedule_file_replica_deletion(file_ctx:ctx(), sync_req:provider_id(), sync_req:transfer_id())
        -> sync_req:provider_response().
schedule_file_replica_deletion(FileCtx, undefined, TransferId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case replica_deletion_master:get_setting_for_deletion_task(FileCtx) of
        undefined ->
            transfer:increment_files_processed_counter(TransferId);
        {FileUuid, ProviderId, Blocks, VV} ->
            schedule_replica_deletion_task(FileUuid, ProviderId, Blocks, VV, TransferId, SpaceId)
    end,
    #provider_response{status = #status{code = ?OK}};
schedule_file_replica_deletion(FileCtx, SupportingProviderId, TransferId) ->
    {LocalFileLocationDoc, FileCtx2} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Size, FileCtx} = file_ctx:get_file_size(FileCtx2),
    VV = file_location:get_version_vector(LocalFileLocationDoc),
    Blocks = [#file_block{offset = 0, size = Size}],
    schedule_replica_deletion_task(FileUuid, SupportingProviderId, Blocks, VV, TransferId,
        SpaceId),
    #provider_response{status = #status{code = ?OK}}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Adds task of replica deletion to replica_deletion_master queue.
%% @end
%%-------------------------------------------------------------------
-spec schedule_replica_deletion_task(file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), transfer:id(), od_space:id()) -> ok.
schedule_replica_deletion_task(FileUuid, Provider, Blocks, VV, TransferId, SpaceId) ->
    replica_deletion_master:enqueue_task(FileUuid, Provider, Blocks, VV, TransferId, eviction, SpaceId).
