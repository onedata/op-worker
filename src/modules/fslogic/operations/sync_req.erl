%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests synchronizing and getting
%%% synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(sync_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

-type block() :: undefined | fslogic_blocks:block().
-type transfer_id() :: undefined | transfer:id().
-type provider_id() :: undefined | od_provider:id().
-type fuse_response() :: fslogic_worker:fuse_response().
-type provider_response() :: fslogic_worker:provider_response().

-export_type([block/0, transfer_id/0, provider_id/0]).

%% API
-export([
    synchronize_block/5,
    synchronize_block_and_compute_checksum/3,
    get_file_distribution/2,
    start_transfer/4
]).

-export([
    schedule_file_replication/4,
    replicate_file/4,
    enqueue_file_replication/6
]).

% exported for tests
-export([get_file_children/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id()) -> fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch, TransferId) ->
    % trigger file_location creation
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    synchronize_block(UserCtx, FileCtx3, #file_block{offset = 0, size = Size},
        Prefetch, TransferId);
synchronize_block(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
    ok = replica_synchronizer:synchronize(UserCtx, FileCtx, Block, Prefetch,
        TransferId),
    {LocationToSend, _FileCtx2} =
        file_ctx:get_file_location_with_filled_gaps(FileCtx, Block),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = LocationToSend
    }.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(), file_ctx:ctx(),
    block()) -> fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx,
    Range = #file_block{offset = Offset, size = Size}
) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    %todo do not use lfm, operate on fslogic directly
    {ok, Handle} = lfm_files:open(SessId, {guid, FileGuid}, read),
    % does sync internally
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size),
    lfm_files:release(Handle),

    Checksum = crypto:hash(md4, Data),
    {LocationToSend, _FileCtx2} =
        file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
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
-spec get_file_distribution(user_ctx:ctx(), file_ctx:ctx()) -> provider_response().
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
%% Marks transfer as enqueued and adds task for replication to worker_pool.
%% @end
%%-------------------------------------------------------------------
-spec start_transfer(user_ctx:ctx(), file_ctx:ctx(), block(), transfer_id()) -> ok.
start_transfer(UserCtx, FileCtx, Block, TransferId) ->
    {ok, _} = transfer:mark_enqueued(TransferId),
    enqueue_file_replication(UserCtx, FileCtx, Block, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    od_provider:id(), transfer:callback()) -> provider_response().
schedule_file_replication(UserCtx, FileCtx, TargetProviderId, Callback) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    schedule_file_replication(UserCtx, FileCtx, FilePath, TargetProviderId, Callback).

%%--------------------------------------------------------------------
%% @doc
%% @equiv replicate_file_insecure/5 with permission check
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(user_ctx:ctx(), file_ctx:ctx(), block(), transfer_id()) ->
    provider_response() | {error, term()}.
replicate_file(UserCtx, FileCtx, Block, TransferId) ->
    try
        check_permissions:execute(
            [traverse_ancestors, ?write_object],
            [UserCtx, FileCtx, Block, TransferId],
            fun replicate_file_insecure/4)
    catch
        error:{badmatch, {error, not_found}} ->
            {error, not_found};
        Error:Reason ->
            erlang:Error(Reason)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file replication to worker from ?TRANSFER_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_replication(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id(), undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_file_replication(UserCtx, FileCtx, Block, TransferId, Retries, NextRetry) ->
    worker_pool:cast(?TRANSFER_WORKERS_POOL,
        {start_file_replication, UserCtx, FileCtx, Block, TransferId, Retries, NextRetry}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:path(), od_provider:id(), transfer:callback()) -> provider_response().
schedule_file_replication(UserCtx, FileCtx, FilePath, TargetProviderId, Callback) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath, undefined,
        TargetProviderId, Callback, false),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.

%%-------------------------------------------------------------------
%% @doc
%% @equiv
%% enqueue_file_replication(UserCtx, FileCtx, Block, TransferId,
%% undefined, undefined).
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    block(), transfer_id()) -> ok.
enqueue_file_replication(UserCtx, FileCtx, Block, TransferId) ->
    enqueue_file_replication(UserCtx, FileCtx, Block, TransferId, undefined, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file_insecure(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id()) -> provider_response().
replicate_file_insecure(UserCtx, FileCtx, Block, TransferId) ->
    case transfer_utils:is_ongoing(TransferId) of
        true ->
            case file_ctx:is_dir(FileCtx) of
                {true, FileCtx2} ->
                    replicate_dir(UserCtx, FileCtx2, Block, 0, TransferId);
                {false, FileCtx2} ->
                    replicate_regular_file(UserCtx, FileCtx2, Block, TransferId)
            end;
        false ->
            throw({transfer_cancelled, TransferId})
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively schedules replication of directory children.
%% @end
%%-------------------------------------------------------------------
-spec replicate_dir(user_ctx:ctx(), file_ctx:ctx(), block(), non_neg_integer(),
    transfer_id()) -> provider_response().
replicate_dir(UserCtx, FileCtx, Block, Offset, TransferId) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    {Children, FileCtx2} = sync_req:get_file_children(FileCtx, UserCtx, Offset, Chunk),
    Length = length(Children),
    case Length < Chunk of
        true ->
            transfer:increase_files_to_process_counter(TransferId, Length),
            enqueue_children_replication(UserCtx, Children, Block, TransferId),
            transfer:increase_files_processed_counter(TransferId),
            #provider_response{status = #status{code = ?OK}};
        false ->
            transfer:increase_files_to_process_counter(TransferId, Chunk),
            enqueue_children_replication(UserCtx, Children, Block, TransferId),
            replicate_dir(UserCtx, FileCtx2, Block, Offset + Chunk, TransferId)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Replicates regular file.
%% @end
%%-------------------------------------------------------------------
-spec replicate_regular_file(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id()) -> provider_response().
replicate_regular_file(UserCtx, FileCtx, Block, TransferId) ->
    #fuse_response{status = Status} =
        synchronize_block(UserCtx, FileCtx, Block, false, TransferId),
    transfer:increase_files_processed_counter(TransferId),
    #provider_response{status = Status}.

%%-------------------------------------------------------------------
%% @doc
%% Wrapper for file_ctx:get_file_children function/4.
%% This function is created mainly for test reason.
%% @end
%%-------------------------------------------------------------------
-spec get_file_children(file_ctx:ctx(), user_ctx:ctx(), Offset :: non_neg_integer(),
    Limit :: non_neg_integer()) ->
    {Children :: [file_ctx:ctx()], NewFileCtx :: file_ctx:ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Chunk) ->
    file_ctx:get_file_children(FileCtx, UserCtx, Offset, Chunk).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates children list.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_children_replication(user_ctx:ctx(), [file_ctx:ctx()], block(),
    transfer_id()) -> ok.
enqueue_children_replication(UserCtx, Children, Block, TransferId) ->
    lists:foreach(fun(ChildCtx) ->
        enqueue_file_replication(UserCtx, ChildCtx, Block, TransferId)
    end, Children).