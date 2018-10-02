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
-type query_view_params() :: transfer:query_view_params().
-type fuse_response() :: fslogic_worker:fuse_response().
-type provider_response() :: fslogic_worker:provider_response().

-export_type([block/0, transfer_id/0, provider_id/0]).

%% API
-export([
    synchronize_block/6,
    request_block_synchronization/6,
    synchronize_block_and_compute_checksum/5,
    get_file_distribution/2
]).

-export([
    schedule_file_replication/6,
    replicate_file/6,
    enqueue_file_replication/6,
    enqueue_file_replication/8
]).

% exported for tests
-export([get_file_children/4]).

-define(DEFAULT_REPLICATION_PRIORITY,
    application:get_env(?APP_NAME, default_replication_priority, 224)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    synchronize_block(UserCtx, FileCtx3, #file_block{offset = 0, size = Size},
        Prefetch, TransferId, Priority);
synchronize_block(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:synchronize(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority) of
        {ok, Ans} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = Ans};
        {error, cancelled} ->
            throw(replication_cancelled);
        {error, _} = Error ->
            throw(Error)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Requests synchronization of given block with remote replicas.
%% Does not wait for sync.
%% @end
%%--------------------------------------------------------------------
-spec request_block_synchronization(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
request_block_synchronization(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    request_block_synchronization(UserCtx, FileCtx3, #file_block{offset = 0, size = Size},
        Prefetch, TransferId, Priority);
request_block_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:request_synchronization(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority) of
        ok ->
            #fuse_response{status = #status{code = ?OK}};
        {error, _} = Error ->
            throw(Error)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(), file_ctx:ctx(),
    block(), boolean(), non_neg_integer()) -> fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx,
    Range = #file_block{offset = Offset, size = Size}, Prefetch, Priority
) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),

    {ok, Ans} = replica_synchronizer:synchronize(UserCtx, FileCtx, Range,
        Prefetch, undefined, Priority),

    %todo do not use lfm, operate on fslogic directly
    {ok, Handle} = lfm_files:open(SessId, {guid, FileGuid}, read),
    % does sync internally
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size, off),
    lfm_files:release(Handle),

    Checksum = crypto:hash(md4, Data),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #sync_response{
            checksum = Checksum,
            file_location_changed = Ans
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
        value = #file_location{provider_id = ProviderId}
    } = FL) ->
        #provider_file_distribution{
            provider_id = ProviderId,
            blocks = fslogic_location_cache:get_blocks(FL)
        }
    end, Locations),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_distribution{
            provider_file_distributions = ProviderDistributions
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    od_provider:id(), transfer:callback(), transfer:index_name(),
    query_view_params()) -> provider_response().
schedule_file_replication(UserCtx, FileCtx, TargetProviderId, Callback,
    IndexName, QueryViewParams
) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    schedule_file_replication(UserCtx, FileCtx, FilePath, TargetProviderId,
        Callback, IndexName, QueryViewParams).

%%--------------------------------------------------------------------
%% @doc
%% @equiv replicate_file_insecure/5 with permission check
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(user_ctx:ctx(), file_ctx:ctx(), block(), transfer_id(),
    transfer:index_name(), query_view_params()) -> provider_response() | {error, term()}.
replicate_file(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams) ->
    try
        check_permissions:execute(
            [traverse_ancestors, ?write_object],
            [UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams],
            fun replicate_file_insecure/6)
    catch
        error:{badmatch, {error, not_found}} ->
            {error, not_found};
        Error:Reason ->
            erlang:Error(Reason)
    end.

%%-------------------------------------------------------------------
%% @doc
%% @equiv
%% enqueue_file_replication(UserCtx, FileCtx, Block, TransferId,
%% undefined, undefined, IndexName, QueryViewParams).
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    block(), transfer_id(), transfer:index_name(), query_view_params()) -> ok.
enqueue_file_replication(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams) ->
    enqueue_file_replication(UserCtx, FileCtx, Block, TransferId, undefined, undefined, IndexName, QueryViewParams).

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file replication to worker from ?REPLICATION_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_replication(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id(), undefined | non_neg_integer(), undefined | non_neg_integer(),
    transfer:index_name(), query_view_params()) -> ok.
enqueue_file_replication(UserCtx, FileCtx, Block, TransferId, Retries,
    NextRetry, IndexName, QueryViewParams
) ->
    worker_pool:cast(?REPLICATION_WORKERS_POOL,
        {start_file_replication, UserCtx, FileCtx, Block, TransferId, Retries, 
            NextRetry, IndexName, QueryViewParams}).

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
    file_meta:path(), od_provider:id(), transfer:callback(),
    transfer:index_name(), query_view_params()) -> provider_response().
schedule_file_replication(UserCtx, FileCtx, FilePath, TargetProviderId, Callback,
    IndexName, QueryViewParams
) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath, undefined,
        TargetProviderId, Callback, IndexName, QueryViewParams),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file_insecure(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id(), transfer:index_name(), query_view_params()) -> provider_response().
replicate_file_insecure(UserCtx, FileCtx, Block, TransferId, undefined, _QueryViewParams) ->
    replicate_fs_subtree(UserCtx, FileCtx, Block, TransferId);
replicate_file_insecure(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams) ->
    replicate_files_from_index(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_fs_subtree(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id()) -> provider_response().
replicate_fs_subtree(UserCtx, FileCtx, Block, TransferId) ->
    case transfer:is_ongoing(TransferId) of
        true ->
            case file_ctx:is_dir(FileCtx) of
                {true, FileCtx2} ->
                    replicate_dir(UserCtx, FileCtx2, Block, 0, TransferId);
                {false, FileCtx2} ->
                    replicate_regular_file(UserCtx, FileCtx2, Block, TransferId)
            end;
        false ->
            throw(already_ended)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates files from specified index.
%% @end
%%--------------------------------------------------------------------
-spec replicate_files_from_index(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id(), transfer:index_name(), query_view_params(), file_meta:uuid()
) ->
    provider_response().
replicate_files_from_index(UserCtx, FileCtx, Block, TransferId, IndexName,
    QueryViewParams, LastDocId
) ->
    case transfer:is_ongoing(TransferId) of
        true ->
            Chunk = application:get_env(?APP_NAME, replication_by_index_batch, 1000),
            replicate_files_from_index(UserCtx, FileCtx, Block, TransferId,
                IndexName, Chunk, QueryViewParams, LastDocId
            );
        false ->
            throw(already_ended)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates files from specified index by specified chunk.
%% @end
%%--------------------------------------------------------------------
-spec replicate_files_from_index(user_ctx:ctx(), file_ctx:ctx(), block(),
    transfer_id(), transfer:index_name(), non_neg_integer(), query_view_params(),
    file_meta:uuid()) -> provider_response().
replicate_files_from_index(UserCtx, FileCtx, Block, TransferId, IndexName, Chunk,
    QueryViewParams, LastDocId
) ->
    QueryViewParams2 = case LastDocId of
        undefined ->
            [{skip, 0} | QueryViewParams];
        _ ->
            [{skip, 1}, {startkey_docid, LastDocId} | QueryViewParams]
    end,
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case index:query_view(SpaceId, IndexName, [{limit, Chunk} | QueryViewParams2]) of
        {ok, {Rows}} ->
            NumberOfFiles = length(Rows),
            {NewLastDocId, Guids} = lists:foldl(fun(Row, AccIn = {_LastDocId, FileGuids}) ->
                {<<"value">>, FileUuid} = lists:keyfind(<<"value">>, 1, Row),
                {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, Row),
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    {DocId, [FileGuid | FileGuids]}
                catch
                    Error:Reason ->
                        ?error_stacktrace("Cannot resolve uuid of file ~p in index ~p, due to error ~p:~p", [FileUuid, IndexName, Error, Reason]),
                        AccIn
                end
            end, {undefined, []}, Rows),
            % Guids are reversed now
            ChildrenCtxs = lists:foldl(fun(Guid, Ctxs) ->
                [file_ctx:new_by_guid(Guid) | Ctxs]
            end, [], Guids),
            transfer:increment_files_to_process_counter(TransferId, NumberOfFiles),
            case NumberOfFiles < Chunk of
                true ->
                    enqueue_children_replication(UserCtx, ChildrenCtxs, undefined, TransferId),
                    transfer:increment_files_processed_counter(TransferId),
                    #provider_response{status = #status{code = ?OK}};
                false ->
                    enqueue_children_replication(UserCtx, ChildrenCtxs, undefined, TransferId),
                    replicate_files_from_index(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams, NewLastDocId)
            end;
        Error = {error, Reason} ->
            ?error("Querying view ~p failed due to ~p when processing transfer ~p", [IndexName, Reason, TransferId]),
            throw(Error)

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
            transfer:increment_files_to_process_counter(TransferId, Length),
            enqueue_children_replication(UserCtx, Children, Block, TransferId),
            transfer:increment_files_processed_counter(TransferId),
            #provider_response{status = #status{code = ?OK}};
        false ->
            transfer:increment_files_to_process_counter(TransferId, Chunk),
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
        synchronize_block(UserCtx, FileCtx, Block, false, TransferId,
            ?DEFAULT_REPLICATION_PRIORITY),
    transfer:increment_files_processed_counter(TransferId),
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
        enqueue_file_replication(UserCtx, ChildCtx, Block, TransferId, undefined, [])
    end, Children).