%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
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
-module(transfer_worker_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").


-record(transfer_params, {
    transfer_id :: transfer:id(),
    user_ctx :: user_ctx:ctx(),
    index_name = undefined :: transfer:index_name(),
    query_view_params = undefined :: transfer:query_view_params(),
    block = undefined :: undefined | fslogic_blocks:block(),
    supporting_provider = undefined :: undefined | od_provider:id()
}).

-type transfer_type() :: replication | eviction.
-type transfer_params() :: #transfer_params{}.

-export_type([transfer_type/0, transfer_params/0]).

%% API
-export([
    enqueue_file_transfer/4
]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Adds task of file replication to worker from ?REPLICATION_WORKERS_POOL.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_file_transfer(file_ctx:ctx(), transfer_params(),
    undefined | non_neg_integer(), undefined | non_neg_integer()
) ->
    ok.
enqueue_file_transfer(FileCtx, TransferParams, RetriesLeft, NextRetry) ->
    worker_pool:cast(?REPLICATION_WORKERS_POOL,
        {transfer_data, FileCtx, TransferParams, RetriesLeft, NextRetry}  % TODO replication/eviction
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs permissions check and transfers data if allowed.
%% @end
%%--------------------------------------------------------------------
-spec transfer_data(file_ctx:ctx(), transfer_params(), transfer_type()) ->
    ok | {error, term()}.
transfer_data(FileCtx, TransferParams, TransferType) ->
    PermissionsToCheck = case TransferType of
        replication ->
            [traverse_ancestors, ?write_object];
        _ ->
            [] %todo VFS-4844
    end,

    try
        check_permissions:execute(
            PermissionsToCheck,
            [FileCtx, TransferParams],
            fun transfer_data_insecure/2
        )
    catch
        error:{badmatch, {error, not_found}} ->
            {error, not_found};
        Error:Reason ->
            erlang:Error(Reason)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transfers data from appropriate source, that is either files returned by
%% querying specified index or file system subtree (regular file or
%% entire directory depending on file ctx).
%% @end
%%--------------------------------------------------------------------
-spec transfer_data_insecure(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_data_insecure(FileCtx, TransferReq = #transfer_params{index_name = undefined}) ->
    transfer_fs_subtree(FileCtx, TransferReq);
transfer_data_insecure(FileCtx, TransferReq) ->
    transfer_files_from_index(FileCtx, TransferReq, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transfers given dir or regular file (depending on file ctx) on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec transfer_fs_subtree(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_fs_subtree(FileCtx, Params = #transfer_params{transfer_id = TransferId}) ->
    case transfer:is_ongoing(TransferId) of
        true ->
            case file_ctx:file_exists_const(FileCtx) of
                true ->
                    case file_ctx:is_dir(FileCtx) of
                        {true, FileCtx2} ->
                            transfer_dir(FileCtx2, 0, Params);
                        {false, FileCtx2} ->
                            transfer_regular_file(FileCtx2, Params)
                    end;
                false ->
                    {error, not_found}
            end;
        false ->
            {error, already_ended}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively schedules transfer of directory children.
%% @end
%%-------------------------------------------------------------------
-spec transfer_dir(file_ctx:ctx(), non_neg_integer(), transfer_params()) ->
    ok | {error, term()}.
transfer_dir(FileCtx, Offset, TransferParams = #transfer_params{
    transfer_id = TransferId,
    user_ctx = UserCtx
}) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    {Children, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Chunk),
    Length = length(Children),
    case Length < Chunk of
        true ->
            transfer:increment_files_to_process_counter(TransferId, Length),
            enqueue_files_transfer(Children, TransferParams),
            transfer:increment_files_processed_counter(TransferId),
            ok;
        false ->
            transfer:increment_files_to_process_counter(TransferId, Chunk),
            enqueue_files_transfer(Children, TransferParams),
            transfer_dir(FileCtx2, Offset + Chunk, TransferParams)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules transfer for each file from list.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_files_transfer([file_ctx:ctx()], transfer_params()) -> ok.
enqueue_files_transfer(FileCtxs, TransferParams) ->
    lists:foreach(fun(FileCtx) ->
        enqueue_file_transfer(FileCtx, TransferParams, undefined, undefined)
    end, FileCtxs).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Transfers regular file.
%% @end
%%-------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, TransferParams) ->       % TODO implement callback
%%    #fuse_response{status = ?OK} =
%%        synchronize_block(UserCtx, FileCtx, Block, false, TransferId,
%%            ?DEFAULT_REPLICATION_PRIORITY),
    transfer:increment_files_processed_counter(TransferParams#transfer_params.transfer_id),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates files from specified index.
%% @end
%%--------------------------------------------------------------------
-spec transfer_files_from_index(file_ctx:ctx(), transfer_params(), file_meta:uuid()) ->
    ok | {error, term()}.
transfer_files_from_index(FileCtx, TransferParams, LastDocId) ->
    case transfer:is_ongoing(TransferParams#transfer_params.transfer_id) of
        true ->
            Chunk = application:get_env(?APP_NAME, replication_by_index_batch, 1000),
%%            Chunk = application:get_env(?APP_NAME, replica_eviction_by_index_batch, 1000), % TODO
            transfer_files_from_index(FileCtx, TransferParams, Chunk, LastDocId);
        false ->
            throw(already_ended)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transfers files returned by querying specified index with specified chunk.
%% @end
%%--------------------------------------------------------------------
-spec transfer_files_from_index(file_ctx:ctx(), transfer_params(),
    non_neg_integer(), file_meta:uuid()) -> ok | {error, term()}.
transfer_files_from_index(FileCtx, TransferParams, Chunk, LastDocId) ->
    #transfer_params{
        transfer_id = TransferId,
        index_name = IndexName,
        query_view_params = QueryViewParams
    } = TransferParams,

    QueryViewParams2 = case LastDocId of
        undefined ->
            [{skip, 0} | QueryViewParams];
        doc_id_missing ->
            % doc_id is missing when view has reduce function defined
            % in such case we must iterate over results using limit and skip
            [{skip, Chunk} | QueryViewParams];
        _ ->
            [{skip, 1}, {startkey_docid, LastDocId} | QueryViewParams]
    end,
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    case index:query(SpaceId, IndexName, [{limit, Chunk} | QueryViewParams2]) of
        {ok, {Rows}} ->
            {NewLastDocId, FileCtxs} = lists:foldl(fun(Row, {_LastDocId, FileCtxsIn}) ->
                {<<"value">>, Value} = lists:keyfind(<<"value">>, 1, Row),
                DocId = case lists:keyfind(<<"id">>, 1, Row) of
                    {<<"id">>, Id} -> Id;
                    _ -> doc_id_missing
                end,
                ObjectIds = case is_list(Value) of
                    true -> lists:flatten(Value);
                    false -> [Value]
                end,
                transfer:increment_files_to_process_counter(TransferId, length(ObjectIds)),
                NewFileCtxs = lists:filtermap(fun(O) ->
                    try
                        {ok, G} = cdmi_id:objectid_to_guid(O),
                        {true, file_ctx:new_by_guid(G)}
                    catch
                    Error:Reason ->
                        transfer:increment_files_failed_and_processed_counters(TransferId),
                        ?error_stacktrace(
                            "Processing result of query index ~p in space ~p failed due to ~p:~p",
                            [IndexName, SpaceId, Error, Reason]),
                        false
                    end
                end, ObjectIds),
                {DocId, FileCtxsIn ++ NewFileCtxs}
            end, {undefined, []}, Rows),

            case length(Rows) < Chunk of
                true ->
                    enqueue_files_transfer(FileCtxs, TransferParams#transfer_params{
                        index_name = undefined, query_view_params = undefined
                    }),
                    transfer:increment_files_processed_counter(TransferId),
                    ok;
                false ->
                    enqueue_files_transfer(FileCtxs, TransferParams#transfer_params{
                        index_name = undefined, query_view_params = undefined
                    }),
                    transfer_files_from_index(FileCtx, TransferParams, NewLastDocId)
            end;
        {error, Reason} = Error ->
            ?error("Querying view ~p failed due to ~p when processing transfer ~p", [
                IndexName, Reason, TransferId
            ]),
            throw(Error)
    end.
