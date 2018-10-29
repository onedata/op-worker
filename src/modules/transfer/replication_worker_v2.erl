%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of worker for replication_workers_pool.
%%% Worker is responsible for replication of one file.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_worker_v2).
-author("Bartosz Walkowicz").

-behaviour(transfer_worker_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([
    enqueue_data_transfer/2, enqueue_data_transfer/4,
    transfer_regular_file/2,
    index_querying_chunk_size/0,
    max_transfer_retries/0,
    required_permissions/0
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec required_permissions() -> list().
required_permissions() ->
    [traverse_ancestors, ?write_object].


-spec max_transfer_retries() -> non_neg_integer().
max_transfer_retries() ->
    application:get_env(?APP_NAME, max_file_replication_retries_per_file, 5).


-spec index_querying_chunk_size() -> non_neg_integer().
index_querying_chunk_size() ->
    application:get_env(?APP_NAME, replication_by_index_batch, 1000).


-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams) ->
    enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined).


-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams, RetriesLeft, NextRetry) ->
    RetriesLeft2 = utils:ensure_defined(RetriesLeft, undefined, max_transfer_retries()),
    worker_pool:cast(?REPLICATION_WORKERS_POOL, ?TRANSFER_DATA_REQ(
        FileCtx, TransferParams, RetriesLeft2, NextRetry
    )),
    ok.


-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, #transfer_params{
    transfer_id = TransferId,
    user_ctx = UserCtx,
    block = Block
}) ->
    case sync_req:synchronize_block(UserCtx, FileCtx, Block, false, TransferId,
        ?DEFAULT_REPLICATION_PRIORITY
    ) of
        #fuse_response{status = ?OK} ->
            ok;
        #fuse_response{status = Status} ->
            {error, Status}
    end.
