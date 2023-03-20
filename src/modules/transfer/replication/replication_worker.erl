%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017-2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of gen_transfer_worker for replication_workers_pool.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_worker).
-author("Jakub Kudzia").
-author("Bartosz Walkowicz").

-behaviour(gen_transfer_worker).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([enqueue_data_transfer/2]).

%% gen_transfer_worker callbacks
-export([
    enqueue_data_transfer/4,
    transfer_regular_file/2,
    view_querying_chunk_size/0,
    max_transfer_retries/0,
    required_permissions/0
]).

-define(DEFAULT_REPLICATION_PRIORITY,
    op_worker:get_env(default_replication_priority, 224)
).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined).
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), gen_transfer_worker:job_ctx()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams) ->
    enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined).

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
    % TODO VFS-10259 use offline session in transfer
    [?TRAVERSE_ANCESTORS].

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback max_transfer_retries/0.
%% @end
%%--------------------------------------------------------------------
-spec max_transfer_retries() -> non_neg_integer().
max_transfer_retries() ->
    op_worker:get_env(max_file_replication_retries_per_file, 5).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback view_querying_chunk_size/0.
%% @end
%%--------------------------------------------------------------------
-spec view_querying_chunk_size() -> non_neg_integer().
view_querying_chunk_size() ->
    op_worker:get_env(replication_by_view_batch, 1000).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback enqueue_data_transfer/4.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), gen_transfer_worker:job_ctx(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_data_transfer(FileCtx, TransferJobCtx = #transfer_job_ctx{transfer_id = TransferId}, RetriesLeft, NextRetry) ->
    case file_ctx:is_trash_dir_const(FileCtx) orelse file_ctx:is_tmp_dir_const(FileCtx) of
        true ->
            % ignore trash directory
            transfer:increment_files_processed_counter(TransferId);
        false ->
            RetriesLeft2 = utils:ensure_defined(RetriesLeft, max_transfer_retries()),
            worker_pool:cast(?REPLICATION_WORKERS_POOL, ?TRANSFER_DATA_REQ(
                FileCtx, TransferJobCtx, RetriesLeft2, NextRetry
            ))
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback transfer_regular_file/2.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), gen_transfer_worker:job_ctx()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, #transfer_job_ctx{
    transfer_id = TransferId,
    user_ctx = UserCtx,
    block = Block
}) ->
    #fuse_response{status = #status{code = ?OK}} = sync_req:synchronize_block(
        UserCtx, FileCtx, Block, false, TransferId, ?DEFAULT_REPLICATION_PRIORITY
    ),
    transfer:increment_files_processed_counter(TransferId),
    case file_popularity:increment_open(FileCtx) of
        ok -> ok;
        {error, not_found} -> ok
    end.
