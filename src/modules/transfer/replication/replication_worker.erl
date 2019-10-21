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
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").

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
    application:get_env(?APP_NAME, default_replication_priority, 224)
).

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

%%%===================================================================
%%% gen_transfer_worker callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback required_permissions/0.
%% @end
%%--------------------------------------------------------------------
-spec required_permissions() -> [fslogic_authz:access_definition()].
required_permissions() ->
    [traverse_ancestors, ?write_object].

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback max_transfer_retries/0.
%% @end
%%--------------------------------------------------------------------
-spec max_transfer_retries() -> non_neg_integer().
max_transfer_retries() ->
    application:get_env(?APP_NAME, max_file_replication_retries_per_file, 5).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback view_querying_chunk_size/0.
%% @end
%%--------------------------------------------------------------------
-spec view_querying_chunk_size() -> non_neg_integer().
view_querying_chunk_size() ->
    application:get_env(?APP_NAME, replication_by_view_batch, 1000).

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback enqueue_data_transfer/4.
%% @end
%%--------------------------------------------------------------------
-spec enqueue_data_transfer(file_ctx:ctx(), transfer_params(),
    undefined | non_neg_integer(), undefined | non_neg_integer()) -> ok.
enqueue_data_transfer(FileCtx, TransferParams, RetriesLeft, NextRetry) ->
    RetriesLeft2 = utils:ensure_defined(RetriesLeft, undefined, max_transfer_retries()),
    worker_pool:cast(?REPLICATION_WORKERS_POOL, ?TRANSFER_DATA_REQ(
        FileCtx, TransferParams, RetriesLeft2, NextRetry
    )),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_worker_behaviour} callback transfer_regular_file/2.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_params()) -> ok | {error, term()}.
transfer_regular_file(FileCtx, #transfer_params{
    transfer_id = TransferId,
    user_ctx = UserCtx,
    block = Block
}) ->
    #fuse_response{status = #status{code = ?OK}} = sync_req:synchronize_block(
        UserCtx, FileCtx, Block, false, TransferId, ?DEFAULT_REPLICATION_PRIORITY
    ),
    transfer:increment_files_processed_counter(TransferId),
    ok = file_popularity:increment_open(FileCtx),
    ok.
