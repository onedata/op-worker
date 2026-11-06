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

-behaviour(transfer_traverse_worker).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% gen_transfer_worker callbacks
-export([
    transfer_regular_file/2,
    required_permissions/0
]).

-define(DEFAULT_REPLICATION_PRIORITY, op_worker:get_env(
    default_replication_priority, 224
)).


%%%===================================================================
%%% transfer_traverse_worker callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_traverse_worker} callback required_permissions/0.
%% @end
%%--------------------------------------------------------------------
-spec required_permissions() -> [data_access_control:requirement()].
required_permissions() ->
    % TODO VFS-10259 use offline session in transfer
    [?TRAVERSE_ANCESTORS].


%%--------------------------------------------------------------------
%% @doc
%% {@link transfer_traverse_worker} callback transfer_regular_file/2.
%% @end
%%--------------------------------------------------------------------
-spec transfer_regular_file(file_ctx:ctx(), transfer_traverse_worker:traverse_info()) ->
    ok | {error, term()}.
transfer_regular_file(FileCtx, #{
    transfer_id := TransferId,
    user_ctx := UserCtx
}) ->
    #fuse_response{status = #status{code = ?OK}} = sync_req:synchronize_block(
        UserCtx, FileCtx, undefined, false, TransferId, ?DEFAULT_REPLICATION_PRIORITY
    ),
    transfer:increment_files_processed_counter(TransferId),
    case file_popularity:increment_open(FileCtx) of
        ok -> ok;
        {error, not_found} -> ok
    end.
