%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module manages pools of processes responsible for replication.
%%% @end
%%%--------------------------------------------------------------------
-module(replication_traverse).
-author("Bartosz Walkowicz").

-include("modules/datastore/transfer.hrl").
-include("tree_traverse.hrl").


%% API
-export([init_pool/0, stop_pool/0, pool_name/0]).
-export([start/1, cancel/1]).


-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(qos_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_pool() -> ok  | no_return().
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = op_worker:get_env(replication_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(replication_slave_jobs_limit, 20),

    % set parallelism limit equal to master jobs limit
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, MasterJobsLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec pool_name() -> traverse:pool().
pool_name() ->
    ?POOL_NAME.


-spec start(transfer:doc()) -> ok.
start(TransferDoc = #document{value = Transfer}) ->
    case transfer:data_source_type(Transfer) of
        file -> start_replication_file_tree_traverse(TransferDoc);
        view -> start_replication_view_traverse(TransferDoc)
    end.


-spec cancel(transfer:doc()) -> ok.
cancel(#document{key = TransferId, value = Transfer}) ->
    case transfer:data_source_type(Transfer) of
        file -> tree_traverse:cancel(?POOL_NAME, TransferId);
        view -> ok  %% TODO view traverse ??
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec start_replication_file_tree_traverse(transfer:doc()) -> ok.
start_replication_file_tree_traverse(#document{key = TransferId, value = #transfer{
    file_uuid = FileUuid,
    space_id = SpaceId
}}) ->
    % TODO VFS-7443 - maybe use referenced guid?
    RootFileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),

    {ok, _} = tree_traverse:run(?POOL_NAME, RootFileCtx, #{
        task_id => TransferId,
        callback_module => transfer_file_tree_traverse,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        children_master_jobs_mode => sync,
        traverse_info => #{
            transfer_id => TransferId,
            user_ctx => user_ctx:new(?ROOT_SESS_ID),
            worker_module => replication_worker
        }
    }),
    ok.


%% @private
-spec start_replication_view_traverse(transfer:doc()) -> ok.
start_replication_view_traverse(TransferDoc = #document{key = TransferId, value = #transfer{
    file_uuid = FileUuid,
    space_id = SpaceId
}}) ->
    % TODO VFS-7443 - maybe use referenced guid?
    RootFileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),

    replication_worker:enqueue_data_transfer(RootFileCtx, #transfer_job_ctx{
        transfer_id = TransferId,
        user_ctx = user_ctx:new(?ROOT_SESS_ID),
        job = #transfer_traverse_job{iterator = transfer_iterator:new(TransferDoc)}
    }),
    ok.
