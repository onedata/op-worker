%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module manages pools of processes responsible for replication
%%% (either by view or file tree).
%%% @end
%%%--------------------------------------------------------------------
-module(replication_traverse).
-author("Bartosz Walkowicz").

-include("modules/datastore/transfer.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_pool/0, stop_pool/0, pool_name/0]).
-export([start/1, cancel/1]).


-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(MASTER_JOBS_LIMIT, op_worker:get_env(replication_master_jobs_limit, 20)).
-define(SLAVE_JOBS_LIMIT, op_worker:get_env(replication_slave_jobs_limit, 60)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(transfer_traverse_list_batch_size, 1000)).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = ?MASTER_JOBS_LIMIT,

    % set parallelism limit equal to master jobs limit
    tree_traverse:init(?MODULE, MasterJobsLimit, ?SLAVE_JOBS_LIMIT, MasterJobsLimit, [?MODULE]).


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
        view -> view_traverse:cancel(?POOL_NAME, TransferId)
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

    try
        {ok, _} = tree_traverse:run(?POOL_NAME, RootFileCtx, #{
            task_id => TransferId,
            callback_module => transfer_file_tree_traverse,
            batch_size => ?TRAVERSE_BATCH_SIZE,
            listing_errors_handling_policy => ignore_known,
            children_master_jobs_mode => sync,
            traverse_info => #{
                space_id => SpaceId,
                transfer_id => TransferId,
                user_ctx => user_ctx:new(?ROOT_SESS_ID),
                worker_module => replication_worker
            }
        })
    catch
        error:{badmatch, {error, not_found}} ->
            % New file has not been synchronized yet
            transfer:mark_traverse_finished(TransferId);
        Class:Reason:Stacktrace ->
            ?error_exception(
                "Failed to start transfer file tree traverse ~p", [TransferId],
                Class, Reason, Stacktrace
            ),
            replication_status:handle_aborting(TransferId)
    end,
    ok.


%% @private
-spec start_replication_view_traverse(transfer:doc()) -> ok.
start_replication_view_traverse(#document{key = TransferId, value = #transfer{
    space_id = SpaceId,
    index_name = ViewName,
    query_view_params = QueryViewParams
}}) ->
    try
        {ok, ViewId} = view_links:get_view_id(ViewName, SpaceId),
        {ok, _} = view_traverse:run(?POOL_NAME, transfer_view_traverse, ViewId, TransferId, #{
            query_opts => maps:merge(
                maps:from_list(QueryViewParams),
                #{limit => ?TRAVERSE_BATCH_SIZE}
            ),
            async_next_batch_job => true,
            info => #{
                space_id => SpaceId,
                transfer_id => TransferId,
                view_name => ViewName,
                user_ctx => user_ctx:new(?ROOT_SESS_ID),
                worker_module => replication_worker
            }
        })
    catch
        error:{badmatch, {error, not_found}} ->
            % New view has not been synchronized yet
            transfer:mark_traverse_finished(TransferId);
        Class:Reason:Stacktrace ->
            ?error_exception(
                "Failed to start transfer view traverse ~p", [TransferId],
                Class, Reason, Stacktrace
            ),
            replication_status:handle_aborting(TransferId)
    end,
    ok.
