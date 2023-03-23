%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for traversing file tree and
%%% transferring regular files.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_file_tree_traverse).
-author("Bartosz Walkowicz").

-behavior(traverse_behaviour).

-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% Traverse behaviour callbacks
-export([
    get_job/1, update_job_progress/5,
    do_master_job/2, do_slave_job/2,
    task_finished/2, task_canceled/2
]).


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================


-spec get_job(traverse:job_id()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()} |
    {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec update_job_progress(
    undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(),
    traverse:pool(),
    transfer:id(),
    traverse:job_status()
) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TransferId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TransferId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), transfer:id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
}, TransferId) ->
    transfer:increment_files_to_process_counter(TransferId, 1),
    transfer_traverse_worker:run_job(TransferId, TraverseInfo, FileCtx).


-spec task_finished(transfer:id(), traverse:pool()) -> ok.
task_finished(TransferId, _PoolName) ->
    transfer:mark_traverse_finished(TransferId),
    ok.


-spec task_canceled(transfer:id(), traverse:pool()) -> ok.
task_canceled(TransferId, PoolName) ->
    task_finished(TransferId, PoolName).
