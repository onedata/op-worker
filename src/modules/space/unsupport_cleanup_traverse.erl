%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for traversing files tree in order to
%%% perform necessary clean up after space was unsupported, i.e foreach file: 
%%%     * deletes it on storage (if storage is not imported),
%%%     * deletes file_qos document
%%%     * deletes local file_location or dir_location document
%%% 
%%% Clean up for directory is started after all its children have been traversed.
%%% @end
%%%--------------------------------------------------------------------
-module(unsupport_cleanup_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start/2,
    init_pool/0, stop_pool/0,
    delete_ended/2,
    is_finished/1
]).

%% Traverse behaviour callbacks
-export([
    task_finished/2,
    get_job/1, 
    update_job_progress/5,
    do_master_job/2, do_slave_job/2
]).

-type task_id() :: tree_traverse:id().
-export_type([task_id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, unsupport_cleanup_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(od_space:id(), storage:id()) -> {ok, task_id()}.
start(SpaceId, StorageId) ->
    TaskId = gen_id(SpaceId, StorageId),
    SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #{
            % do not remove storage files if storage was imported
            remove_storage_files => not storage:is_imported(StorageId)
        },
        track_subtree_status => true,
        children_master_jobs_mode => async,
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"storage_id">> => StorageId
        }
    },
    {ok, _} = tree_traverse:run(?POOL_NAME, file_ctx:new_by_guid(SpaceDirGuid), Options).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_parallelism_limit, 20),
    
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

-spec delete_ended(od_space:id(), storage:id()) -> ok.
delete_ended(SpaceId, StorageId) ->
    traverse_task:delete_ended(?POOL_NAME, gen_id(SpaceId, StorageId)).

-spec is_finished(task_id()) -> boolean().
is_finished(TaskId) ->
    case traverse_task:get(?POOL_NAME, TaskId) of
        {ok, #document{value = #traverse_task{status = finished}}} -> true;
        _ -> false
    end.

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec task_finished(task_id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"storage_id">> := StorageId
    }} = traverse_task:get_additional_data(Pool, TaskId),
    space_unsupport:report_cleanup_traverse_finished(SpaceId, StorageId).

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), task_id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), task_id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
}, #{task_id := TaskId} = MasterJobArgs
) ->
    RemoveStorageFiles = maps:get(remove_storage_files, TraverseInfo),

    BatchProcessingPrehook = fun(_SlaveJobs, _MasterJobs, _ListExtendedInfo, SubtreeProcessingStatus) ->
        maybe_cleanup_dir(SubtreeProcessingStatus, TaskId, FileCtx, RemoveStorageFiles)
    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPrehook).

-spec do_slave_job(tree_traverse:slave_job(), task_id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
}, TaskId) ->
    RemoveStorageFiles = maps:get(remove_storage_files, TraverseInfo),
    fslogic_delete:cleanup_file(FileCtx, RemoveStorageFiles),
    file_processed(TaskId, FileCtx, RemoveStorageFiles).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gen_id(od_space:id(), storage:id()) -> task_id().
gen_id(SpaceId, StorageId) ->
    datastore_key:new_from_digest([SpaceId, StorageId]).


%% @private
-spec cleanup_dir(task_id(), file_ctx:ctx(), boolean()) -> ok.
cleanup_dir(TaskId, FileCtx, RemoveStorageFiles) ->
    fslogic_delete:cleanup_file(FileCtx, RemoveStorageFiles),
    tree_traverse:delete_subtree_status_doc(TaskId, file_ctx:get_uuid_const(FileCtx)),
    case file_ctx:is_space_dir_const(FileCtx) of
        true -> ok;
        false -> file_processed(TaskId, FileCtx, RemoveStorageFiles)
    end.


%% @private
-spec file_processed(task_id(), file_ctx:ctx(), boolean()) -> ok.
file_processed(TaskId, FileCtx, RemoveStorageFiles) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ParentFileCtx, _FileCtx} = file_ctx:get_parent(FileCtx, UserCtx),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    ParentStatus = tree_traverse:report_child_processed(TaskId, ParentUuid),
    maybe_cleanup_dir(ParentStatus, TaskId, ParentFileCtx, RemoveStorageFiles).


%% @private
-spec maybe_cleanup_dir(tree_traverse_progress:status(), task_id(), file_ctx:ctx(), boolean()) -> ok.
maybe_cleanup_dir(?SUBTREE_PROCESSED, TaskId, FileCtx, RemoveStorageFiles) ->
    cleanup_dir(TaskId, FileCtx, RemoveStorageFiles);
maybe_cleanup_dir(?SUBTREE_NOT_PROCESSED, _TaskId, _FileCtx, _RemoveStorageFiles) -> ok.
