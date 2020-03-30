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
%%%     * deletes it on storage (when storage sync is not enabled), 
%%%     * deletes file_qos document
%%%     * deletes local file_location or dir_location document
%%% 
%%% Clean up for directory is started after all its children have been traversed. 
%%% This is determined using using cleanup_traverse_status document, 
%%% which is created for each directory.
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

-type id() :: traverse:id().
-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, unsupport_cleanup_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(od_space:id(), storage:id()) -> {ok, id()}.
start(SpaceId, StorageId) ->
    TaskId = gen_id(SpaceId, StorageId),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #{
            % do not remove storage files if storage sync was enabled
            remove_storage_files => not is_storage_sync_enabled(SpaceId, StorageId)
        },
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"storage_id">> => StorageId
        }
    },
    cleanup_traverse_status:create(TaskId, SpaceDirUuid),
    {ok, _} = tree_traverse:run(?POOL_NAME, file_ctx:new_by_guid(SpaceDirGuid), Options).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_parallelism_limit, 20),
    
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    traverse:stop_pool(?POOL_NAME).

-spec delete_ended(od_space:id(), storage:id()) -> ok.
delete_ended(SpaceId, StorageId) ->
    traverse_task:delete_ended(?POOL_NAME, gen_id(SpaceId, StorageId)).

-spec is_finished(id()) -> boolean().
is_finished(TaskId) ->
    case traverse_task:get(?POOL_NAME, TaskId) of
        {ok, #document{value = #traverse_task{status = finished}}} -> true;
        _ -> false
    end.

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"storage_id">> := StorageId
    }} = traverse_task:get_additional_data(Pool, TaskId),
    space_unsupport:report_cleanup_traverse_finished(SpaceId, StorageId).

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{traverse_info = TraverseInfo} = Job, MasterJobArgs) ->
    #{remove_storage_files := RemoveStorageFiles} = TraverseInfo,
    
    MasterJobFinishedCallback = fun(TaskId, SlaveJobs, MasterJobs, _SpaceId, Uuid, _BatchLastFilename) ->
        ChildrenCount = length(SlaveJobs) + length(MasterJobs),
        cleanup_traverse_status:report_children_listed(TaskId, Uuid, ChildrenCount),
    
        lists:foreach(fun(#tree_traverse{doc = #document{key = ChildDirUuid}}) ->
            cleanup_traverse_status:create(TaskId, ChildDirUuid)
        end, MasterJobs)
    end,
    
    LastBatchFinishedCallback = fun(TaskId, Uuid, SpaceId) ->
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId)),
        Status = cleanup_traverse_status:report_last_batch(TaskId, Uuid),
        maybe_cleanup_dir(Status, TaskId, FileCtx, RemoveStorageFiles)
    end, 
    
    {ok, #{master_jobs := MasterJobs, slave_jobs := SlaveJobs}} = 
        tree_traverse:do_master_job(Job, MasterJobArgs, MasterJobFinishedCallback, LastBatchFinishedCallback),
    {ok, #{slave_jobs => SlaveJobs, async_master_jobs => MasterJobs}}.

-spec do_slave_job(traverse:job(), id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId}, TraverseInfo}, TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    #{remove_storage_files := RemoveStorageFiles} = TraverseInfo,
    
    ok = file_qos:delete(FileUuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    LocationId = file_location:local_id(FileUuid),
    
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, UserCtx),
    
    RemoveStorageFiles andalso sd_utils:delete_storage_file(FileCtx1, UserCtx),
    fslogic_location_cache:clear_blocks(FileCtx1, LocationId),
    fslogic_location_cache:delete_location(FileUuid, LocationId),
    
    file_traverse_finished(TaskId, ParentFileCtx, RemoveStorageFiles).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gen_id(od_space:id(), storage:id()) -> id().
gen_id(SpaceId, StorageId) ->
    datastore_key:new_from_digest([SpaceId, StorageId]).


%% @private
-spec is_storage_sync_enabled(od_space:id(), storage:id()) -> boolean().
is_storage_sync_enabled(SpaceId, StorageId) ->
    {ok, SyncConfigs} = space_strategies:get_sync_configs(SpaceId),
    SyncConfig = maps:get(StorageId, SyncConfigs, undefined),
    case SyncConfig of
        undefined -> false;
        _ ->
            {ImportEnabled, _ImportConfig} = space_strategies:get_import_details(SyncConfig),
            ImportEnabled
    end.


%% @private
-spec cleanup_dir(id(), file_ctx:ctx(), boolean()) -> ok.
cleanup_dir(TaskId, FileCtx, RemoveStorageFiles) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    RemoveStorageFiles andalso sd_utils:delete_storage_dir(FileCtx, UserCtx),
    dir_location:delete(file_ctx:get_uuid_const(FileCtx)),
    case file_ctx:is_space_dir_const(FileCtx) of
        true -> ok;
        false ->
            {ParentFileCtx, _FileCtx} = file_ctx:get_parent(FileCtx, UserCtx),
            file_traverse_finished(TaskId, ParentFileCtx, RemoveStorageFiles)
    end.


%% @private
-spec file_traverse_finished(id(), file_ctx:ctx(), boolean()) -> ok.
file_traverse_finished(TaskId, ParentFileCtx, RemoveStorageFiles) ->
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    ParentStatus = cleanup_traverse_status:report_child_traversed(TaskId, ParentUuid),
    maybe_cleanup_dir(ParentStatus, TaskId, ParentFileCtx, RemoveStorageFiles).


%% @private
-spec maybe_cleanup_dir(cleanup_traverse_status:status(), id(), file_ctx:ctx(), boolean()) -> ok.
maybe_cleanup_dir(traversed, TaskId, FileCtx, RemoveStorageFiles) ->
    cleanup_dir(TaskId, FileCtx, RemoveStorageFiles);
maybe_cleanup_dir(not_traversed, _TaskId, _FileCtx, _RemoveStorageFiles) -> ok.
