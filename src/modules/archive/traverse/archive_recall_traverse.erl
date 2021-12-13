%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour. 
%%% It is responsible for copying archive content to the specified location.
%%% All archive content is copied exactly except for symlinks to nested archives 
%%% which are resolved and nested archives are also copied.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/3
]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_finished/2,
    get_sync_info/1,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: traverse:id().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = op_worker:get_env(archivisation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(archivisation_traverse_slave_jobs_limit, 20),
    ParallelismLimit = op_worker:get_env(archivisation_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec start(archive:doc(), user_ctx:ctx(), file_id:file_guid()) -> ok | {error, term()}.
start(ArchiveDoc, UserCtx, TargetGuid) ->
    TaskId = datastore_key:new(),
    {ok, DataFileGuid} = archive:get_data_dir_guid(ArchiveDoc),
    % archive data dir contains only one file which is a copy of dataset file
    {[StartFileCtx], _, _} = dir_req:get_children_ctxs(UserCtx, file_ctx:new_by_guid(DataFileGuid), #{size => 1, offset => 0}),
    {Name, StartFileCtx1} = file_ctx:get_aliased_name(StartFileCtx, UserCtx),
    try files_tree:get_child(file_ctx:new_by_guid(TargetGuid), Name, UserCtx) of
        _ ->
            ?ERROR_ALREADY_EXISTS
    catch
        throw:?ENOENT ->
            case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
                ok ->
                    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
                    AdditionalData = #{
                        <<"archiveId">> => ArchiveId
                    },
                    UserId = user_ctx:get_user_id(UserCtx),
                    Options = #{
                        task_id => TaskId,
                        children_master_jobs_mode => async,
                        % do not resolve symlinks automatically, custom symlinks 
                        % resolution takes place in NewJobsPreprocessor in master job
                        follow_symlinks => false,
                        traverse_info => #{
                            current_parent => TargetGuid
                        },
                        additional_data => AdditionalData
                    },
                    {ok, TaskId} = tree_traverse:run(
                        ?POOL_NAME, StartFileCtx1, UserId, Options),
                    ok = archive_recall:create(TaskId, TargetGuid),
                    ok;
                {error, _} = Error ->
                    Error
            end
    end.


%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, Pool) ->
    {ok, TaskDoc} = traverse_task:get(Pool, TaskId),
    {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
    ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
    archive:report_recall_started(ArchiveId, TaskId),
    
    ?debug("Archive recall traverse ~p started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    {ok, TaskDoc} = traverse_task:get(Pool, TaskId),
    {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
    ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
    archive:report_recall_finished(ArchiveId, TaskId),
    archive_recall:delete(TaskId),
    
    ?debug("Archive recall traverse ~p finished", [TaskId]).


-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).


-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), tree_traverse:pool(), id()}  | {error, term()}.
get_job(DocOrId) ->
    tree_traverse:get_job(DocOrId).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), tree_traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{file_ctx = FileCtx} = Job, #{task_id := TaskId} = MasterJobArgs) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            do_dir_master_job(Job#tree_traverse{file_ctx = FileCtx2}, MasterJobArgs);
        false ->
            tree_traverse:do_master_job(Job, MasterJobArgs, prepare_new_jobs_preprocessor(Job, TaskId))
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId,
    traverse_info = #{current_parent := TargetParentGuid},
    relative_path = ResolvedFilePath
}, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileName = filename:basename(ResolvedFilePath),
    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, false),
    {FileSize, _} = file_ctx:get_local_storage_file_size(file_ctx:new_by_guid(CopyGuid)),
    ok = archive_recall:report_file_copied(TaskId, FileSize).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_dir_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) -> 
    {ok, traverse:master_job_map()}.
do_dir_master_job(#tree_traverse{
    user_id = UserId, 
    file_ctx = SourceDirCtx,
    traverse_info = #{current_parent := TargetParentGuid} = TraverseInfo,
    relative_path = ResolvedFilePath
} = Job, MasterJobArgs) ->
    DirName = filename:basename(ResolvedFilePath),
    #{task_id := TaskId} = MasterJobArgs,
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    SourceDirGuid = file_ctx:get_logical_guid_const(SourceDirCtx),
    % only directory is copied therefore recursive=false is passed to copy function
    {ok, CopyGuid, _} = file_copy:copy(SessionId, SourceDirGuid, TargetParentGuid, DirName, false),
    NewTraverseInfo = TraverseInfo#{current_parent => CopyGuid},
    UpdatedJob = Job#tree_traverse{traverse_info = NewTraverseInfo},
    
    tree_traverse:do_master_job(UpdatedJob, MasterJobArgs, prepare_new_jobs_preprocessor(UpdatedJob, TaskId)).


-spec prepare_new_jobs_preprocessor(tree_traverse:master_job(), id()) -> 
    tree_traverse:new_jobs_preprocessor().
prepare_new_jobs_preprocessor(Job, TaskId) ->    
    fun(OrigSlaveJobs, OrigMasterJobs, _ListingInfo, _SubtreeProcessingStatus) ->
        lists:foldl(fun(SlaveJob, {SlaveJobs, MasterJobs}) ->
            case update_job_for_symlink(Job, SlaveJob, TaskId) of
                {slave_job, UpdatedJob} ->
                    {[UpdatedJob | SlaveJobs], MasterJobs};
                {master_job, UpdatedJob} ->
                    {SlaveJobs, [UpdatedJob | MasterJobs]}
            end
        end, {[], OrigMasterJobs}, OrigSlaveJobs)
    end.


-spec update_job_for_symlink(tree_traverse:master_job(), tree_traverse:slave_job(), id) -> 
    {slave_job, tree_traverse:slave_job()} | {master_job, tree_traverse:master_job()}.
update_job_for_symlink(Job, #tree_traverse_slave{file_ctx = SlaveCtx} = SlaveJob, TaskId) ->
    case file_ctx:is_symlink_const(SlaveCtx) of
        true ->
            case resolve_symlink(Job, SlaveCtx, TaskId) of
                {resolved, Ctx} ->
                    {Name, Ctx2} = file_ctx:get_aliased_name(Ctx, undefined),
                    case file_ctx:is_dir(Ctx2) of
                        {true, Ctx3} ->
                            {master_job, tree_traverse:get_child_master_job(Job, Ctx3, Name)};
                        {false, Ctx3} ->
                            {slave_job, tree_traverse:get_child_slave_job(Job, Ctx3, Name)}
                    end;
                ignore ->
                    {slave_job, SlaveJob}
            end;
        false ->
            {slave_job, SlaveJob}
    end.


-spec resolve_symlink(tree_traverse:master_job(), file_ctx:ctx(), id()) -> 
    {resolved, file_ctx:ctx()} | ignore.
resolve_symlink(#tree_traverse{user_id = UserId} = Job, SymlinkCtx, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    case tree_traverse:resolve_symlink(Job, SymlinkCtx, UserCtx) of
        ignore -> ignore;
        {ok, ResolvedCtx} ->
            {CanonicalPath, ResolvedCtx2} = file_ctx:get_canonical_path(ResolvedCtx),
            {ok, TaskDoc} = traverse_task:get(?POOL_NAME, TaskId),
            {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
            ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
            case archivisation_tree:extract_archive_id(CanonicalPath) of
                %% @TODO VFS-8663 Currently symlinks in nested archives to self will be resolved instead of copied, which can result in infinite loops
                %% @TODO VFS-8663 remove below line after proper symlink resolution is implemented
                ArchiveId -> ignore; % symlink leads to this archive, copy it
                ?ERROR_NOT_FOUND -> ignore; % symlink does not lead to any archive, copy it
                _ -> {resolved, ResolvedCtx2} % symlink leads to other archive, resolve it and copy its content
            end
    end.
