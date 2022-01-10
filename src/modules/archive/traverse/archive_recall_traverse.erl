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
-export([init_pool/0, stop_pool/0, start/4, cancel/1]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_finished/2,
    task_canceled/2,
    get_sync_info/1,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(COPY_OPTIONS(TaskId), #{
    recursive => false, 
    overwrite => true,
    on_write_callback => fun(BytesCopied) -> 
        archive_recall:report_bytes_copied(TaskId, BytesCopied) 
    end
}). 

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


-spec start(archive:doc(), user_ctx:ctx(), file_id:file_guid(), file_meta:name()) -> 
    {ok, file_id:file_guid()} | {error, term()}.
start(ArchiveDoc, UserCtx, TargetParentGuid, TargetRootName) ->
    {ok, DataFileGuid} = archive:get_data_dir_guid(ArchiveDoc),
    % archive data dir contains only one file which is a copy of dataset file
    {[StartFileCtx], _, _} = dir_req:get_children_ctxs(UserCtx, file_ctx:new_by_guid(DataFileGuid), 
        #{size => 1, offset => 0}),
    {FinalName, StartFileCtx1} = case TargetRootName of
        default ->
            file_ctx:get_aliased_name(StartFileCtx, UserCtx);
        _ ->
            {TargetRootName, StartFileCtx}
    end,
    % create root file before traverse starts to have its uuid so progress can be shown
    case create_root_file(user_ctx:get_session_id(UserCtx), StartFileCtx, TargetParentGuid, FinalName) of
        {ok, Guid} ->
            TaskId = file_id:guid_to_uuid(Guid),
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
                        follow_symlinks => external,
                        traverse_info => #{
                            current_parent => TargetParentGuid,
                            root_file_name => FinalName
                        },
                        additional_data => AdditionalData
                    },
                    ok = archive_recall:create(TaskId, TargetParentGuid, FinalName,
                        archive_api:get_aggregated_stats(ArchiveDoc)),
                    {ok, TaskId} = tree_traverse:run(
                        ?POOL_NAME, StartFileCtx1, UserId, Options),
                    ok = archive:report_recall_scheduled(ArchiveId, TaskId),
                    {ok, Guid};
                {error, _} = Error ->
                    Error
            end;
        {error, eexist} ->
            ?ERROR_ALREADY_EXISTS;
        Error ->
            Error
    end.


-spec cancel(id()) -> ok | {error, term()}.
cancel(TaskId) ->
    tree_traverse:cancel(?POOL_NAME, TaskId).

%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    archive_recall:report_started(TaskId),
    
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


-spec task_canceled(id(), tree_traverse:pool()) -> ok.
task_canceled(TaskId, _Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    archive_recall:delete(TaskId),
    
    ?debug("Archive recall traverse ~p cancelled", [TaskId]).


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
do_master_job(#tree_traverse{file_ctx = FileCtx} = Job, MasterJobArgs) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            do_dir_master_job(Job#tree_traverse{file_ctx = FileCtx2}, MasterJobArgs);
        false ->
            tree_traverse:do_master_job(Job, MasterJobArgs)
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId,
    traverse_info = #{current_parent := TargetParentGuid} = TraverseInfo,
    relative_path = ResolvedFilePath
}, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileName = case maps:get(root_file_name, TraverseInfo, undefined) of
        undefined -> filename:basename(ResolvedFilePath);
        Name -> Name
    end,
    {ok, _, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, ?COPY_OPTIONS(TaskId)),
    ok = archive_recall:report_file_finished(TaskId).


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
    DirName = case maps:get(root_file_name, TraverseInfo, undefined) of
        undefined -> filename:basename(ResolvedFilePath);
        Name -> Name
    end,
    #{task_id := TaskId} = MasterJobArgs,
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    SourceDirGuid = file_ctx:get_logical_guid_const(SourceDirCtx),
    {ok, CopyGuid, _} = file_copy:copy(SessionId, SourceDirGuid, TargetParentGuid, DirName, ?COPY_OPTIONS(TaskId)),
    NewTraverseInfo = maps:remove(root_file_name, TraverseInfo#{current_parent => CopyGuid}),
    UpdatedJob = Job#tree_traverse{traverse_info = NewTraverseInfo},
    
    tree_traverse:do_master_job(UpdatedJob, MasterJobArgs).


% fixme spec etc
create_root_file(SessId, FileCtx, TargetParentGuid, TargetRootName) -> %fixme return file_ctx
    case file_ctx:is_dir(FileCtx) of
        {true, _} ->
            lfm:mkdir(SessId, TargetParentGuid, TargetRootName, ?DEFAULT_DIR_MODE);
        {false, _} ->
            lfm:create(SessId, TargetParentGuid, TargetRootName, ?DEFAULT_FILE_MODE)
    end.