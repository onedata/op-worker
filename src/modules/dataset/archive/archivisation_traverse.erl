%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour.
%%% It is used to traverse tree rooted in root of passed Dataset
%%% and build archive of the dataset.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_traverse).
-author("Jakub Kudzia").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/3, get_description/1,
    get_files_to_archive_count/1, get_files_archived_count/1,
    get_files_failed_count/1, get_byte_size/1
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

-type id() :: tree_traverse:id().
-type description() :: traverse:description().

-export_type([id/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = op_worker:get_env(archivisation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(archivisation_traverse_slave_jobs_limit, 10),
    ParallelismLimit = op_worker:get_env(archivisation_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec start(archive:doc(), dataset:doc(), user_ctx:ctx()) -> ok | {error, term()}.
start(ArchiveDoc, DatasetDoc, UserCtx) ->
    TaskId = datastore_key:new(),
    case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
        ok ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
            UserId = user_ctx:get_user_id(UserCtx),
            {ok, ArchiveDirUuid} = archivisation_tree:create_archive_dir(ArchiveId, DatasetId, SpaceId),
            ArchiveDirGuid = file_id:pack_guid(ArchiveDirUuid, SpaceId),
            DatasetRootCtx = dataset_api:get_associated_file_ctx(DatasetDoc),

            AdditionalData = #{
                <<"archiveId">> => ArchiveId,
                <<"datasetId">> => DatasetId
            },
            {ok, CallbackOrUndefined} = archive:get_preserved_callback(ArchiveDoc),
            AdditionalData2 = maps_utils:put_if_defined(AdditionalData, <<"callback">>, CallbackOrUndefined),
            {ok, ArchiveDoc2} = archive:set_job_id(ArchiveId, TaskId),
            Options = #{
                task_id => TaskId,
                children_master_jobs_mode => async,
                traverse_info => #{
                    archive_doc => ArchiveDoc2,
                    target_parent_guid => ArchiveDirGuid,
                    dataset_root_guid => file_ctx:get_logical_guid_const(DatasetRootCtx)
                },
                additional_data => AdditionalData2
            },
            {ok, TaskId} = tree_traverse:run(?POOL_NAME, DatasetRootCtx, UserId, Options),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec get_description(id()) -> {ok, description()} | {error, term()}.
get_description(TaskId) ->
    {ok, TaskDoc} = traverse_task:get(?POOL_NAME, TaskId),
    traverse_task:get_description(TaskDoc).


-spec get_files_archived_count(description()) -> non_neg_integer().
get_files_archived_count(JobDescription) ->
    maps:get(slave_jobs_done, JobDescription, 0).


-spec get_files_to_archive_count(description()) -> non_neg_integer().
get_files_to_archive_count(JobDescription) ->
    maps:get(slave_jobs_delegated, JobDescription, 0).


-spec get_files_failed_count(description()) -> non_neg_integer().
get_files_failed_count(JobDescription) ->
    maps:get(slave_jobs_failed, JobDescription, 0).


-spec get_byte_size(description()) -> non_neg_integer().
get_byte_size(JobsDescription) ->
    maps:get(byte_size, JobsDescription, 0).

%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("Archivisation job ~p started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    ?debug("Archivisation job ~p finished", [TaskId]),
    tree_traverse_session:close_for_task(TaskId),
    {ok, TaskDoc} = traverse_task:get(?POOL_NAME, TaskId),
    {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
    {ok, Description} = traverse_task:get_description(TaskDoc),

    FilesToArchive = get_files_to_archive_count(Description),
    FilesArchived = get_files_archived_count(Description),
    FilesFailed = get_files_failed_count(Description),
    ByteSize = get_byte_size(Description),

    ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
    DatasetId = maps:get(<<"datasetId">>, AdditionalData),
    CallbackUrlOrUndefined = maps:get(<<"callback">>, AdditionalData, undefined),

    SlaveJobsFailed = maps:get(slave_jobs_failed, Description, 0),
    MasterJobsFailed = maps:get(master_jobs_failed, Description, 0),
    case SlaveJobsFailed + MasterJobsFailed =:= 0 of
        true ->
            ok = archive:mark_preserved(ArchiveId, FilesToArchive, FilesArchived, FilesFailed, ByteSize),
            archivisation_callback:notify_preserved(ArchiveId, DatasetId, CallbackUrlOrUndefined);
        false ->
            ok = archive:mark_failed(ArchiveId, FilesToArchive, FilesArchived, FilesFailed, ByteSize),
            % TODO VFS-7662 send more descriptive error description to archivisation callback
            ErrorDescription = <<"Errors occurered during archivisation job.">>,
            archivisation_callback:notify_preservation_failed(ArchiveId, DatasetId, CallbackUrlOrUndefined, ErrorDescription)
    end.


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
do_master_job(Job = #tree_traverse{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = TraverseInfo = #{
        dataset_root_guid := DatasetRootGuid,
        archive_doc := ArchiveDoc,
        target_parent_guid := TargetParentGuid
    },
    token = ListingToken
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    case file_ctx:get_logical_guid_const(FileCtx) =:= DatasetRootGuid of
        true -> archive:mark_building(ArchiveId);
        false -> ok
    end,

    IsFirstBatch = ListingToken =:= ?INITIAL_LS_TOKEN,
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),

    Job2 = case IsDir andalso IsFirstBatch of
        true ->
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            {DirName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
            DirGuid = file_ctx:get_logical_guid_const(FileCtx3),
            {ok, CopyGuid, _} = file_copy:copy(user_ctx:get_session_id(UserCtx), DirGuid, TargetParentGuid, DirName, false),
            % set parent guid for children jobs
            Job#tree_traverse{traverse_info = TraverseInfo#{target_parent_guid => CopyGuid}};
        false ->
            Job
    end,
    tree_traverse:do_master_job(Job2, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> {ok, description()}.
do_slave_job(#tree_traverse_slave{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = #{target_parent_guid := TargetParentGuid}
}, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx2),
    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, false),

    CopyCtx = file_ctx:new_by_guid(CopyGuid),
    {FileSize, CopyCtx2} = file_ctx:get_local_storage_file_size(CopyCtx),
    {SDHandle, _} = storage_driver:new_handle(SessionId, CopyCtx2),
    ok = storage_driver:flushbuffer(SDHandle, FileSize),

    {ok, #{byte_size => FileSize}}.