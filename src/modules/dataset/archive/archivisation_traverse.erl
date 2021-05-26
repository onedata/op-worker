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

-type id() :: tree_traverse:id().

-export_type([id/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = op_worker:get_env(archivisation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(archivisation_traverse_slave_jobs_limit, 100),
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
            {ok, ArchiveDirUuid} = archivisation_tree:create_archive_dir(ArchiveId, DatasetId, SpaceId, UserId),
            ArchiveDirGuid = file_id:pack_guid(ArchiveDirUuid, SpaceId),
            DatasetRootCtx = dataset_api:get_associated_file_ctx(DatasetDoc),

            AdditionalData = #{
                <<"archiveId">> => ArchiveId,
                <<"datasetId">> => DatasetId
            },
            {ok, CallbackOrUndefined} = archive:get_preserved_callback(ArchiveDoc),
            AdditionalData2 = maps_utils:put_if_defined(AdditionalData, <<"callback">>, CallbackOrUndefined),
            Options = #{
                task_id => TaskId,
                track_subtree_status => true,
                children_master_jobs_mode => async,
                traverse_info => #{
                    current_archive_doc => ArchiveDoc,
                    target_parent_guid => ArchiveDirGuid,
                    scheduled_dataset_root_guid => file_ctx:get_logical_guid_const(DatasetRootCtx)
                },
                additional_data => AdditionalData2
            },
            {ok, TaskId} = tree_traverse:run(?POOL_NAME, DatasetRootCtx, UserId, Options),
            ok;
        {error, _} = Error ->
            Error
    end.


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

    ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
    DatasetId = maps:get(<<"datasetId">>, AdditionalData),
    CallbackUrlOrUndefined = maps:get(<<"callback">>, AdditionalData, undefined),

    SlaveJobsFailed = maps:get(slave_jobs_failed, Description, 0),
    MasterJobsFailed = maps:get(master_jobs_failed, Description, 0),
    case SlaveJobsFailed + MasterJobsFailed =:= 0 of
        true ->
            archivisation_callback:notify_preserved(ArchiveId, DatasetId, CallbackUrlOrUndefined);
        false ->
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
        scheduled_dataset_root_guid := ScheduledDatasetRootGuid,
        target_parent_guid := TargetParentGuid,
        current_archive_doc := CurrentArchiveDoc
    },
    token = ListingToken
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    mark_building_if_first_job(Job),
    IsFirstBatch = ListingToken =:= ?INITIAL_LS_TOKEN,
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),

    case IsDir of
        true ->
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            {FinalArchiveDoc, FinalTargetParentGuid} = case IsFirstBatch of
                true ->

                    % check if there is dataset attach to current directory
                    % if true, create a nested archive associated with the dataset
                    {ok, CurrentArchiveDoc2} =
                        create_nested_archive_if_direct_dataset_is_attached(FileCtx2, CurrentArchiveDoc, TargetParentGuid),

                    {ok, ArchivedDirCtx} = archive_dir_only(FileCtx2, TargetParentGuid, UserCtx),

                    % return archive doc and guid of archived dir for children jobs
                    {CurrentArchiveDoc2, file_ctx:get_logical_guid_const(ArchivedDirCtx)};
                false ->
                    {CurrentArchiveDoc, TargetParentGuid}
            end,

            NewJobsPreprocessor = fun(_SlaveJobs, _MasterJobs, _ListExtendedInfo, SubtreeProcessingStatus) ->
                case SubtreeProcessingStatus of
                    ?SUBTREE_PROCESSED ->
                        {ok, FinalArchiveRootGuid} = archive:get_dataset_root_file_guid(FinalArchiveDoc),
                        mark_finished_and_propagate_up(FileCtx2, UserCtx, ScheduledDatasetRootGuid,
                            FinalArchiveDoc, FinalArchiveRootGuid, TaskId);
                    ?SUBTREE_NOT_PROCESSED ->
                        ok
                end
            end,

            TraverseInfo2 = TraverseInfo#{
                target_parent_guid => FinalTargetParentGuid,
                current_archive_doc => FinalArchiveDoc
            },
            Job2 = Job#tree_traverse{traverse_info = TraverseInfo2},
            tree_traverse:do_master_job(Job2, MasterJobArgs, NewJobsPreprocessor);
        false ->
            tree_traverse:do_master_job(Job, MasterJobArgs)
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = #{
        target_parent_guid := TargetParentGuid,
        scheduled_dataset_root_guid := ScheduledDatasetRootGuid,
        current_archive_doc := CurrentArchiveDoc
    }
}, TaskId) ->
    {ok, CurrentArchiveDoc2} = create_nested_archive_if_direct_dataset_is_attached(FileCtx, CurrentArchiveDoc, TargetParentGuid),
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    case archive_file(FileCtx, TargetParentGuid, UserCtx) of
        {ok, CopyCtx} ->
            {FileSize, _} = file_ctx:get_local_storage_file_size(CopyCtx),
            archive:mark_file_archived(CurrentArchiveDoc2, FileSize);
        error ->
            archive:mark_file_failed(CurrentArchiveDoc2)
    end,
    {ok, CurrentArchiveRootGuid} = archive:get_dataset_root_file_guid(CurrentArchiveDoc2),
    mark_finished_and_propagate_up(FileCtx, UserCtx, ScheduledDatasetRootGuid, CurrentArchiveDoc2,
        CurrentArchiveRootGuid, TaskId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_nested_archive_if_direct_dataset_is_attached(file_ctx:ctx(), archive:doc(), file_id:file_guid()) ->
    {ok, archive:doc()} | {error, term()}.
create_nested_archive_if_direct_dataset_is_attached(FileCtx, ParentArchiveDoc, TargetParentGuid) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, ParentDatasetId} = archive:get_dataset_id(ParentArchiveDoc),
    case file_meta_dataset:get_id_if_attached(FileDoc) of
        undefined ->
            {ok, ParentArchiveDoc};
        ParentDatasetId ->
            {ok, ParentArchiveDoc};
        DatasetId ->
            archive_api:create_child_archive(DatasetId, ParentArchiveDoc, TargetParentGuid)
    end.


-spec mark_finished_and_propagate_up(file_ctx:ctx(), user_ctx:ctx(), file_id:file_guid(), archive:doc(),
    file_id:file_guid(), id()) -> ok.
mark_finished_and_propagate_up(CurrentFileCtx, UserCtx, ScheduledDatasetRootGuid, CurrentArchiveDoc,
    CurrentArchiveRootGuid, TaskId
) ->
    {NextArchiveDoc, NextArchiveRootGuid} = mark_finished_if_current_archive_is_rooted_in_current_file(
        CurrentFileCtx, CurrentArchiveDoc, CurrentArchiveRootGuid
    ),
    propagate_up(CurrentFileCtx, UserCtx, ScheduledDatasetRootGuid, NextArchiveDoc, NextArchiveRootGuid, TaskId).


-spec propagate_up(file_ctx:ctx(), user_ctx:ctx(), file_id:file_guid(), archive:doc(),
    file_id:file_guid(), id()) -> ok.
propagate_up(FileCtx, UserCtx, ScheduledDatasetRootGuid, NextArchiveDoc, NextArchiveRootGuid, TaskId) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    case FileGuid =:= ScheduledDatasetRootGuid of
        true ->
            ok;
        false ->
            {ParentFileCtx, _} = files_tree:get_parent(FileCtx, UserCtx),
            ParentUuid = file_ctx:get_logical_uuid_const(ParentFileCtx),
            ParentStatus = tree_traverse:report_child_processed(TaskId, ParentUuid),
            case ParentStatus of
                ?SUBTREE_PROCESSED ->
                    mark_finished_and_propagate_up(ParentFileCtx, UserCtx, ScheduledDatasetRootGuid,
                        NextArchiveDoc, NextArchiveRootGuid, TaskId);
                ?SUBTREE_NOT_PROCESSED ->
                    ok
            end
    end.


-spec mark_building_if_first_job(tree_traverse:master_job()) -> ok.
mark_building_if_first_job(Job = #tree_traverse{
    traverse_info = #{
        target_parent_guid := TargetParentGuid,
        current_archive_doc := CurrentArchiveDoc
    }
}) ->
    case is_first_job(Job) of
        true -> ok = archive:mark_building(CurrentArchiveDoc, TargetParentGuid);
        false -> ok
    end.


-spec is_first_job(tree_traverse:master_job()) -> boolean().
is_first_job(#tree_traverse{
    file_ctx = FileCtx,
    traverse_info = #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid},
    token = ListingToken
}) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {IsDir, _} = file_ctx:is_dir(FileCtx),
    FileGuid =:= ScheduledDatasetRootGuid andalso (
        (IsDir andalso (ListingToken =:= ?INITIAL_LS_TOKEN))
        orelse not IsDir
    ).


-spec mark_finished_if_current_archive_is_rooted_in_current_file(file_ctx:ctx(), archive:doc(), file_id:file_guid()) ->
    {archive:doc(), file_id:file_guid()}.
mark_finished_if_current_archive_is_rooted_in_current_file(CurrentFileCtx, CurrentArchiveDoc, CurrentArchiveRootGuid) ->
    CurrentFileGuid = file_ctx:get_logical_guid_const(CurrentFileCtx),
    case CurrentFileGuid =:= CurrentArchiveRootGuid of
        true ->
            calculate_stats_and_mark_finished(CurrentArchiveDoc),
            case archive:get_parent_doc(CurrentArchiveDoc) of
                {ok, undefined} ->
                    {undefined, undefined};
                {ok, ParentDoc} ->
                    {ok, ParentArchiveRootFileGuid} = archive:get_dataset_root_file_guid(ParentDoc),
                    {ParentDoc, ParentArchiveRootFileGuid}
            end;
        false ->
            {CurrentArchiveDoc, CurrentArchiveRootGuid}
    end.


-spec calculate_stats_and_mark_finished(archive:doc()) -> ok.
calculate_stats_and_mark_finished(ArchiveDoc) ->
    NestedArchiveStats = archive_api:get_nested_archives_stats(ArchiveDoc),
    ok = archive:mark_finished(ArchiveDoc, NestedArchiveStats).


-spec archive_dir_only(file_ctx:ctx(), file_id:file_guid(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
archive_dir_only(FileCtx, TargetParentGuid, UserCtx) ->
    try
        archive_dir_only_insecure(FileCtx, TargetParentGuid, UserCtx)
    catch
        Class:Reason ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured during archivisation of directory ~s.", [Class, Reason, Guid]),
            error
    end.


-spec archive_dir_only_insecure(file_ctx:ctx(), file_id:file_guid(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
archive_dir_only_insecure(FileCtx, TargetParentGuid, UserCtx) ->
    {DirName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    DirGuid = file_ctx:get_logical_guid_const(FileCtx2),
    {ok, CopyGuid, _} = file_copy:copy(user_ctx:get_session_id(UserCtx), DirGuid, TargetParentGuid, DirName, false),
    {ok, file_ctx:new_by_guid(CopyGuid)}.


-spec archive_file(file_ctx:ctx(), file_id:file_guid(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | error.
archive_file(FileCtx, TargetParentGuid, UserCtx) ->
    try
        archive_file_insecure(FileCtx, TargetParentGuid, UserCtx)
    catch
        Class:Reason ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured during archivisation of file ~s.", [Class, Reason, Guid]),
            error
    end.

-spec archive_file_insecure(file_ctx:ctx(), file_id:file_guid(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | error.
archive_file_insecure(FileCtx, TargetParentGuid, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx2),

    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, false),

    CopyCtx = file_ctx:new_by_guid(CopyGuid),
    {FileSize, CopyCtx2} = file_ctx:get_local_storage_file_size(CopyCtx),
    {SDHandle, CopyCtx3} = storage_driver:new_handle(SessionId, CopyCtx2),
    ok = storage_driver:flushbuffer(SDHandle, FileSize),
    {ok, CopyCtx3}.