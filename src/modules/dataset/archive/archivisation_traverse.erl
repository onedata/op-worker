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
-include("proto/oneclient/fuse_messages.hrl").
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

% TODO refactor i trzeba przerobić testy

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
            {ok, ArchiveDoc2} = archive:set_dir_guid(ArchiveDoc, ArchiveDirGuid),
            AdditionalData = #{
                <<"archiveId">> => ArchiveId,
                <<"datasetId">> => DatasetId
            },
            {ok, CallbackOrUndefined} = archive:get_preserved_callback(ArchiveDoc2),
            AdditionalData2 = maps_utils:put_if_defined(AdditionalData, <<"callback">>, CallbackOrUndefined),
            Options = #{
                task_id => TaskId,
                track_subtree_status => true,
                children_master_jobs_mode => async,
                traverse_info => #{
                    current_archive_doc => ArchiveDoc2,
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
                    % check if there is dataset attached to current directory
                    % if true, create a nested archive associated with the dataset
                    case is_nested_dataset_attached(FileCtx2, CurrentArchiveDoc) of
                        {true, NestedDatasetId} ->
                            {ok, NestedArchiveDoc} = create_nested_archive(NestedDatasetId, CurrentArchiveDoc, UserId),
                            {ok, NestedArchiveDirGuid} = archive:get_dir_guid(NestedArchiveDoc),
                            {ok, ArchivedDirCtx} = archive_dir_only(FileCtx2, NestedArchiveDirGuid, UserCtx),
                            make_symlink(ArchivedDirCtx, file_ctx:new_by_guid(TargetParentGuid), UserCtx),
                            {NestedArchiveDoc, file_ctx:get_logical_guid_const(ArchivedDirCtx)};
                        false ->
                            {ok, ArchivedDirCtx} = archive_dir_only(FileCtx2, TargetParentGuid, UserCtx),
                            {CurrentArchiveDoc, file_ctx:get_logical_guid_const(ArchivedDirCtx)}
                    end;
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
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),

    CurrentArchiveDoc2 = case is_nested_dataset_attached(FileCtx, CurrentArchiveDoc) of
        {true, NestedDatasetId} ->
            {ok, NestedArchiveDoc} = create_nested_archive(NestedDatasetId, CurrentArchiveDoc, UserId),
            {ok, NestedArchiveDirGuid} = archive:get_dir_guid(NestedArchiveDoc),

            case archive_file_and_mark_finished(FileCtx, NestedArchiveDirGuid, NestedArchiveDoc, UserCtx) of
                {ok, ArchivedFileCtx} ->
                    make_symlink(ArchivedFileCtx, file_ctx:new_by_guid(TargetParentGuid), UserCtx);
                error ->
                    ok
            end,
            NestedArchiveDoc;
        false ->
            archive_file_and_mark_finished(FileCtx, TargetParentGuid, CurrentArchiveDoc, UserCtx),
            CurrentArchiveDoc
    end,

    {ok, CurrentArchiveRootGuid} = archive:get_dataset_root_file_guid(CurrentArchiveDoc2),
    mark_finished_and_propagate_up(FileCtx, UserCtx, ScheduledDatasetRootGuid, CurrentArchiveDoc2,
        CurrentArchiveRootGuid, TaskId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec is_nested_dataset_attached(file_ctx:ctx(), archive:doc()) -> false | {true, NestedDatasetId :: dataset:id()}.
is_nested_dataset_attached(FileCtx, ParentArchiveDoc) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, ParentDatasetId} = archive:get_dataset_id(ParentArchiveDoc),
    case file_meta_dataset:get_id_if_attached(FileDoc) of
        undefined -> false;
        ParentDatasetId -> false;
        DatasetId -> {true, DatasetId}
    end.


-spec create_nested_archive(dataset:id(), archive:doc(), od_user:id()) -> {ok, archive:doc()}.
create_nested_archive(DatasetId, ParentArchiveDoc, UserId) ->
    {ok, SpaceId} = archive:get_space_id(ParentArchiveDoc),
    {ok, NestedArchiveDoc} = archive_api:create_child_archive(DatasetId, ParentArchiveDoc),
    {ok, NestedArchiveId} = archive:get_id(NestedArchiveDoc),
    {ok, NestedArchiveDirUuid} =
        archivisation_tree:create_archive_dir(NestedArchiveId, DatasetId, SpaceId, UserId),
    NestedArchiveDirGuid = file_id:pack_guid(NestedArchiveDirUuid, SpaceId),
    {ok, NestedArchiveDoc2} = archive:set_dir_guid(NestedArchiveDoc, NestedArchiveDirGuid),
    {ok, NestedArchiveDoc2}.


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
mark_building_if_first_job(Job = #tree_traverse{traverse_info = #{current_archive_doc := CurrentArchiveDoc}}) ->
    case is_first_job(Job) of
        true -> ok = archive:mark_building(CurrentArchiveDoc);
        false -> ok
    end.


-spec is_dataset_root(tree_traverse:job()) -> boolean().
is_dataset_root(#tree_traverse{
    file_ctx = FileCtx,
    traverse_info = #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid}
}) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileGuid =:= ScheduledDatasetRootGuid;
is_dataset_root(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid}
}) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileGuid =:= ScheduledDatasetRootGuid.


-spec is_first_job(tree_traverse:master_job()) -> boolean().
is_first_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    token = ListingToken
}) ->
    {IsDir, _} = file_ctx:is_dir(FileCtx),
    is_dataset_root(Job) andalso (
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


-spec archive_file_and_mark_finished(file_ctx:ctx(), file_id:file_guid(), archive:doc(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | error.
archive_file_and_mark_finished(FileCtx, TargetParentGuid, CurrentArchiveDoc, UserCtx) ->
    case archive_file(FileCtx, TargetParentGuid, UserCtx) of
        {ok, CopyCtx} ->
            {FileSize, _} = file_ctx:get_local_storage_file_size(CopyCtx),
            archive:mark_file_archived(CurrentArchiveDoc, FileSize),
            {ok, CopyCtx};
        error ->
            archive:mark_file_failed(CurrentArchiveDoc),
            error
    end.


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


-spec make_symlink(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> ok.
make_symlink(TargetCtx, ParentCtx, UserCtx) ->
    SpaceId = file_ctx:get_space_id_const(TargetCtx),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    {FileName, TargetCtx2} = file_ctx:get_aliased_name(TargetCtx, UserCtx),
    {TargetCanonicalPath, _} = file_ctx:get_canonical_path(TargetCtx2),
    [_Sep, _SpaceId | Rest] = filename:split(TargetCanonicalPath),
    SymlinkValue = filename:join([SpaceIdPrefix | Rest]),
    ?FUSE_OK_RESP = file_req:make_symlink(UserCtx, ParentCtx, FileName, SymlinkValue),
    ok.