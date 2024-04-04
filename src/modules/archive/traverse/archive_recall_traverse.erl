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
%%% which are resolved and their content is then recursively copied. 
%%% Recall root file is created before traverse starts to check user privileges 
%%% and whether such file does not already exist. Uuid of this file is then used 
%%% as the recall identifier.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
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

% exported for mocking/calling in tests
-export([
    do_dir_master_job_unsafe/2,
    do_slave_job_unsafe/2,
    setup_recall_traverse/6
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(COPY_OPTIONS(TaskId), #{
    recursive => false, 
    overwrite => true,
    on_write_callback => fun(BytesCopied) ->
        archive_recall:report_bytes_copied(TaskId, BytesCopied),
        case traverse:is_job_cancelled(TaskId) of
            true -> abort;
            false -> continue
        end
    end
}). 

-type id() :: file_meta:uuid().

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
start(ArchiveDoc, UserCtx, ParentGuid, TargetFilename) ->
    {ok, DataFileGuid} = archive:get_data_dir_guid(ArchiveDoc),
    % archive data dir contains only one file which is a copy of a dataset file
    {[StartFileCtx], _, _} = dir_req:list_children_ctxs(UserCtx, file_ctx:new_by_guid(DataFileGuid),
        #{limit => 1, offset => 0, tune_for_large_continuous_listing => false}),
    {FinalName, StartFileCtx1} = case TargetFilename of
        default ->
            file_ctx:get_aliased_name(StartFileCtx, UserCtx);
        _ ->
            {TargetFilename, StartFileCtx}
    end,
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    {IsDir, StartFileCtx2} = file_ctx:is_dir(StartFileCtx1),
    case ensure_recall_allowed(SpaceId, UserCtx, ParentGuid) of
        ok ->
            case create_root_file(IsDir, user_ctx:get_session_id(UserCtx), ParentGuid, FinalName) of
                {ok, RootFileGuid} ->
                    {RootPath, _} = file_ctx:get_canonical_path(file_ctx:new_by_guid(RootFileGuid)),
                    [_Sep, _SpaceId | RootPathTokens] = filename:split(RootPath),
                    TraverseInfo = #{
                        archive_doc => ArchiveDoc,
                        current_parent => ParentGuid,
                        root_path_tokens => RootPathTokens,
                        root_file_name => FinalName
                    },
                    setup_recall_traverse(
                        SpaceId, ArchiveDoc, RootFileGuid, TraverseInfo, StartFileCtx2, UserCtx),
                    {ok, RootFileGuid};
                {error, eexist} ->
                    ?ERROR_ALREADY_EXISTS;
                {error, Reason} ->
                    ?ERROR_POSIX(Reason)
            end;
        Error ->
            Error
    end.


-spec cancel(id()) -> ok | {error, term()}.
cancel(TaskId) ->
    case tree_traverse:cancel(?POOL_NAME, TaskId) of
        ok -> archive_recall_details:report_cancel_started(TaskId);
        {error, _} = Error -> Error 
    end.

%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    archive_recall:report_started(TaskId),
    ?debug("Archive recall traverse ~tp started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    {ok, TaskDoc} = traverse_task:get(Pool, TaskId),
    {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
    SpaceId = maps:get(<<"spaceId">>, AdditionalData),
    archive_recall:report_finished(TaskId, SpaceId),
    ?debug("Archive recall traverse ~tp finished", [TaskId]).


-spec task_canceled(id(), tree_traverse:pool()) -> ok.
task_canceled(TaskId, Pool) ->
    task_finished(TaskId, Pool).


-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).


-spec get_job(traverse:job_id()) ->
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
do_master_job(InitialJob, MasterJobArgs = #{task_id := TaskId}) ->
    ErrorHandler = fun(Job, Reason, Stacktrace) ->
        report_error(TaskId, Job, Reason, Stacktrace),
        {ok, #{}} % unexpected error logged by report_error - no jobs can be created
    end,
    archive_traverses_common:do_master_job(?MODULE, InitialJob, MasterJobArgs, ErrorHandler).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(InitialJob, TaskId) ->
    ErrorHandler = fun(Job, Reason, Stacktrace) ->
        report_error(TaskId, Job, Reason, Stacktrace)
    end,
    archive_traverses_common:execute_unsafe_job(
        ?MODULE, do_slave_job_unsafe, [TaskId], InitialJob, ErrorHandler).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec ensure_recall_allowed(od_space:id(), user_ctx:ctx(), file_id:file_guid()) -> 
    ok | {error, term()}.
ensure_recall_allowed(SpaceId, UserCtx, TargetParentGuid) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case lfm:stat(SessId, ?FILE_REF(TargetParentGuid)) of
        {ok, #file_attr{type = ?SYMLINK_TYPE}} ->
            case lfm:resolve_symlink(SessId, ?FILE_REF(TargetParentGuid)) of
                {ok, TargetGuid} ->
                    can_start_recall(SpaceId, TargetGuid);
                Error ->
                    Error
            end;
        {ok, _} ->
            can_start_recall(SpaceId, TargetParentGuid);
        {error, _} = Error ->
            Error
    end.


%% @private
-spec can_start_recall(od_space:id(), file_id:file_guid()) -> ok | {error, term()}.
can_start_recall(SpaceId, Guid) ->
    case archive_recall_cache:get(SpaceId, file_id:guid_to_uuid(Guid)) of
        {ok, {ongoing, _}} ->
            ?ERROR_RECALL_TARGET_CONFLICT;
        _ ->
            ok
    end.


%% @private
-spec setup_recall_traverse(od_space:id(), archive:doc(), file_id:file_guid(),
    tree_traverse:traverse_info(), file_ctx:ctx(), user_ctx:ctx()) -> ok | {error, term()}.
setup_recall_traverse(SpaceId, ArchiveDoc, RootFileGuid, TraverseInfo, StartFileCtx, UserCtx) ->
    RecallId = file_id:guid_to_uuid(RootFileGuid),
    case tree_traverse_session:setup_for_task(UserCtx, RecallId) of
        ok ->
            AdditionalData = #{
                <<"spaceId">> => SpaceId
            },
            UserId = user_ctx:get_user_id(UserCtx),
            Options = #{
                task_id => RecallId,
                listing_errors_handling_policy => retry_infinitely,
                children_master_jobs_mode => async,
                %% @TODO VFS-8851 do not resolve external symlinks (that are not nested archive) 
                %% when archive was created without following symlinks
                symlink_resolution_policy => follow_external,
                traverse_info => TraverseInfo,
                additional_data => AdditionalData
            },
            ok = archive_recall:create_docs(RecallId, ArchiveDoc),
            {ok, RecallId} = tree_traverse:run(?POOL_NAME, StartFileCtx, UserId, Options);
        {error, _} = Error ->
            Error
    end.


%% @private
-spec do_dir_master_job_unsafe(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_dir_master_job_unsafe(#tree_traverse{
    user_id = UserId, 
    file_ctx = SourceDirCtx,
    traverse_info = #{
        current_parent := TargetParentGuid
    } = TraverseInfo,
    relative_path = ResolvedFilePath} = Job, #{task_id := TaskId} = MasterJobArgs
) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    DirName = case maps:get(root_file_name, TraverseInfo, undefined) of
        undefined -> filename:basename(ResolvedFilePath);
        Name -> Name
    end,
    SessionId = user_ctx:get_session_id(UserCtx),
    SourceDirGuid = file_ctx:get_logical_guid_const(SourceDirCtx),
    {ok, CopyGuid, _} = file_copy:copy(SessionId, SourceDirGuid, TargetParentGuid, DirName,
        ?COPY_OPTIONS(TaskId)),
    NewTraverseInfo = maps:remove(root_file_name, TraverseInfo#{current_parent => CopyGuid}),
    UpdatedJob = Job#tree_traverse{traverse_info = NewTraverseInfo},
    
    tree_traverse:do_master_job(UpdatedJob, MasterJobArgs).


%% @private
-spec do_slave_job_unsafe(tree_traverse:slave_job(), id()) -> ok.
do_slave_job_unsafe(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId,
    traverse_info = #{
        current_parent := TargetParentGuid, 
        archive_doc := ArchiveDoc,
        root_path_tokens := RootPathTokens
    } = TraverseInfo,
    relative_path = ResolvedFilePath}, TaskId
) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    SessionId = user_ctx:get_session_id(UserCtx),
    
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileName = case maps:get(root_file_name, TraverseInfo, undefined) of
        undefined -> filename:basename(ResolvedFilePath);
        Name -> Name
    end,
    case file_ctx:is_symlink_const(FileCtx) of
        true ->
            recall_symlink(
                FileCtx, TargetParentGuid, RootPathTokens, FileName, ArchiveDoc, UserCtx);
        false ->
            {ok, _, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName,
                ?COPY_OPTIONS(TaskId))
    end,
    case traverse:is_job_cancelled(TaskId) of
        true -> ok;
        false -> archive_recall:report_file_finished(TaskId)
    end.


%% @private
-spec recall_symlink(file_ctx:ctx(), file_id:file_guid(), [file_meta:name()], file_meta:name(), 
    archive:doc(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
recall_symlink(FileCtx, TargetParentGuid, RootPathTokens, FileName, ArchiveDoc, UserCtx) ->
    {ok, ArchiveDataGuid} = archive:get_data_dir_guid(ArchiveDoc),
    %% @TODO VFS-8938 - handle symlink relative value
    {ok, SymlinkValue} = lfm:read_symlink(user_ctx:get_session_id(UserCtx), 
        ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx))),
    {ArchiveDataCanonicalPath, _ArchiveFileCtx} = file_ctx:get_canonical_path(
        file_ctx:new_by_guid(ArchiveDataGuid)),
    [_Sep, _SpaceId | ArchivePathTokens] = filename:split(ArchiveDataCanonicalPath),
    [SpaceIdPrefix | SymlinkValueTokens] = filename:split(SymlinkValue),
    FinalSymlinkPath = case lists:prefix(ArchivePathTokens, SymlinkValueTokens) of
        true ->
            [_DatasetName | RelativePathTokens] = SymlinkValueTokens -- ArchivePathTokens,
            filename:join([SpaceIdPrefix] ++ RootPathTokens ++ RelativePathTokens);
        _ ->
            SymlinkValue
    end,
    {ok, #file_attr{guid = Guid}} = lfm:make_symlink(
        user_ctx:get_session_id(UserCtx), ?FILE_REF(TargetParentGuid), FileName, FinalSymlinkPath),
    {ok, file_ctx:new_by_guid(Guid)}.


%% @private
-spec report_error(id(), tree_traverse:job(), term(), list()) -> ok.
report_error(TaskId, Job, Reason, Stacktrace) ->
    {FileCtx, RelativePath, ArchiveDoc} = infer_job_context(Job),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    ?error_exception("~ts", [?autoformat([TaskId, FileGuid, ArchiveId])], error, Reason, Stacktrace),
    archive_recall:report_file_failed(TaskId, FileGuid, RelativePath, Reason).


%% @private
-spec create_root_file(IsDir :: boolean(), session:id(), file_id:file_guid(), file_meta:name()) -> 
    {ok, file_id:file_guid()} | {error, term()}.
create_root_file(true, SessId, TargetParentGuid, TargetRootName) ->
    lfm:mkdir(SessId, TargetParentGuid, TargetRootName, ?DEFAULT_DIR_MODE);
create_root_file(false, SessId, TargetParentGuid, TargetRootName) -> 
    lfm:create(SessId, TargetParentGuid, TargetRootName, ?DEFAULT_FILE_MODE).


%% @private
-spec infer_job_context(tree_traverse:job()) -> 
    {file_ctx:ctx(), file_meta:path(), archive:doc()}.
infer_job_context(#tree_traverse_slave{
    file_ctx = FileCtx, 
    relative_path = RelativePath,
    traverse_info = #{archive_doc := ArchiveDoc}}
) ->
    {FileCtx, RelativePath, ArchiveDoc};
infer_job_context(#tree_traverse{
    file_ctx = FileCtx,
    relative_path = RelativePath,
    traverse_info = #{archive_doc := ArchiveDoc}}
) ->
    {FileCtx, RelativePath, ArchiveDoc}.
