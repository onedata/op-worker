%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour. 
%%% It is responsible for verifying files in given archive by comparing 
%%% checksums calculated during archive creation with existing ones.
%%% Traverse task id is the same as id of the archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_verification_traverse).
-author("Michal Stanisz").

-behaviour(traverse_behaviour).

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/1, cancel/1]).
-export([block_archive_modification/1, unblock_archive_modification/1]).

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

-export([
    do_slave_job_unsafe/2,
    do_dir_master_job_unsafe/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: archive:id().

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


-spec start(archive:doc()) -> ok | {error, term()}.
start(ArchiveDoc) ->
    {ok, TaskId} = archive:get_id(ArchiveDoc),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, DataFileGuid} = archive:get_data_dir_guid(ArchiveDoc),
    case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
        ok ->
            UserId = user_ctx:get_user_id(UserCtx),
            Options = #{
                task_id => TaskId,
                children_master_jobs_mode => async,
                initial_relative_path => filename:join([<<"/">>, <<"archive_", TaskId/binary>>])
            },
            {ok, TaskId} = tree_traverse:run(
                ?POOL_NAME, file_ctx:new_by_guid(DataFileGuid), UserId, Options),
            ok;
        {error, _} = Error ->
            Error
    end.

-spec cancel(id()) -> ok | {error, term()}.
cancel(TaskId) ->
    tree_traverse:cancel(?POOL_NAME, TaskId).


-spec block_archive_modification(archive:doc()) -> ok.
block_archive_modification(#document{value = #archive{root_dir_guid = RootDirGuid}}) -> 
    FlagsToSet = ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION),
    % as protection flags work only with datasets, create a dummy dataset on archive root dir
    ?extract_ok(dataset_api:establish(file_ctx:new_by_guid(RootDirGuid), FlagsToSet, internal)).


-spec unblock_archive_modification(archive:doc()) -> ok.
unblock_archive_modification(#document{value = #archive{root_dir_guid = RootDirGuid}}) -> 
    ?extract_ok(dataset_api:remove(file_id:guid_to_uuid(RootDirGuid))).


%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("Archive verification job ~p started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    archive:mark_preserved(TaskId),
    ?debug("Archive verification job ~p finished", [TaskId]).


-spec task_canceled(id(), tree_traverse:pool()) -> ok.
task_canceled(TaskId, _Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    archive:mark_cancelled(TaskId), % does nothing if archive already marked as verification failed
    ?debug("Archive verification job ~p cancelled", [TaskId]).


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
        handle_verification_error(TaskId, Job),
        ?error_stacktrace("Unexpected error during verification of archive ~p:~n~p", 
            [TaskId, Reason], Stacktrace),
        {ok, #{}} % unexpected error - no jobs can be created
    end,
    archive_traverses_common:do_master_job(?MODULE, InitialJob, MasterJobArgs, ErrorHandler).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(InitialJob, TaskId) ->
    ErrorHandler = fun(Job, Reason, Stacktrace) ->
        handle_verification_error(TaskId, Job),
        ?error_stacktrace("Unexpected error during verification of archive ~p:~n~p", 
            [TaskId, Reason], Stacktrace)
    end,
    archive_traverses_common:execute_unsafe_job(
        ?MODULE, do_slave_job_unsafe, [TaskId], InitialJob, ErrorHandler).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_slave_job_unsafe(tree_traverse:slave_job(), id()) -> ok.
do_slave_job_unsafe(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId
} = Job, TaskId) ->
    case file_ctx:is_symlink_const(FileCtx) of
        true ->
            ok;
        false ->
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            case archivisation_checksum:has_file_changed(FileCtx, FileCtx, UserCtx) of
                false -> 
                    ok;
                true ->
                    ?warning("Invalid checksum for file ~p in archive ~p",
                        [file_ctx:get_logical_guid_const(FileCtx), TaskId]),
                    handle_verification_error(TaskId, Job)
            end
    end.


-spec do_dir_master_job_unsafe(tree_traverse:master_job(), traverse:master_job_extended_args()) -> 
    {ok, traverse:master_job_map()}.
do_dir_master_job_unsafe(#tree_traverse{
    user_id = UserId, 
    file_ctx = FileCtx
} = Job, #{task_id := TaskId} = MasterJobArgs) ->
    NewJobsPreprocessor = fun(SlaveJobs, MasterJobs, ListingToken, _SubtreeProcessingStatus) ->
        DirUuid = file_ctx:get_logical_uuid_const(FileCtx),
        ChildrenCount = length(SlaveJobs) + length(MasterJobs),
        ok = archive_traverses_common:update_children_count(
            ?POOL_NAME, TaskId, DirUuid, ChildrenCount),
        case file_listing:is_finished(ListingToken) of
            false ->
                ok;
            true ->
                TotalChildrenCount = archive_traverses_common:take_children_count(
                    ?POOL_NAME, TaskId, DirUuid),
                {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
                case archivisation_checksum:has_dir_changed(
                    FileCtx, FileCtx, UserCtx, TotalChildrenCount)
                of
                    false -> 
                        ok;
                    true ->
                        ?warning("Invalid checksum for dir ~p in archive ~p", 
                            [file_ctx:get_logical_guid_const(FileCtx), TaskId]),
                        handle_verification_error(TaskId, Job)
                end
        end
    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, NewJobsPreprocessor).


-spec handle_verification_error(id(), tree_traverse:job()) -> ok.
handle_verification_error(TaskId, Job) ->
    {FileCtx, FilePath} = job_to_error_info(Job),
    {FileType, FileCtx2} = file_ctx:get_type(FileCtx, effective),
    archivisation_audit_log:report_file_verification_failed(
        TaskId, file_ctx:get_logical_guid_const(FileCtx2), FilePath, FileType),
    archive:mark_verification_failed(TaskId),
    cancel(TaskId).


-spec job_to_error_info(tree_traverse:job()) -> {file_ctx:ctx(), file_meta:path()}.
job_to_error_info(#tree_traverse{file_ctx = FileCtx, relative_path = Path}) ->
    {FileCtx, Path};
job_to_error_info(#tree_traverse_slave{file_ctx = FileCtx, relative_path = Path}) ->
    {FileCtx, Path}.
