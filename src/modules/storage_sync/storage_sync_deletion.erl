%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing full_update for
%%% storage_update strategy.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_deletion). % todo rename this module ss_deletion_traverse??? albo cos innego z traverse?
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%% Pool API
-export([init_pool/0, stop_pool/0, run/3, run/4, run/5]).
%% Files API
-export([delete_imported_file/2]).
%% Traverse callbacks
-export([
    do_master_job/2, do_slave_job/2,
    get_job/1, update_job_progress/5,
    task_started/1, task_finished/1
]).

% master job
-record(storage_sync_deletion, {
    storage_file_ctx :: storage_file_ctx:ctx(),
    file_ctx :: file_ctx:ctx(),
    mode :: mode(),
    file_meta_token = #link_token{} :: datastore_links_iter:token(),
    file_meta_children = [] :: file_meta_children(),
    sync_links_token = #link_token{} :: datastore_links_iter:token(),
    sync_links_children = [] :: sync_links_children(),
    update_sync_counters = true :: boolean()
}).

-record(slave_job, {
    file_ctx :: file_ctx:ctx(),
    storage_id :: storage:id(),
    update_sync_counters = true :: boolean()
}).


-type job() :: #storage_sync_deletion{}.
-type slave_job() :: #slave_job{}.
-type mode() :: object | block.
-type file_meta_children() :: [#child_link_uuid{}].
-type sync_links_children() :: [{storage_sync_links:link_name(), storage_sync_links:link_target()}].

-define(BATCH_SIZE, application:get_env(?APP_NAME, storage_sync_deletion_batch_size, 1000)).

% TODO W JAKI SPOSOB OGARNAC< ZEBY SYNC CZEKAL AZ SKONCZY SIE TO ZADANIE???
% todo ogarnąć counteyr żeby dobrze działały !!!!

-define(POOL, atom_to_binary(?MODULE, utf8)).
-define(MASTER_JOBS_LIMIT,
    application:get_env(?APP_NAME, storage_sync_deletion_master_jobs_limit, 10)).
-define(SLAVE_JOBS_LIMIT,
    application:get_env(?APP_NAME, storage_sync_deletion_slave_workers_limit, 10)).
-define(PARALLEL_TASKS_LIMIT,
    application:get_env(?APP_NAME, storage_sync_deletion_parallel_orders_limit, 10)).


%%%===================================================================
%%% Pool API
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    traverse:init_pool(?POOL, ?MASTER_JOBS_LIMIT, ?SLAVE_JOBS_LIMIT, ?PARALLEL_TASKS_LIMIT,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec stop_pool() -> ok.
stop_pool() ->
    traverse:stop_pool(?POOL).

-spec run(storage_file_ctx:ctx(), file_ctx:ctx(), mode()) -> ok.
run(StorageFileCtx, FileCtx, Mode) ->
    run(undefined, StorageFileCtx, FileCtx, Mode).

-spec run(traverse:id() | undefined, storage_file_ctx:ctx(), file_ctx:ctx(), mode()) -> ok.
run(TaskId, StorageFileCtx, FileCtx, Mode) ->
    run(TaskId, StorageFileCtx, FileCtx, Mode, true).

-spec run(traverse:id() | undefined, storage_file_ctx:ctx(), file_ctx:ctx(), mode(), boolean()) -> ok.
run(TaskId, StorageFileCtx, FileCtx, Mode, UpdateSyncCounters) ->
    % todo czy TaskId powinno byc jakos powiązane z task_id skanu synca?
    DefinedTaskId = case TaskId =:= undefined of
        true -> datastore_utils:gen_key();
        false -> TaskId
    end,
    Job = #storage_sync_deletion{
        storage_file_ctx = StorageFileCtx,
        file_ctx = FileCtx,
        mode = Mode,
        update_sync_counters = UpdateSyncCounters
    },
    traverse:run(?POOL, DefinedTaskId, Job).

%%===================================================================
%% Files API
%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Remove files that had been earlier imported.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_file(file_meta:name(), file_ctx:ctx()) -> ok.
delete_imported_file(ChildName, ParentCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {FileCtx, _} = file_ctx:get_child(ParentCtx, ChildName, RootUserCtx),
        delete_file(FileCtx)
    catch
        throw:?ENOENT ->
            ok
    end.

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec do_master_job(job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(Job = #storage_sync_deletion{
    storage_file_ctx = StorageFileCtx,
    file_ctx = FileCtx,
    sync_links_token = SLToken,
    sync_links_children = SLChildren,
    file_meta_token = FMToken,
    file_meta_children = FMChildren
}, _Args) ->
    case refill_file_meta_children(FMChildren, FileCtx, FMToken) of
        {error, not_found} ->
            {ok, #{}};
        {[], _NewFMToken} ->
            {ok, #{}};
        {FMChildren2, FMToken2} ->
            {SLChildren2, SLToken2} = refill_sync_links_children(SLChildren, StorageFileCtx, SLToken),
            {MasterJobs, SlaveJobs} = compare_children_lists(Job, SLChildren2, SLToken2, FMChildren2, FMToken2),
            {ok, #{
                slave_jobs => SlaveJobs,
                async_master_jobs => MasterJobs
            }}
    end.

-spec do_slave_job(slave_job(), traverse:id()) -> ok.
do_slave_job(#slave_job{
    file_ctx = FileCtx,
    storage_id = StorageId,
    update_sync_counters = UpdateSyncCounters
}, _Task) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    maybe_delete_imported_file_and_update_counters(FileCtx, SpaceId, StorageId, UpdateSyncCounters).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(_ID, _Job, _Pool, _TaskID, _Status) ->
    % TODO persist deletion traverse jobs
    ID2 = list_to_binary(ref_to_list(make_ref())),
    {ok, ID2}.

-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(_DocOrID) ->
    % TODO persist deletion traverse jobs
    ok.

-spec task_started(traverse:id()) -> ok.
task_started(TaskId) ->
    ?debug("Storage sync deletion traverse ~p started", [TaskId]).

-spec task_finished(traverse:id()) -> ok.
task_finished(TaskId) ->
    % todo debug
    % todo notify sync process !!!
    % todo remove links tree !!!
    ?alert("Storage sync deletion ~p finished", [traverse_task:get(?POOL, TaskId)]).

%%===================================================================
%% Internal functions
%%===================================================================

-spec refill_sync_links_children(sync_links_children(), storage_file_ctx:ctx(),
    datastore_links_iter:token()) -> {sync_links_children(), datastore_links_iter:token()}.
refill_sync_links_children(CurrentChildren, StorageFileCtx, Token) ->
    case length(CurrentChildren) < ?BATCH_SIZE of
        true ->
            RootStorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            ToFetch = ?BATCH_SIZE - length(CurrentChildren),
            {{ok, NewChildren}, NewToken} = storage_sync_links:list(RootStorageFileId, SpaceId, StorageId, Token, ToFetch),
            {CurrentChildren ++ NewChildren, NewToken};
        false ->
            {CurrentChildren, Token}
    end.

-spec refill_file_meta_children(file_meta_children(), file_ctx:ctx(), datastore_links_iter:token()) ->
    {file_meta_children(), datastore_links_iter:token()}.
refill_file_meta_children(CurrentChildren, FileCtx, Token) ->
    case length(CurrentChildren) < ?BATCH_SIZE of
        true ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            ToFetch = ?BATCH_SIZE - length(CurrentChildren),
            case file_meta:list_children({uuid, FileUuid}, 0, ToFetch, Token) of
                {ok, NewChildren, #{token := NewToken}} ->
                    {CurrentChildren ++ NewChildren, NewToken};
                Error = {error, _} ->
                    Error
            end;
        false ->
            {CurrentChildren, Token}
    end.

-spec compare_children_lists(job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token()) -> {[job()] | [slave_job()]}.
compare_children_lists(Job, SLChildren, SLToken, FMChildren, FMToken) ->
    compare_children_lists(Job, SLChildren, SLToken, FMChildren, FMToken, [], []).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for comparing two lists:
%%     * list of file_meta children
%%     * list of storage children, acquired from storage_sync_links
%% Both lists are sorted in the same order which allows to compare them
%% in linear time.
%% Function looks for files which are missing in the storage list and
%% are still present in the file_meta list.
%% Such files have potentially been deleted from storage and must be
%% checked whether they might be deleted from the system.
%% These checks are performed in SlaveJobs returned from this function.
%% The function returns also MasterJob for next batch if one of compared
%% lists is empty.
%% NOTE!!!
%% On object storages detecting deletions is performed by a single traverse
%% over whole file system to avoid efficiency issues associated with
%% listing files in a canonical-like way on storage.
%% Files are listed using listobjects function which returns a flat structure.
%% Basing on the files absolute paths, we created storage_sync_links trees
%% which are then compared with file_meta links by this function.
%% In such case storage_sync_links are created for all files from the storage
%% and therefore we have to traverse whole structure, not only direct children.
%% @end
%%-------------------------------------------------------------------
-spec compare_children_lists(job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token(), [job()], [slave_job()]) -> {[job()] | [slave_job()]}.
compare_children_lists(_Job, _SLChildren, _SLFinished, [], #link_token{is_last = true}, MasterJobs, SlaveJobs) ->
    % there are no more children in file_meta links, we can finish the job;
    {MasterJobs, SlaveJobs};
compare_children_lists(Job, SLChildren, SLToken, [], FMToken = #link_token{is_last = false}, MasterJobs, SlaveJobs) ->
    % sync_links must be processed after refilling file_meta children list
    NextBatchJob = next_batch_master_job(Job, SLChildren, SLToken, [], FMToken),
    {[NextBatchJob| MasterJobs], SlaveJobs};
compare_children_lists(Job, [], #link_token{is_last = true}, FMChildren, #link_token{is_last = true}, MasterJobs, SlaveJobs) ->
    % there are no more children in sync links and in file_meta (except those in FMChildren)
    % all left file_meta children (those in FMChildren) must be deleted
    SlaveJobs2 = lists:foldl(fun(#child_link_uuid{uuid = ChildUuid}, AccIn) ->
        % order of slave jobs doesn't matter as they will be processed in parallel
        [new_slave_job(Job, ChildUuid) | AccIn]
    end, SlaveJobs, FMChildren),
    {MasterJobs, SlaveJobs2};
compare_children_lists(Job, [], SLToken = #link_token{is_last = true}, FMChildren, FMToken, MasterJobs, SlaveJobs) ->
    % there are no more children in sync links
    % all left file_meta children must be deleted
    SlaveJobs2 = lists:foldl(fun(#child_link_uuid{uuid = ChildUuid}, AccIn) ->
        % order of slave jobs doesn't matter as they will be processed in parallel
        [new_slave_job(Job, ChildUuid) | AccIn]
    end, SlaveJobs, FMChildren),
    % we must schedule next batch to refill file_meta children
    NextBatchJob = next_batch_master_job(Job, [], SLToken, [], FMToken),
    {[NextBatchJob | MasterJobs], SlaveJobs2};
compare_children_lists(Job, [], SLToken, FMChildren, FMToken, MasterJobs, SlaveJobs) ->
    % all left file_meta children must be processed after refilling sl children
    NextBatchJob = next_batch_master_job(Job, [], SLToken, FMChildren, FMToken),
    {[NextBatchJob | MasterJobs], SlaveJobs};
compare_children_lists(Job = #storage_sync_deletion{mode = block},
    [{Name, _} | RestSLChildren], SLToken, [#child_link_uuid{name = Name} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on block storage we process only direct children of a directory,
    % we do not go deeper in the files' structure
    compare_children_lists(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, SlaveJobs);
compare_children_lists(Job = #storage_sync_deletion{mode = object},
    [{Name, undefined} | RestSLChildren], SLToken, [#child_link_uuid{name = Name} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on object storage if child link's target is undefined it means that it's a regular file's link
    compare_children_lists(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, SlaveJobs);
compare_children_lists(Job = #storage_sync_deletion{mode = object},
    [{Name, _} | RestSLChildren], SLToken, [#child_link_uuid{name = Name, uuid = Uuid} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on object storage if child link's target is NOT undefined it means that it's a directory's link
    % therefore we schedule master job for this directory, as on object storage we traverse whole file system
    % for more info read the function's doc
    ChildMasterJob = new_child_master_job(Job, Name, Uuid),
    compare_children_lists(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, [ChildMasterJob | MasterJobs], SlaveJobs);
compare_children_lists(Job, AllSLChildren = [{SLName, _} | _], SLToken,
    [#child_link_uuid{name = FMName, uuid = ChildUuid} | RestFMChildren], FMToken, MasterJobs, SlaveJobs)
    when SLName > FMName ->
    % FMName is missing on the sync links list so it probably was deleted on storage
    SlaveJob = new_slave_job(Job, ChildUuid),
    compare_children_lists(Job, AllSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, [SlaveJob | SlaveJobs]);
compare_children_lists(Job, [{SLName, _} | RestSLChildren], SLToken,
    AllFMChildren = [#child_link_uuid{name = FMName} | _], FMToken, MasterJobs, SlaveJobs)
    when SLName < FMName ->
    % SLName is missing on the file_meta list, we can ignore it, storage_sync will synchronise this file
    compare_children_lists(Job, RestSLChildren, SLToken, AllFMChildren, FMToken, MasterJobs, SlaveJobs).


-spec new_slave_job(job(), file_meta:uuid()) -> slave_job().
new_slave_job(#storage_sync_deletion{
    storage_file_ctx = StorageFileCtx,
    update_sync_counters = UpdateSyncCounters
}, ChildUuid) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    #slave_job{
        file_ctx = file_ctx:new_by_guid(file_id:pack_guid(ChildUuid, SpaceId)),
        storage_id = StorageId,
        update_sync_counters = UpdateSyncCounters
    }.

-spec next_batch_master_job(job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token()) -> job().
next_batch_master_job(SSD, SLChildrenToProcess, SLToken, FMChildrenToProcess, FMToken) ->
    SSD#storage_sync_deletion{
        sync_links_token = SLToken,
        sync_links_children = SLChildrenToProcess,
        file_meta_token = FMToken,
        file_meta_children = FMChildrenToProcess
    }.

-spec new_child_master_job(job(), file_meta:name(), file_meta:uuid()) -> job().
new_child_master_job(#storage_sync_deletion{
    mode = Mode,
    storage_file_ctx = StorageFileCtx,
    update_sync_counters = UpdateSyncCounters
}, ChildName, ChildUuid) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    #storage_sync_deletion{
        storage_file_ctx = storage_file_ctx:get_child_ctx_const(StorageFileCtx, ChildName),
        file_ctx = file_ctx:new_by_guid(file_id:pack_guid(ChildUuid, SpaceId)),
        mode = Mode,
        update_sync_counters = UpdateSyncCounters
    }.
%%-------------------------------------------------------------------
%% @doc
%% This functions checks whether file is a directory or a regular file
%% and delegates decision about deleting or not deleting file to
%% suitable functions.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_imported_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id(),
    boolean()) -> ok.
maybe_delete_imported_file_and_update_counters(FileCtx, SpaceId, StorageId, UpdateSyncCounters) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    try
        {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
        case IsDir of
            true ->
                maybe_delete_dir_and_update_counters(
                    FileCtx2, SpaceId, StorageId, UpdateSyncCounters);
            false ->
                maybe_delete_regular_file_and_update_counters(
                    FileCtx2, SpaceId, StorageId, UpdateSyncCounters)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("maybe_delete_imported_file_and_update_counters failed due to ~p",
                [{Error, Reason}]),
                maybe_mark_failed_file(SpaceId, StorageId, UpdateSyncCounters)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given directory can be deleted by sync.
%% If true, this function deletes it and updates sync counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_dir_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id(), boolean()) -> ok.
maybe_delete_dir_and_update_counters(FileCtx, SpaceId, StorageId, UpdateSyncCounters) ->
    {DirLocation, FileCtx2} = file_ctx:get_dir_location_doc(FileCtx),
    case dir_location:is_storage_file_created(DirLocation) of
        true ->
            delete_dir(FileCtx2, SpaceId, StorageId, UpdateSyncCounters),
            {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx2),
            storage_sync_logger:log_deletion(StorageFileId, SpaceId),
            maybe_mark_deleted_file(SpaceId, StorageId, UpdateSyncCounters);
        false ->
            maybe_mark_processed_file(SpaceId, StorageId, UpdateSyncCounters)
    end.


%%-------------------------------------------------------------------
%% @doc
%% Checks whether given regular file can be deleted by sync.
%% If true, this function deletes it and updates sync counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_regular_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id(),
    boolean()) -> ok.
maybe_delete_regular_file_and_update_counters(FileCtx, SpaceId, StorageId, UpdateSyncCounters) ->
    {FileLocation, FileCtx2} = file_ctx:get_local_file_location_doc(FileCtx, false),
    case file_location:is_storage_file_created(FileLocation) of
        true ->
            {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx2),
            delete_file(FileCtx2),
            storage_sync_logger:log_deletion(StorageFileId, SpaceId),
            maybe_mark_deleted_file(SpaceId, StorageId, UpdateSyncCounters),
            ok;
        false ->
            maybe_mark_processed_file(SpaceId, StorageId, UpdateSyncCounters)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Deletes directory that has been deleted on storage from the system.
%% It deleted directory recursively.
%% @end
%%-------------------------------------------------------------------
-spec delete_dir(file_ctx:ctx(), od_space:id(), storage:id(), boolean()) -> ok.
delete_dir(FileCtx, SpaceId, StorageId, UpdateSyncCounters) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    {ok, FileCtx2} = delete_children(FileCtx, RootUserCtx, 0, ChunkSize, SpaceId, StorageId, UpdateSyncCounters),
    delete_file(FileCtx2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively deletes children of directory.
%% @end
%%-------------------------------------------------------------------
-spec delete_children(file_ctx:ctx(), user_ctx:ctx(), non_neg_integer(), non_neg_integer(),
    od_space:id(), storage:id(), boolean()) -> {ok, file_ctx:ctx()}.
delete_children(FileCtx, UserCtx, Offset, ChunkSize, SpaceId, StorageId, UpdateSyncCounters) ->
    try
        {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, ChunkSize),
        maybe_increase_to_process_counter(SpaceId, StorageId, length(ChildrenCtxs), UpdateSyncCounters),
        lists:foreach(fun(ChildCtx) ->
            maybe_delete_imported_file_and_update_counters(ChildCtx, SpaceId, StorageId, UpdateSyncCounters)
        end, ChildrenCtxs),
        case length(ChildrenCtxs) < ChunkSize of
            true ->
                {ok, FileCtx2};
            false ->
                delete_children(FileCtx2, UserCtx, Offset + ChunkSize, ChunkSize, SpaceId,
                    StorageId, UpdateSyncCounters)
        end
    catch
        throw:?ENOENT ->
            {ok, FileCtx}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Deletes file that has been deleted on storage from the system.
%% It deletes both regular files and directories.
%% NOTE!!!
%% This function does not delete directory recursively.
%% Directory children must be deleted before calling this function.
%% @end
%%-------------------------------------------------------------------
-spec delete_file(file_ctx:ctx()) -> ok.
delete_file(FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        ok = fslogic_delete:remove_file(FileCtx, RootUserCtx, false, true),
        fslogic_event_emitter:emit_file_removed(FileCtx, []),
        ok = fslogic_delete:remove_file_handles(FileCtx),
        fslogic_delete:remove_auxiliary_documents(FileCtx)
    catch
        throw:?ENOENT ->
            ok
    end.


-spec maybe_mark_failed_file(od_space:id(), storage:id(), boolean()) -> ok.
maybe_mark_failed_file(SpaceId, StorageId, true) ->
    storage_sync_monitoring:mark_failed_file(SpaceId, StorageId);
maybe_mark_failed_file(_SpaceId, _StorageId, false) ->
    ok.

-spec maybe_mark_processed_file(od_space:id(), storage:id(), boolean()) -> ok.
maybe_mark_processed_file(SpaceId, StorageId, true) ->
    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId);
maybe_mark_processed_file(_SpaceId, _StorageId, false) ->
    ok.

-spec maybe_mark_deleted_file(od_space:id(), storage:id(), boolean()) -> ok.
maybe_mark_deleted_file(SpaceId, StorageId, true) ->
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId);
maybe_mark_deleted_file(_SpaceId, _StorageId, false) ->
    ok.

-spec maybe_increase_to_process_counter(od_space:id(), storage:id(), non_neg_integer(), boolean()) -> ok.
maybe_increase_to_process_counter(SpaceId, StorageId, ToProcessNum, true) ->
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcessNum);
maybe_increase_to_process_counter(_SpaceId, _StorageId, _ToProcessNum, false) ->
    ok.