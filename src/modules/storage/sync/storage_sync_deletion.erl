%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for detecting which files in the
%%% synchronized space were deleted on storage and therefore should be
%%% deleted from the Onedata file system.
%%% It uses storage_sync_links to compare lists of files on the storage
%%% with files in the database.
%%% Functions in this module are called from master and slave jobs
%%% executed by storage_sync_traverse pool.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_deletion).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

% API
-export([do_master_job/2, do_slave_job/2, get_master_job/1, delete_file_and_update_counters/3]).

-type master_job() :: storage_sync_traverse:master_job().
-type slave_job() :: storage_sync_traverse:slave_job().
-type file_meta_children() :: [#child_link_uuid{}].
-type sync_links_children() :: [{storage_sync_links:link_name(), storage_sync_links:link_target()}].

-define(BATCH_SIZE, application:get_env(?APP_NAME, storage_sync_deletion_batch_size, 1000)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_master_job(master_job()) -> master_job().
get_master_job(Job = #storage_traverse_master{info = Info}) ->
    Job#storage_traverse_master{
        info = Info#{
            detect_deletions => true,
            sync_links_token => #link_token{},
            sync_links_children => [],
            file_meta_token => #link_token{},
            file_meta_children => []
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Performs master job responsible for detecting which files in the
%% synchronized space were deleted on storage and therefore should be
%% deleted from the Onedata file system.
%% It compares list of children of directory associated with StorageFileCtx,
%% acquired from storage_sync_links, with list of children of the directory
%% acquired from file_meta links.
%% The lists are sorted in the same order so it is possible to compare them in
%% linear time.
%% This job is executed by storage_sync_traverse pool.
%% Files that are missing on the storage_sync_links list are scheduled to be
%% deleted in slave jobs.
%% NOTE!!!
%% On storages traversed using ?TREE_ITERATOR (posix storages), only direct children are compared.
%% On storages traverse using ?FLAT_ITERATOR (object storages), whole file structure is compared.
%% Traversing whole file structure (on object storages) is performed
%% by scheduling master jobs for directories (virtual directories as they do not exist on storage but exist in
%% the Onedata file system)
%% NOTE!!!
%% Object storage must have ?CANONICAL_PATH_TYPE so the mechanism can understand the structure of files on
%% the storage.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(Job = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = #{
        file_ctx := FileCtx,
        sync_links_token := SLToken,
        sync_links_children := SLChildren,
        file_meta_token := FMToken,
        file_meta_children := FMChildren,
        iterator_type := IteratorType
}}, _Args) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    Result = try
        case refill_file_meta_children(FMChildren, FileCtx, FMToken) of
            {error, not_found} ->
                {ok, #{}};
            {[], _NewFMToken} ->
                {ok, #{finish_callback => finish_callback(StorageFileCtx)}};
            {FMChildren2, FMToken2} ->
                case refill_sync_links_children(SLChildren, StorageFileCtx, SLToken) of
                    {error, not_found} ->
                        {ok, #{}};
                    {SLChildren2, SLToken2} ->
                        {MasterJobs, SlaveJobs} = generate_deletion_jobs(Job, SLChildren2, SLToken2, FMChildren2, FMToken2),
                        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, length(SlaveJobs) + length(MasterJobs)),
                        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId, length(MasterJobs)),
                        {ok, #{
                            slave_jobs => SlaveJobs,
                            async_master_jobs => MasterJobs,
                            finish_callback => finish_callback(StorageFileCtx)
                        }}
                end
        end
    catch
        throw:?ENOENT ->
            {ok, #{}}
    end,
    case IteratorType of
        ?FLAT_ITERATOR ->
            % with ?FLAT_ITERATOR, deletion master_job for root triggers traverse of whole storage (which is
            % compared with whole space file system) therefore we cannot delete whole storage_sync_links
            % tree now (see storage_sync_links.erl for more details)
            ok;
        ?TREE_ITERATOR ->
            % with ?TREE_ITERATOR each directory is processed separately (separate deletion master_jobs)
            % so we can safely delete its links tree
            storage_sync_links:delete_recursive(StorageFileId, StorageId)
    end,
    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId),
    Result.

%%--------------------------------------------------------------------
%% @doc
%% Performs job responsible for deleting file, which has been deleted on
%% synced storage from the Onedata file system.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(slave_job(), traverse:id()) -> ok.
do_slave_job(#storage_traverse_slave{info = #{file_ctx := FileCtx, storage_id := StorageId}}, _Task) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    maybe_delete_file_and_update_counters(FileCtx, SpaceId, StorageId).

%%===================================================================
%% Internal functions
%%===================================================================

-spec refill_sync_links_children(sync_links_children(), storage_file_ctx:ctx(),
    datastore_links_iter:token()) -> {sync_links_children(), datastore_links_iter:token()} | {error, term()}.
refill_sync_links_children(CurrentChildren, StorageFileCtx, Token) ->
    case length(CurrentChildren) < ?BATCH_SIZE of
        true ->
            RootStorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            ToFetch = ?BATCH_SIZE - length(CurrentChildren),
            case storage_sync_links:list(RootStorageFileId, StorageId, Token, ToFetch) of
                {{ok, NewChildren}, NewToken} ->
                    {CurrentChildren ++ NewChildren, NewToken};
                Error = {error, _} ->
                    Error
            end;
        false ->
            {CurrentChildren, Token}
    end.

-spec refill_file_meta_children(file_meta_children(), file_ctx:ctx(), datastore_links_iter:token()) ->
    {file_meta_children(), datastore_links_iter:token()} | {error, term()}.
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

-spec generate_deletion_jobs(master_job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token()) -> {[master_job()], [slave_job()]}.
generate_deletion_jobs(Job, SLChildren, SLToken, FMChildren, FMToken) ->
    generate_deletion_jobs(Job, SLChildren, SLToken, FMChildren, FMToken, [], []).

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
-spec generate_deletion_jobs(master_job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token(), [master_job()], [slave_job()]) -> {[master_job()], [slave_job()]}.
generate_deletion_jobs(_Job, _SLChildren, _SLFinished, [], #link_token{is_last = true}, MasterJobs, SlaveJobs) ->
    % there are no more children in file_meta links, we can finish the job;
    {MasterJobs, SlaveJobs};
generate_deletion_jobs(Job, SLChildren, SLToken, [], FMToken = #link_token{is_last = false}, MasterJobs, SlaveJobs) ->
    % sync_links must be processed after refilling file_meta children list
    NextBatchJob = next_batch_master_job(Job, SLChildren, SLToken, [], FMToken),
    {[NextBatchJob | MasterJobs], SlaveJobs};
generate_deletion_jobs(Job, [], #link_token{is_last = true}, FMChildren, #link_token{is_last = true}, MasterJobs, SlaveJobs) ->
    % there are no more children in sync links and in file_meta (except those in FMChildren)
    % all left file_meta children (those in FMChildren) must be deleted
    SlaveJobs2 = lists:foldl(fun(#child_link_uuid{uuid = ChildUuid}, AccIn) ->
        % order of slave jobs doesn't matter as they will be processed in parallel
        [new_slave_job(Job, ChildUuid) | AccIn]
    end, SlaveJobs, FMChildren),
    {MasterJobs, SlaveJobs2};
generate_deletion_jobs(Job, [], SLToken = #link_token{is_last = true}, FMChildren, FMToken, MasterJobs, SlaveJobs) ->
    % there are no more children in sync links
    % all left file_meta children must be deleted
    SlaveJobs2 = lists:foldl(fun(#child_link_uuid{uuid = ChildUuid}, AccIn) ->
        % order of slave jobs doesn't matter as they will be processed in parallel
        [new_slave_job(Job, ChildUuid) | AccIn]
    end, SlaveJobs, FMChildren),
    % we must schedule next batch to refill file_meta children
    NextBatchJob = next_batch_master_job(Job, [], SLToken, [], FMToken),
    {[NextBatchJob | MasterJobs], SlaveJobs2};
generate_deletion_jobs(Job, [], SLToken, FMChildren, FMToken, MasterJobs, SlaveJobs) ->
    % all left file_meta children must be processed after refilling sl children
    NextBatchJob = next_batch_master_job(Job, [], SLToken, FMChildren, FMToken),
    {[NextBatchJob | MasterJobs], SlaveJobs};
generate_deletion_jobs(Job = #storage_traverse_master{info = #{iterator_type := ?TREE_ITERATOR}},
    [{Name, _} | RestSLChildren], SLToken, [#child_link_uuid{name = Name} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on storage iterated using ?TREE_ITERATOR (block storage) we process only direct children of a directory,
    % we do not go deeper in the files' structure as separate deletion_jobs will be scheduled for subdirectories
    generate_deletion_jobs(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, SlaveJobs);
generate_deletion_jobs(Job = #storage_traverse_master{info = #{iterator_type := ?FLAT_ITERATOR}},
    [{Name, undefined} | RestSLChildren], SLToken, [#child_link_uuid{name = Name} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on storage iterated using ?FLAT_ITERATOR (object storage) if child link's target is undefined it
    % means that it's a regular file's link
    generate_deletion_jobs(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, SlaveJobs);
generate_deletion_jobs(Job = #storage_traverse_master{info = #{iterator_type := ?FLAT_ITERATOR}},
    [{Name, _} | RestSLChildren], SLToken, [#child_link_uuid{name = Name, uuid = Uuid} | RestFMChildren], FMToken,
    MasterJobs, SlaveJobs
) ->
    % file with name Name is on both lists therefore we cannot delete it
    % on storage iterated using ?FLAT_ITERATOR (object storage) if child link's target is NOT undefined
    % it means that it's a directory's link therefore we schedule master job for this directory,
    % as with ?FLAT_ITERATOR deletion_jobs for root traverses whole file system
    % for more info read the function's doc
    ChildMasterJob = new_child_master_job(Job, Name, Uuid),
    generate_deletion_jobs(Job, RestSLChildren, SLToken, RestFMChildren, FMToken, [ChildMasterJob | MasterJobs], SlaveJobs);
generate_deletion_jobs(Job, AllSLChildren = [{SLName, _} | _], SLToken,
    [#child_link_uuid{name = FMName, uuid = ChildUuid} | RestFMChildren], FMToken, MasterJobs, SlaveJobs)
    when SLName > FMName ->
    % FMName is missing on the sync links list so it probably was deleted on storage
    SlaveJob = new_slave_job(Job, ChildUuid),
    generate_deletion_jobs(Job, AllSLChildren, SLToken, RestFMChildren, FMToken, MasterJobs, [SlaveJob | SlaveJobs]);
generate_deletion_jobs(Job, [{SLName, _} | RestSLChildren], SLToken,
    AllFMChildren = [#child_link_uuid{name = FMName} | _], FMToken, MasterJobs, SlaveJobs)
    when SLName < FMName ->
    % SLName is missing on the file_meta list, we can ignore it, storage_sync will synchronise this file
    generate_deletion_jobs(Job, RestSLChildren, SLToken, AllFMChildren, FMToken, MasterJobs, SlaveJobs).


-spec new_slave_job(master_job(), file_meta:uuid()) -> slave_job().
new_slave_job(#storage_traverse_master{storage_file_ctx = StorageFileCtx}, ChildUuid) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    #storage_traverse_slave{
        info = #{
            detect_deletions => true,
            file_ctx => file_ctx:new_by_guid(file_id:pack_guid(ChildUuid, SpaceId)),
            storage_id => StorageId
        }}.

-spec next_batch_master_job(master_job(), sync_links_children(), datastore_links_iter:token(),
    file_meta_children(), datastore_links_iter:token()) -> master_job().
next_batch_master_job(Job = #storage_traverse_master{info = Info}, SLChildrenToProcess, SLToken, FMChildrenToProcess, FMToken) ->
    Job#storage_traverse_master{
        info = Info#{
            sync_links_token => SLToken,
            sync_links_children => SLChildrenToProcess,
            file_meta_token => FMToken,
            file_meta_children => FMChildrenToProcess
    }}.

-spec new_child_master_job(master_job(), file_meta:name(), file_meta:uuid()) -> master_job().
new_child_master_job(Job = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = #{iterator_type := IteratorType}
}, ChildName, ChildUuid) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    ChildMasterJob = Job#storage_traverse_master{
        storage_file_ctx = storage_file_ctx:get_child_ctx_const(StorageFileCtx, ChildName),
        info = #{
            iterator_type => IteratorType,
            file_ctx => file_ctx:new_by_guid(file_id:pack_guid(ChildUuid, SpaceId))}
    },
    get_master_job(ChildMasterJob).


%%-------------------------------------------------------------------
%% @doc
%% This functions checks whether file is a directory or a regular file
%% and delegates decision about deleting or not deleting file to
%% suitable functions.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
maybe_delete_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    try
        {SDHandle, FileCtx2} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx),
        {IsStorageFileCreated, FileCtx3} = file_ctx:is_storage_file_created(FileCtx2),
        case IsStorageFileCreated and not storage_driver:exists(SDHandle) of
            true ->
                % file is still missing on storage we can delete it from db
               delete_file_and_update_counters(FileCtx3, SpaceId, StorageId);
            false ->
                storage_sync_monitoring:mark_processed_file(SpaceId, StorageId)
        end
    catch
        throw:?ENOENT ->
            storage_sync_monitoring:mark_processed_file(SpaceId, StorageId),
            ok;
        Error:Reason ->
            ?error_stacktrace("~p:maybe_delete_file_and_update_counters failed due to ~p",
                [?MODULE, {Error, Reason}]),
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId)
    end.

-spec delete_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            delete_dir_recursive_and_update_counters(FileCtx2, SpaceId, StorageId);
        {false, FileCtx2} ->
            delete_regular_file_and_update_counters(FileCtx2, SpaceId, StorageId)
    end.

%%-------------------------------------------------------------------
%% @doc
%% This function deletes directory recursively it and updates sync counters.
%% @end
%%-------------------------------------------------------------------
-spec delete_dir_recursive_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_dir_recursive_and_update_counters(FileCtx, SpaceId, StorageId) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    delete_dir_recursive(FileCtx3, SpaceId, StorageId),
    storage_sync_logger:log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @doc
%% This function deletes regular file and updates sync counters.
%% @end
%%-------------------------------------------------------------------
-spec delete_regular_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_regular_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    delete_file(FileCtx3),
    storage_sync_logger:log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Deletes directory that has been deleted on storage from the system.
%% It deletes directory recursively.
%% @end
%%-------------------------------------------------------------------
-spec delete_dir_recursive(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_dir_recursive(FileCtx, SpaceId, StorageId) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    {ok, FileCtx2} = delete_children(FileCtx, RootUserCtx, 0, ChunkSize, SpaceId, StorageId),
    delete_file(FileCtx2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively deletes children of directory.
%% @end
%%-------------------------------------------------------------------
-spec delete_children(file_ctx:ctx(), user_ctx:ctx(), non_neg_integer(), non_neg_integer(),
    od_space:id(), storage:id()) -> {ok, file_ctx:ctx()}.
delete_children(FileCtx, UserCtx, Offset, ChunkSize, SpaceId, StorageId) ->
    try
        {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, ChunkSize),
        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, length(ChildrenCtxs)),
        lists:foreach(fun(ChildCtx) ->
            delete_file_and_update_counters(ChildCtx, SpaceId, StorageId)
        end, ChildrenCtxs),
        case length(ChildrenCtxs) < ChunkSize of
            true ->
                {ok, FileCtx2};
            false ->
                delete_children(FileCtx2, UserCtx, Offset + ChunkSize, ChunkSize, SpaceId, StorageId)
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
    try
        fslogic_delete:handle_file_deleted_on_synced_storage(FileCtx)
    catch
        throw:?ENOENT ->
            ok
    end.


-spec finish_callback(storage_file_ctx:ctx()) -> function().
finish_callback(StorageFileCtx) ->
    % todo sprawdzic czy to jest wolane dla wirtualnych katalogow, jak tak to trzeba temu zaradzic !!!!
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    MTime = try
        {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
        STMtime
    catch
        throw:?ENOENT ->
            undefined
    end,
    ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, MTime)
    end).