%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements simple_scan strategy, used by storage_import
%%% and storage_update.
%%% It recursively traverses filesystem and add jobs for importing/updating
%%% to pool.
%%% @end
%%%-------------------------------------------------------------------
-module(simple_scan).
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").


%% API
-export([run/1, maybe_import_storage_file_and_children/1,
    maybe_import_storage_file/1, import_children/5,
    handle_already_imported_file/3, generate_jobs_for_importing_children/4,
    import_regular_subfiles/1, increase_files_to_handle_counter/3]).

%%--------------------------------------------------------------------
%% @doc
%% Implementation for 'simple_scan' strategy.
%% @end
%%--------------------------------------------------------------------
-spec run(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run(Job = #space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_file_ctx := StorageFileCtx
}}) when StorageFileCtx =/= undefined ->

    Module = storage_sync_utils:module(Job),
    storage_sync_monitoring:update_queue_length_spirals(SpaceId, -1),
    delegate(Module, maybe_import_storage_file_and_children, [Job], 1);
run(Job = #space_strategy_job{
    data = Data = #{
        parent_ctx := ParentCtx,
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId
}}) ->

    {CanonicalPath, ParentCtx2} = file_ctx:get_child_canonical_path(ParentCtx, FileName),
    StorageFileCtx = storage_file_ctx:new(CanonicalPath, SpaceId, StorageId),

    case storage_file_ctx:get_stat_buf(StorageFileCtx) of
        Error = {error, _} ->
            storage_sync_monitoring:update_queue_length_spirals(SpaceId, -1),
            {Error, []};
        {_StatBuf, StorageFileCtx2} ->
            Data2 = Data#{
                parent_ctx => ParentCtx2,
                storage_file_ctx => StorageFileCtx2
            },
            run(Job#space_strategy_job{data = Data2})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File may be space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file_and_children(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
maybe_import_storage_file_and_children(Job0 = #space_strategy_job{
    data = Data0 = #{
        storage_file_ctx := StorageFileCtx0
    }
}) ->
    {#statbuf{
        st_mtime = StorageMtime,
        st_mode = Mode
    }, StorageFileCtx1} = storage_file_ctx:get_stat_buf(StorageFileCtx0),

    Job1 = Job0#space_strategy_job{data = Data0#{storage_file_ctx => StorageFileCtx1}},
    {LocalResult, FileCtx, Job2} = maybe_import_storage_file(Job1),
    Data2 = Job2#space_strategy_job.data,
    Offset = maps:get(dir_offset, Data2, 0),
    Type = file_meta:type(Mode),

    Job3 = case Offset of
        0 ->
            Data3 = Data2#{mtime => StorageMtime},  %remember mtime to save after importing all subfiles
            Job2#space_strategy_job{data=Data3};
        _ -> Job2
    end,
    SubJobs = import_children(Job3, Type, Offset, FileCtx, ?DIR_BATCH),
    {LocalResult, SubJobs}.

%%--------------------------------------------------------------------
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File is, for sure, not space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file(space_strategy:job()) ->
    {space_strategy:job_result(), file:ctx(), space_strategy:job()} | no_return().
maybe_import_storage_file(Job = #space_strategy_job{
    strategy_type = StrategyType,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
    }}) ->

    case file_meta_exists(FileName, ParentCtx) of
        false ->
            {LocalResult, FileCtx} = import_file(StorageId, SpaceId, FileName,
                ParentCtx, StrategyType, StorageFileCtx),
            {LocalResult, FileCtx, Job};
        {true, FileCtx0, _} ->
            maybe_import_file_with_existing_metadata(Job, FileCtx0)
    end.

%%--------------------------------------------------------------------
%% @doc
%% If file is a directory, this function prepares jobs for importing
%% its children. Otherwise it does nothing.
%% @end
%%--------------------------------------------------------------------
-spec import_children(space_strategy:job(), file_meta:type(),
    Offset :: non_neg_integer(), file_ctx:ctx(), non_neg_integer()) ->
    [space_strategy:job()].
import_children(Job = #space_strategy_job{
    strategy_type = StrategyType,
    data = #{
        max_depth := MaxDepth,
        storage_file_ctx := StorageFileCtx,
        mtime := Mtime
    }},
    ?DIRECTORY_TYPE, Offset, FileCtx, BatchSize
) when MaxDepth > 0 ->

    BatchKey = Offset div BatchSize,
    {ChildrenStorageCtxsBatch1, _} =
        storage_file_ctx:get_children_ctxs_batch(StorageFileCtx, Offset, BatchSize),
    {BatchHash, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch1),
    SubJobs = {FilesJobs, DirsJobs} = generate_jobs_for_importing_children(Job, Offset,
        FileCtx, ChildrenStorageCtxsBatch2),

    FileUuid = file_ctx:get_uuid_const(FileCtx),
    increase_files_to_handle_counter(Job, FileCtx, SubJobs),
    FilesResults = import_regular_subfiles(FilesJobs),

    case StrategyType:strategy_merge_result(FilesJobs, FilesResults) of
        ok ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            case storage_sync_utils:all_subfiles_imported(DirsJobs, FileUuid) of
                true ->
                    storage_sync_info:update(FileUuid, Mtime, BatchKey, BatchHash);
                _ ->
                    storage_sync_info:update(FileUuid, undefined, BatchKey, BatchHash)
            end;
        _ -> ok
    end,
    DirsJobs;
import_children(#space_strategy_job{}, _Type, _Offset, _FileCtx, _) ->
    [].

%%-------------------------------------------------------------------
%% @doc
%% Imports regular files associated with FilesJobs
%% @end
%%-------------------------------------------------------------------
-spec import_regular_subfiles([space_strategy:job()]) -> [space_strategy:job_result()].
import_regular_subfiles(FilesJobs) ->
    utils:pmap(fun(Job) ->
        worker_pool:call(?STORAGE_SYNC_FILE_POOL_NAME, {?MODULE, run, [Job]},
            worker_pool:default_strategy(), ?FILES_IMPORT_TIMEOUT)
    end, FilesJobs).

%%-------------------------------------------------------------------
%% @doc
%% Increases files_to_import counter basing on number of SubJobs.
%% @end
%%-------------------------------------------------------------------
-spec increase_files_to_handle_counter(space_strategy:job(), file_ctx:ctx(),
    {[space_strategy:job()], [space_strategy:job()]}) -> ok.
increase_files_to_handle_counter(#space_strategy_job{
    strategy_type = StrategyType,
    data = #{
        space_id := SpaceId
    }},
    FileCtx, {FilesJobs, DirJobs}
) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FilesToImportNum = case storage_sync_utils:all_subfiles_imported(DirJobs, FileUuid) of
        true ->
            length(FilesJobs) + length(DirJobs);
        _ ->
            length(FilesJobs) + length(DirJobs) - 1 %not counting base directory

    end,
    storage_sync_monitoring:update_queue_length_spirals(SpaceId, length(FilesJobs) + length(DirJobs)),
    storage_sync_monitoring:update_to_do_counter(SpaceId, StrategyType, FilesToImportNum).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is fully imported
%% (.i.e. for regular files checks if its file_location exists).
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_file_with_existing_metadata(space_strategy:job(), file_ctx:ctx())->
    {space_strategy:job_result(), file_ctx:ctx(), space_strategy:job()}.
maybe_import_file_with_existing_metadata(Job = #space_strategy_job{
    strategy_type = StrategyType,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
    }},
    FileCtx
) ->
    {#statbuf{st_mode = Mode}, StorageFileCtx2} =
        storage_file_ctx:get_stat_buf(StorageFileCtx),
    CallbackModule = storage_sync_utils:module(Job),
    FileType = file_meta:type(Mode),
    LogicalAttrsResponse = #fuse_response{fuse_response = FileAttr} = get_attr(FileCtx),
    {CanonicalPath, FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    case is_imported(StorageId, CanonicalPath, FileType, LogicalAttrsResponse) of
        true ->
            {LocalResult, Job2} = delegate(CallbackModule, handle_already_imported_file, [
                Job, FileAttr, FileCtx2], 3),
            {LocalResult, FileCtx2, Job2};
        false ->
            {LocalResult, FileCtx3} = import_file(StorageId, SpaceId, FileName,
                ParentCtx, StrategyType, StorageFileCtx2),
            {LocalResult, FileCtx3, Job}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(storage:id(), od_space:id(), file_meta:path(), file_ctx:ctx(),
    space_strategy:type(), storage_file_ctx:ctx()) ->
    {ok, file_ctx:ctx()}| no_return().
import_file(StorageId, SpaceId, FileName, ParentCtx,
    StrategyType, StorageFileCtx
) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{
        st_mode = Mode,
        st_atime = ATime,
        st_ctime = CTime,
        st_mtime = MTime,
        st_size = FSize
    } = StatBuf,
    {SFMHandle, StorageFileCtx3} = storage_file_ctx:get_handle(StorageFileCtx2),
    OwnerId = get_owner_id(StorageFileCtx3),
    GroupId = get_group_owner_id(StorageFileCtx3),
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    FileMetaDoc = file_meta:new_doc(FileName, file_meta:type(Mode),
        Mode band 8#1777, OwnerId, GroupId, FSize, ParentUuid),
    {ParentPath, _} = file_ctx:get_canonical_path(ParentCtx),
    {ok, FileUuid} = create_file_meta(FileMetaDoc, ParentUuid),
    {ok, _} = create_times(FileUuid, MTime, ATime, CTime, SpaceId),
    CanonicalPath = filename:join([ParentPath, FileName]),
    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE ->
            create_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, FSize);
        _ ->
            ok
    end,
    FileCtx = file_ctx:new_by_doc(FileMetaDoc#document{key = FileUuid}, SpaceId, undefined),
    import_nfs4_acl(FileCtx, StorageFileCtx3),
    storage_sync_monitoring:increase_imported_files_spirals(SpaceId),
    storage_sync_monitoring:increase_imported_files_counter(SpaceId),
    storage_sync_monitoring:update_to_do_counter(SpaceId, StrategyType, -1),
    StorageFileId = SFMHandle#sfm_handle.file,
    ?debug("Import storage file ~p", [{StorageFileId, CanonicalPath}]),
    {ok, FileCtx}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file_meta of file FileName (which is child of
%% file associated with ParentCtx) exists in onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec file_meta_exists(file_meta:path(), file_ctx:ctx()) ->
    {true, ChildCtx :: file_ctx:ctx(), NewParentCtx :: file_ctx:ctx()} | false.
file_meta_exists(FileName, ParentCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try file_ctx:get_child(ParentCtx, FileName, RootUserCtx) of
        {FileCtx, ParentCtx2} ->
            {true, FileCtx, ParentCtx2}
    catch
        throw:?ENOENT ->
            false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given file on given storage is already imported to
%% onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec is_imported(storage:id(), helpers:file(), file_meta:type(),
    fslogic_worker:fuse_response()) -> boolean().
is_imported(_StorageId, _CanonicalPath, ?DIRECTORY_TYPE, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{type = ?DIRECTORY_TYPE}
}) ->
    true;
is_imported(StorageId, CanonicalPath, ?REGULAR_FILE_TYPE, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{type = ?REGULAR_FILE_TYPE, guid = FileGuid}
}) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    case file_ctx:get_or_create_local_file_location_doc(FileCtx) of
        {undefined, _FileCtx2} ->
            false;
        {#document{
            value = #file_location{
                storage_id = SID,
                file_id = FID
        }}, _FileCtx2} ->
            (StorageId == SID) andalso (CanonicalPath == FID)
    end;
is_imported(_StorageId, _CanonicalPath, _FileType, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{}
}) ->
    false;
is_imported(_StorageId, _CanonicalPath, _FileType, #fuse_response{
    status = #status{code = ?ENOENT}
}) ->
    false;
is_imported(_StorageId, _CanonicalPath, _FileType, #fuse_response{
    status = #status{code = ?EAGAIN}
}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Generates jobs for importing children of given directory to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec generate_jobs_for_importing_children(space_strategy:job(),
    non_neg_integer(), file_ctx:ctx(), [storage_file_ctx:ctx()]) ->
    {[space_strategy:job()], [space_strategy:job()]}.
generate_jobs_for_importing_children(#space_strategy_job{}, _Offset, _FileCtx, []) ->
    {[], []};
generate_jobs_for_importing_children(Job = #space_strategy_job{data = Data},
    Offset, FileCtx, ChildrenStorageCtxsBatch
) ->
    DirsOnly = maps:get(import_dirs_only, Data, false),
    DataClean = maps:remove(import_dirs_only, Data),

    Jobs = {FileJobs, DirJobs} =
        generate_jobs_for_subfiles(Job#space_strategy_job{data = DataClean},
            ChildrenStorageCtxsBatch, FileCtx, DirsOnly),

    case length(ChildrenStorageCtxsBatch) < ?DIR_BATCH of
        true ->
            Jobs;
        false ->
            {FileJobs, [
                Job#space_strategy_job{
                    data = DataClean#{
                        dir_offset => Offset + length(ChildrenStorageCtxsBatch)
                    }
                } | DirJobs
            ]}
    end.

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Generate jobs for subfiles of given directory. If DirsOnly is true
%%% only jobs subdirectories will be generated.
%%% @end
%%%-------------------------------------------------------------------
-spec generate_jobs_for_subfiles(space_strategy:job(), [storage_file_ctx:ctx()],
    file_ctx:ctx(), boolean()) -> {[space_strategy:job()], [space_strategy:job()]}.
generate_jobs_for_subfiles(Job = #space_strategy_job{
    data = Data = #{max_depth := MaxDepth}
},
    ChildrenStorageCtxsBatch, FileCtx, DirsOnly
) ->
    ChildrenData = Data#{
        dir_offset => 0,
        parent_ctx => FileCtx,
        max_depth => MaxDepth - 1
    },

    lists:foldr(fun(ChildStorageCtx, AccIn = {FileJobsIn, DirJobsIn}) ->
        case storage_file_ctx:get_stat_buf(ChildStorageCtx) of
            {#statbuf{st_mode = Mode}, ChildStorageCtx2} ->
                FileName = storage_file_ctx:get_file_id_const(ChildStorageCtx2),
                case file_meta:is_hidden(FileName) of
                    false ->
                        case file_meta:type(Mode) of
                            ?DIRECTORY_TYPE ->
                                ChildStorageCtx3 = storage_file_ctx:reset(ChildStorageCtx2),
                                {FileJobsIn, [new_job(Job, ChildrenData, ChildStorageCtx3) | DirJobsIn]};
                            ?REGULAR_FILE_TYPE when DirsOnly ->
                                AccIn;
                            ?REGULAR_FILE_TYPE ->
                                ChildStorageCtx3 = storage_file_ctx:reset(ChildStorageCtx2),
                                {[new_job(Job, ChildrenData, ChildStorageCtx3) | FileJobsIn], DirJobsIn}
                        end;
                    _ ->
                        AccIn
                end;
            {error, _} ->
                AccIn
        end
    end, {[], []}, ChildrenStorageCtxsBatch).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns new job basing on old Job, setting Data and
%%% changing file_name and storage_file_ctx in Data.
%%% @end
%%%-------------------------------------------------------------------
-spec new_job(space_strategy:job(), space_strategy:job_data(),
    storage_file_ctx:ctx()) -> space_strategy:job().
new_job(Job, Data, StorageFileCtx) ->
    FileId = storage_file_ctx:get_file_id_const(StorageFileCtx),
    Job#space_strategy_job{
        data = Data#{
            file_name => filename:basename(FileId),
            storage_file_ctx => StorageFileCtx
        }}.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec handle_already_imported_file(space_strategy:job(), #file_attr{},
    file_ctx:ctx()) -> {ok, space_strategy:job()}.
handle_already_imported_file(Job = #space_strategy_job{
    strategy_type = StrategyType,
    data = Data = #{
        space_id := SpaceId,
        storage_file_ctx := StorageFileCtx
}}, FileAttr, FileCtx) ->

    case storage_file_ctx:get_stat_buf(StorageFileCtx) of
        {#statbuf{st_mode = Mode}, StorageFileCtx2} ->
            maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx2, Mode, SpaceId);
        {error, _} ->
            ok
    end,

    case file_ctx:is_dir(FileCtx) of
        {true, _} ->
            case maps:get(dir_offset, Data, 0) of
                0 ->
                    storage_sync_monitoring:update_to_do_counter(SpaceId, StrategyType, -1);
                _ ->
                    ok
            end;
        _ ->
            storage_sync_monitoring:update_to_do_counter(SpaceId, StrategyType, -1)
    end,
    {ok, Job}.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_attrs(#file_attr{}, file_ctx:ctx(), storage_file_ctx:ctx(),
    file_meta:mode(), od_space:id()) -> ok.
maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx, Mode, SpaceId) ->
    {FileStat, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    Results = [
        maybe_update_size(FileAttr, FileStat, FileCtx, file_meta:type(Mode)),
        maybe_update_mode(FileAttr, FileStat, FileCtx),
        maybe_update_times(FileAttr, FileStat, FileCtx),
        maybe_update_owner(FileAttr, StorageFileCtx2, FileCtx),
        maybe_update_nfs4_acl(StorageFileCtx2, FileCtx)
    ],
    case lists:member(updated, Results) of
        true ->
            storage_sync_monitoring:increase_updated_files_spirals(SpaceId);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get file attr, catching all exceptions and returning always fuse_response
%% @end
%%--------------------------------------------------------------------
-spec get_attr(file_ctx:ctx()) -> fslogic_worker:fuse_response().
get_attr(FileCtx) ->
    try
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx)
    catch
        _:Error ->
            #fuse_response{status = fslogic_errors:gen_status_message(Error)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's size if it has changed since last import.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_size(#file_attr{}, #statbuf{}, file_ctx:ctx(),
    file_meta:type()) -> updated | not_updated | {error, term()}.
maybe_update_size(#file_attr{}, #statbuf{}, _FileCtx, ?DIRECTORY_TYPE) ->
    not_updated;
maybe_update_size(#file_attr{size = OldSize}, #statbuf{st_size = OldSize},
    _FileCtx, _Type
) ->
    not_updated;
maybe_update_size(#file_attr{size = _OldSize}, #statbuf{st_size = NewSize},
    FileCtx, ?REGULAR_FILE_TYPE
) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = lfm_event_utils:emit_file_truncated(FileGuid, NewSize, ?ROOT_SESS_ID),
    updated.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's mode if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_mode(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> updated | not_updated.
maybe_update_mode(#file_attr{mode = OldMode}, #statbuf{st_mode = Mode}, FileCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        false ->
            case Mode band 8#1777 of
                OldMode ->
                    not_updated;
                NewMode ->
                    update_mode(FileCtx, NewMode),
                    updated
            end;
        _ ->
            not_updated
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's mode.
%% @end
%%--------------------------------------------------------------------
-spec update_mode(file_ctx:ctx(), file_meta:mode()) ->
    ok | fslogic_worker:fuse_response().
update_mode(FileCtx, NewMode) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        _ ->
            ok = attr_req:chmod_attrs_only_insecure(FileCtx, NewMode)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's times if they've changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_times(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> updated | not_updated.
maybe_update_times(#file_attr{atime = ATime, mtime = MTime, ctime = CTime},
    #statbuf{st_atime = ATime, st_mtime = MTime, st_ctime = CTime},
    _FileCtx
) ->
    not_updated;
maybe_update_times(#file_attr{atime = _ATime, mtime = _MTime, ctime = _CTime},
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    FileCtx
) ->
    ok = fslogic_times:update_times_and_emit(FileCtx, #{
        atime => StorageATime,
        mtime => StorageMTime,
        ctime => StorageCTime
    }),
    updated.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's owner if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_owner(#file_attr{}, storage_file_ctx:ctx(),
    file_ctx:ctx()) -> updated | not_updated.
maybe_update_owner(#file_attr{owner_id = OldOwnerId}, StorageFileCtx, FileCtx) ->
    case get_owner_id(StorageFileCtx) of
        OldOwnerId ->
            not_updated;
        NewOwnerId ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            {ok, FileUuid} = file_meta:update(FileUuid, #{owner => NewOwnerId}),
            updated
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file meta
%% @end
%%--------------------------------------------------------------------
-spec create_file_meta(datastore:document(), file_meta:uuid()) -> {ok, file_meta:uuid()}.
create_file_meta(FileMetaDoc, ParentUuid) ->
    file_meta:create({uuid, ParentUuid}, FileMetaDoc, true).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates times
%% @end
%%--------------------------------------------------------------------
-spec create_times(file_meta:uuid(), times:time(), times:time(), times:time(),
    od_space:id()) ->
    {ok, datastore:key()}.
create_times(FileUuid, MTime, ATime, CTime, SpaceId) ->
    times:save_new(#document{
        key = FileUuid,
        value = #times{
            mtime = MTime,
            atime = ATime,
            ctime = CTime
        },
        scope=SpaceId}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file_location
%% @end
%%--------------------------------------------------------------------
-spec create_file_location(od_space:id(), storage:id(), file_meta:uuid(),
    file_meta:path(), file_meta:size()) -> ok.
create_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, Size) ->
    Location = #file_location{
        blocks = [#file_block{
            offset = 0,
            size = Size
        }],
        provider_id = oneprovider:get_provider_id(),
        file_id = CanonicalPath,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size,
        storage_file_created = true
    },
    {ok, _LocId} = file_location:save_and_bump_version(
        #document{
            key = file_location:local_id(FileUuid),
            value = Location,
            scope = SpaceId
    }),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function delegates execution of given Function to Module.
%% If Module doesn't have matching function, function from this ?MODULE
%% is called by default.
%% @end
%%-------------------------------------------------------------------
-spec delegate(atom(), atom(), [term()], non_neg_integer()) -> term().
delegate(Module, Function, Args, Arity) ->
    case erlang:function_exported(Module, Function, Arity) of
        true ->
            erlang:apply(Module, Function, Args);
        _ ->
            erlang:apply(?MODULE, Function, Args)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_owner_id(storage_file_ctx:ctx()) -> od_user:id().
get_owner_id(StorageFileCtx) ->
    {StatBuf, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{st_uid = Uid, st_gid = Gid} = StatBuf,
    {StorageDoc, _} = storage_file_ctx:get_storage_doc(StorageFileCtx),
    {ok, OwnerId} = reverse_luma:get_user_id(Uid, Gid, StorageDoc),
    OwnerId.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_group_owner_id(storage_file_ctx:ctx()) -> od_group:id() | undefined.
get_group_owner_id(StorageFileCtx) ->
    {StatBuf, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{st_gid = Gid} = StatBuf,
    {StorageDoc, _} = storage_file_ctx:get_storage_doc(StorageFileCtx),
    {ok, GroupId} = reverse_luma:get_group_id(Gid, StorageDoc),
    GroupId.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Import file's nfs4 ACL if it has changed.
%% @end
%%-------------------------------------------------------------------
-spec import_nfs4_acl(file_ctx:ctx(), storage_file_ctx:ctx()) -> ok.
import_nfs4_acl(FileCtx, StorageFileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case storage_file_ctx:get_nfs4_acl(StorageFileCtx) of
        {error, ?ENOTSUP} ->
            ok;
        {error, ?ENOENT} ->
            ok;
        {ACLHex, _}  ->
            {ok,  ACL} = nfs4_acl:decode(ACLHex),
            #provider_response{status = #status{code = ?OK}} =
                acl_req:set_acl(UserCtx, FileCtx, ACL, true, false),
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's nfs4 ACL if it has changed.
%% @end
%%-------------------------------------------------------------------
-spec maybe_update_nfs4_acl(storage_file_ctx:ctx(), file_ctx:ctx()) -> updated | not_updated.
maybe_update_nfs4_acl(StorageFileCtx, FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    #provider_response{provider_response = ACL} = acl_req:get_acl(UserCtx, FileCtx),
    case storage_file_ctx:get_nfs4_acl(StorageFileCtx) of
        {error, ?ENOTSUP} ->
            not_updated;
        {error, ?ENOENT} ->
            not_updated;
        {ACLHex, _ } ->
            {ok, NewACL} = nfs4_acl:decode(ACLHex),
            case NewACL of
                ACL ->
                    not_updated;
                _ ->
                    #provider_response{status = #status{code = ?OK}} =
                        acl_req:set_acl(UserCtx, FileCtx, NewACL, false, false),
                    updated
            end
    end.