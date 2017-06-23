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


%% API
-export([run/1, maybe_import_storage_file_and_children/1,
    maybe_import_storage_file/1, import_children/5,
    handle_already_imported_file/3, generate_jobs_for_importing_children/4]).


%%--------------------------------------------------------------------
%% @doc
%% Implementation for 'simple_scan' strategy.
%% @end
%%--------------------------------------------------------------------
-spec run(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]} |
    {space_strategy:job_result(), [space_strategy:job()], space_strategy:posthook()}.
run(Job = #space_strategy_job{
    data = Data = #{
        parent_ctx := ParentCtx,
        storage_file_ctx := _StorageFileCtx
    }}) ->
    Module = storage_sync_utils:module(Job),
    delegate(Module, maybe_import_storage_file_and_children,
        [Job#space_strategy_job{data = Data#{parent_ctx => ParentCtx}}], 1);
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
    {space_strategy:job_result(), [space_strategy:job()], space_strategy:posthook()}.
maybe_import_storage_file_and_children(Job0 = #space_strategy_job{
    data = Data0 = #{
        storage_file_ctx := StorageFileCtx0,
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
    {ok, BatchSize} = application:get_env(?APP_NAME, dir_batch_size),
    Type = file_meta:type(Mode),

    Job3 = case Offset of
        0 ->
            Data3 = Data2#{mtime => StorageMtime},
            Job2#space_strategy_job{data=Data3};
        _ -> Job2
    end,

    SubJobs = import_children(Job3, Type, Offset, FileCtx, BatchSize),

    {LocalResult, SubJobs}.



%%--------------------------------------------------------------------
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File is, for sure, not space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file(space_strategy:job()) -> {ok, file:ctx()} | no_return().
maybe_import_storage_file(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
    }}) ->
    SFMHandle = storage_file_ctx:get_handle_const(StorageFileCtx),
    {FileStats, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    case file_meta_exists(FileName, ParentCtx) of
        false ->
            {LocalResult, FileCtx} = import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, FileName, ParentCtx),
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
    data = Data0 = #{
        space_id := SpaceId,
        storage_id := StorageId,
        max_depth := MaxDepth,
        storage_file_ctx := StorageFileCtx,
        mtime := Mtime
    }},
    ?DIRECTORY_TYPE, Offset, FileCtx, BatchSize
) when MaxDepth > 0 ->

    BatchKey = Offset div BatchSize,
    ChildrenStorageCtxsBatch1 =
        storage_file_ctx:get_children_ctxs_batch_const(StorageFileCtx, Offset, BatchSize),
    {BatchHash, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch1),
    {FilesJobs, DirJobs} =
        generate_jobs_for_importing_children(Job#space_strategy_job{data = Data0},
        Offset, FileCtx, ChildrenStorageCtxsBatch2),

    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FilesToImportNum = case storage_sync_utils:all_subfiles_imported(DirJobs, FileUuid) of
        true ->
            length(FilesJobs) + length(DirJobs);
        _ ->
            length(FilesJobs) + length(DirJobs) - 1

    end,

    storage_sync_monitoring:update_files_to_import_counter(SpaceId, StorageId, FilesToImportNum),

    FilesResults = utils:pmap(fun(Job) ->
        worker_pool:call(?STORAGE_SYNC_FILE_POOL_NAME, {?MODULE, run, [Job]},
            worker_pool:default_strategy(), ?FILES_IMPORT_TIMEOUT)
    end, FilesJobs),

    case StrategyType:strategy_merge_result(FilesJobs, FilesResults) of
        ok ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            case length(FilesJobs) < BatchSize of
                true ->
                    storage_sync_info:update(FileUuid, Mtime, BatchKey, BatchHash);
                _ ->
                    storage_sync_info:update(FileUuid, undefined, BatchKey, BatchHash)
            end;
        _ -> ok
    end,
    DirJobs;
import_children(#space_strategy_job{}, _Type, _Offset, _FileCtx, _) ->
    [].

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
    {space_strategy:job_result(), file_ctx:ctx()}.
maybe_import_file_with_existing_metadata(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
    }},
    FileCtx
) ->

    SFMHandle = storage_file_ctx:get_handle_const(StorageFileCtx),
    {FileStats = #statbuf{st_mode = Mode}, _} =
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
            {LocalResult, FileCtx} = import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, FileName, ParentCtx),
            {LocalResult, FileCtx, Job}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(storage:id(), od_space:id(), #statbuf{}, file_meta:path(),
    file_meta:path(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}| no_return().
import_file(StorageId, SpaceId, StatBuf, StorageFileId, FileName, ParentCtx) ->
    {ParentPath, _} = file_ctx:get_canonical_path(ParentCtx),
    CanonicalPath = filename:join([ParentPath, FileName]),

    #statbuf{
        st_mode = Mode,
        st_atime = ATime,
        st_ctime = CTime,
        st_mtime = MTime,
        st_size = FSize
    } = StatBuf,

    FileMetaDoc = file_meta:new_doc(FileName, file_meta:type(Mode), Mode band 8#1777,
        ?ROOT_USER_ID, FSize),

    {ok, FileUuid} = create_file_meta(FileMetaDoc, ParentPath),
    {ok, _} = create_times(FileUuid, MTime, ATime, CTime, SpaceId),

    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE ->
            create_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, FSize);
        _ ->
            ok
    end,
    storage_sync_monitoring:increase_imported_files_counter(SpaceId, StorageId),
    storage_sync_monitoring:update_files_to_import_counter(SpaceId, StorageId, -1),
    FileCtx = file_ctx:new_by_doc(FileMetaDoc#document{key = FileUuid}, SpaceId, undefined),
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
    case file_meta:get_local_locations({guid, FileGuid}) of
        [] ->
            false;
        [#document{
            value = #file_location{
                storage_id = SID,
                file_id = FID
            }}] ->
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
generate_jobs_for_importing_children(Job = #space_strategy_job{
    data = Data = #{
        file_name := FileName,
        max_depth := MaxDepth
    }
}, Offset, FileCtx, ChildrenStorageCtxsBatch) ->
    DirsOnly = maps:get(import_dirs_only, Data, false),
    DataClean = maps:remove(import_dirs_only, Data),

    ChildrenData = DataClean#{
        dir_offset => 0,
        parent_ctx => FileCtx,
        max_depth => MaxDepth - 1
    },

    Jobs = {FileJobs, DirJobs} = generate_jobs_for_subfiles(Job#space_strategy_job{data = DataClean},
        ChildrenData, ChildrenStorageCtxsBatch, DirsOnly),


    {ok, DirBatch} = application:get_env(?APP_NAME, dir_batch_size),
    ?alert("Adding jobs for children of ~p in range: [~p - ~p). Jobs: ~p~n", [FileName, Offset, Offset + length(ChildrenStorageCtxsBatch), length(DirJobs)]),
    case length(ChildrenStorageCtxsBatch) < DirBatch of
        true ->
            Jobs;
        false ->
            {FileJobs, [
                Job#space_strategy_job{
                    data = Data#{
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
%%todo spec
generate_jobs_for_subfiles(Job, Data, ChildrenStorageCtxsBatch, DirsOnly) ->

    lists:foldr(fun(ChildStorageCtx, AccIn = {FileJobsIn, DirJobsIn}) ->
        FileName = storage_file_ctx:get_file_id_const(ChildStorageCtx),
        {#statbuf{st_mode = Mode}, ChildStorageCtx2} =
            storage_file_ctx:get_stat_buf(ChildStorageCtx),
        FileName = storage_file_ctx:get_file_id_const(ChildStorageCtx2),
        case file_meta:is_hidden(FileName) of
            false ->
                case file_meta:type(Mode) of
                    ?DIRECTORY_TYPE ->
                        {FileJobsIn, [new_job(Job, Data, ChildStorageCtx) | DirJobsIn]};
                    ?REGULAR_FILE_TYPE when DirsOnly ->
                        AccIn;
                    ?REGULAR_FILE_TYPE ->
                        {[new_job(Job, Data, ChildStorageCtx) | FileJobsIn], DirJobsIn}
                end;
            _ ->
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
    data = #{storage_file_ctx := StorageFileCtx}
}, FileAttr, FileCtx
) ->
    {FileStat = #statbuf{st_mode = Mode}, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    maybe_update_size(FileAttr, FileStat, FileCtx, file_meta:type(Mode)),
    maybe_update_mode(FileAttr, FileStat, FileCtx),
    maybe_update_times(FileAttr, FileStat, FileCtx),
    {ok, Job}.


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
    file_meta:type()) -> ok | {error, term()}.
maybe_update_size(#file_attr{}, #statbuf{}, _FileCtx, ?DIRECTORY_TYPE) ->
    ok;
maybe_update_size(#file_attr{size = OldSize}, #statbuf{st_size = OldSize},
    _FileCtx, _Type
) ->
    ok;
maybe_update_size(#file_attr{size = _OldSize}, #statbuf{st_size = NewSize},
    FileCtx, ?REGULAR_FILE_TYPE
) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = lfm_event_utils:emit_file_truncated(FileGuid, NewSize, ?ROOT_SESS_ID).



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's mode if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_mode(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> ok.
maybe_update_mode(#file_attr{mode = OldMode}, #statbuf{st_mode = Mode}, FileCtx) ->
    case Mode band 8#1777 of
        OldMode ->
            ok;
        NewMode ->
            update_mode(FileCtx, NewMode)
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
-spec maybe_update_times(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> ok.
maybe_update_times(#file_attr{atime = ATime, mtime = MTime, ctime = CTime},
    #statbuf{st_atime = ATime, st_mtime = MTime, st_ctime = CTime},
    _FileCtx
) ->
    ok;
maybe_update_times(#file_attr{atime = _ATime, mtime = _MTime, ctime = _CTime},
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    FileCtx
) ->
    ok = fslogic_times:update_times_and_emit(FileCtx, #{
        atime => StorageATime,
        mtime => StorageMTime,
        ctime => StorageCTime
    }).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file meta
%% @end
%%--------------------------------------------------------------------
-spec create_file_meta(datastore:document(), file_meta:path()) -> {ok, file_meta:uuid()}.
create_file_meta(FileMetaDoc, ParentPath) ->
    file_meta:create({path, ParentPath}, FileMetaDoc, true).

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
    times:create(#document{
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
        size = Size
    },
    {ok, LocId} = file_location:save_and_bump_version(
        #document{
            key = datastore_utils:gen_uuid(),
            value = Location,
            scope = SpaceId
    }),
    ok = file_meta:attach_location({uuid, FileUuid}, LocId,
        oneprovider:get_provider_id()).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% This function delegates execution of given Function to Module.
%%% If Module doesn't have matching function, function from this ?MODULE
%%% is called by default.
%%% @end
%%%-------------------------------------------------------------------
-spec delegate(atom(), atom(), [term()], non_neg_integer()) -> term().
delegate(Module, Function, Args, Arity) ->
    case erlang:function_exported(Module, Function, Arity) of
        true ->
            erlang:apply(Module, Function, Args);
        _ ->
            erlang:apply(?MODULE, Function, Args)
    end.


