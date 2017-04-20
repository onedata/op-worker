%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import).
-author("Rafal Slota").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DIR_BATCH, 100).

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% Callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1,
    strategy_merge_result/2, strategy_merge_result/3]).

%% API
-export([start/6]).

%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback available_strategies/0.
%% @end
%%--------------------------------------------------------------------
-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    [
        #space_strategy{
            name = bfs_scan,
            result_merge_type = merge_all,
            arguments = [
                #space_strategy_argument{
                    name = scan_interval,
                    type = integer,
                    description = <<"Scan interval in seconds">>}
            ],
            description = <<"Simple BFS-like full filesystem scan">>
        },
        #space_strategy{
            name = no_import,
            arguments = [],
            description = <<"Don't perform any storage import">>
        }
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(),
    space_strategy:job_data()) -> [space_strategy:job()].
strategy_init_jobs(no_import, _, _) ->
    [];
strategy_init_jobs(_, _, #{last_import_time := LastImportTime})
    when is_integer(LastImportTime) -> [];
strategy_init_jobs(bfs_scan, Args, #{last_import_time := undefined} = Data) ->
    [#space_strategy_job{
        strategy_name = bfs_scan,
        strategy_args = Args,
        data = Data
    }];
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StrategyArgs, InitData}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = bfs_scan} = Job) ->
    run_bfs_scan(Job);
strategy_handle_job(#space_strategy_job{strategy_name = no_import}) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result(_Jobs, Results) ->
    Reasons = [Reason || {error, Reason} <- Results],
    case Reasons of
        [] -> ok;
        _ ->
            {error, Reasons}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(),
    LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(#space_strategy_job{
    data=#{
        space_id := SpaceId,
        storage_id := StorageId}
}, ok, ok) ->
    space_strategies:update_last_import_time(SpaceId, StorageId, os:system_time(milli_seconds)),
    ok;
strategy_merge_result(_Job, Error, ok) ->
    Error;
strategy_merge_result(_Job, ok, Error) ->
    Error;
strategy_merge_result(_Job, {error, Reason1}, {error, Reason2}) ->
    {error, [Reason1, Reason2]}.

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Function responsible for starting storage import.
%% @end
%%--------------------------------------------------------------------
-spec start(od_space:id(), storage:id(), integer() | undefined, file_ctx:ctx(),
    file_meta:path(), non_neg_integer()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
start(SpaceId, StorageId, LastImportTime, ParentCtx, FileName, MaxDepth) ->
    InitialImportJobData = #{
        last_import_time => LastImportTime,
        space_id => SpaceId,
        storage_id => StorageId,
        file_name => FileName,
        max_depth => MaxDepth,
        parent_ctx => ParentCtx
    },
    ImportInit = space_sync_worker:init(?MODULE, SpaceId, StorageId, InitialImportJobData),
    space_sync_worker:run(ImportInit).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Implementation for 'bfs_scan' strategy.
%% @end
%%--------------------------------------------------------------------
-spec run_bfs_scan(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run_bfs_scan(Job = #space_strategy_job{data = Data}) ->
    #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx
    } = Data,

    {ok, Storage} = storage:get(StorageId),
    {ParentPath, ParentCtx2} = file_ctx:get_canonical_path(ParentCtx),
    CanonicalPath = filename:join(ParentPath, FileName),

    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, CanonicalPath, undefined),

    case storage_file_manager:stat(SFMHandle) of
        {ok, StatBuf = #statbuf{}} ->
            maybe_import_storage_file_and_children(Job#space_strategy_job{
                data=Data#{parent_ctx => ParentCtx2}
            }, SFMHandle, StatBuf);
        ErrorResponse ->
            {ErrorResponse, []}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File may be space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file_and_children(space_strategy:job(),
    storage_file_manager:handle(), #statbuf{}) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
maybe_import_storage_file_and_children(Job = #space_strategy_job{data = Data = #{}},
    SFMHandle,
    FileStats = #statbuf{st_mode = Mode}
) ->
    {LocalResult, FileCtx} = maybe_import_storage_file(Data, SFMHandle, FileStats),
    SubJobs = import_children(SFMHandle, file_type(Mode), Job,
        maps:get(dir_offset, Data, 0), ?DIR_BATCH, FileCtx),

    {LocalResult, SubJobs}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File is, for sure, not space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file(space_strategy:job_data(),
    storage_file_manager:handle(), #statbuf{}) -> {ok, file:ctx()} | no_return().
maybe_import_storage_file(Data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx
    },
    SFMHandle,
    FileStats
) ->
    case file_meta_exists(FileName, ParentCtx) of
        false ->
            import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, FileName, ParentCtx);
        {true, FileCtx0, _} ->
            maybe_import_file_with_existing_metadata(FileCtx0,
                SFMHandle, Data ,FileStats)
    end.

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
    false.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is fully imported
%% (.i.e. for regular files checks if its file_location exists).
%%
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_file_with_existing_metadata(file_ctx:ctx(),
    storage_file_manager:handle(), space_strategy:job_data(), #statbuf{}) ->
    {space_strategy:job_result(), file_ctx:ctx()}.
maybe_import_file_with_existing_metadata(FileCtx, SFMHandle,
    _Data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx
    },
    FileStats = #statbuf{st_mode = Mode}
)->
    FileType = file_type(Mode),
    LogicalAttrsResponse = #fuse_response{fuse_response = FileAttr} = get_attr(FileCtx),

    {CanonicalPath, FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    case is_imported(StorageId, CanonicalPath, FileType, LogicalAttrsResponse) of
        true ->
            {handle_already_imported_file(FileAttr, FileStats, FileCtx2), FileCtx2};
        false ->
            import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, FileName, ParentCtx)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(storage:id(), od_space:id(), #statbuf{}, file_meta:path(),
    file_meta:path(), file_ctx:ctx()) ->  {ok, file_ctx:ctx()}| no_return().
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

    FileMetaDoc = file_meta:new_doc(FileName, file_type(Mode), Mode band 8#1777,
        ?ROOT_USER_ID, FSize),

    {ok, FileUuid} = create_file_meta(FileMetaDoc, ParentPath),
    {ok, _} = create_times(FileUuid, MTime, ATime, CTime),

    case file_type(Mode) of
        ?REGULAR_FILE_TYPE ->
            create_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, FSize);
        _ ->
            ok
    end,
    FileCtx = file_ctx:new_by_doc(FileMetaDoc#document{key=FileUuid}, SpaceId, undefined),
    ?debug("Import storage file ~p", [{StorageFileId, CanonicalPath}]),
    {ok, FileCtx}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If file is a directory, this function prepares jobs for importing
%% its children. Otherwise does nothing.
%% @end
%%--------------------------------------------------------------------
-spec import_children(storage_file_manager:handle(), file_meta:type(),
    space_strategy:job(), Offset :: non_neg_integer(), Count :: non_neg_integer(),
    file_ctx:ctx()) -> [space_strategy:job()].
import_children(SFMHandle, ?DIRECTORY_TYPE,
    Job = #space_strategy_job{data = #{max_depth := MaxDepth}},
    Offset, Count, FileCtx) when MaxDepth > 0 ->

    {ok, ChildrenIds} = storage_file_manager:readdir(SFMHandle, Offset, Count),
    generate_jobs_for_importing_children(Job, Offset, FileCtx, ChildrenIds);
import_children(_SFMHandle, _, #space_strategy_job{}, _Offset, _Count, _FileCtx) ->
    [].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates jobs for importing children of given directory to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec generate_jobs_for_importing_children(space_strategy:job(),
    non_neg_integer(), file_ctx:ctx(), [helpers:file_id()]) -> [space_strategy:job()].
generate_jobs_for_importing_children(#space_strategy_job{}, _Offset, _FileCtx, []) ->
    [];
generate_jobs_for_importing_children(Job = #space_strategy_job{
    data = Data = #{
        max_depth := MaxDepth
    }
}, Offset, FileCtx, ChildrenIds) ->

    Data0 = Data#{dir_offset => 0, parent_ctx => FileCtx},
    Jobs = lists:map(fun(ChildId) ->
        Job#space_strategy_job{
            data = Data0#{
                file_name =>ChildId,
                max_depth => MaxDepth - 1
            }}
    end, ChildrenIds),
    [
        Job#space_strategy_job{
            data = Data#{dir_offset => Offset + length(ChildrenIds)}
        } | Jobs
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates mode, times and size of already imported file.
%%--------------------------------------------------------------------
-spec handle_already_imported_file(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> ok.
handle_already_imported_file(FileAttr = #file_attr{},
    FileStat = #statbuf{st_mode=Mode},
    FileCtx
) ->
    maybe_update_size(FileAttr, FileStat, FileCtx, file_type(Mode)),
    maybe_update_mode(FileAttr, FileStat, FileCtx),
    maybe_update_times(FileAttr, FileStat, FileCtx).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return type of file depending on its posix mode.
%% @end
%%--------------------------------------------------------------------
-spec file_type(Mode :: non_neg_integer()) -> file_meta:type().
file_type(Mode) ->
    IsDir = (Mode band 8#100000) == 0,
    case IsDir of
        true -> ?DIRECTORY_TYPE;
        false -> ?REGULAR_FILE_TYPE
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
            attr_req:chmod_attrs_only_insecure(FileCtx, NewMode)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's times if they've changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_times(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> ok.
maybe_update_times(#file_attr{atime=ATime, mtime=MTime, ctime=CTime},
    #statbuf{st_atime = ATime, st_mtime = MTime, st_ctime = CTime},
    _FileCtx
) ->
    ok;
maybe_update_times(#file_attr{atime=_ATime, mtime=_MTime, ctime=_CTime},
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    FileCtx
) ->
    fslogic_times:update_times_and_emit(FileCtx, #{
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
-spec create_times(file_meta:uuid(), times:time(), times:time(), times:time())
        -> {ok, datastore:key()}.
create_times(FileUuid, MTime, ATime, CTime) ->
    times:create(#document{
        key = FileUuid,
        value = #times{
            mtime = MTime,
            atime = ATime,
            ctime = CTime
        }}).

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
        #document{key = datastore_utils:gen_uuid(), value = Location}),
    ok = file_meta:attach_location({uuid, FileUuid}, LocId,
        oneprovider:get_provider_id()).