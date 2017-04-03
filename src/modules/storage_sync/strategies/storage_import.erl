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
-export([start/5]).

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
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StartegyArgs, InitData}]).

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
strategy_merge_result(_Job, ok, ok) ->
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
-spec start(od_space:id(), storage:id(), integer() | undefined,
    file_meta:path(), non_neg_integer()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
start(SpaceId, StorageId, LastImportTime, StorageLogicalFileId, MaxDepth) ->
    CanonicalPath = fslogic_path:logical_to_canonical_path(StorageLogicalFileId, SpaceId),
    InitialImportJobData = #{
        last_import_time => LastImportTime,
        space_id => SpaceId,
        storage_id => StorageId,
        storage_logical_file_id => StorageLogicalFileId,
        max_depth => MaxDepth,
        parent_ctx => file_ctx:get_parent_by_path(CanonicalPath)
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
        storage_logical_file_id := StorageLogicalFileId,
        space_id := SpaceId,
        storage_id := StorageId
    } = Data,

    {ok, Storage} = storage:get(StorageId),
    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, StorageLogicalFileId, undefined),

    case storage_file_manager:stat(SFMHandle) of
        {ok, StatBuf = #statbuf{}} ->
            maybe_import_storage_file(Job, SFMHandle, StatBuf);
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
-spec maybe_import_storage_file(space_strategy:job(),
    storage_file_manager:handle(), #statbuf{}) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
maybe_import_storage_file(Job = #space_strategy_job{
    data = Data = #{
        storage_logical_file_id := StorageLogicalFileId,
        parent_ctx := ParentCtx
    }},
    SFMHandle,
    FileStats = #statbuf{st_mode = Mode}
) ->

    {LocalResult, FileCtx} =
        case file_ctx:is_root_dir_const(ParentCtx) of
            true ->
                % StorageLogicalFile is space if ParentCtx is root_dir
                {ok, file_ctx:new_by_canonical_path(
                    user_ctx:new(?ROOT_SESS_ID), StorageLogicalFileId)};
            false ->
                maybe_import_not_space_storage_file(Data, SFMHandle, FileStats)
        end,

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
-spec maybe_import_not_space_storage_file(space_strategy:job_data(),
    storage_file_manager:handle(), #statbuf{}) -> {ok, file:ctx()} | no_return().
maybe_import_not_space_storage_file(Data = #{
        storage_logical_file_id := StorageLogicalFileId,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx
    },
    SFMHandle,
    FileStats
) ->

    case file_meta_exists(StorageLogicalFileId, SpaceId, ParentCtx) of
        false ->
            import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, StorageLogicalFileId);
        {true, FileCtx0, _} ->
            maybe_import_file_with_existing_metadata(FileCtx0,
                SFMHandle, Data ,FileStats)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file_meta of StorageLogicalFileId (which is child of
%% file associated with ParentCtx) exists in onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec file_meta_exists(file_meta:path(), od_space:id(), file_ctx:ctx()) ->
    {true, ChildCtx :: file_ctx:ctx(), NewParentCtx :: file_ctx:ctx()} | false.
file_meta_exists(StorageLogicalFileId, SpaceId, ParentCtx) ->
    CanonicalPath = fslogic_path:logical_to_canonical_path(StorageLogicalFileId, SpaceId),
    {BaseName, _Parent} = fslogic_path:basename_and_parent(CanonicalPath),
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try file_ctx:get_child(ParentCtx, BaseName, RootUserCtx) of
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
is_imported(_StorageId, _StorageLogicalFileId, ?DIRECTORY_TYPE, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{type = ?DIRECTORY_TYPE}
}) ->
    true;
is_imported(StorageId, StorageLogicalFileId, ?REGULAR_FILE_TYPE, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{type = ?REGULAR_FILE_TYPE, guid = FileGuid}
}) ->
    FileIds = [
        {SID, FID} || #document{
            value = #file_location{storage_id = SID, file_id = FID}
        } <- file_meta:get_local_locations({guid, FileGuid})
    ],
    lists:member({StorageId, StorageLogicalFileId}, FileIds);
is_imported(_StorageId, _StorageLogicalFileId, _FileType, #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{}
}) ->
    false;
is_imported(_StorageId, _StorageLogicalFileId, _FileType, #fuse_response{
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
        storage_logical_file_id := StorageLogicalFileId,
        space_id := SpaceId,
        storage_id := StorageId
    },
    FileStats = #statbuf{st_mode = Mode}
)->
    FileType = file_type(Mode),
    LogicalAttrsResponse = #fuse_response{fuse_response = FileAttr} = get_attr(FileCtx),

    case is_imported(StorageId, StorageLogicalFileId, FileType, LogicalAttrsResponse) of
        true ->
            {handle_already_imported_file(FileAttr, FileStats), FileCtx};
        false ->
            import_file(StorageId, SpaceId, FileStats,
                SFMHandle#sfm_handle.file, StorageLogicalFileId)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(storage:id(), od_space:id(), #statbuf{}, file_meta:path(),
    file_meta:path()) ->  {ok, file_ctx:ctx()}| no_return().
import_file(StorageId, SpaceId, StatBuf, StorageFileId, StorageLogicalFileId) ->
    {FileName, ParentPath} =
        fslogic_path:basename_and_parent(StorageLogicalFileId),

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
            create_file_location(SpaceId, StorageId, FileUuid, StorageLogicalFileId, FSize);
        _ ->
            ok
    end,
    FileCtx = file_ctx:new_by_doc(FileMetaDoc#document{key=FileUuid}, SpaceId, undefined),
    ?debug("Import storage file ~p", [{StorageFileId, StorageLogicalFileId}]),
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
        max_depth := MaxDepth,
        storage_logical_file_id := StorageLogicalFileId
    }
}, Offset, FileCtx, ChildrenIds) ->

    Data0 = Data#{dir_offset => 0, parent_ctx => FileCtx},
    Jobs = lists:map(fun(ChildId) ->
        Job#space_strategy_job{
            data = Data0#{
                storage_logical_file_id =>
                <<StorageLogicalFileId/binary, "/", ChildId/binary>>,
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
%% Updates mode and times of already imported file.
%%--------------------------------------------------------------------
-spec handle_already_imported_file(#file_attr{}, #statbuf{}) -> ok.
handle_already_imported_file(FileAttr = #file_attr{}, FileStat = #statbuf{}) ->
    maybe_update_mode(FileAttr, FileStat),
    maybe_update_times(FileAttr, FileStat).

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
%% Updates file's mode if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_mode(#file_attr{}, #statbuf{}) -> ok.
maybe_update_mode(#file_attr{mode = OldMode}, #statbuf{st_mode = Mode}) ->
    case Mode band 8#1777 of
        OldMode ->
            ok;
        _NewMode ->
            %% todo deal with different posix mode for space dirs on storage vs db
            %% fslogic_req_generic:chmod(user_ctx:new(?ROOT_SESS_ID), {guid, FileGuid}, NewMode),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's times if they've changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_times(#file_attr{}, #statbuf{}) -> ok.
maybe_update_times(#file_attr{guid = FileGuid}, #statbuf{
    st_atime = StorageATime,
    st_mtime = StorageMTime,
    st_ctime = StorageCTime
}) ->

    FileUuid = fslogic_uuid:guid_to_uuid(FileGuid),
    case times:get(FileUuid) of
        {ok, Doc = #document{value = Times}} ->
            NewTimes = times:update_record(StorageATime, StorageMTime,
                StorageCTime, Times),
            case NewTimes of
                Times ->
                    ok;
                _ ->
                    times:save(Doc#document{value = NewTimes}),
                    ok
            end;
        {error, {not_found, _}} ->
            NewTimes = #times{
                atime = StorageATime,
                mtime = StorageMTime,
                ctime = StorageCTime
            },
            times:save(#document{key = FileUuid, value = NewTimes}),
            %todo should we handle error of updating timestamps ?
            %todo should we update space_dir timestamps ?
            ok
    end.

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
%% Creates times
%% @end
%%--------------------------------------------------------------------
-spec create_file_location(od_space:id(), storage:id(), file_meta:uuid(),
    file_meta:path(), file_meta:size()) -> ok.
create_file_location(SpaceId, StorageId, FileUuid, StorageLogicalFileId, Size) ->
    Location = #file_location{
        blocks = [#file_block{
            offset = 0,
            size = Size
        }],
        provider_id = oneprovider:get_provider_id(),
        file_id = StorageLogicalFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size
    },
    {ok, LocId} = file_location:save_and_bump_version(
        #document{key = datastore_utils:gen_uuid(), value = Location}),
    ok = file_meta:attach_location({uuid, FileUuid}, LocId,
        oneprovider:get_provider_id()).