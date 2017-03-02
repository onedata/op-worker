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
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1]).
-export([strategy_merge_result/2, strategy_merge_result/3]).

%% API
-export([]).

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
%% Implementation for 'bfs_scan' strategy.
%% @end
%%--------------------------------------------------------------------
-spec run_bfs_scan(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run_bfs_scan(#space_strategy_job{data = Data} = Job) ->
    #{
        storage_logical_file_id := StorageLogicalFileId,
        space_id := SpaceId,
        storage_id := StorageId
    } = Data,
    StorageFilePath = filename_mapping:to_storage_path(SpaceId, StorageId, StorageLogicalFileId),
    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID,
        fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
        undefined, StorageId, StorageLogicalFileId, undefined,
        oneprovider:get_provider_id()),
    case storage_file_manager:stat(SFMHandle) of
        {ok, FileStats = #statbuf{
            st_mode = Mode,
            st_atime = StorageATime,
            st_mtime = StorageMTime,
            st_ctime = StorageCTime}
        } ->
            FileType = file_type(Mode),
            [<<"/">>, _SpaceName | Rest] = fslogic_path:split(StorageLogicalFileId),
            CanonicalPath = fslogic_path:join([<<"/">>, SpaceId | Rest]),
            {IsImported, LogicalAttrsResponse} =
                case file_meta:to_uuid({path, CanonicalPath}) of
                    {error,{not_found,file_meta}} ->
                        {false, undefined};
                    {ok, Uuid} ->
                        Guid = fslogic_uuid:uuid_to_guid(Uuid),
                        File = file_ctx:new_by_guid(Guid),
                        LogicalAttrsResponse_ = get_attr(File),
                        IsImported_ = is_imported(StorageId, StorageLogicalFileId,
                            FileType, LogicalAttrsResponse_
                        ),
                        {IsImported_, LogicalAttrsResponse_}
                end,
            LocalResult = case IsImported of
                true ->
                    #fuse_response{
                        fuse_response = #file_attr{
                            mode = OldMode,
                            guid = FileGuid
                        }} = LogicalAttrsResponse,
                    FileUUID = fslogic_uuid:guid_to_uuid(FileGUID),
                    case Mode band 8#1777 of
                        OldMode ->
                            ok;
                        NewMode ->
                            %% todo deal with different posix mode for space dirs on storage vs db
                            %% fslogic_req_generic:chmod(user_ctx:new(?ROOT_SESS_ID), {guid, FileGuid}, NewMode),
                            ok
                    end,

                    case times:get(fslogic_uuid:guid_to_uuid(FileGuid)) of
                        {ok, Doc = #document{
                            value = Times = #times{
                                atime = ATime,
                                ctime = CTime,
                                mtime = MTime
                        }}} ->
                            NewTimes = Times#times{
                                atime = max(ATime, StorageATime),
                                mtime = max(MTime, StorageMTime),
                                ctime = max(CTime, StorageCTime)
                            },
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
                            times:save(#document{key = fslogic_uuid:guid_to_uuid(FileGuid), value = NewTimes}),
                            ok
                    end;
                false ->
                    import_file(StorageId, SpaceId, FileStats, StorageFilePath, StorageLogicalFileId, Data)
            end,

            SubJobs = import_children(SFMHandle, FileType, Job,
                maps:get(dir_offset, Data, 0), ?DIR_BATCH),

            {LocalResult, SubJobs}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given file on given storage is already imported to onedata filesystem.
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
        } <- fslogic_utils:get_local_file_locations({guid, FileGuid})
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
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(storage:id(), od_space:id(), #statbuf{}, file_meta:path(),
    file_meta:path(), space_strategy:job_data()) -> ok | no_return().
import_file(StorageId, SpaceId, StatBuf, StorageFileId, StorageLogicalFileId, JobData) ->
    {FileName, ParentPath} = fslogic_path:basename_and_parent(StorageLogicalFileId),
    {_StorageFileName, StorageParentPath} =
        fslogic_path:basename_and_parent(StorageFileId),
    #statbuf{
        st_mode = Mode,
        st_atime = ATime,
        st_ctime = CTime,
        st_mtime = MTime,
        st_size = FSize
    } = StatBuf,

    File = #document{value = #file_meta{
        name = FileName,
        type = file_type(Mode),
        mode = Mode band 8#1777,
        owner = ?ROOT_USER_ID,
        size = FSize
    }},

    {ok, FileUuid} =
        case file_meta:create({path, ParentPath}, File, true) of
            {ok, FileUuid0} ->
                {ok, FileUuid0};
            {error, {not_found, _}} ->
                InitParent = space_sync_worker:init(storage_update, SpaceId,
                    StorageId, JobData#{
                        storage_logical_file_id => StorageParentPath,
                        max_depth => 0
                    }),
                space_sync_worker:run(InitParent),
                file_meta:create({path, ParentPath}, File, true)
        end,
    {ok, _} = times:create(#document{key = FileUuid, value = #times{
        mtime = MTime, atime = ATime, ctime = CTime}}),

    case file_type(Mode) of
        ?REGULAR_FILE_TYPE ->
            Location = #file_location{
                blocks = [#file_block{
                    offset = 0,
                    size = FSize
                }],
                provider_id = oneprovider:get_provider_id(),
                file_id = StorageLogicalFileId,
                storage_id = StorageId,
                uuid = FileUuid,
                space_id = SpaceId,
                size = FSize
            },
            {ok, LocId} = file_location:save_and_bump_version(
                #document{key = datastore_utils:gen_uuid(), value = Location}),
            ok = file_meta:attach_location({uuid, FileUuid}, LocId,
                oneprovider:get_provider_id());
        _ ->
            ok
    end,

    ?debug("Import storage file ~p", [{StorageFileId, StorageLogicalFileId}]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates jobs for importing children of given directory to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_children(storage_file_manager:handle(), file_meta:type(), space_strategy:job(),
    Offset :: non_neg_integer(), Count :: non_neg_integer()) ->
    [space_strategy:job()].
import_children(SFMHandle, ?DIRECTORY_TYPE, Job = #space_strategy_job{
    data = Data = #{max_depth := MaxDepth}
}, Offset, Count) when MaxDepth > 0 ->

    #{storage_logical_file_id := StorageLogicalFileId} = Data,
    {ok, ChildrenIds} = storage_file_manager:readdir(SFMHandle, Offset, Count),
    case ChildrenIds of
        [] -> [];
        _ ->
            Data0 = Data#{dir_offset => 0},
            Jobs = [
                Job#space_strategy_job{
                    data = Data0#{
                        storage_logical_file_id =>
                            <<StorageLogicalFileId/binary, "/", ChildId/binary>>,
                        max_depth => MaxDepth - 1
                    }}
                || ChildId <- ChildrenIds
            ],
            [
                Job#space_strategy_job{
                    data = Data#{dir_offset => Offset + length(ChildrenIds)}
                } | Jobs
            ]
    end;
import_children(_SFMHandle, _, _Job = #space_strategy_job{data = _Data}, _Offset, _Count) ->
    [].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return type of file depending on its posix mode.
%% @end
%%--------------------------------------------------------------------
-spec file_type(Mode :: non_neg_integer()) ->
    file_meta:type().
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
get_attr(File) ->
    try
        attr_req:get_file_attr_insecure(
            user_ctx:new(?ROOT_SESS_ID), File)
    catch
        _:Error ->
            #fuse_response{status = fslogic_errors:gen_status_message(Error)}
    end.
