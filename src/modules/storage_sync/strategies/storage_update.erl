%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for updating storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_update).
-author("Rafal Slota").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").


%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% Callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1, worker_pools_config/0, main_worker_pool/0]).
-export([strategy_merge_result/2, strategy_merge_result/3]).
%%simple_scan callbacks
-export([handle_already_imported_file/3, maybe_import_storage_file_and_children/1]).

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
            name = simple_scan,
            result_merge_type = return_none,
            arguments = [
                #space_strategy_argument{
                    name = max_depth,
                    type = integer,
                    description = <<"Max depth of file tree that will be scanned">>
                },
                #space_strategy_argument{
                    name = scan_interval,
                    type = integer,
                    description = <<"Scan interval in seconds">>
                },
                #space_strategy_argument{
                    name = delete_enable, %todo jk find better name
                    type = boolean,
                    description = <<"Enables deletion of already imported files">>
                },
                #space_strategy_argument{
                    name = write_once, %todo jk find better name
                    type = boolean,
                    description = <<"Allows modifying already imported files">>
                }
            ],
            description = <<"Simple full filesystem scan">>
        },
        #space_strategy{
            name = no_update,
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
strategy_init_jobs(no_update, _, _) ->
    [];
strategy_init_jobs(_, _, #{last_import_time := undefined}) ->
    [];
strategy_init_jobs(simple_scan,
    Args = #{
        scan_interval := ScanIntervalSeconds,
        max_depth := MaxDepth
    },
    Data = #{
        last_import_time := LastImportTime
    }) ->
    case LastImportTime + timer:seconds(ScanIntervalSeconds) < os:system_time(milli_seconds) of
        true ->
            [#space_strategy_job{
                strategy_name = simple_scan,
                strategy_args = Args,
                data = Data#{max_depth => MaxDepth}
            }];
        false ->
            []
    end;
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StrategyArgs, InitData}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(Job = #space_strategy_job{strategy_name = simple_scan}) ->
    simple_scan:run(Job);
strategy_handle_job(#space_strategy_job{strategy_name = no_update}) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result(Jobs, Results) ->
    storage_import:strategy_merge_result(Jobs, Results).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(),
    LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(Job, LocalResult, ChildrenResult) ->
    storage_import:strategy_merge_result(Job, LocalResult, ChildrenResult).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback worker_pools_config/0.
%% @end
%%--------------------------------------------------------------------
-spec worker_pools_config() -> [{worker_pool:name(), non_neg_integer()}].
worker_pools_config() ->
    {ok, FileWorkersNum} = application:get_env(?APP_NAME, ?STORAGE_SYNC_FILE_WORKERS_NUM_KEY),
    {ok, DirWorkersNum} = application:get_env(?APP_NAME, ?STORAGE_SYNC_DIR_WORKERS_NUM_KEY),
    [
        {?STORAGE_SYNC_DIR_POOL_NAME, DirWorkersNum},
        {?STORAGE_SYNC_FILE_POOL_NAME, FileWorkersNum}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback main_worker_pool/0.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_pool() -> worker_pool:name().
main_worker_pool() ->
    ?STORAGE_SYNC_DIR_POOL_NAME.


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function responsible for starting storage update.
%% @end
%%--------------------------------------------------------------------
-spec start(od_space:id(), storage:id(), integer() | undefined, file_ctx:ctx(),
    file_meta:path()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
start(SpaceId, StorageId, LastImportTime, ParentCtx, FileName) ->
    InitialImportJobData = #{
        last_import_time => LastImportTime,
        space_id => SpaceId,
        storage_id => StorageId,
        file_name => FileName,
        parent_ctx => ParentCtx
    },
    ImportInit = space_sync_worker:init(?MODULE, SpaceId, StorageId,
        InitialImportJobData),
    space_sync_worker:run(ImportInit).

%%%===================================================================
%%% simple_scan callbacks
%%%===================================================================


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
        file_name := FileName,
        storage_file_ctx := StorageFileCtx0,
        space_id := SpaceId,
        storage_id := StorageId
    }
}) ->
    {#statbuf{
        st_mtime = StorageMtime,
        st_mode = Mode
    }, StorageFileCtx1} = storage_file_ctx:get_stat_buf(StorageFileCtx0),

    Job1 = Job0#space_strategy_job{data = Data0#{storage_file_ctx => StorageFileCtx1}},
    {LocalResult, FileCtx, Job2} = simple_scan:maybe_import_storage_file(Job1),
    Data2 = Job2#space_strategy_job.data,
    Offset = maps:get(dir_offset, Data2, 0),
    {ok, BatchSize} = application:get_env(?APP_NAME, dir_batch_size),
    Job3 = case Offset of
        0 ->
            Data3 = Data2#{mtime => StorageMtime},
            Job2#space_strategy_job{data=Data3};
        _ -> Job2
    end,

    SubJobs = import_children(Job3, file_meta:type(Mode), Offset, FileCtx, BatchSize),
    FileUuid = file_ctx:get_uuid_const(FileCtx),

    case storage_sync_utils:all_subfiles_imported(SubJobs, FileUuid) of
%%        todo fix setting counters, implement checking status in storage sync minitoring basing on coutners values
        true ->
            storage_sync_monitoring:update_files_to_update_counter(SpaceId, StorageId, length(SubJobs) - 1);
        false ->
            storage_sync_monitoring:update_files_to_update_counter(SpaceId, StorageId, length(SubJobs) - 2)
    end,
    {LocalResult, SubJobs}.


%%%--------------------------------------------------------------------
%%% @doc
%%% Updates mode, times and size of already imported file.
%%% Callback called by simple_scan module
%%% @end
%%%--------------------------------------------------------------------
-spec handle_already_imported_file(space_strategy:job(), #file_attr{},
    file_ctx:ctx()) -> {ok, space_strategy:job()}.
handle_already_imported_file(Job = #space_strategy_job{
    data = #{storage_file_ctx := StorageFileCtx}
},
    FileAttr, FileCtx
) ->
    {#statbuf{st_mode = Mode}, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),

    case file_meta:type(Mode) of
        ?DIRECTORY_TYPE ->
            handle_already_imported_directory(Job, FileAttr, FileCtx);
        ?REGULAR_FILE_TYPE ->
            simple_scan:handle_already_imported_file(Job, FileAttr, FileCtx)
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
    strategy_args = #{
        write_once := true}
}, Type = ?DIRECTORY_TYPE, Offset, FileCtx, BatchSize
) ->
    simple_scan:import_children(Job, Type, Offset, FileCtx, BatchSize);
import_children(Job = #space_strategy_job{
    strategy_name = StrategyName,
    strategy_type = StrategyType,
    strategy_args = #{
        write_once := false
    },
    data = Data0 = #{
        max_depth := MaxDepth,
        storage_file_ctx := StorageFileCtx,
        mtime := Mtime
    }},
    ?DIRECTORY_TYPE, Offset, FileCtx, BatchSize
) when MaxDepth > 0 ->

    BatchKey = Offset div BatchSize,
    {BatchHash, ChildrenStorageCtxsBatch, Data} =
        case storage_sync_utils:take_hash_for_batch(BatchKey, Data0) of
            {undefined, _} ->
                count_batch_hash(Offset, BatchSize, Data0, StorageFileCtx);
            {BatchHash0, Data1} ->
                {ChildrenStorageCtxsBatch0, Data2} =
                    storage_sync_utils:take_children_storage_ctxs_for_batch(BatchKey, Data1),
                {BatchHash0, ChildrenStorageCtxsBatch0, Data2}
        end,
    {FilesJobs, DirJobs} = simple_scan:generate_jobs_for_importing_children(
        Job#space_strategy_job{data = Data}, Offset, FileCtx, ChildrenStorageCtxsBatch),

    FilesResults = utils:pmap(fun(Job) ->
        worker_pool:call(?STORAGE_SYNC_FILE_POOL_NAME, {StrategyName, run, [Job]},
            worker_pool:default_strategy(), ?FILES_IMPORT_TIMEOUT)
    end, FilesJobs),

    case StrategyType:strategy_merge_result(FilesJobs, FilesResults) of
        true ->
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

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Handles update of directory that has already been imported.
%%% @end
%%%-------------------------------------------------------------------
-spec handle_already_imported_directory(space_strategy:job(), #file_attr{},
    file_ctx:ctx()) -> {ok, space_strategy:job()}.
handle_already_imported_directory(Job = #space_strategy_job{
    data = #{storage_file_ctx := StorageFileCtx}
}, FileAttr, FileCtx
) ->
    {#document{value = FileMeta}, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    case storage_sync_changes:mtime_has_changed(FileMeta, StorageFileCtx) of
        true ->
            handle_already_imported_directory_changed_mtime(Job, FileAttr, FileCtx2);
        false ->
            handle_already_imported_directory_unchanged_mtime(Job, FileAttr, FileCtx2)
    end.


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Handles update of directory that has already been imported and its
%%% mtime has changed (which means that children of this directory was
%%% created or deleted).
%%% @end
%%%-------------------------------------------------------------------
-spec handle_already_imported_directory_changed_mtime(space_strategy:job(),
    #file_attr{}, file_ctx:ctx()) -> {ok, space_strategy:job()}.
handle_already_imported_directory_changed_mtime(Job = #space_strategy_job{
    strategy_args = #{delete_enable := true}
}, _FileAttr, FileCtx) ->
    full_update:run(Job, FileCtx);
handle_already_imported_directory_changed_mtime(Job = #space_strategy_job{
    strategy_args = #{delete_enable := false}
}, FileAttr, FileCtx) ->
    simple_scan:handle_already_imported_file(Job, FileAttr, FileCtx).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Handles update of directory that has already been imported and its
%%% mtime has not changed.
%%% @end
%%%-------------------------------------------------------------------
-spec handle_already_imported_directory_unchanged_mtime(space_strategy:job(),
    #file_attr{}, file_ctx:ctx()) -> {ok, space_strategy:job()}.
handle_already_imported_directory_unchanged_mtime(Job = #space_strategy_job{
    strategy_args = #{write_once := false},
    data = Data0 = #{
        storage_file_ctx := StorageFileCtx
    }
}, FileAttr, FileCtx
) ->
    Offset = maps:get(dir_offset, Data0, 0),
    {#document{value = FileMeta}, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, BatchSize} = application:get_env(?APP_NAME, dir_batch_size),
    ChildrenStorageCtxsBatch = storage_file_ctx:get_children_ctxs_batch_const(
        StorageFileCtx, Offset, BatchSize),
    {BatchHash, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch),

    ChildrenStorageCtxs = maps:get(children_storage_file_ctxs, Data0, #{}),
    HashesMap = maps:get(hashes_map, Data0, #{}),
    BatchKey = Offset div BatchSize,

    Job2 = Job#space_strategy_job{
        data = Data0#{
            children_storage_file_ctxs => ChildrenStorageCtxs#{
                BatchKey => ChildrenStorageCtxsBatch2
            },
            hashes_map => HashesMap#{
                BatchKey => BatchHash
            },
            dir_offset => Offset
        }},

    case storage_sync_changes:children_attrs_hash_has_changed(FileMeta,
        BatchHash, BatchKey)
    of
        true ->
            handle_already_imported_directory_changed_hash(Job2, FileAttr,
                FileCtx2, BatchHash);
        false ->
            import_dirs_only(Job2, FileAttr, FileCtx2)
    end;
handle_already_imported_directory_unchanged_mtime(Job = #space_strategy_job{
    strategy_args = #{write_once := true}
}, FileAttr, FileCtx
) ->
    import_dirs_only(Job, FileAttr, FileCtx).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Handles update of directory that has already been imported and
%%% hash of its children hash has changed (which means that its children
%%% should be updated).
%%% @end
%%%-------------------------------------------------------------------
-spec handle_already_imported_directory_changed_hash(space_strategy:job(),
    #file_attr{}, file_ctx:ctx(), storage_sync_changes:hash()) -> {ok, space_strategy:job()}.
handle_already_imported_directory_changed_hash(Job = #space_strategy_job{
    data = Data0 = #{dir_offset := Offset}
}, FileAttr, FileCtx, CurrentHash
) ->

    {ok, DirBatch} = application:get_env(?APP_NAME, dir_batch_size),
    Job2 = Job#space_strategy_job{
        data = Data0#{
            hashes_map => #{Offset div DirBatch => CurrentHash}
        }},
    simple_scan:handle_already_imported_file(Job2, FileAttr, FileCtx).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% This functions adds flag import_dirs_only to Job's data so that
%%% only its subdirectories will be imported.
%%% @end
%%%-------------------------------------------------------------------
import_dirs_only(Job = #space_strategy_job{data = Data0},
    FileAttr, FileCtx
) ->
    Job2 = Job#space_strategy_job{data = Data0#{import_dirs_only => true}},
    simple_scan:handle_already_imported_file(Job2, FileAttr, FileCtx).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns hash for given batch of children files. Tries to get hash
%%% from job data. If it's not cached, it counts it.
%%% @end
%%%-------------------------------------------------------------------
-spec count_batch_hash(non_neg_integer(), non_neg_integer(),
    space_strategy:job_data(), storage_file_ctx:ctx()) ->
    {binary(), [storage_file_ctx:ctx()], space_strategy:job_data()}.
count_batch_hash(Offset, BatchSize, Data0, StorageFileCtx) ->
    BatchKey = Offset div BatchSize,
    {ChildrenStorageCtxsBatch1, Data1} =
        case storage_sync_utils:take_children_storage_ctxs_for_batch(BatchKey, Data0) of
            {undefined, _} ->
                get_children_ctxs_batch(Offset, BatchSize, Data0, StorageFileCtx);
            {ChildrenStorageCtxsBatch0, Data2} ->
                {ChildrenStorageCtxsBatch0, Data2}
        end,
    {BatchHash0, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch1),
    {BatchHash0, ChildrenStorageCtxsBatch2, Data1}.


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns list of storage_file_ctx for given batch. Tries to get if
%%% from job data. If it's not there, it lists directory on storage.
%%% @end
%%%-------------------------------------------------------------------
-spec get_children_ctxs_batch(non_neg_integer(), non_neg_integer(),
    space_strategy:job_data(), storage_file_ctx:ctx()) ->
    {[storage_file_ctx:ctx()], space_strategy:job_data()}.
get_children_ctxs_batch(Offset, BatchSize, Data0, StorageFileCtx) ->
    ChildrenStorageCtxsBatch0 =
        storage_file_ctx:get_children_ctxs_batch_const(
            StorageFileCtx, Offset, BatchSize),
    {ChildrenStorageCtxsBatch0, Data0}.