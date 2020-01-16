%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing full_update for
%%% storage_update strategy.
%%% @end
%%%-------------------------------------------------------------------
-module(full_update).
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-define(STORAGE_TABLE_PREFIX, <<"st_children_">>).
-define(DB_TABLE_PREFIX, <<"db_children_">>).

-type key() :: file_meta:name() | atom().

%% API
-export([run/2, delete_imported_file/2]).

%%-------------------------------------------------------------------
%% @doc
%% Runs full update in directory referenced by FileName.
%% Deletes all files that exist in database but were deleted on storage
%% @end
%%-------------------------------------------------------------------
-spec run(space_strategy:job(), file_ctx:ctx()) -> ok | {error, term()}.
run(#space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId,
        storage_file_ctx := StorageFileCtx,
        file_name := FileName
    }}, FileCtx) ->

    FileUuid = file_ctx:get_uuid_const(FileCtx),
    StorageTable = storage_table_name(FileUuid),
    DBTable = db_storage_name(FileUuid),
    StorageTable = create_ets(StorageTable),
    DBTable = create_ets(DBTable),
    try
        save_db_children_names(DBTable, FileCtx),
        save_storage_children_names(StorageTable, StorageFileCtx),
        ok = remove_files_not_existing_on_storage(StorageTable, DBTable, FileCtx, SpaceId, StorageId)
    catch
        Error:Reason ->
            ?error_stacktrace("full_update:run failed for file: ~p due to ~p",
                [FileName, {Error, Reason}]),
            {error, {Error, Reason}}
    after
        true = ets:delete(StorageTable),
        true = ets:delete(DBTable)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Remove files that had been earlier imported and updates suitable counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_imported_file_and_update_counters(file_meta:name(),
    file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
maybe_delete_imported_file_and_update_counters(ChildName, ParentCtx, SpaceId, StorageId) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {FileCtx, _ParentCtx2} = file_ctx:get_child(ParentCtx, ChildName, UserCtx),
        maybe_delete_imported_file_and_update_counters(FileCtx, SpaceId, StorageId)
    catch
        throw:?ENOENT ->
            storage_sync_monitoring:mark_processed_file(SpaceId, StorageId),
            ok;
        Error:Reason ->
            ?error_stacktrace(
                "full_update:maybe_delete_imported_file_and_update_counters for file ~p failed with ~p",
                [ChildName, {Error, Reason}]
            ),
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% This functions checks whether file is a directory or a regular file
%% and delegates decision about deleting or not deleting file to
%% suitable functions.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_imported_file_and_update_counters(file_ctx:ctx(), od_space:id(),
    storage:id()) -> ok.
maybe_delete_imported_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    try
        {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
        case IsDir of
            true ->
                maybe_delete_imported_dir_and_update_counters(
                    FileCtx2, SpaceId, StorageId);
            false ->
                maybe_delete_imported_regular_file_and_update_counters(
                    FileCtx2, SpaceId, StorageId)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("maybe_delete_imported_file_and_update_counters failed due to ~p",
                [{Error, Reason}]),
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            ok
    end.

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
        delete_imported_file(FileCtx)
    catch
        throw:?ENOENT ->
            ok
    end.

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Remove files that had been earlier imported.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_file(file_ctx:ctx()) -> ok.
delete_imported_file(FileCtx) ->
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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Remove directory that had been earlier imported.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_dir(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_imported_dir(FileCtx, SpaceId, StorageId) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    {ok, FileCtx2} = delete_imported_dir_children(FileCtx, RootUserCtx, 0,
        ChunkSize, SpaceId, StorageId),
    delete_imported_file(FileCtx2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively deleted children of imported directory.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_dir_children(file_ctx:ctx(), user_ctx:ctx(),
    non_neg_integer(), non_neg_integer(), od_space:id(), storage:id()) -> {ok, file_ctx:ctx()}.
delete_imported_dir_children(FileCtx, UserCtx, Offset, ChunkSize, SpaceId, StorageId) ->
    try
        {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx,
            UserCtx, Offset, ChunkSize),
        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, length(ChildrenCtxs)),
        lists:foreach(fun(ChildCtx) ->
            maybe_delete_imported_file_and_update_counters(ChildCtx, SpaceId, StorageId)
        end, ChildrenCtxs),
        case length(ChildrenCtxs) < ChunkSize of
            true ->
                {ok, FileCtx2};
            false ->
                delete_imported_dir_children(FileCtx2, UserCtx,
                    Offset + ChunkSize, ChunkSize, SpaceId, StorageId)
        end
    catch
        throw:?ENOENT ->
            {ok, FileCtx}
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc @equiv ets_name(?STORAGE_TABLE_PREFIX, FileUuid).
%% @end
%%-------------------------------------------------------------------
-spec storage_table_name(file_meta:uuid()) -> atom().
storage_table_name(FileUuid) ->
    ets_name(?STORAGE_TABLE_PREFIX, FileUuid).

%%-------------------------------------------------------------------
%% @private
%% @doc @equiv  ets_name(?DB_TABLE_PREFIX, FileUuid).
%% @end
%%-------------------------------------------------------------------
-spec db_storage_name(file_meta:uuid()) -> atom().
db_storage_name(FileUuid) ->
    ets_name(?DB_TABLE_PREFIX, FileUuid).

%%-------------------------------------------------------------------
%% @private
%% @doc Returns atom Prefix_fileUuid name for ets.
%% @end
%%-------------------------------------------------------------------
-spec ets_name(binary(), file_meta:uuid()) -> atom().
ets_name(Prefix, FileUuid) ->
    binary_to_atom(<<Prefix/binary, FileUuid/binary>>, latin1).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv ets:new(Name, [named_table, ordered_set, public]).
%% @end
%%-------------------------------------------------------------------
-spec create_ets(atom()) -> atom().
create_ets(Name) ->
    Name = ets:new(Name, [named_table, ordered_set, public]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function removes all files from database that doesn't have matching
%% files on storage.
%% @end
%%-------------------------------------------------------------------
-spec remove_files_not_existing_on_storage(atom(), atom(), file_ctx:ctx(),
    od_space:id(), storage:id()) -> ok.
remove_files_not_existing_on_storage(StorageTable, DBTable, FileCtx, SpaceId, StorageId) ->
    StorageFirst = ets:first(StorageTable),
    DBFirst = ets:first(DBTable),
    ok = iterate_and_remove(StorageFirst, StorageTable, DBFirst, DBTable,
        FileCtx, SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function iterates over 2 ETS tables (one for db children one
%% for storage children) and deletes children from db, that doesn't
%% have matching files on storage.
%% @end
%%-------------------------------------------------------------------
-spec iterate_and_remove(key(), atom(), key(), atom(),file_ctx:ctx(),
    od_space:id(), storage:id()) -> ok.
iterate_and_remove('$end_of_table', _, '$end_of_table', _, _FileCtx, _SpaceId, _StorageId) ->
    ok;
iterate_and_remove(StKey, StorageTable, DBKey = '$end_of_table', DBTable,
    FileCtx, SpaceId, StorageId
) ->
    Next = ets:next(StorageTable, StKey),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, FileCtx, SpaceId, StorageId);
iterate_and_remove(StKey = '$end_of_table', StTable, DBKey, DBTable, FileCtx,
    SpaceId, StorageId
) ->
    Next = ets:next(DBTable, DBKey),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
    maybe_delete_imported_file_and_update_counters(DBKey, FileCtx, SpaceId, StorageId),
    iterate_and_remove(StKey, StTable, Next, DBTable, FileCtx, SpaceId, StorageId);
iterate_and_remove(Key, StorageTable, Key, DBTable, FileCtx, SpaceId, StorageId) ->
    StNext = ets:next(StorageTable, Key),
    DBNext = ets:next(DBTable, Key),
    iterate_and_remove(StNext, StorageTable, DBNext, DBTable, FileCtx, SpaceId,
        StorageId);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, FileCtx, SpaceId, StorageId
) when StKey > DBKey ->
    Next = ets:next(DBTable, DBKey),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
    maybe_delete_imported_file_and_update_counters(DBKey, FileCtx, SpaceId, StorageId),
    iterate_and_remove(StKey, StorageTable, Next, DBTable, FileCtx, SpaceId, StorageId);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, FileCtx, SpaceId, StorageId) ->
    Next = ets:next(StorageTable, StKey),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, FileCtx, SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (in db) of file associated with FileCtx
%% and save their names in ets TableName.
%% @end
%%-------------------------------------------------------------------
-spec save_db_children_names(atom(), file_ctx:ctx()) -> ok.
save_db_children_names(TableName, FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    save_db_children_names(TableName, FileCtx, UserCtx, 0, ?DIR_BATCH),
    First = ets:first(TableName),
    filter_children_in_db(First, FileCtx, UserCtx, TableName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Helper function for save_db_children_names/2
%% @end
%%-------------------------------------------------------------------
-spec save_db_children_names(atom(), file_ctx:ctx(), user_ctx:ctx(),
    non_neg_integer(), non_neg_integer()) -> ok.
save_db_children_names(TableName, FileCtx, UserCtx, Offset, Batch) ->
    {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Batch),
    lists:foreach(fun(ChildCtx) ->
        {FileName, _} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
        ets:insert(TableName, {FileName, undefined})
    end, ChildrenCtxs),

    case length(ChildrenCtxs) < Batch of
        true ->
            ok;
        false ->
            save_db_children_names(TableName, FileCtx2, UserCtx, Offset + Batch, Batch)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (in db ets) and deletes files/dirs without
%% location doc.
%% @end
%%-------------------------------------------------------------------
-spec filter_children_in_db(key(), file_ctx:ctx(), user_ctx:ctx(), atom()) -> ok.
filter_children_in_db('$end_of_table', _FileCtx, _UserCtx, _TableName) ->
    ok;
filter_children_in_db(LinkName, FileCtx, UserCtx, TableName) ->
    try
        {ChildCtx, _} = file_ctx:get_child(FileCtx, LinkName, UserCtx),
        {IsDir, ChildCtx2} = file_ctx:is_dir(ChildCtx),
        case IsDir of
            true ->
                {DirLocation, _} = file_ctx:get_dir_location_doc(ChildCtx2),
                case dir_location:is_storage_file_created(DirLocation) of
                    true ->
                        ok;
                    _ ->
                        ets:delete(TableName, LinkName)
                end;
            _ ->
                {FileLocation, _} = file_ctx:get_local_file_location_doc(ChildCtx2, false),
                case file_location:is_storage_file_created(FileLocation) of
                    true ->
                        ok;
                    _ ->
                        ets:delete(TableName, LinkName)
                end
        end
    catch
        throw:?ENOENT ->
            ?warning_stacktrace("full_update:filter_children_in_db failed with enoent for file ~p", [LinkName]),
            ets:delete(TableName, LinkName);
        Error:Reason  ->
            ?error_stacktrace("full_update:filter_children_in_db failed with unexpected ~p:~p for file ~p", [Error, Reason, LinkName]),
            ets:delete(TableName, LinkName)
    end,
    filter_children_in_db(ets:next(TableName, LinkName), FileCtx, UserCtx, TableName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (on storage) of file associated with
%% StorageFileCtx and save their names in ets TableName.
%% @end
%%-------------------------------------------------------------------
-spec save_storage_children_names(atom(), storage_file_ctx:ctx()) -> term().
save_storage_children_names(TableName, StorageFileCtx) ->
    save_storage_children_names(TableName, StorageFileCtx, 0, ?DIR_BATCH).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (on storage) of file associated with
%% StorageFileCtx and save their names in ets TableName.
%% @end
%%-------------------------------------------------------------------
-spec save_storage_children_names(atom(), storage_file_ctx:ctx(),
    non_neg_integer(), non_neg_integer()) -> term().
save_storage_children_names(TableName, StorageFileCtx, Offset, BatchSize) ->
    {SFMHandle, StorageFileCtx2} = storage_file_ctx:get_handle(StorageFileCtx),
    {ok, ChildrenIds} = storage_file_manager:readdir(SFMHandle, Offset, BatchSize),

    lists:foreach(fun(ChildId) ->
        case file_meta:is_hidden(ChildId) of
            false ->
                ets:insert(TableName, {filename:basename(ChildId), undefined});
            _ -> ok
        end
    end, ChildrenIds),

    ListedChildrenNumber = length(ChildrenIds),
    case ListedChildrenNumber < BatchSize of
        true ->
            ok;
        _ ->
            save_storage_children_names(TableName, StorageFileCtx2,
                Offset + ListedChildrenNumber, BatchSize)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given directory can be deleted by sync.
%% If true, this function deletes it and update syn counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_imported_dir_and_update_counters(file_ctx:ctx(),
    od_space:id(), storage:id()) -> ok.
maybe_delete_imported_dir_and_update_counters(FileCtx, SpaceId, StorageId) ->
    delete_imported_dir(FileCtx, SpaceId, StorageId),
    {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx),
    storage_sync_utils:log_deletion(StorageFileId, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% This function deletes regular file and update syn counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_imported_regular_file_and_update_counters(file_ctx:ctx(),
    od_space:id(), storage:id()) -> ok.
maybe_delete_imported_regular_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    delete_imported_file(FileCtx),
    {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx),
    storage_sync_utils:log_deletion(StorageFileId, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId),
    ok.


