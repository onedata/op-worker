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

-define(ETS_NAME(Prefix, DirUuid), binary_to_atom(<<Prefix/binary, DirUuid/binary>>, utf8)).

-define(STORAGE_TABLE_PREFIX, <<"st_children_">>).
-define(STORAGE_TABLE(DirUuid), ?ETS_NAME(?STORAGE_TABLE_PREFIX, DirUuid)).

-define(DB_TABLE_PREFIX, <<"db_children_">>).
-define(DB_TABLE(DirUuid), ?ETS_NAME(?DB_TABLE_PREFIX, DirUuid)).

-type key() :: file_meta:name() | '$end_of_table'.

%% API
-export([run/2, delete_file/2, delete_file_and_update_counters/3]).

%% exported for mocking in tests
-export([save_storage_children_names/2, storage_readdir/3]).

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
    }
}, DirCtx) ->
    DirUuid = file_ctx:get_uuid_const(DirCtx),
    StorageTable = ?STORAGE_TABLE(DirUuid),
    DBTable = ?DB_TABLE(DirUuid),
    StorageTable = create_ets(StorageTable),
    DBTable = create_ets(DBTable),
    try
        DirCtx2 = save_db_children_names(DBTable, DirCtx),
        full_update:save_storage_children_names(StorageTable, StorageFileCtx),
        ok = remove_files_not_existing_on_storage(StorageTable, DBTable, DirCtx2, SpaceId, StorageId)
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
%% This functions checks whether file is a directory or a regular file
%% and delegates deleting the file to suitable functions.
%% @end
%%-------------------------------------------------------------------
-spec delete_file_and_update_counters(file_ctx:ctx(), od_space:id(),
    storage:id()) -> ok.
delete_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    try
        {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
        case IsDir of
            true ->
                delete_dir_recursive_and_update_counters(FileCtx2, SpaceId, StorageId);
            false ->
                delete_regular_file_and_update_counters(FileCtx2, SpaceId, StorageId)
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
-spec delete_file(file_meta:name(), file_ctx:ctx()) -> ok.
delete_file(ChildName, ParentCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {FileCtx, _} = file_ctx:get_child(ParentCtx, ChildName, RootUserCtx),
        delete_file(FileCtx)
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
%% Remove files that had been earlier imported and updates suitable counters.
%% @end
%%-------------------------------------------------------------------
-spec maybe_delete_file_and_update_counters(file_meta:name(), file_meta:uuid(),
    file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
maybe_delete_file_and_update_counters(ChildName, ChildUuid, ParentCtx, SpaceId, StorageId) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {FileCtx, _ParentCtx2} = file_ctx:get_child(ParentCtx, ChildName, UserCtx),
        {SFMHandle, FileCtx2} = storage_file_manager:new_handle(?ROOT_SESS_ID, FileCtx),
        case {file_ctx:get_uuid_const(FileCtx), storage_file_manager:stat(SFMHandle)} of
            {ChildUuid, {error, ?ENOENT}} ->
                % uuid matches ChildUuid from ets, and file is still missing on storage
                % we can delete it from db
                delete_file_and_update_counters(FileCtx2, SpaceId, StorageId);
            _ ->
                storage_sync_monitoring:mark_processed_file(SpaceId, StorageId),
                ok
        end
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
%% @private
%% @doc
%% Deletes file (regular or directory) associated with FileCtx.
%% If the file is a directory, it is deleted recursively.
%% @end
%%-------------------------------------------------------------------
-spec delete_file(file_ctx:ctx()) -> ok.
delete_file(FileCtx) ->
    try
        ok = fslogic_delete:handle_file_deleted_on_synced_storage(FileCtx),
        fslogic_event_emitter:emit_file_removed(FileCtx, []),
        ok = fslogic_delete:remove_file_handles(FileCtx),
        fslogic_delete:remove_auxiliary_documents(FileCtx)
    catch
        throw:?ENOENT ->
            ok
    end.

-spec delete_dir_recursive(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_dir_recursive(FileCtx, SpaceId, StorageId) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    {ok, FileCtx2} = delete_children(FileCtx, RootUserCtx, 0, ChunkSize, SpaceId, StorageId),
    delete_file(FileCtx2).


-spec delete_children(file_ctx:ctx(), user_ctx:ctx(),
    non_neg_integer(), non_neg_integer(), od_space:id(), storage:id()) -> {ok, file_ctx:ctx()}.
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
-spec iterate_and_remove(key(), ets:tab(), key(), ets:tab(), file_ctx:ctx(),
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
    [{DBKey, ChildUuid}] = ets:lookup(DBTable, DBKey),
    maybe_delete_file_and_update_counters(DBKey, ChildUuid, FileCtx, SpaceId, StorageId),
    iterate_and_remove(StKey, StTable, Next, DBTable, FileCtx, SpaceId, StorageId);
iterate_and_remove(Key, StorageTable, Key, DBTable, FileCtx, SpaceId, StorageId) ->
    StNext = ets:next(StorageTable, Key),
    DBNext = ets:next(DBTable, Key),
    iterate_and_remove(StNext, StorageTable, DBNext, DBTable, FileCtx, SpaceId, StorageId);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, FileCtx, SpaceId, StorageId
) when StKey > DBKey ->
    Next = ets:next(DBTable, DBKey),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
    [{DBKey, ChildUuid}] = ets:lookup(DBTable, DBKey),
    maybe_delete_file_and_update_counters(DBKey, ChildUuid, FileCtx, SpaceId, StorageId),
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
-spec save_db_children_names(ets:tab(), file_ctx:ctx()) -> file_ctx:ctx().
save_db_children_names(TableName, FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    FileCtx2 = save_db_children_names(TableName, FileCtx, UserCtx, 0, ?DIR_BATCH),
    First = ets:first(TableName),
    filter_children_in_db(First, FileCtx2, UserCtx, TableName).

-spec save_db_children_names(ets:tab(), file_ctx:ctx(), user_ctx:ctx(),
    non_neg_integer(), non_neg_integer()) -> file_ctx:ctx().
save_db_children_names(TableName, FileCtx, UserCtx, Offset, Batch) ->
    {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Batch),
    lists:foreach(fun(ChildCtx) ->
        {FileName, _} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
        FileUuid = file_ctx:get_uuid_const(ChildCtx),
        % save FileUuid to ensure whether proper file will be deleted
        % in case of race with recreating file with the same name
        ets:insert(TableName, {FileName, FileUuid})
    end, ChildrenCtxs),

    case length(ChildrenCtxs) < Batch of
        true ->
            FileCtx2;
        false ->
            save_db_children_names(TableName, FileCtx2, UserCtx, Offset + Batch, Batch)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (in db ets) and deletes files/dirs without
%% location doc.
%% This ensures that remotely created files won't be deleted.
%% @end
%%-------------------------------------------------------------------
-spec filter_children_in_db(key(), file_ctx:ctx(), user_ctx:ctx(), atom()) -> file_ctx:ctx().
filter_children_in_db('$end_of_table', FileCtx, _UserCtx, _TableName) ->
    FileCtx;
filter_children_in_db(LinkName, FileCtx, UserCtx, TableName) ->
    FileCtx3 = try
        {ChildCtx, FileCtx2} = file_ctx:get_child(FileCtx, LinkName, UserCtx),
        case file_ctx:is_storage_file_created_const(ChildCtx) of
            true ->
                FileCtx2;
            false ->
                ets:delete(TableName, LinkName),
                FileCtx2
        end
    catch
        throw:?ENOENT ->
            ?debug_stacktrace("full_update:filter_children_in_db failed with enoent for file ~p", [LinkName]),
            ets:delete(TableName, LinkName),
            FileCtx;
        Error:Reason  ->
            ?error_stacktrace("full_update:filter_children_in_db failed with unexpected ~p:~p for file ~p", [Error, Reason, LinkName]),
            ets:delete(TableName, LinkName),
            FileCtx
    end,
    filter_children_in_db(ets:next(TableName, LinkName), FileCtx3, UserCtx, TableName).


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

-spec save_storage_children_names(atom(), storage_file_ctx:ctx(),
    non_neg_integer(), non_neg_integer()) -> term().
save_storage_children_names(TableName, StorageFileCtx, Offset, BatchSize) ->
    {SFMHandle, StorageFileCtx2} = storage_file_ctx:get_handle(StorageFileCtx),
    {ok, ChildrenIds} = full_update:storage_readdir(SFMHandle, Offset, BatchSize),

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
            save_storage_children_names(TableName, StorageFileCtx2, Offset + ListedChildrenNumber, BatchSize)
    end.


-spec delete_dir_recursive_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_dir_recursive_and_update_counters(FileCtx, SpaceId, StorageId) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    delete_dir_recursive(FileCtx3, SpaceId, StorageId),
    storage_sync_utils:log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId),
    ok.


-spec delete_regular_file_and_update_counters(file_ctx:ctx(), od_space:id(), storage:id()) -> ok.
delete_regular_file_and_update_counters(FileCtx, SpaceId, StorageId) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    delete_file(FileCtx3),
    storage_sync_utils:log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    storage_sync_monitoring:mark_deleted_file(SpaceId, StorageId),
    ok.


-spec storage_readdir(storage_file_manager:handle(), non_neg_integer(), non_neg_integer()) ->
    {ok, [helpers:file_id()]} | {error, term()}.
storage_readdir(SFMHandle, Offset, BatchSize) ->
    storage_file_manager:readdir(SFMHandle, Offset, BatchSize).