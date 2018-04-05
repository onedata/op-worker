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
-export([run/2, delete_imported_file_and_update_counters/3, delete_imported_file/2]).

%%-------------------------------------------------------------------
%% @doc
%% Runs full update in directory referenced by FileName.
%% Deletes all files that exist in database but were deleted on storage
%% @end
%%-------------------------------------------------------------------
-spec run(space_strategy:job(), file_ctx:ctx()) -> {ok, space_strategy:job()}.
run(Job = #space_strategy_job{
    data = #{
        space_id := SpaceId,
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
        ok = remove_files_not_existing_on_storage(StorageTable, DBTable, FileCtx, SpaceId),
        storage_sync_monitoring:increase_updated_files_counter(SpaceId)
    catch
        _:_ ->
            ?error("Error in full_update:run for file: ~p", [FileName]),
            storage_sync_monitoring:increase_failed_file_updates_counter(SpaceId)

    after
        true = ets:delete(StorageTable),
        true = ets:delete(DBTable)
    end,
    {ok, Job}.

%%-------------------------------------------------------------------
%% @doc
%% Remove files that had been earlier imported and updates suitable counters.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_file_and_update_counters(file_meta:name(),
    file_ctx:ctx(), od_space:id()) -> ok.
delete_imported_file_and_update_counters(ChildName, FileCtx, SpaceId) ->
    storage_sync_monitoring:update_queue_length_spirals(SpaceId, -1),
    try
        delete_imported_file(ChildName, FileCtx),
        storage_sync_monitoring:increase_deleted_files_spirals(SpaceId),
        storage_sync_monitoring:increase_deleted_files_counter(SpaceId)
    catch
        _:_ ->
            storage_sync_monitoring:increase_failed_file_deletions_counter(SpaceId)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Remove files that had been earlier imported.
%% @end
%%-------------------------------------------------------------------
-spec delete_imported_file(file_meta:name(), file_ctx:ctx()) -> ok.
delete_imported_file(ChildName, FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {ChildCtx, _} = file_ctx:get_child(FileCtx, ChildName, RootUserCtx),
        ok = fslogic_delete:remove_file_and_file_meta(ChildCtx, RootUserCtx,
            false, false, true),
        ok = fslogic_delete:remove_file_handles(ChildCtx)
    catch
        throw:?ENOENT  ->
            ok
    end.

%%===================================================================
%% Internal functions
%%===================================================================

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
    od_space:id()) -> ok.
remove_files_not_existing_on_storage(StorageTable, DBTable, FileCtx, SpaceId) ->
    StorageFirst = ets:first(StorageTable),
    DBFirst = ets:first(DBTable),
    ok = iterate_and_remove(StorageFirst, StorageTable, DBFirst, DBTable, 0,
        ?DIR_BATCH, FileCtx, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function iterates over 2 ETS tables (one for db children one
%% for storage children) and deletes children from db, that doesn't
%% have matching files on storage.
%% @end
%%-------------------------------------------------------------------
-spec iterate_and_remove(key(), atom(), key(), atom(), non_neg_integer(),
    non_neg_integer(), file_ctx:ctx(), od_space:id()) -> ok.
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Offset, BatchSize,
    FileCtx, SpaceId)
when (Offset + 1) rem BatchSize == 0  ->
    % finished batch
    storage_sync_monitoring:update_files_to_sync_counter(SpaceId, 1),
    iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Offset + 1,
        BatchSize, FileCtx, SpaceId);
iterate_and_remove('$end_of_table', _, '$end_of_table', _,  _Offset, _BatchSize,
    _FileCtx, _SpaceId) ->
    ok;
iterate_and_remove(StKey, StorageTable, DBKey = '$end_of_table', DBTable,
    Offset, BatchSize, FileCtx, SpaceId
) ->
    Next = ets:next(StorageTable, StKey),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, Offset,
        BatchSize, FileCtx, SpaceId);
iterate_and_remove(StKey = '$end_of_table', StTable, DBKey, DBTable, Offset,
    BatchSize, FileCtx, SpaceId
) ->
    Next = ets:next(DBTable, DBKey),
    storage_sync_monitoring:update_files_to_sync_counter(SpaceId, 1),
    cast_deletion_of_imported_file(DBKey, FileCtx, SpaceId),
    iterate_and_remove(StKey, StTable, Next, DBTable, Offset,
        BatchSize, FileCtx, SpaceId);
iterate_and_remove(Key, StorageTable, Key, DBTable, Offset, BatchSize, FileCtx,
    SpaceId
) ->
    StNext = ets:next(StorageTable, Key),
    DBNext = ets:next(DBTable, Key),
    iterate_and_remove(StNext, StorageTable, DBNext, DBTable, Offset, BatchSize,
        FileCtx, SpaceId);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Offset, BatchSize,
    FileCtx, SpaceId)
when StKey > DBKey ->
    Next = ets:next(DBTable, DBKey),
    storage_sync_monitoring:update_files_to_sync_counter(SpaceId, 1),
    cast_deletion_of_imported_file(DBKey, FileCtx, SpaceId),
    iterate_and_remove(StKey, StorageTable, Next, DBTable, Offset, BatchSize,
        FileCtx, SpaceId);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Offset, BatchSize,
    FileCtx, SpaceId
) ->
    Next = ets:next(StorageTable, StKey),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, Offset, BatchSize,
        FileCtx, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Iterates over all children (in db) of file associated with FileCtx
%% and save their names in ets TableName.
%% @end
%%-------------------------------------------------------------------
-spec save_db_children_names(atom(), file_ctx:ctx()) -> term().
save_db_children_names(TableName, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    file_meta:foreach_child({uuid, FileUuid}, fun
        (LinkName, _LinkTarget, AccIn) ->
            case file_meta:is_hidden(LinkName) of
                true ->
                    AccIn;
                false ->
                    ets:insert(TableName, {LinkName, undefined}),
                    AccIn
            end
    end, []).

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
%% @private
%% @doc
%% Remove files that had been earlier imported.
%% @end
%%-------------------------------------------------------------------
-spec cast_deletion_of_imported_file(file_meta:name(), file_ctx:ctx(), od_space:id()) -> ok.
cast_deletion_of_imported_file(ChildName, FileCtx, SpaceId) ->
    storage_sync_monitoring:update_queue_length_spirals(SpaceId, 1),
    delete_imported_file_and_update_counters(ChildName, FileCtx, SpaceId).