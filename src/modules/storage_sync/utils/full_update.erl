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


%% API
-export([run/2]).

-define(STORAGE_TABLE_PREFIX, <<"st_children_">>).
-define(DB_TABLE_PREFIX, <<"db_children_">>).

-record(iteration_context, {
    offset = 0 :: non_neg_integer(),
    dir_batch_size :: non_neg_integer(),
    children_storage_ctxs_batch = [] :: [storage_file_ctx:ctx()]
}).

%% TODO maybe delete iteration context as we don't cache children stfilectxs
%%%-------------------------------------------------------------------
%%% @doc
%%% Runs full update in directory referenced by FileName.
%%% Deletes all files that exist in database but were deleted on storage
%%% @end
%%%-------------------------------------------------------------------
-spec run(space_strategy:job(), file_ctx:ctx()) ->
    {ok, file_ctx:ctx(), space_strategy:job()}.
run(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        storage_file_ctx := StorageFileCtx
    }}, FileCtx) ->

    FileUuid = file_ctx:get_uuid_const(FileCtx),
    StorageTable = storage_table_name(FileUuid),
    DBTable = db_storage_name(FileUuid),
    StorageTable = create_ets(StorageTable),
    DBTable = create_ets(DBTable),
    save_db_children_names(DBTable, FileCtx),
    save_storage_children_names(StorageTable, StorageFileCtx),

    Job2 = remove_files_not_existing_on_storage(StorageTable, DBTable, Job, FileCtx),
    true = ets:delete(StorageTable),
    true = ets:delete(DBTable),

    {ok, Job2}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
%%% @private
%%% @doc
%%% @equiv ets:new(Name, [named_table, ordered_set, public]).
%%% @end
%%-------------------------------------------------------------------
-spec create_ets(atom()) -> atom().
create_ets(Name) ->
    Name = ets:new(Name, [named_table, ordered_set, public]).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% This function removes all files from database that doesn't have matching
%%% files on storage.
%%% @end
%%%-------------------------------------------------------------------
-spec remove_files_not_existing_on_storage(atom(), atom(), space_strategy:job(),
    file_ctx:ctx()) -> space_strategy:job().
remove_files_not_existing_on_storage(StorageTable, DBTable, Job, FileCtx) ->
    {ok, BatchSize} = application:get_env(?APP_NAME, dir_batch_size),
    StorageFirst = ets:first(StorageTable),
    DBFirst = ets:first(DBTable),
    ItCtx = #iteration_context{dir_batch_size = BatchSize},
    {Job2, _} = iterate_and_remove(StorageFirst, StorageTable, DBFirst,
        DBTable, Job, ItCtx, FileCtx),
    Job2.

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% This function iterates over 2 ETS tables (one for db children one
%%% for storage children) and deletes children from db, that doesn't
%%% have matching files on storage.
%%% @end
%%%-------------------------------------------------------------------
-spec iterate_and_remove(file_meta:name(), atom(), file_meta:name(), atom(),
    space_strategy:job(), #iteration_context{}, file_ctx:ctx()) -> space_strategy:job().
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Job,
    ItCtx = #iteration_context{
        offset = Offset,
        dir_batch_size = DirBatchSize
    }, FileCtx) when (Offset + 1) rem DirBatchSize == 0 % finished batch
    ->
    iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Job, finish_batch(ItCtx), FileCtx);
iterate_and_remove('$end_of_table', _, '$end_of_table', _, Job, IterationCtx, _FileCtx) ->
    {Job, finish_batch(IterationCtx)};
iterate_and_remove(StKey, StorageTable, DBKey = '$end_of_table', DBTable, Job,
    ItCtx, FileCtx
) ->
    ChildStorageCtx = get_child_storage_ctx(Job, StKey),
    Next = ets:next(StorageTable, StKey),
    ItCtx2 = add_child_storage_ctx(ChildStorageCtx, ItCtx),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, Job, ItCtx2, FileCtx);
iterate_and_remove(StKey = '$end_of_table', StTable, DBKey, DBTable, Job, AccIn, FileCtx) ->
    Next = ets:next(DBTable, DBKey),
    delete_imported_file(Job, DBKey, FileCtx),
    iterate_and_remove(StKey, StTable, Next, DBTable, Job, AccIn, FileCtx);
iterate_and_remove(Key, StorageTable, Key, DBTable, Job, ItCtx, FileCtx) ->
    StNext = ets:next(StorageTable, Key),
    DBNext = ets:next(DBTable, Key),
    ChildStorageCtx = get_child_storage_ctx(Job, Key),
    ItCtx2 = add_child_storage_ctx(ChildStorageCtx, ItCtx),
    iterate_and_remove(StNext, StorageTable, DBNext, DBTable, Job, ItCtx2, FileCtx);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Job, ItCtx, FileCtx) when StKey > DBKey ->
    Next = ets:next(DBTable, DBKey),
    delete_imported_file(Job, DBKey, FileCtx),
    iterate_and_remove(StKey, StorageTable, Next, DBTable, Job, ItCtx, FileCtx);
iterate_and_remove(StKey, StorageTable, DBKey, DBTable, Job, ItCtx, FileCtx) ->
    ChildStorageCtx = get_child_storage_ctx(Job, StKey),
    Next = ets:next(StorageTable, StKey),
    ItCtx2 = add_child_storage_ctx(ChildStorageCtx, ItCtx),
    iterate_and_remove(Next, StorageTable, DBKey, DBTable, Job, ItCtx2, FileCtx).



%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Iterates over all children (in db) of file associated with FileCtx
%%% and save their names in ets TableName.
%%% @end
%%%-------------------------------------------------------------------
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


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Iterates over all children (on storage) of file associated with
%%% StorageFileCtx and save their names in ets TableName.
%%% @end
%%%-------------------------------------------------------------------
-spec save_storage_children_names(atom(), storage_file_ctx:ctx()) -> term().
save_storage_children_names(TableName, StorageFileCtx) ->
    {ok, BatchSize} = application:get_env(?APP_NAME, dir_batch_size),
    save_storage_children_names(TableName, StorageFileCtx, 0, BatchSize).

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Iterates over all children (on storage) of file associated with
%%% StorageFileCtx and save their names in ets TableName.
%%% @end
%%%-------------------------------------------------------------------
-spec save_storage_children_names(atom(), storage_file_ctx:ctx(),
    non_neg_integer(), non_neg_integer()) -> term().
save_storage_children_names(TableName, StorageFileCtx, Offset, BatchSize) ->
    SFMHandle = storage_file_ctx:get_handle_const(StorageFileCtx),
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
            save_storage_children_names(TableName, StorageFileCtx,
                Offset + ListedChildrenNumber, BatchSize)
    end.

finish_batch(IterationCtx = #iteration_context{offset = Offset}) ->
    IterationCtx#iteration_context{offset = Offset + 1}.

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Adds child storage_file_ctx to iteration_context
%%% @end
%%%-------------------------------------------------------------------
-spec add_child_storage_ctx(storage_file_ctx:ctx(), #iteration_context{}) ->
    #iteration_context{}.
add_child_storage_ctx(ChildStorageFileCtx, ItCtx = #iteration_context{
    children_storage_ctxs_batch = CurrentCtxs
}) ->
    ItCtx#iteration_context{
        children_storage_ctxs_batch = [ChildStorageFileCtx | CurrentCtxs]}.


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns storage_file_ctx of child with given name.
%%% @end
%%%-------------------------------------------------------------------
-spec get_child_storage_ctx(space_strategy:job(), file_meta:name()) ->
    storage_file_ctx:ctx().
get_child_storage_ctx(#space_strategy_job{data = #{
    storage_file_ctx := StorageFileCtx}
}, ChildName) ->
    storage_file_ctx:get_child_ctx(StorageFileCtx, ChildName).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Remove files that had been earlier imported.
%%% @end
%%%-------------------------------------------------------------------
-spec delete_imported_file(space_strategy:job(), file_meta:name(),
    file_ctx:ctx()) -> ok.
delete_imported_file(#space_strategy_job{}, ChildName, FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ChildCtx, _} = file_ctx:get_child(FileCtx, ChildName, RootUserCtx),
    ok = fslogic_delete:remove_file_and_file_meta(ChildCtx, RootUserCtx, true, false),
    ok = fslogic_delete:remove_file_handles(ChildCtx).