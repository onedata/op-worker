%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a generic datastore model that is used by all
%%% modules that implement LUMA DB tables.
%%%
%%% Ids of documents of this model are results of hashing 3 keys:
%%%  - storage:id(),
%%%  - table() - name of the module that implements LUMA DB table
%%%  - db_key() - internal key in the table
%%%
%%% Single document of this model stores custom record (db_record())
%%% of one of the modules implementing LUMA DB table.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/luma/luma.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    get/3,
    get_or_acquire/4, get_or_acquire/5,
    store/5, store/7,
    update/4, update_or_store/6,
    delete/3, delete/4,
    clear_all/2,
    get_and_describe/3, get_and_describe/4,
    delete_if_auto_feed/3
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

% Definitions of datastore types
-type doc_id() :: datastore_doc:key().
-type doc_record() :: #luma_db{}.
-type doc() :: datastore_doc:doc(doc_record()).

-export_type([doc_id/0]).

% Modules that implement LUMA DB tables.
% @formatter:off
-type table() ::
    luma_storage_users |
    luma_spaces_display_defaults |
    luma_spaces_posix_storage_defaults |
    luma_onedata_users |
    luma_onedata_groups.

-type db_key() :: luma_storage_users:key() |
                  luma_spaces_display_defaults:key() |
                  luma_spaces_posix_storage_defaults:key() |
                  luma_onedata_users:key() |
                  luma_onedata_groups:key().

-type db_record() :: luma_storage_users:record() |
                     luma_spaces_display_defaults:record() |
                     luma_spaces_posix_storage_defaults:record() |
                     luma_onedata_users:record() |
                     luma_onedata_groups:record().

-type db_acquire_fun() :: fun(() -> {ok, db_record(), luma:feed()} | {error, term()}).
-type db_diff() :: luma_storage_user:user_map() |
                   luma_posix_credentials:credentials_map()  |
                   luma_onedata_user:user_map() |
                   luma_onedata_group:group_map().

-type db_pred() :: fun((db_record()) -> boolean()).
% @formatter:on

-export_type([db_key/0, db_record/0, table/0, db_acquire_fun/0, db_diff/0]).

-type storage() :: storage:id() | storage:data().
-type overwrite_opt() :: ?FORCE_OVERWRITE | ?NO_OVERWRITE.
-type constraint() :: ?POSIX_STORAGE | ?IMPORTED_STORAGE | ?NON_IMPORTED_STORAGE.
-type constraints() :: [constraint()].
-define(BATCH_SIZE, 1000).

%%%===================================================================
%%% API functions
%%%===================================================================


-spec get(storage(), db_key(), table()) ->
    {ok, db_record()} | {error, term()}.
get(Storage, Key, Table) ->
    Id = id(Storage, Table, Key),
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #luma_db{record = Record}}} ->
            {ok, Record};
        {error, _} = Error ->
            Error
    end.


-spec get_or_acquire(storage(), db_key(), table(), db_acquire_fun()) ->
    {ok, db_record()} | {error, term()}.
get_or_acquire(Storage, Key, Table, AcquireFun) ->
    get_or_acquire(Storage, Key, Table, AcquireFun, []).

%%--------------------------------------------------------------------
%% @doc
%% This function returns record associated with Key from table Table
%% associated with Storage.
%% First, it validates Constraints.
%% Next, it checks whether record is already in the db.
%% If it's missing, it calls AcquireFun and stores the result if
%% it has returned successfully.
%% @end
%%--------------------------------------------------------------------
-spec get_or_acquire(storage(), db_key(), table(), db_acquire_fun(), constraints()) ->
    {ok, db_record()} | {error, term()}.
get_or_acquire(Storage, Key, Table, AcquireFun, Constraints) ->
    validate_constraints_end_execute(Storage, Constraints, fun() ->
        case get(Storage, Key, Table) of
            {ok, Record} ->
                {ok, Record};
            {error, not_found} ->
                acquire_and_store(AcquireFun, Storage, Key, Table)
        end
    end).

-spec store(storage(), db_key(), table(), db_record(), luma:feed()) ->
    ok | {error, term()}.
store(Storage, Key, Table, Record, Feed) ->
    store(Storage, Key, Table, Record, Feed, ?FORCE_OVERWRITE, []).

%%--------------------------------------------------------------------
%% @doc
%% This function stores record associated with Key in table Table
%% associated with Storage.
%% If Force == true datastore_model:save will be used to store document.
%% otherwise datastore_model:create will be used.
%% @end
%%--------------------------------------------------------------------
-spec store(storage(), db_key(), table(), db_record(), luma:feed(), overwrite_opt(), constraints()) ->
    ok | {error, term()}.
store(Storage, Key, Table, Record, Feed, OverwriteFlag, Constraints) ->
    validate_constraints_end_execute(Storage, Constraints, fun() ->
        Id = id(Storage, Table, Key),
        Doc = new_doc(Id, Storage, Table, Record, Feed),
        case store_internal(Doc, OverwriteFlag) of
            ok ->
                luma_db_links:add_link(Table, Storage, Key, Id),
                ok;
            {error, _} = Error ->
                Error
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Updates record, already existing in the LUMA DB.
%% NOTE:
%% callback luma_db_record:update/2 callback must be implemented by
%% rec
%% @end
%%--------------------------------------------------------------------
-spec update(storage(), db_key(), table(), db_diff()) -> {ok, db_record()} | {error, term()}.
update(Storage, Key, Table, Diff) ->
    Id = id(Storage, Table, Key),
    UpdateFun = fun(LumaDb = #luma_db{record = Record}) ->
        case luma_db_record:update(Record, Diff) of
            {ok, Record2} ->
                {ok, LumaDb#luma_db{record = Record2}};
            Error ->
                Error
        end
    end,
    case datastore_model:update(?CTX, Id, UpdateFun) of
        {ok, #document{value = #luma_db{record = Record}}} ->
            {ok, Record};
        Error ->
            Error
    end.


-spec update_or_store(storage(), db_key(), table(), db_diff(), db_record(), luma:feed()) ->
    ok | {error, term()}.
update_or_store(Storage, Key, Table, Diff, DefaultRecord, Feed) ->
    Id = id(Storage, Table, Key),
    UpdateFun = fun(LumaDb = #luma_db{record = Record}) ->
        case luma_db_record:update(Record, Diff) of
            {ok, Record2} ->
                {ok, LumaDb#luma_db{record = Record2}};
            Error ->
                Error
        end
    end,
    Default = #luma_db{
        storage_id = storage:get_id(Storage),
        table = Table,
        record = DefaultRecord,
        feed = Feed
    },
    case ?extract_ok(datastore_model:update(?CTX, Id, UpdateFun, Default)) of
        ok ->
            luma_db_links:add_link(Table, Storage, Key, Id);
        Error ->
            Error
    end.


-spec delete(storage:id(), db_key(), table()) -> ok.
delete(StorageId, Key, Table) ->
    delete(StorageId, Key, Table, []).

%%--------------------------------------------------------------------
%% @doc
%% This function deletes single record associated with Key, that is
%% stored in table Table associated with StorageId.
%% @end
%%--------------------------------------------------------------------
-spec delete(storage:id(), db_key(), table(), constraints()) -> ok | {error, term()}.
delete(StorageId, Key, Table, Constraints) ->
    validate_constraints_end_execute(StorageId, Constraints, fun() ->
        Id = id(StorageId, Table, Key),
        delete_doc_and_link(Id, StorageId, Key, Table)
    end).

%%--------------------------------------------------------------------
%% @doc
% This functions deletes record from LUMA DB if and only if
% it was created by ?AUTO_FEED.
% This allows to ensure that user defined records won't be deleted.
%% @end
%%--------------------------------------------------------------------
-spec delete_if_auto_feed(storage:id(), db_key(), table()) -> ok.
delete_if_auto_feed(StorageId, Key, Table) ->
    Id = id(StorageId, Table, Key),
    delete_doc_and_link(Id, StorageId, Key, Table, fun(#luma_db{feed = Feed}) ->
        Feed =:= ?AUTO_FEED
    end).

%%--------------------------------------------------------------------
%% @doc
%% This function deletes all records stored in table Table associated
%% with StorageId.
%% @end
%%--------------------------------------------------------------------
-spec clear_all(storage:id(), table()) -> ok.
clear_all(StorageId, Table) ->
    clear_all(StorageId, Table, undefined, ?BATCH_SIZE).


-spec get_and_describe(storage(), db_key(), table()) ->
    {ok, json_utils:json_map()} | {error, term()}.
get_and_describe(Storage, Key, Table) ->
    get_and_describe(Storage, Key, Table, []).

-spec get_and_describe(storage(), db_key(), table(), constraints()) ->
    {ok, json_utils:json_map()} | {error, term()}.
get_and_describe(Storage, Key, Table, Constraints) ->
    validate_constraints_end_execute(Storage, Constraints, fun() ->
        case get(Storage, Key, Table) of
            {ok, Record} ->
                {ok, luma_db_record:to_json(Record)};
            {error, _} = Error ->
                Error
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec id(storage(), table(), db_key()) -> doc_id().
id(Storage, Table, Key) ->
    StorageId = storage:get_id(Storage),
    datastore_key:new_from_digest([StorageId, atom_to_binary(Table, utf8), Key]).

-spec acquire_and_store(db_acquire_fun() ,storage(), db_key(), table()) ->
    {ok, db_record()} | {error, term()}.
acquire_and_store(AcquireFun, Storage, Key, TableModule) ->
    % ensure Storage is a document
    case AcquireFun() of
        {ok, Record, Feed} ->
            store(Storage, Key, TableModule, Record, Feed),
            {ok, Record};
        Error ->
            Error
    end.

-spec store_internal(doc(), overwrite_opt()) -> ok | {error, term()}.
store_internal(Doc, ?FORCE_OVERWRITE) ->
    ?extract_ok(datastore_model:save(?CTX, Doc));
store_internal(Doc, ?NO_OVERWRITE) ->
    ?extract_ok(datastore_model:create(?CTX, Doc)).


-spec validate_constraints_end_execute(storage(), constraints(), function()) ->
    ok | {ok, term()} | {error, term()}.
validate_constraints_end_execute(_Storage, [], Fun) ->
    Fun();
validate_constraints_end_execute(Storage, [Constraint | Rest], Fun) ->
    case validate_constraint(Storage, Constraint) of
        ok -> validate_constraints_end_execute(Storage, Rest, Fun);
        {error, _} = Error -> Error
    end.

-spec validate_constraint(storage(), constraint()) -> ok | {error, term()}.
validate_constraint(Storage, ?POSIX_STORAGE) ->
    case storage:is_posix_compatible(Storage) of
        true -> ok;
        false -> ?ERROR_REQUIRES_POSIX_COMPATIBLE_STORAGE(storage:get_id(Storage), ?POSIX_COMPATIBLE_HELPERS)
    end;
validate_constraint(Storage, ?IMPORTED_STORAGE) ->
    case storage:is_imported(Storage) of
        true -> ok;
        false -> ?ERROR_REQUIRES_IMPORTED_STORAGE(storage:get_id(Storage))
    end;
validate_constraint(Storage, ?NON_IMPORTED_STORAGE) ->
    case storage:is_imported(Storage) of
        false -> ok;
        true -> ?ERROR_REQUIRES_NON_IMPORTED_STORAGE(storage:get_id(Storage))
    end.

-spec new_doc(doc_id(), storage(), table(), db_record(), luma:feed()) -> doc().
new_doc(Id, Storage, Table, Record, Feed) ->
    #document{
        key = Id,
        value = #luma_db{
            table = Table,
            record = Record,
            storage_id = storage:get_id(Storage),
            feed = Feed
        }
    }.

-spec clear_all(storage:id(), table(), luma_db_links:token(), luma_db_links:limit()) -> ok.
clear_all(StorageId, Table, Token, Limit) ->
    case luma_db_links:list(Table, StorageId, Token, Limit) of
        {{ok, KeysAndDocIds}, NewToken} ->
            lists:foreach(fun({Key, DocId}) ->
                delete_doc_and_link(DocId, StorageId, Key, Table)
            end, KeysAndDocIds),
            case NewToken#link_token.is_last of
                true ->
                    ok;
                false ->
                    clear_all(StorageId, Table, NewToken, Limit)
            end;
        {error, not_found} ->
            ok
    end.

-spec delete_doc_and_link(doc_id(), storage:id(), db_key(), table()) -> ok.
delete_doc_and_link(DocId, StorageId, Key, Table) ->
    ok  = delete(DocId),
    luma_db_links:delete_link(Table, StorageId, Key).

-spec delete_doc_and_link(doc_id(), storage:id(), db_key(), table(), db_pred()) -> ok.
delete_doc_and_link(DocId, StorageId, Key, Table, Pred) ->
    case delete(DocId, Pred) of
        ok ->
            luma_db_links:delete_link(Table, StorageId, Key);
        {error, {not_satisfied, _}} ->
            ok
    end.

-spec delete(doc_id()) -> ok | {error, term()}.
delete(DocId) ->
    delete(DocId, fun(_) -> true end).

-spec delete(doc_id(), datastore_doc:pred(doc_record())) -> ok | {error, term()}.
delete(DocId, Pred) ->
    datastore_model:delete(?CTX, DocId, Pred).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {table, atom},
        {record, {custom, json, {luma_db_record, encode, decode}}},
        {storage_id, string},
        {feed, atom}
    ]}.