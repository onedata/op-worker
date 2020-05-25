%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a generic datastore model that is used by all
%%% modules associated with LUMA DB that implement
%%% luma_db_table behaviour.
%%%
%%% Ids of documents of this model are results of hashing 3 keys:
%%%  - storage:id(),
%%%  - table() - name of the module that implements luma_db_table behaviour
%%%  - db_key() - internal key in the table
%%%
%%% Single document of this model stores custom record (db_record())
%%% of one of the modules implementing luma_db_table behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/3, store/4, delete/3, clear_all/2]).

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

% Definitions of luma_db behaviour types
-type table() :: luma_storage_users | luma_spaces_defaults | luma_onedata_users | luma_onedata_groups.
% @formatter:off
-type db_key() :: luma_storage_users:key() | luma_spaces_defaults:key() |
                  luma_onedata_users:key() | luma_onedata_groups:key().
-type db_record() :: luma_storage_users:record() | luma_spaces_defaults:record() |
                     luma_onedata_users:record() | luma_onedata_groups:record().
% @formatter:on

-export_type([db_key/0, db_record/0, table/0]).

-type storage() :: storage:id() | storage:data().
-define(BATCH_SIZE, 1000).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function returns record associated with Key from table Table
%% associated with Storage.
%% First, it checks whether record is already in the db.
%% If it's missing, it calls acquire/2 callback that must
%% be implemented by modules implementing luma_db_table behaviour.
%% @end
%%--------------------------------------------------------------------
-spec get(storage(), db_key(), table()) -> {ok, db_record()} | {error, term()}.
get(Storage, Key, Table) ->
    Id = id(Storage, Table, Key),
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #luma_db{record = Record}}} ->
            {ok, Record};
        {error, not_found} ->
            acquire_and_store(Storage, Key, Table)
    end.

%%--------------------------------------------------------------------
%% @doc
%% This function stores record associated with Key in table Table
%% associated with Storage.
%% @end
%%--------------------------------------------------------------------
-spec store(storage(), db_key(), table(), db_record()) -> ok | {error, term()}.
store(Storage, Key, Table, Record) ->
    Id = id(Storage, Table, Key),
    Doc = new_doc(Id, Storage, Table, Record),
    case ?extract_ok(datastore_model:create(?CTX, Doc)) of
        ok ->
            luma_db_links:add_link(Table, Storage, Key, Id),
            ok;
        {error, already_exists} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% This function deletes single record associated with Key, that is
%% stored in table Table associated with StorageId.
%% @end
%%--------------------------------------------------------------------
-spec delete(storage(), db_key(), table()) -> ok.
delete(Storage, Key, Table) ->
    Id = id(Storage, Table, Key),
    delete_doc_and_link(Id, storage:get_id(Storage), Key, Table).

%%--------------------------------------------------------------------
%% @doc
%% This function deletes all records stored in table Table associated
%% with StorageId.
%% @end
%%--------------------------------------------------------------------
-spec clear_all(storage:id(), table()) -> ok.
clear_all(StorageId, Table) ->
    clear_all(StorageId, Table, undefined, ?BATCH_SIZE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec id(storage(), table(), db_key()) -> doc_id().
id(Storage, Table, Key) ->
    StorageId = storage:get_id(Storage),
    datastore_key:new_from_digest([StorageId, atom_to_binary(Table, utf8), Key]).

-spec acquire_and_store(storage(), db_key(), table()) -> {ok, db_record()} | {error, term()}.
acquire_and_store(Storage, Key, TableModule) ->
    % ensure Storage is a document
    {ok, StorageData} = storage:get(Storage),
    case TableModule:acquire(StorageData, Key) of
        {ok, Record} ->
            store(Storage, Key, TableModule, Record),
            {ok, Record};
        Error ->
            Error
    end.

-spec new_doc(doc_id(), storage(), table(), db_record()) -> doc().
new_doc(Id, Storage, Table, Record) ->
    #document{
        key = Id,
        value = #luma_db{
            table = Table,
            record = Record,
            storage_id = storage:get_id(Storage)
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
    case datastore_model:delete(?CTX, DocId) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    luma_db_links:delete_link(Table, StorageId, Key).

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
        {record, term},
        {storage_id, string}
    ]}.