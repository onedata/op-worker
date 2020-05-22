%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for storing dir's location data
%%% @end
%%%-------------------------------------------------------------------
-module(dir_location).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([mark_dir_created_on_storage/2, mark_dir_created_on_storage/3,
    mark_deleted_from_storage/1,
    is_storage_file_created/1, get_synced_gid/1, get/1,
    delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2]).

-type key() :: file_meta:uuid().
-type dir_location() :: #dir_location{}.
-type doc() :: datastore_doc:doc(dir_location()).

-export_type([key/0, dir_location/0, doc/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec mark_dir_created_on_storage(key(), storage:id()) -> {ok | error, term()}.
mark_dir_created_on_storage(FileUuid, StorageId) ->
    mark_dir_created_on_storage(FileUuid, StorageId, undefined).

-spec mark_dir_created_on_storage(key(), storage:id(), luma:gid() | undefined) ->
    ok |{error, term()}.
mark_dir_created_on_storage(FileUuid, StorageId, SyncedGid) ->
    Default = #dir_location{
        storage_file_created = true,
        storage_id = StorageId,
        synced_gid = SyncedGid
    },
    ?extract_ok(datastore_model:update(?CTX, FileUuid, fun(DirLocation) ->
        {ok, DirLocation#dir_location{
            storage_file_created = true, 
            storage_id = StorageId
        }}
    end, Default)).


-spec mark_deleted_from_storage(key()) -> ok | {error, term()}.
mark_deleted_from_storage(FileUuid) ->
    ?extract_ok(datastore_model:update(?CTX, FileUuid, fun(DirLocation) ->
        {ok, DirLocation#dir_location{storage_file_created = false}}
    end)).


-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key)  ->
    datastore_model:get(?CTX, Key).


-spec is_storage_file_created(doc() | dir_location() | undefined) -> boolean().
is_storage_file_created(undefined) ->
    false;
is_storage_file_created(#dir_location{storage_file_created = StorageFileCreated}) ->
    StorageFileCreated;
is_storage_file_created(#document{value = DirLocation}) ->
    is_storage_file_created(DirLocation).


-spec get_synced_gid(doc() | dir_location() | undefined) -> luma:gid() | undefined.
get_synced_gid(undefined) ->
    undefined;
get_synced_gid(#document{value = DirLocation}) ->
    get_synced_gid(DirLocation);
get_synced_gid(#dir_location{synced_gid = SyncedGid}) ->
    SyncedGid.

-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

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
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {storage_file_created, boolean}
    ]};
get_record_struct(2) ->
    {record, [
        {storage_file_created, boolean},
        {storage_id, string},
        {synced_gid, integer}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, StorageFileCreated}) ->
    {2, {?MODULE, StorageFileCreated, undefined, undefined}}.