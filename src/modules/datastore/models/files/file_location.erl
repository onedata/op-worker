%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' location data.
%%% @end
%%%-------------------------------------------------------------------
-module(file_location).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneclient/common_messages.hrl").

% API
-export([local_id/1, id/2]).
-export([
    get/1, get/2, get_local/1,
    get_version_vector/1, get_owner_id/1, get_synced_gid/1,
    get_last_replication_timestamp/1, is_storage_file_created/1
]).
-export([
    create/2, create_and_update_quota/2,
    save/1, save_and_bump_version/2, save_and_update_quota/2,
    update/2, set_last_replication_timestamp/2,
    delete/1, delete_and_update_quota/1
]).


%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: datastore:key().
-type record() :: #file_location{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, doc/0, diff/0, record/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns id of local file location
%% @end
%%--------------------------------------------------------------------
-spec local_id(file_meta:uuid()) -> file_location:id().
local_id(FileUuid) ->
    id(FileUuid, oneprovider:get_id()).

%%--------------------------------------------------------------------
%% @doc
%% Returns id of local file location
%% @end
%%--------------------------------------------------------------------
-spec id(file_meta:uuid(), od_provider:id()) -> file_location:id().
id(FileUuid, ProviderId) ->
    datastore_key:build_adjacent(ProviderId, FileUuid).


-spec create(doc(), boolean()) -> {ok, doc()} | {error, term()}.
create(Doc = #document{value = #file_location{space_id = SpaceId}}, true) ->
    datastore_model:save(?CTX#{generated_key => true},
        Doc#document{scope = SpaceId});
create(Doc = #document{value = #file_location{space_id = SpaceId}}, _) ->
    datastore_model:create(?CTX, Doc#document{scope = SpaceId}).


-spec create_and_update_quota(doc(), boolean()) -> {ok, doc()} | {error, term()}.
create_and_update_quota(Doc = #document{value = #file_location{
    uuid = FileUuid,
    space_id = SpaceId
}}, GeneratedKey) ->
    NewSize = count_bytes(Doc),
    space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
    case get_owner_id(FileUuid) of
        {ok, UserId} ->
            monitoring_event_emitter:emit_storage_used_updated(
                SpaceId, UserId, NewSize);
        {error, not_found} ->
            ok
    end,
    case GeneratedKey of
        true ->
            datastore_model:save(?CTX#{generated_key => GeneratedKey},
                Doc#document{scope = SpaceId});
        _ ->
            datastore_model:create(?CTX,
                Doc#document{scope = SpaceId})
    end.


-spec save_and_bump_version(doc(), od_user:id()) -> {ok, doc()} | {error, term()}.
save_and_bump_version(FileLocationDoc, UserId) ->
    fslogic_location_cache:save_location(
        version_vector:bump_version(FileLocationDoc), UserId).


-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc = #document{value = #file_location{space_id = SpaceId}}) ->
    ?extract_key(datastore_model:save(?CTX, Doc#document{scope = SpaceId})).


-spec save_and_update_quota(doc(), od_user:id() | undefined) -> {ok, id()} | {error, term()}.
save_and_update_quota(Doc = #document{
    value = #file_location{uuid = FileUuid}
}, undefined) ->
    {ok, UserId} = get_owner_id(FileUuid),
    save_and_update_quota(Doc, UserId);
save_and_update_quota(Doc = #document{
    key = Key,
    value = #file_location{space_id = SpaceId}
}, UserId) ->
    NewSize = count_bytes(Doc),
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #file_location{space_id = SpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize - OldSize),
            monitoring_event_emitter:emit_storage_used_updated(
                SpaceId, UserId, NewSize - OldSize);

        {ok, #document{value = #file_location{space_id = OldSpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),
            space_quota:apply_size_change_and_maybe_emit(OldSpaceId, -1 * OldSize),
            monitoring_event_emitter:emit_storage_used_updated(
                OldSpaceId, UserId, -1 * OldSize),

            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
            monitoring_event_emitter:emit_storage_used_updated(
                SpaceId, UserId, NewSize);
        _ ->
            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
            monitoring_event_emitter:emit_storage_used_updated(
                SpaceId, UserId, NewSize)
    end,
    ?extract_key(datastore_model:save(?CTX, Doc#document{scope = SpaceId})).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).


%%--------------------------------------------------------------------
%% @doc
%% Returns file location associated with given FileUuid and ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get(file_meta:uuid(), od_provider:id()) -> {ok, doc()} | {error, term()}.
get(FileUuid, ProviderId) ->
    ?MODULE:get(?MODULE:id(FileUuid, ProviderId)).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get(FileUuid, oneprovider:get_id()).
%% @end
%%--------------------------------------------------------------------
-spec get_local(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get_local(FileUuid) ->
    ?MODULE:get(FileUuid, oneprovider:get_id()).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).


-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
   % TODO VFS-4739 - delete links
   datastore_model:delete(?CTX, Key).


-spec delete_and_update_quota(id()) -> ok | {error, term()}.
delete_and_update_quota(Key) ->
    case datastore_model:get(?CTX, Key) of
        {ok, Doc = #document{value = #file_location{
            uuid = FileUuid,
            space_id = SpaceId
        }}} ->
            Size = count_bytes(Doc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, -1 * Size),
            {ok, UserId} = get_owner_id(FileUuid),
            monitoring_event_emitter:emit_storage_used_updated(
                SpaceId, UserId, -1 * Size);
        _ ->
            ok
    end,
    datastore_model:delete(?CTX, Key).


-spec is_storage_file_created(doc() | record()) -> boolean().
is_storage_file_created(#file_location{storage_file_created = StorageFileCreated}) ->
    StorageFileCreated;
is_storage_file_created(#document{value=FileLocation}) ->
    is_storage_file_created(FileLocation).


-spec get_version_vector(doc() | record()) -> version_vector:version_vector().
get_version_vector(#document{value = FileLocation}) ->
    get_version_vector(FileLocation);
get_version_vector(#file_location{version_vector = VV}) ->
    VV.


-spec get_last_replication_timestamp(doc() | record()) -> non_neg_integer() | undefined.
get_last_replication_timestamp(#document{value = FL}) ->
    get_last_replication_timestamp(FL);
get_last_replication_timestamp(FL = #file_location{}) ->
    FL#file_location.last_replication_timestamp.


-spec get_synced_gid(doc() | record()) -> luma:gid() | undefined.
get_synced_gid(#document{value = FL}) ->
    get_synced_gid(FL);
get_synced_gid(#file_location{synced_gid = SyncedGid}) ->
    SyncedGid.


-spec set_last_replication_timestamp(doc(), non_neg_integer()) -> doc().
set_last_replication_timestamp(Doc = #document{value = FL}, Timestamp) ->
    Doc#document{
        value = FL#file_location{
            last_replication_timestamp = Timestamp
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns total size used by given file_location.
%% @end
%%--------------------------------------------------------------------
-spec count_bytes(doc()) -> non_neg_integer().
count_bytes(#document{value = #file_location{blocks = Blocks}}) ->
    count_bytes(Blocks, 0).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns total size used by given file_location.
%% @end
%%--------------------------------------------------------------------
-spec count_bytes([fslogic_blocks:block()], non_neg_integer()) -> 
    non_neg_integer().
count_bytes([], Size) ->
    Size;
count_bytes([#file_block{size = Size} | T], TotalSize) ->
    count_bytes(T, TotalSize + Size).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return user id for given file uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_owner_id(file_meta:uuid()) -> {ok, od_user:id()} | {error, term()}.
get_owner_id(FileUuid) ->
    case file_meta:get_including_deleted(FileUuid) of
        {ok, #document{value = #file_meta{owner = UserId}}} ->
            {ok, UserId};
        {error, Reason} ->
            {error, Reason}
    end.

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
    6.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {handle_id, string},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}}
    ]};
get_record_struct(2) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {handle_id, string},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}}
    ]};
get_record_struct(3) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}},
        {storage_file_created, boolean}
    ]};
get_record_struct(4) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}},
        {storage_file_created, boolean},
        {last_replication_timestamp, integer}
    ]};
get_record_struct(5) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        % added field rename_src_file_id which allows to determine
        % that file on storage is being renamed at the moment
        {rename_src_file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}},
        {storage_file_created, boolean},
        {last_replication_timestamp, integer}
    ]};
get_record_struct(6) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {rename_src_file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}},
        {storage_file_created, boolean},
        {last_replication_timestamp, integer},
        % added field synced_gid
        {synced_gid, integer}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, _HandleId, SpaceId, RecentChanges, LastRename}) ->
    {2, {?MODULE,
        Uuid, ProviderId, StorageId, FileId, Blocks, VersionVector, Size,
        SpaceId, RecentChanges, LastRename
    }};
upgrade_record(2, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, SpaceId, RecentChanges, LastRename}) ->
    {3, {?MODULE,
        Uuid, ProviderId, StorageId, FileId, Blocks, VersionVector, Size,
        SpaceId, RecentChanges, LastRename, true
    }};
upgrade_record(3, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, SpaceId, RecentChanges, LastRename, StorageFileCreated}) ->
    {4, {?MODULE,
        Uuid, ProviderId, StorageId, FileId, Blocks, VersionVector, Size,
        SpaceId, RecentChanges, LastRename, StorageFileCreated, undefined
    }};
upgrade_record(4, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, SpaceId, RecentChanges, LastRename, StorageFileCreated, LastReplicationTimestamp}) ->
    {5, {?MODULE,
        Uuid, ProviderId, StorageId, FileId, undefined, Blocks, VersionVector, Size,
        SpaceId, RecentChanges, LastRename, StorageFileCreated, LastReplicationTimestamp
    }};
upgrade_record(5, {?MODULE, Uuid, ProviderId, StorageId, FileId, RenameSrcFileId, Blocks,
    VersionVector, Size, SpaceId, RecentChanges, LastRename, StorageFileCreated, LastReplicationTimestamp}) ->
    {6, {?MODULE,
        Uuid, ProviderId, StorageId, FileId, RenameSrcFileId, Blocks, VersionVector, Size,
        SpaceId, RecentChanges, LastRename, StorageFileCreated, LastReplicationTimestamp, undefined
    }}.