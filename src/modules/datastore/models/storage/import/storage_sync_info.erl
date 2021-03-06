%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains implementation of helper model used by
%%% storage import. It is used to store information required
%%% to determine whether there were changes introduced to file
%%% or children files on storage since last scan.
%%% It stores last synchronized mtime and timestamp
%%% of stat operation which was performed to read the synced mtime.
%%% For directories it also stores map of hashes computed from children
%%% attributes.
%%% Each batch (where batch is identified by an integer BatchKey = Offset div BatchSize)
%%% is associated with hash computed out of attributes of the directory's children
%%% from batch identified by Offset and BatchSize.
%%% As consequent batches of the directory can be processed in parallel,
%%% there are 2 counters introduced: batches_to_process and batches_processed.
%%% children_hashes map and mtime fields are updated for directory only after
%%% batches_processed counter reaches batches_to_process value.
%%% Until then, hashes are stored in hashes_to_update map.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_info).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type key() :: datastore:key().
-type doc() :: datastore_doc:doc(record()).
-type record() :: #storage_sync_info{}.
-type error() :: {error, term()}.
-type diff() :: datastore_doc:diff(record()).
-type hash() :: storage_import_hash:hash().
-type hashes() :: #{non_neg_integer() => storage_import_hash:hash()}.

-export_type([key/0, doc/0, record/0, hashes/0]).

%% API
-export([
    get/2,
    get_mtime/1, get_batch_hash/3, get_guid/1,
    are_all_batches_processed/1,
    update_mtime/5,
    delete/2
]).
-export([
    maybe_set_guid/4, set_guid/3,
    init_batch_counters/2,
    increase_batches_to_process/2, increase_batches_to_process/3,
    mark_processed_batch/4, mark_processed_batch/7, mark_processed_batch/8,
    mark_protected_child_has_changed/2, set_any_protected_child_changed/3
]).

% exported for CT tests
-export([create_or_update/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2, id/2]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(helpers:file_id(), od_space:id()) -> {ok, doc()} | error().
get(StorageFileId, SpaceId) ->
    datastore_model:get(?CTX, id(StorageFileId, SpaceId)).


-spec get_mtime(record() | doc()) -> times:m_time().
get_mtime(#document{value = SSI}) ->
    get_mtime(SSI);
get_mtime(#storage_sync_info{mtime = Mtime}) ->
    Mtime.


-spec get_batch_hash(non_neg_integer(), non_neg_integer(), doc()) -> hash() | undefined.
get_batch_hash(Offset, BatchSize, #document{value = #storage_sync_info{children_hashes = Hashes}}) ->
    maps:get(batch_key(Offset, BatchSize), Hashes, undefined).

-spec maybe_set_guid(helpers:file_id(), od_space:id(), storage:id(), file_id:file_guid()) -> ok.
maybe_set_guid(StorageFileId, SpaceId, StorageId, Guid) ->
    case storage:is_imported(StorageId) of
        true -> set_guid(StorageFileId, SpaceId, Guid);
        false -> ok
    end.


-spec set_guid(helpers:file_id(), od_space:id(), file_id:file_guid()) -> ok.
set_guid(StorageFileId, SpaceId, Guid) ->
    create_or_update(StorageFileId, SpaceId, fun(SSI) ->
        {ok, SSI#storage_sync_info{guid = Guid}}
    end).


-spec get_guid(record() | doc()) -> file_id:file_guid() | undefined.
get_guid(#document{value = StorageSyncInfo}) ->
    get_guid(StorageSyncInfo);
get_guid(#storage_sync_info{guid = GuidOrUndefined}) ->
    GuidOrUndefined.


-spec delete(helpers:file_id(), od_space:id()) -> ok | error().
delete(StorageFileId, SpaceId) ->
    datastore_model:delete(?CTX, id(StorageFileId, SpaceId)).


-spec are_all_batches_processed(record() | doc()) -> boolean().
are_all_batches_processed(#document{value = SSI}) ->
    are_all_batches_processed(SSI);
are_all_batches_processed(#storage_sync_info{
    batches_to_process = BatchesToProcess,
    batches_processed = BatchesProcessed
}) ->
    BatchesToProcess =:= BatchesProcessed.


%%-------------------------------------------------------------------
%% @doc
%% This function is called before scheduling job for processing first
%% batch of directory's children. It resets counters which are responsible
%% for determining when jobs for all batches were finished.
%% @end
%%-------------------------------------------------------------------
-spec init_batch_counters(helpers:file_id(), od_space:id()) -> ok.
init_batch_counters(StorageFileId, SpaceId) ->
    create_or_update(StorageFileId, SpaceId, fun(SSI) ->
        {ok, SSI#storage_sync_info{
            batches_to_process = 1,
            batches_processed = 0,
            hashes_to_update = #{}
        }}
    end).


-spec update_mtime(helpers:file_id(), od_space:id(), file_id:file_guid(), times:m_time(), non_neg_integer()) -> ok.
update_mtime(StorageFileId, SpaceId, Guid, NewMtime, StatTimestamp) ->
    create_or_update(StorageFileId, SpaceId, fun(SSI) ->
        {ok, SSI#storage_sync_info{
            guid = Guid,
            mtime = NewMtime,
            last_stat = StatTimestamp
        }}
    end).


-spec mark_protected_child_has_changed(helpers:file_id(), od_space:id()) -> ok | error().
mark_protected_child_has_changed(StorageFileId, SpaceId) ->
    set_any_protected_child_changed(StorageFileId, SpaceId, true).


-spec set_any_protected_child_changed(helpers:file_id(), od_space:id(), boolean()) -> ok | error().
set_any_protected_child_changed(StorageFileId, SpaceId, AnyProtectedChildChanged) ->
    create_or_update(StorageFileId, SpaceId, fun(SSI) ->
        {ok, SSI#storage_sync_info{any_protected_child_changed = AnyProtectedChildChanged}}
    end).


-spec increase_batches_to_process(helpers:file_id(), od_space:id()) -> ok.
increase_batches_to_process(StorageFileId, SpaceId) ->
   increase_batches_to_process(StorageFileId, SpaceId, 1).

-spec increase_batches_to_process(helpers:file_id(), od_space:id(), non_neg_integer()) -> ok.
increase_batches_to_process(StorageFileId, SpaceId, Number) ->
    create_or_update(StorageFileId, SpaceId,
        fun(SSI = #storage_sync_info{batches_to_process = ToProcess}) ->
            {ok, SSI#storage_sync_info{batches_to_process = ToProcess + Number}}
        end
    ).


-spec mark_processed_batch(helpers:file_id(), od_space:id(), file_id:file_guid(),
    undefined | times:m_time()) -> {ok, doc()}.
mark_processed_batch(StorageFileId, SpaceId, Guid, Mtime) ->
    mark_processed_batch(StorageFileId, SpaceId, Guid, Mtime, undefined, undefined, undefined, true).

-spec mark_processed_batch(helpers:file_id(), od_space:id(), file_id:file_guid(), undefined | times:m_time(),
    undefined | non_neg_integer(), undefined | non_neg_integer(), undefined | hash()) -> {ok, doc()}.
mark_processed_batch(StorageFileId, SpaceId, Guid, Mtime, Offset, Length, BatchHash) ->
    mark_processed_batch(StorageFileId, SpaceId, Guid, Mtime, Offset, Length, BatchHash, true).

%%-------------------------------------------------------------------
%% @doc
%% This function is called after processing batch starting from
%% Offset of length BatchSize has been finished.
%% It increases batches_processed counter and checks whether it reached
%% batches_to_process counter.
%% If counters become equal it merges children_hashes map with
%% hashes_to_update and updates mtime.
%% It is possible to delay update of children_to_hashes and mtime by
%% setting UpdateHashesOnFinish flag to false.
%% WARNING!!!
%% After passing UpdateHashesOnFinishes = false, it is necessary to call
%% update_hashes/3 function to enforce update of children_to_hashes and
%% mtime fields.
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_batch(helpers:file_id(), od_space:id(), file_id:file_guid(), undefined | times:m_time(),
    undefined | non_neg_integer(), undefined | non_neg_integer(), undefined | hash(), boolean()) -> {ok, doc()}.
mark_processed_batch(StorageFileId, SpaceId, Guid, Mtime, Offset, BatchSize, BatchHash, UpdateHashesOnFinish) ->
    update(StorageFileId, SpaceId, fun(SSI = #storage_sync_info{
        mtime = OldMtime,
        batches_processed = BatchesProcessed,
        batches_to_process = BatchesToProcess,
        children_hashes = ChildrenHashes,
        hashes_to_update = HashesToUpdate,
        any_protected_child_changed = AnyProtectedChildChanged
    }) ->
        BatchesProcessed2 = BatchesProcessed + 1,
        HashesToUpdate2 = update_hashes_map(Offset, BatchSize, BatchHash, HashesToUpdate),
        SSI2 = SSI#storage_sync_info{
            guid = Guid,
            batches_processed = BatchesProcessed2
        },
        case {UpdateHashesOnFinish and (BatchesProcessed2 =:= BatchesToProcess), AnyProtectedChildChanged} of
            {true, false} ->
                {ok, SSI2#storage_sync_info{
                    hashes_to_update = #{},
                    children_hashes = maps:merge(ChildrenHashes, HashesToUpdate2),
                    mtime = utils:ensure_defined(Mtime, OldMtime)
                }};
            {false, false} ->
                {ok, SSI2#storage_sync_info{
                    hashes_to_update = HashesToUpdate2
                }};
            {_, true} ->
                {ok, SSI2}
        end
    end).

%%===================================================================
%% Exported for CT tests
%%===================================================================

-spec id(helpers:file_id(), od_space:id()) -> key().
id(StorageFileId, SpaceId) ->
    datastore_key:adjacent_from_digest([StorageFileId], SpaceId).

-spec create_or_update(helpers:file_id(), od_space:id(), diff()) -> ok | error().
create_or_update(StorageFileId, SpaceId, Diff) ->
    Id = id(StorageFileId, SpaceId),
    DefaultDoc = default_doc(Id, Diff, SpaceId),
    ok = ?extract_ok(datastore_model:update(?CTX, Id, Diff, DefaultDoc)).

%%===================================================================
%% Internal functions
%%===================================================================

-spec default_doc(key(), diff(), od_space:id()) -> doc().
default_doc(Key, Diff, SpaceId) ->
    {ok, NewSSI} = Diff(#storage_sync_info{}),
    #document{
        key = Key,
        value = NewSSI,
        scope = SpaceId
    }.

-spec update(helpers:file_id(), od_space:id(), diff()) -> {ok, doc()} | error().
update(StorageFileId, SpaceId, Diff) ->
    Id = id(StorageFileId, SpaceId),
    datastore_model:update(?CTX, Id, Diff).

-spec update_hashes_map(non_neg_integer() | undefined, non_neg_integer() | undefined, hash() | undefined, hashes()) ->
    hashes().
update_hashes_map(Offset, BatchSize, Value, Map) ->
    update_hashes_map(batch_key(Offset, BatchSize), Value, Map).

-spec update_hashes_map(non_neg_integer() | undefined, hash() | undefined, hashes()) -> hashes().
update_hashes_map(undefined, _Value, Map) -> Map;
update_hashes_map(_Key, undefined, Map) -> Map;
update_hashes_map(Key, Value, Map) -> Map#{Key => Value}.

-spec batch_key(non_neg_integer() | undefined, non_neg_integer() | undefined) -> non_neg_integer() | undefined.
batch_key(undefined, _Length) -> undefined;
batch_key(_Offset, undefined) -> undefined;
batch_key(Offset, Length) -> Offset div Length.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

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
        {children_attrs_hash, #{integer => binary}},
        {last_synchronized_mtime, integer}
    ]};
get_record_struct(2) ->
    {record, [
        {children_attrs_hash, #{integer => binary}},
        {mtime, integer}
    ]};
get_record_struct(3) ->
    {record, [
        {children_attrs_hash, #{integer => binary}},
        {mtime, integer},
        {last_stat, integer}
    ]};
get_record_struct(4) ->
    {record, [
        {children_hashes, #{integer => binary}},
        {mtime, integer},
        {last_stat, integer},
        {batches_to_process, integer},
        {batches_processed, integer},
        {hashes_to_update, #{integer => binary}}
    ]};
get_record_struct(5) ->
    {record, [
        % field guid has been added in this version
        {guid, string},
        {children_hashes, #{integer => binary}},
        {mtime, integer},
        {last_stat, integer},
        {batches_to_process, integer},
        {batches_processed, integer},
        {hashes_to_update, #{integer => binary}}
    ]};
get_record_struct(6) ->
    {record, [
        {guid, string},
        {children_hashes, #{integer => binary}},
        {mtime, integer},
        {last_stat, integer},
        {batches_to_process, integer},
        {batches_processed, integer},
        {hashes_to_update, #{integer => binary}},
        % field any_protected_child_changed has been added in this version
        {any_protected_child_changed, boolean}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, ChildrenAttrsHash, MTime}) ->
    {2, {?MODULE, ChildrenAttrsHash, MTime}};
upgrade_record(2, {?MODULE, ChildrenAttrsHash, MTime}) ->
    {3, {?MODULE, ChildrenAttrsHash, MTime, MTime + 1}};
upgrade_record(3, {?MODULE, ChildrenAttrsHash, MTime, LastStat}) ->
    {4, {?MODULE, ChildrenAttrsHash, MTime, LastStat, 0, 0, #{}}};
upgrade_record(4, {?MODULE, ChildrenAttrsHash, MTime, LastStat, BatchesToProcess, BatchesProcessed, HashesToUpdate}) ->
    {5, {?MODULE,
        % field guid has been added in this version
        undefined,
        ChildrenAttrsHash,
        MTime,
        LastStat,
        BatchesToProcess,
        BatchesProcessed,
        HashesToUpdate
    }};
upgrade_record(5, {?MODULE, Guid, ChildrenAttrsHash, MTime, LastStat, BatchesToProcess, BatchesProcessed, HashesToUpdate}) ->
    {6, #storage_sync_info{
        guid = Guid,
        children_hashes = ChildrenAttrsHash,
        mtime = MTime,
        last_stat = LastStat,
        batches_to_process = BatchesToProcess,
        batches_processed = BatchesProcessed,
        hashes_to_update = HashesToUpdate,
        % field any_protected_child_changed has been added in this version
        any_protected_child_changed = false
    }}.