%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains implementation of helper model used by
%%% storage_sync.
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
-type hash() :: storage_sync_hash:hash().
-type hashes() :: #{non_neg_integer() => storage_sync_hash:hash()}.

-export_type([key/0, doc/0, record/0, hashes/0]).

%% API
-export([get/2, get_mtime/1, delete/2, init_new_scan/2,
    update_mtime/4, increase_batches_to_process/2,
    mark_processed_batch/6, all_batches_processed/1, mark_processed_batch/7]).

% exported for CT tests
-export([create_or_update/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2, id/2]).

-define(CTX, #{
    model => ?MODULE,
    routing => global
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(helpers:file_id(), od_space:id()) -> {ok, doc()} | error().
get(StorageFileId, SpaceId) ->
    datastore_model:get(?CTX, id(StorageFileId, SpaceId)).

-spec get_mtime(record() | doc()) -> non_neg_integer().
get_mtime(#document{value = SSI}) ->
    get_mtime(SSI);
get_mtime(#storage_sync_info{mtime = Mtime}) ->
    Mtime.


-spec delete(helpers:file_id(), od_space:id()) -> ok | error().
delete(StorageFileId, SpaceId) ->
    datastore_model:delete(?CTX, id(StorageFileId, SpaceId)).

-spec all_batches_processed(record() | doc()) -> boolean().
all_batches_processed(#document{value = SSI}) ->
    all_batches_processed(SSI);
all_batches_processed(#storage_sync_info{
    batches_to_process = BatchesToProcess,
    batches_processed = BatchesProcessed
}) ->
    BatchesToProcess =:= BatchesProcessed.

-spec init_new_scan(helpers:file_id(), od_space:id()) -> ok.
init_new_scan(StorageFileId, SpaceId) ->
    ok = ?extract_ok(create_or_update(StorageFileId, SpaceId,
        fun(SSI) ->
            {ok, SSI#storage_sync_info{
                batches_to_process = 1,
                batches_processed = 0,
                delayed_children_attrs_hashes = #{}
            }}
        end
    )).

-spec update_mtime(helpers:file_id(), od_space:id(), non_neg_integer(), non_neg_integer()) -> ok.
update_mtime(StorageFileId, SpaceId, NewMtime, StatTimestamp) ->
    ok = ?extract_ok(create_or_update(StorageFileId, SpaceId, fun(SSI) ->
        {ok, SSI#storage_sync_info{
            mtime = NewMtime,
            last_stat = StatTimestamp
        }}
    end)).

-spec increase_batches_to_process(helpers:file_id(), od_space:id()) -> ok.
increase_batches_to_process(StorageFileId, SpaceId) ->
    ok = ?extract_ok(create_or_update(StorageFileId, SpaceId,
        fun(SSI = #storage_sync_info{batches_to_process = ToProcess}) ->
            {ok, SSI#storage_sync_info{batches_to_process = ToProcess + 1}}
        end
    )).

-spec mark_processed_batch(helpers:file_id(), od_space:id(), undefined | non_neg_integer(),
    undefined | non_neg_integer(), undefined | hash(), boolean()) -> {ok, doc()}.
mark_processed_batch(StorageFileId, SpaceId, Mtime, BatchKey, BatchHash, DelayHashUpdate) ->
    mark_processed_batch(StorageFileId, SpaceId, Mtime, BatchKey, BatchHash, DelayHashUpdate, true).

-spec mark_processed_batch(helpers:file_id(), od_space:id(), undefined | non_neg_integer(),
    undefined | non_neg_integer(), undefined | hash(), boolean(), boolean()) -> {ok, doc()}.
mark_processed_batch(StorageFileId, SpaceId, Mtime, BatchKey, BatchHash, DelayHashUpdate,
    UpdateDelayedHashesWhenFinished
) ->
    update(StorageFileId, SpaceId, fun(SSI0 = #storage_sync_info{
        batches_processed = BatchesProcessed,
        batches_to_process = BatchesToProcess,
        children_attrs_hashes = CAH,
        delayed_children_attrs_hashes = DCAH
    }) ->
        SSI1 = case DelayHashUpdate of
            true ->
                SSI0#storage_sync_info{delayed_children_attrs_hashes = update_hashes_map(BatchKey, BatchHash, DCAH)};
            false ->
                SSI0#storage_sync_info{children_attrs_hashes = update_hashes_map(BatchKey, BatchHash, CAH)}
        end,
        SSI2 = case BatchesProcessed + 1 =:= BatchesToProcess of
            true ->
                SSI1#storage_sync_info{
                    batches_to_process = 0,
                    batches_processed = 0,
                    mtime = Mtime
                };
            false ->
                SSI1#storage_sync_info{batches_processed = BatchesProcessed + 1}
        end,
        SSI3 = case UpdateDelayedHashesWhenFinished of
            true ->
                SSI2#storage_sync_info{
                    children_attrs_hashes = maps:merge(CAH, DCAH),
                    delayed_children_attrs_hashes = #{}
                };
            false ->
                SSI2
        end,
        {ok, SSI3}
    end).

%%===================================================================
%% Exported for CT tests
%%===================================================================

-spec id(helpers:file_id(), od_space:id()) -> key().
id(StorageFileId, SpaceId) ->
    datastore_utils:gen_key(SpaceId, StorageFileId).

-spec create_or_update(helpers:file_id(), od_space:id(), diff()) -> ok | error().
create_or_update(StorageFileId, SpaceId, Diff) ->
    Id = id(StorageFileId, SpaceId),
    DefaultDoc = default_doc(Id, Diff, SpaceId),
    datastore_model:update(?CTX, Id, Diff, DefaultDoc).

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

-spec update(helpers:file_id(), od_space:id(), diff()) -> ok | error().
update(StorageFileId, SpaceId, Diff) ->
    Id = id(StorageFileId, SpaceId),
    datastore_model:update(?CTX, Id, Diff).

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
    4.

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
        {children_attrs_hashes, #{integer => binary}},
        {delayed_children_attrs_hashes, #{integer => binary}},
        {mtime, integer},
        {last_stat, integer},
        {batches_to_process, integer},
        {batches_processed, integer}
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
upgrade_record(3, {?MODULE, ChildrenAttrsHash, MTime, LastSTat}) ->
    {4, #storage_sync_info{
        children_attrs_hashes = ChildrenAttrsHash,
        delayed_children_attrs_hashes = #{},
        mtime = MTime,
        last_stat = LastSTat,
        batches_to_process = 0,
        batches_processed = 0
    }}.

-spec update_hashes_map(non_neg_integer() | undefined, hash() | undefined, map()) -> map().
update_hashes_map(undefined, _Value, Map) -> Map;
update_hashes_map(_Key, undefined, Map) -> Map;
update_hashes_map(Key, Value, Map) -> Map#{Key => Value}.