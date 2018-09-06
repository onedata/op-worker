%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains implementation of helper model for used by
%%% storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_info).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").

-type key() :: file_meta:uuid().
-type doc() :: datastore_model:doc(record()).
-type record() :: #storage_sync_info{}.
-type error() :: {error, term()}.

-export_type([key/0, doc/0, record/0]).

%% API
-export([delete/1, get/1, create_or_update/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2]).

-define(CTX, #{
    model => ?MODULE,
    routing => global
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% @equiv datastore_model:get(?CTX, Uuid).
%% @end
%%-------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | error().
get(Uuid) ->
    datastore_model:get(?CTX, Uuid).

%%-------------------------------------------------------------------
%% @doc
%% @equiv datastore_model:delete(?CTX, Uuid).
%% @end
%%-------------------------------------------------------------------
-spec delete(key()) -> ok | error().
delete(Uuid) ->
    datastore_model:delete(?CTX, Uuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns value of given field from ?MODULE record.
%% @end
%%-------------------------------------------------------------------
-spec get_field_by_name(atom(), record()) -> term().
get_field_by_name(FieldName, StorageSyncInfo) ->
    FieldsList = record_info(fields, ?MODULE),
    Index = index(FieldName, FieldsList),
    element(Index + 1, StorageSyncInfo).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns index of Key in given List.
%% @end
%%-------------------------------------------------------------------
-spec index(atom(), [atom()]) -> non_neg_integer().
index(Key, List) ->
    {Index, _} = lists:keyfind(Key, 2, lists:zip(lists:seq(1, length(List)), List)),
    Index.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Ensures that given field value is defined.
%% @end
%%-------------------------------------------------------------------
-spec ensure_defined(atom(), maps:map(), record()) -> term().
ensure_defined(children_attrs_hashes, #{
    hash_key := NewHashKey,
    hash_value := NewHashValue
}, #storage_sync_info{children_attrs_hashes = ChildrenAttrsHashes0}) ->
    case {NewHashKey, NewHashValue} of
        {undefined, _} -> ChildrenAttrsHashes0;
        {_, undefined} -> ChildrenAttrsHashes0;
        {_, <<"">>} -> ChildrenAttrsHashes0;
        {_, _} -> ChildrenAttrsHashes0#{NewHashKey => NewHashValue}
    end;
ensure_defined(FieldName, NewValuesMap, StorageSyncInfo) ->
    NewValue = maps:get(FieldName, NewValuesMap, undefined),
    utils:ensure_defined(NewValue, undefined, get_field_by_name(FieldName, StorageSyncInfo)).

%%--------------------------------------------------------------------
%% @doc
%% Creates or updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(key(), maps:map(), od_space:id()) -> {ok, doc()} | error().
create_or_update(Uuid, NewValuesMap, SpaceId
) ->
    Diff = fun(SSI = #storage_sync_info{
    }) ->
        MTime = ensure_defined(mtime, NewValuesMap, SSI),
        ChildrenAttrsHashes = ensure_defined(children_attrs_hashes, NewValuesMap, SSI),
        StatTime = ensure_defined(last_stat, NewValuesMap, SSI),
        {ok, SSI#storage_sync_info{
            mtime = MTime,
            children_attrs_hashes = ChildrenAttrsHashes,
            last_stat = StatTime
        }}
    end,
    NewDoc = new_doc(Uuid, NewValuesMap, SpaceId),
    datastore_model:update(?CTX, Uuid, Diff, NewDoc).

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns ?MODULE:doc()
%% @end
%%-------------------------------------------------------------------
-spec new_doc(key(), maps:map(), od_space:id()) -> doc().
new_doc(Key, NewValuesMap, SpaceId) ->
    NewMTime = maps:get(mtime, NewValuesMap, undefined),
    NewLastStat = maps:get(last_stat, NewValuesMap, undefined),
    #document{
        key = Key,
        value = #storage_sync_info{
            mtime = NewMTime,
            children_attrs_hashes = new_children_attrs_hashes(NewValuesMap),
            last_stat = NewLastStat
        },
        scope = SpaceId
    }.


-spec new_children_attrs_hashes(maps:map()) -> maps:map().
new_children_attrs_hashes(#{hash_key := undefined}) -> #{};
new_children_attrs_hashes(#{hash_value := undefined}) -> #{};
new_children_attrs_hashes(#{
    hash_key := NewHashKey,
    hash_value := NewHashValue
}) ->
    #{NewHashKey => NewHashValue};
new_children_attrs_hashes(#{}) -> #{}.

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
    3.

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
    {3, {?MODULE, ChildrenAttrsHash, MTime, MTime + 1}}.