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
-export([update_mtime_and_children_hash/5, update_mtime_and_stat_time/4,
    update_children_hash/4, update_stat_time/3, update_mtime/3,
    delete/1, get/1, new_doc/6]).

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

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_and_children_hash(key(), undefined | non_neg_integer(),
    non_neg_integer() | undefined, binary() | undefined, od_space:id()) -> 
    {ok, doc()} | error().
update_mtime_and_children_hash(Uuid, NewMTime, NewHashKey, NewHashValue, 
    SpaceId
) ->
    create_or_update(Uuid, NewMTime, NewHashKey, NewHashValue, undefined, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec update_children_hash(key(), undefined | non_neg_integer(),
    binary() | undefined, od_space:id()) -> {ok, doc()} | error().
update_children_hash(Uuid, NewHashKey, NewHashValue, SpaceId) ->
    create_or_update(Uuid, undefined, NewHashKey, NewHashValue, undefined, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec update_mtime(key(), undefined | non_neg_integer(), od_space:id()) ->
    {ok, doc()} | error().
update_mtime(Uuid, NewMTime, SpaceId) ->
    create_or_update(Uuid, NewMTime, undefined, undefined, undefined, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec update_stat_time(key(), undefined | non_neg_integer(), od_space:id()) ->
    {ok, doc()} | error().
update_stat_time(Uuid, NewStatTime, SpaceId) ->
    create_or_update(Uuid, undefined, undefined, undefined, NewStatTime, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_and_stat_time(key(), undefined | non_neg_integer(),
    undefined | non_neg_integer(), od_space:id()) -> {ok, doc()} | error().
update_mtime_and_stat_time(Uuid, NewMTime, StatTime, SpaceId) ->
    create_or_update(Uuid, NewMTime, undefined, undefined, StatTime, SpaceId).

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

%%--------------------------------------------------------------------
%% @doc
%% Updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(key(), undefined | non_neg_integer(),
    undefined | non_neg_integer(), binary() | undefined,
    undefined | non_neg_integer(), od_space:id()) -> {ok, doc()} | error().
create_or_update(Uuid, NewMTime, NewHashKey, NewHashValue, NewStatTime, SpaceId) ->
    Diff = fun(SSI = #storage_sync_info{
        mtime = MTime0,
        children_attrs_hashes = ChildrenAttrsHashes0,
        last_stat = StatTime0
    }) ->
        MTime = utils:ensure_defined(NewMTime, undefined, MTime0),
        ChildrenAttrsHashes = case {NewHashKey, NewHashValue} of
            {undefined, _} -> ChildrenAttrsHashes0;
            {_, undefined} -> ChildrenAttrsHashes0;
            {_, <<"">>} -> ChildrenAttrsHashes0;
            {_, _} -> ChildrenAttrsHashes0#{NewHashKey => NewHashValue}
        end,
        StatTime = utils:ensure_defined(NewStatTime, undefined, StatTime0),
        {ok, SSI#storage_sync_info{
            mtime = MTime,
            children_attrs_hashes = ChildrenAttrsHashes,
            last_stat = StatTime
        }}
    end,
    NewDoc = new_doc(Uuid, NewMTime, NewHashKey, NewHashValue, NewStatTime, SpaceId),
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
-spec new_doc(key(), non_neg_integer() | undefined, non_neg_integer() | undefined,
    binary() | undefined, non_neg_integer() | undefined, od_space:id()) -> doc().
new_doc(Key, NewMTime, NewHashKey, NewHashValue, StatTime, SpaceId) when
    NewHashKey =:= undefined;
    NewHashValue =:= undefined
->
    #document{
        key = Key,
        value = #storage_sync_info{
            mtime = NewMTime,
            children_attrs_hashes = #{},
            last_stat = StatTime
        },
        scope = SpaceId
    };
new_doc(Key, NewMTime, NewHashKey, NewHashValue, StatTime, SpaceId) ->
    #document{
        key = Key,
        value = #storage_sync_info{
            mtime = NewMTime,
            children_attrs_hashes = #{NewHashKey => NewHashValue},
            last_stat = StatTime
        },
        scope = SpaceId
    }.

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