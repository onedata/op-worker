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
-export([create_or_update/5, delete/1, get/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

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
-spec create_or_update(key(), undefined | non_neg_integer(), undefined | non_neg_integer(),
    binary() | undefined, od_space:id()) -> {ok, doc()} | error().
create_or_update(Uuid, NewMTime, NewHashKey, NewHashValue, SpaceId) ->
    case datastore_model:exists(?CTX, Uuid) of
        {ok, true} -> update(Uuid,  NewMTime, NewHashKey, NewHashValue);
        {ok, false} -> create(Uuid, NewMTime, NewHashKey, NewHashValue, SpaceId)
    end.

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
%% Updates existing storage_sync_info document
%% @end
%%-------------------------------------------------------------------
-spec update(key(), non_neg_integer() | undefined, undefined | non_neg_integer(),
    binary() | undefined) -> {ok, doc()} | error().
update(Uuid, NewMTime, NewHashKey, NewHashValue) ->
    Diff = fun(SSI = #storage_sync_info{
        last_synchronized_mtime = MTime0,
        children_attrs_hashes = ChildrenAttrsHashes0
    }) ->
        MTime = utils:ensure_defined(NewMTime, undefined, MTime0),
        ChildrenAttrsHashes = case {NewHashKey, NewHashValue} of
            {undefined, _} -> ChildrenAttrsHashes0;
            {_, undefined} -> ChildrenAttrsHashes0;
            {_, <<"">>} -> ChildrenAttrsHashes0;
            {_, _} -> ChildrenAttrsHashes0#{NewHashKey => NewHashValue}
        end,
        {ok, SSI#storage_sync_info{
            last_synchronized_mtime = MTime,
            children_attrs_hashes = ChildrenAttrsHashes
        }}
    end,
    datastore_model:update(?CTX, Uuid, Diff).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates new storage_sync_info document.
%% @end
%%-------------------------------------------------------------------
-spec create(key(), non_neg_integer() | undefined, undefined | non_neg_integer(),
    binary() | undefined, od_space:id()) -> {ok, doc()} | error().
create(Key, NewMTime, NewHashKey, NewHashValue, SpaceId) ->
    Doc = #document{
        key = Key,
        value = #storage_sync_info{
            last_synchronized_mtime = NewMTime,
            children_attrs_hashes = #{NewHashKey => NewHashValue}
        },
        scope = SpaceId
    },
    datastore_model:create(?CTX, Doc).

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
        {children_attrs_hash, #{integer => binary}},
        {last_synchronized_mtime, integer}
    ]}.
