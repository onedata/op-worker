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

-type key() :: datastore:key().
-type doc() :: datastore_doc:doc(record()).
-type record() :: #storage_sync_info{}.
-type error() :: {error, term()}.
-type diff() :: datastore_doc:diff(record()).

-export_type([key/0, doc/0, record/0]).

%% API
-export([delete/2, get/2, create_or_update/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2, id/2]).

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
-spec get(helpers:file_id(), od_space:id()) -> {ok, doc()} | error().
get(StorageFileId, SpaceId) ->
    datastore_model:get(?CTX, id(StorageFileId, SpaceId)).

%%--------------------------------------------------------------------
%% @doc
%% Creates or updates storage_sync_info document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(helpers:file_id(), diff(), od_space:id()) ->
    {ok, doc()} | error().
create_or_update(StorageFileId, Diff, SpaceId) ->
    Id = id(StorageFileId, SpaceId),
    DefaultDoc = default_doc(Id, Diff, SpaceId),
    datastore_model:update(?CTX, Id, Diff, DefaultDoc).

%%-------------------------------------------------------------------
%% @doc
%% @equiv datastore_model:delete(?CTX, Uuid).
%% @end
%%-------------------------------------------------------------------
-spec delete(undefined | helpers:file_id(), od_space:id()) -> ok | error().
delete(undefined, _SpaceId) ->
    ok;
delete(StorageFileId, SpaceId) ->
    datastore_model:delete(?CTX, id(StorageFileId, SpaceId)).

%%-------------------------------------------------------------------
%% @doc
%% Generates key basing of given FilePath
%% @end
%%-------------------------------------------------------------------
-spec id(helpers:file_id(), od_space:id()) -> key().
id(StorageFileId, SpaceId) ->
    datastore_key:adjacent_from_digest([StorageFileId], SpaceId).

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns default doc on which Diff has been performed.
%% @end
%%-------------------------------------------------------------------
-spec default_doc(key(), diff(), od_space:id()) -> doc().
default_doc(Key, Diff, SpaceId) ->
    {ok, NewSSI} = Diff(#storage_sync_info{}),
    #document{
        key = Key,
        value = NewSSI,
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