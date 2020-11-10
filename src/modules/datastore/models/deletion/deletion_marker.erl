%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(deletion_marker).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

%% API
-export([add/2, remove/2, check/2]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type link_name() :: helpers:file_id().
-type link_target() :: file_meta:uuid().
-type error() :: {error, term()}.


-define(CTX, #{model => ?MODULE}).

% RootId of a links tree associated with directory identified by ParentUuid
-define(ROOT_ID(ParentUuid), <<"deletion_marker_", ParentUuid/binary>>).

%TODO Co ze starymi deletion_linkami i upgradem?
%TODO Tak naprawdę to zostały tylko dla otwartych plików, a i tak zostaną usunięte
%TODO no i zostały dla katalogów, dla których był fail przy usuwaniu

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
add(ParentUuid, ChildCtx) ->
    {ChildStorageBasename, ChildCtx2} = storage_file_basename(ChildCtx),
    ChildUuid = file_ctx:get_uuid_const(ChildCtx2),
    add_link(ParentUuid, ChildStorageBasename, ChildUuid),
    ChildCtx2.

-spec remove(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
remove(ParentUuid, ChildCtx) ->
    {ChildStorageBasename, ChildCtx2} = storage_file_basename(ChildCtx),
    delete_link(ParentUuid, ChildStorageBasename),
    ChildCtx2.

-spec check(file_meta:uuid(), helpers:file_id()) -> {ok, link_target()} | error().
check(ParentUuid, ChildStorageBasename) ->
    get_link(ParentUuid, ChildStorageBasename).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_link(file_meta:uuid(), link_name()) -> {ok, link_target()} | error().
get_link(ParentUuid, ChildStorageBasename) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:get_links(?CTX, ?ROOT_ID(ParentUuid), TreeId, ChildStorageBasename) of
        {ok, [#link{target = ChildUuid}]} -> {ok, ChildUuid};
        Error -> Error
    end.


-spec add_link(file_meta:uuid(), link_name(), link_target()) -> ok.
add_link(ParentUuid, ChildStorageBasename, ChildUuid) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:add_links(?CTX, ?ROOT_ID(ParentUuid), TreeId, {ChildStorageBasename, ChildUuid}) of
        {ok, _} -> ok;
        % deletion marker may be added many times if removing file from storage returned an error
        {error, already_exists} -> ok
    end.


-spec delete_link(file_meta:uuid(), link_name()) -> ok.
delete_link(ParentUuid, ChildStorageBasename) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:delete_links(?CTX, ?ROOT_ID(ParentUuid), TreeId, ChildStorageBasename) of
        [] -> ok;
        ok -> ok
    end.


-spec storage_file_basename(file_ctx:ctx()) -> {helpers:file_id(), file_ctx:ctx()}.
storage_file_basename(FileCtx) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {filename:basename(StorageFileId), FileCtx2}.


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
