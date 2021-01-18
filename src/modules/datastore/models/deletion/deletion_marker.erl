%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements persistent map which associates name of
%%% a storage file with its uuid.
%%% The map is used to mark that given storage file (regular or directory)
%%% is associated with certain logical file that has been deleted or is
%%% currently being deleted.
%%% It is implemented using datastore links.
%%% Exactly one links forest (with only one tree) is associated with
%%% one file or directory.
%%%
%%% Deletion markers allow storage import to determine that given
%%% storage file should not be imported, to prevent re-imports.
%%%
%%% Deletion markers are added in 2 cases:
%%%  * when an opened file is deleted and storage on which the file is
%%%    located supports ?DELETION_MARKER method for handling deletion of
%%%    opened files (see fslogic_delete and opened_file_deletion_method() type)
%%%  * when a directory is moved to trash
%%%
%%% As deletion markers are used only by the storage import, they are
%%% added only on the imported storages.
%%% @end
%%%-------------------------------------------------------------------
-module(deletion_marker).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

%% API
-export([add/2, remove/2, remove_by_name/2, check/2]).

%% datastore_model callbacks
-export([get_ctx/0]).

% name of file on storage
-type storage_file_basename() :: binary().
-type link_name() :: storage_file_basename().
-type link_target() :: file_meta:uuid().
-type error() :: {error, term()}.


-define(CTX, #{model => ?MODULE}).

% RootId of a links tree associated with directory identified by ParentUuid
-define(FOREST_ID(ParentUuid), <<"deletion_marker_", ParentUuid/binary>>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
add(ParentUuid, FileCtx) ->
    {StorageFileBasename, FileCtx2} = storage_file_basename(FileCtx),
    ChildUuid = file_ctx:get_uuid_const(FileCtx2),
    add_link(ParentUuid, StorageFileBasename, ChildUuid),
    FileCtx2.

-spec remove(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
remove(ParentUuid, FileCtx) ->
    {StorageFileBasename, FileCtx2} = storage_file_basename(FileCtx),
    remove_by_name(ParentUuid, StorageFileBasename),
    FileCtx2.

-spec remove_by_name(file_meta:uuid(), storage_file_basename()) -> ok.
remove_by_name(ParentUuid, StorageFileBasename) ->
    delete_link(ParentUuid, StorageFileBasename).

-spec check(file_meta:uuid(), storage_file_basename()) -> {ok, link_target()} | error().
check(ParentUuid, StorageFileBasename) ->
    get_link(ParentUuid, StorageFileBasename).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_link(file_meta:uuid(), link_name()) -> {ok, link_target()} | error().
get_link(ParentUuid, StorageFileBasename) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:get_links(?CTX, ?FOREST_ID(ParentUuid), TreeId, StorageFileBasename) of
        {ok, [#link{target = FileUuid}]} -> {ok, FileUuid};
        Error -> Error
    end.


-spec add_link(file_meta:uuid(), link_name(), link_target()) -> ok.
add_link(ParentUuid, StorageFileBasename, FileUuid) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:add_links(?CTX, ?FOREST_ID(ParentUuid), TreeId, {StorageFileBasename, FileUuid}) of
        {ok, _} -> ok;
        % deletion marker may be added many times if removing file from storage returned an error
        {error, already_exists} -> ok
    end.


-spec delete_link(file_meta:uuid(), link_name()) -> ok.
delete_link(ParentUuid, StorageFileBasename) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:delete_links(?CTX, ?FOREST_ID(ParentUuid), TreeId, StorageFileBasename) of
        [] -> ok;
        ok -> ok
    end.


-spec storage_file_basename(file_ctx:ctx()) -> {storage_file_basename(), file_ctx:ctx()}.
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
