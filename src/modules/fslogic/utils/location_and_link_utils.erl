%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on file_location.
%%% @end
%%%--------------------------------------------------------------------
-module(location_and_link_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").

%% API
-export([get_new_file_location_doc/3, is_location_created/2,
    mark_location_created/3]).
-export([create_imported_file_location/6, update_imported_file_location/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates file location of storage file
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location_doc(file_ctx:ctx(), StorageFileCreated :: boolean(),
    GeneratedKey :: boolean()) -> {file_location:record(), file_ctx:ctx()}.
get_new_file_location_doc(FileCtx, StorageFileCreated, GeneratedKey) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {Size, FileCtx4} = file_ctx:get_file_size(FileCtx3),
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = StorageFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = StorageFileCreated,
        size = Size
    },
    LocId = file_location:local_id(FileUuid),
    case fslogic_location_cache:create_location(#document{
        key = LocId,
        value = Location
    }, GeneratedKey) of
        {ok, _LocId} ->
            FileCtx5 = file_ctx:add_file_location(FileCtx4, LocId),
            {Location, FileCtx5};
        {error, already_exists} ->
            {#document{value = FileLocation}, FileCtx5} =
                file_ctx:get_local_file_location_doc(FileCtx4),
            {FileLocation, FileCtx5}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file location is created.
%% @end
%%--------------------------------------------------------------------
-spec is_location_created(file_meta:uuid(), file_location:id()) -> boolean().
is_location_created(FileUuid, FileLocationId) ->
    case fslogic_location_cache:get_location(FileLocationId, FileUuid, false) of
        {ok, #document{
            value = #file_location{storage_file_created = Created}
        }} ->
            Created;
        {ok, _} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that file is created on storage.
%% @end
%%--------------------------------------------------------------------
-spec mark_location_created(file_meta:uuid(), file_location:id(),
    helpers:file_id()) -> {ok, file_location:doc()} | {error, term()}.
mark_location_created(FileUuid, FileLocationId, StorageFileId) ->
    fslogic_location_cache:update_location(FileUuid, FileLocationId,
        fun(FileLocation = #file_location{storage_file_created = false}) ->
            {ok, FileLocation#file_location{storage_file_created = true,
                file_id = StorageFileId}}
        end, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates file_location
%% @end
%%--------------------------------------------------------------------
-spec create_imported_file_location(od_space:id(), storage:id(), file_meta:uuid(),
    file_meta:path(), file_meta:size(), od_user:id()) -> ok.
create_imported_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, Size, OwnerId) ->
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = CanonicalPath,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size,
        storage_file_created = true
    },
    LocationDoc = #document{
        key = file_location:local_id(FileUuid),
        value = Location,
        scope = SpaceId
    },
    LocationDoc2 = fslogic_location_cache:set_blocks(LocationDoc, create_file_blocks(Size)),
    {ok, _LocId} = file_location:save_and_bump_version(LocationDoc2, OwnerId),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Updates file_location
%% @end
%%-------------------------------------------------------------------
-spec update_imported_file_location(file_ctx:ctx(), non_neg_integer()) -> ok.
update_imported_file_location(FileCtx, StorageSize) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    NewFileBlocks = create_file_blocks(StorageSize),
    replica_updater:update(FileCtx, NewFileBlocks, StorageSize, true),
    ok = lfm_event_emitter:emit_file_written(
        FileGuid, NewFileBlocks, StorageSize, {exclude, ?ROOT_SESS_ID}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list containing one block with given size.
%% Is Size == 0 returns empty list.
%% @end
%%-------------------------------------------------------------------
-spec create_file_blocks(non_neg_integer()) -> fslogic_blocks:blocks().
create_file_blocks(0) -> [];
create_file_blocks(Size) -> [#file_block{offset = 0, size = Size}].