%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This provides functions operating on file_location use during file
%%% creation/deletion/import/sync/rename.
%%% @end
%%%--------------------------------------------------------------------
-module(file_location_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("proto/oneclient/common_messages.hrl").

%% API
-export([get_new_file_location_doc/3, create_file_location/2, delete_file_location/1,
    create_imported_file_location/6, update_imported_file_location/2]).
-export([try_to_resolve_child_link/2, try_to_resolve_child_deletion_link/2]).

%%%===================================================================
%%% API
%%%===================================================================

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

create_file_location(FileCtx, CreateOnStorageFun) ->
    % TODO - dac opcje zeby nie sprawdzac z wyprzedeniem (pod create) jesli mozna (co wtedy jesli pojawi sie sync?)
    {#document{
        key = FileLocationId,
        value = #file_location{storage_file_created = StorageFileCreated}
    }, FileCtx2} = Ans = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),

    case StorageFileCreated of
        false ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            replica_synchronizer:apply(FileCtx, fun() ->
                case fslogic_location_cache:get_location(FileLocationId, FileUuid, false) of
                    {ok, #document{
                        value = #file_location{storage_file_created = true}
                    }} ->
                        Ans;
                    {ok, _} ->
                        FileCtx3 = CreateOnStorageFun(FileCtx2),
                        {StorageFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),

                        {ok, #document{} = Doc} =
                            fslogic_location_cache:update_location(FileUuid, FileLocationId, fun
                                (FileLocation = #file_location{storage_file_created = false}) ->
                                    {ok, FileLocation#file_location{storage_file_created = true,
                                        file_id = StorageFileId}}
                            end, false),
                        {Doc, FileCtx4}
                end
            end);
        true ->
            Ans
    end.

delete_file_location(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    fslogic_location_cache:delete_location(FileUuid, file_location:local_id(FileUuid)).

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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function tries to resolve child with name FileName of
%% directory associated with ParentCtx.
%% @end
%%-------------------------------------------------------------------
-spec try_to_resolve_child_link(file_meta:name(), file_ctx:ctx()) ->
    {ok, file_meta:uuid()} | {error, term()}.
try_to_resolve_child_link(FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    fslogic_path:to_uuid(ParentUuid, FileName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function tries to resolve child's deletion_link
%% @end
%%-------------------------------------------------------------------
-spec try_to_resolve_child_deletion_link(file_meta:name(), file_ctx:ctx()) ->
    {ok, file_meta:uuid()} | {error, term()}.
try_to_resolve_child_deletion_link(FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    fslogic_path:to_uuid(ParentUuid, ?FILE_DELETION_LINK_NAME(FileName)).

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