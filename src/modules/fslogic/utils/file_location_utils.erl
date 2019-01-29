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

%% API
-export([get_new_file_location_doc/3, create_file_location/2, delete_file_location/1]).

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
                        {StorageFileId, FileCtx3} = CreateOnStorageFun(FileCtx2),

                        {ok, #document{} = Doc} =
                            fslogic_location_cache:update_location(FileUuid, FileLocationId, fun
                                (FileLocation = #file_location{storage_file_created = false}) ->
                                    {ok, FileLocation#file_location{storage_file_created = true,
                                        file_id = StorageFileId}}
                            end, false),
                        {Doc, FileCtx3}
                end
            end);
        true ->
            Ans
    end.

delete_file_location(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    fslogic_location_cache:delete_location(FileUuid, file_location:local_id(FileUuid)).