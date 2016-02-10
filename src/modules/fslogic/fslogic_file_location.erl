%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% file location management
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_file_location).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([create_storage_file_if_not_exists/4, create_storage_file/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(binary(), file_meta:uuid(), session:id(), file_meta:posix_permissions()) -> ok.
create_storage_file_if_not_exists(SpaceId, FileUuid, SessId, Mode) ->
    case file_meta:get_locations({uuid, FileUuid}) of
        {ok, []} ->
            {ok, _} = create_storage_file(SpaceId, FileUuid, SessId, Mode),
            ok;
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Create file_location and storage file
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(binary(), file_meta:uuid(), session:id(), file_meta:posix_permissions()) ->
    {ok, {FileId :: binary(), StorageId :: storage:id()}} | {error, already_exists}.
create_storage_file(SpaceId, FileUuid, SessId, Mode) ->
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),
    FileId = fslogic_utils:gen_storage_file_id({uuid, FileUuid}),
    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid},
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),

    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    LeafLess = fslogic_path:dirname(FileId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, undefined, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, Mode),
    {StorageId, FileId}.


%%%===================================================================
%%% Internal functions
%%%===================================================================