%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for storage file manager module.
%%% @end
%%%--------------------------------------------------------------------
-module(sfm_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/5,
    rename_storage_file_updating_location/3, create_storage_file_if_not_exists/1,
    create_storage_file/2, delete_storage_file/2, delete_storage_file_without_location/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_file(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:posix_permissions()) -> ok | no_return().
chmod_storage_file(UserCtx, FileCtx, Mode) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case file_ctx:is_dir(FileCtx) of
        {true, _FileCtx2} ->
            ok;
        {false, FileCtx2} ->
            SFMHandle = storage_file_manager:new_handle(SessId, FileCtx2),
            ok = storage_file_manager:chmod(SFMHandle, Mode)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage, updates replica info.
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file_updating_location(user_ctx:ctx(), file_ctx:ctx(), od_space:id()) -> ok.
rename_storage_file_updating_location(UserCtx, SourceFileCtx, TargetSpaceId) ->
    {#document{value = #file_meta{mode = Mode}}, SourceFileCtx2} =
        file_ctx:get_file_doc(SourceFileCtx),
    {[#document{value = LocalLocation}], SourceFileCtx3} =
        file_ctx:get_local_file_location_docs(SourceFileCtx2),
    {FileId, SourceFileCtx4} = file_ctx:get_storage_file_id(SourceFileCtx3),
    [<<"/">>, _SourceSpaceId | Rest] = fslogic_path:split(FileId),
    TargetFileId = filename:join([<<"/">>, TargetSpaceId | Rest]),
    SessId = user_ctx:get_session_id(UserCtx),
    ok = sfm_utils:rename_storage_file(SessId, LocalLocation, TargetFileId, TargetSpaceId, Mode),
    ok = replica_updater:rename(SourceFileCtx4, TargetFileId, TargetSpaceId),

    FileUuid = file_ctx:get_uuid_const(SourceFileCtx),
    TargetFileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, TargetSpaceId)),
    ok = sfm_utils:chmod_storage_file(
        user_ctx:new(?ROOT_SESS_ID),
        TargetFileCtx,
        Mode
    ).

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file(session:id(), Location :: #file_location{},
    TargetFileId :: helpers:file_id(), TargetSpaceId :: od_space:id(),
    file_meta:posix_permissions()) -> ok.
rename_storage_file(SessId, Location, TargetFileId, TargetSpaceId, Mode) ->
    #file_location{uuid = FileUuid, space_id = SourceSpaceId,
        storage_id = SourceStorageId, file_id = SourceFileId} = Location,

    %create target dir
    {ok, TargetStorage = #document{key = TargetStorageId}} =
        fslogic_storage:select_storage(TargetSpaceId),
    TargetDir = filename:dirname(TargetFileId),
    TargetDirHandle = storage_file_manager:new_handle(?ROOT_SESS_ID,
        TargetSpaceId, undefined, TargetStorage, TargetDir, undefined),
    case storage_file_manager:mkdir(TargetDirHandle,
        ?AUTO_CREATED_PARENT_DIR_MODE, true)
    of
        ok ->
            ok;
        {error, eexist} ->
            ok
    end,

    {ok, SourceStorage} = storage:get(SourceStorageId),
    SourceHandle = storage_file_manager:new_handle(SessId, SourceSpaceId,
        FileUuid, SourceStorage, SourceFileId, undefined),
    TargetHandle = storage_file_manager:new_handle(SessId,
        TargetSpaceId, FileUuid, TargetStorage, TargetFileId, undefined),
    case storage_file_manager:stat(TargetHandle) of
        {ok, _} ->
            ok;
        _ ->
            case TargetStorageId =:= SourceStorageId of
                true ->
                    case SourceFileId =/= TargetFileId of
                        true ->
                            case storage_file_manager:mv(SourceHandle, TargetFileId) of
                                ok ->
                                    ok;
                                _ ->
                                    storage_file_manager:create(TargetHandle, Mode),
                                    ok = copy_file_contents_sfm(SourceHandle, TargetHandle),
                                    ok = storage_file_manager:unlink(SourceHandle)
                            end;
                        false ->
                            ok
                    end;
                false ->
                    storage_file_manager:create(TargetHandle, 8#770),
                    ok = copy_file_contents_sfm(SourceHandle, TargetHandle),
                    ok = storage_file_manager:unlink(SourceHandle)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(file_ctx:ctx()) -> ok | {error, term()}.
create_storage_file_if_not_exists(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    file_location:critical_section(FileUuid,
        fun() ->
            case file_ctx:get_local_file_location_docs(file_ctx:reset(FileCtx)) of
                {[], _} ->
                    {_, FileCtx2} = create_storage_file(user_ctx:new(?ROOT_SESS_ID), FileCtx),
                    files_to_chown:chown_or_schedule_chowning(FileCtx2),
                    ok;
                _ ->
                    ok
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Create file_location and storage file.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(user_ctx:ctx(), file_ctx:ctx()) ->
    {{helpers:file_id(), storage:id()}, file_ctx:ctx()}.
create_storage_file(UserCtx, FileCtx) ->
    FileCtx2 = create_parent_dirs(FileCtx),

    %create file on storage
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    {#document{key = StorageId}, FileCtx3} =
        file_ctx:get_storage_doc(FileCtx2),
    {#document{value = #file_meta{mode = Mode}}, FileCtx4} =
        file_ctx:get_file_doc(FileCtx3),
    {FileId, FileCtx5} = file_ctx:get_storage_file_id(FileCtx4),
    SFMHandle1 = storage_file_manager:new_handle(SessId, FileCtx),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, Mode),

    %create its location in db
    SpaceId = file_ctx:get_space_id_const(FileCtx5),
    Location = #file_location{
        provider_id = oneprovider:get_provider_id(),
        file_id = FileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId
    },
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),
    FileCtx6 = file_ctx:add_file_location(FileCtx5, LocId),

    {{StorageId, FileId}, FileCtx6}.

%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    FileLocationId = delete_storage_file_without_location(FileCtx, UserCtx),
    file_location:delete(FileLocationId).

%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file_without_location(file_ctx:ctx(), user_ctx:ctx()) ->
    datastore:ext_key().
delete_storage_file_without_location(FileCtx, UserCtx) ->
    {[#document{key = FileLocationId}], FileCtx2} =
        file_ctx:get_local_file_location_docs(FileCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    SFMHandle = storage_file_manager:new_handle(SessId, FileCtx2),
    storage_file_manager:unlink(SFMHandle),
    FileLocationId.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Copies file contents to another file on sfm level.
%% @end
%%--------------------------------------------------------------------
-spec copy_file_contents_sfm(HandleFrom :: storage_file_manager:handle(),
    HandleTo :: storage_file_manager:handle()) -> ok.
copy_file_contents_sfm(FromHandle, ToHandle) ->
    {ok, OpenFromHandle} = storage_file_manager:open(FromHandle, read),
    {ok, OpenToHandle} = storage_file_manager:open(ToHandle, write),
    {ok, ChunkSize} = application:get_env(?APP_NAME, rename_file_chunk_size),
    copy_file_contents_sfm(OpenFromHandle, OpenToHandle, 0, ChunkSize).

-spec copy_file_contents_sfm(HandleFrom :: storage_file_manager:handle(),
    HandleTo :: storage_file_manager:handle(), Offset :: non_neg_integer(),
    Size :: non_neg_integer()) -> ok.
copy_file_contents_sfm(FromHandle, ToHandle, Offset, Size) ->
    {ok, Data} = storage_file_manager:read(FromHandle, Offset, Size),
    DataSize = size(Data),
    {ok, DataSize} = storage_file_manager:write(ToHandle, Offset, Data),
    case DataSize of
        0 ->
            ok;
        _ ->
            copy_file_contents_sfm(FromHandle, ToHandle, Offset + DataSize, Size)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates parent dir on storage
%% @end
%%--------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx()) -> file_ctx:ctx().
create_parent_dirs(FileCtx) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {Storage, FileCtx3} = file_ctx:get_storage_doc(FileCtx2),

    LeafLess = filename:dirname(StorageFileId),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        FileUuid, Storage, LeafLess, undefined),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok ->
            FileCtx3;
        {error, eexist} ->
            FileCtx3
    end.