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
-export([chmod_storage_files/3, rename_storage_file/5, rename_on_storage/3,
    create_storage_file_if_not_exists/2, create_storage_file/4, chown_file/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_files(user_ctx:ctx(), file_meta:entry(), file_meta:posix_permissions()) ->
    ok | no_return().
chmod_storage_files(UserCtx, File, Mode) ->  %todo use file_context
    SessId = user_ctx:get_session_id(UserCtx),
    case file_meta:get(File) of
        {ok, #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}} = FileDoc} ->
            {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, user_ctx:get_user_id(UserCtx)),
            Results = lists:map(
                fun({SID, FID} = Loc) ->
                    {ok, Storage} = storage:get(SID),
                    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),
                    {Loc, storage_file_manager:chmod(SFMHandle, Mode)}
                end, fslogic_utils:get_local_storage_file_locations(File)),

            case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
                [] -> ok;
                Errors ->
                    [?error("Unable to chmod [FileId: ~p] [StoragId: ~p] to mode ~p due to: ~p", [FID, SID, Mode, Reason])
                        || {{SID, FID}, {error, Reason}} <- Errors],

                    throw(?EAGAIN)
            end;
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% @doc Renames file on storage. Tries following methods until one succeeds:
%%  * link new file and remove old link
%%  * move file
%%  * create new file, copy contents and delete old file
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file(session:id(), Location :: #file_location{},
    TargetFileId :: helpers:file(), TargetSpaceId :: binary(), FileIdVersion :: non_neg_integer()) -> {ok, helpers:file()}.
rename_storage_file(SessId, Location, TargetFileId, TargetSpaceId, FileIdVersion) ->
    {ok, UserId} = session:get_user_id(SessId),
    #file_location{uuid = FileUUID, space_id = SourceSpaceId,
        storage_id = SourceStorageId, file_id = SourceFileId} = Location,
    {ok, #document{value = #file_meta{mode = Mode}}} = file_meta:get(FileUUID),

    {ok, TargetStorage} = fslogic_storage:select_storage(TargetSpaceId),
    SourceSpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SourceSpaceId),
    TargetSpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(TargetSpaceId),

    {ok, SourceStorage} = storage:get(SourceStorageId),

    TargetDir = fslogic_path:dirname(TargetFileId),
    TargetDirHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, TargetSpaceUUID, undefined, TargetStorage, TargetDir),
    case storage_file_manager:mkdir(TargetDirHandle, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok ->
            ok;
        {error, eexist} ->
            ok
    end,

    NextFileId = fslogic_utils:gen_storage_file_id({uuid, FileUUID}, TargetFileId, FileIdVersion),

    SourceHandle = storage_file_manager:new_handle(SessId, SourceSpaceUUID, FileUUID, SourceStorage, SourceFileId),
    case storage_file_manager:link(SourceHandle, TargetFileId) of
        ok ->
            ok = storage_file_manager:unlink(SourceHandle),
            {ok, TargetFileId};
        {error, eexist} ->
            rename_storage_file(SessId, Location, NextFileId, TargetSpaceId, FileIdVersion + 1);
        Error ->
            TargetHandleTest = storage_file_manager:new_handle(SessId, TargetSpaceUUID, FileUUID, TargetStorage, TargetFileId),
            case storage_file_manager:create(TargetHandleTest, Mode, true) of
                {error, eexist} ->
                    rename_storage_file(SessId, Location, NextFileId, TargetSpaceId, FileIdVersion + 1);
                _ ->
                    SourceRootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SourceSpaceUUID, FileUUID, SourceStorage, SourceFileId),
                    case storage_file_manager:mv(SourceRootHandle, TargetFileId) of
                        ok ->
                            TargetRootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, TargetSpaceUUID, FileUUID, TargetStorage, TargetFileId),
                            ok = storage_file_manager:chown(TargetRootHandle, UserId, TargetSpaceId);
                        Error ->
                            TargetHandle = storage_file_manager:new_handle(SessId, TargetSpaceUUID, FileUUID, TargetStorage, TargetFileId),
                            ok = copy_file_contents_sfm(SourceHandle, TargetHandle),
                            ok = storage_file_manager:unlink(SourceHandle)
                    end,
                    {ok, TargetFileId}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage and all its locations.
%% @end
%%--------------------------------------------------------------------
-spec rename_on_storage(user_ctx:ctx(), binary(), file_meta:entry()) -> ok.
rename_on_storage(UserCtx, TargetSpaceId, SourceEntry) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, #document{key = SourceUUID, value = #file_meta{mode = Mode}}} = file_meta:get(SourceEntry),

    lists:foreach(fun(#document{value = Location}) ->
        TempFileId = fslogic_utils:gen_storage_file_id({uuid, SourceUUID}),
        {ok, TargetFileId} = sfm_utils:rename_storage_file(SessId, Location, TempFileId, TargetSpaceId, 0),
        ok = replica_updater:rename(SourceUUID, TargetFileId, TargetSpaceId)
    end, fslogic_utils:get_local_file_locations(SourceEntry)),

    ok = sfm_utils:chmod_storage_files(
        user_ctx:new(?ROOT_SESS_ID),
        SourceEntry,
        Mode
    ).

%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(od_space:id(), datastore:document()) ->
    ok | {error, term()}.
create_storage_file_if_not_exists(SpaceId, FileDoc = #document{key = FileUuid,
    value = #file_meta{mode = Mode, uid = UserId}}) ->
    file_location:critical_section(FileUuid,
        fun() ->
            case fslogic_utils:get_local_file_locations(FileDoc) of
                [] ->
                    create_storage_file(SpaceId, FileUuid, ?ROOT_SESS_ID, Mode),
                    chown_file(FileUuid, UserId, SpaceId),
                    ok;
                _ ->
                    ok
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Create file_location and storage file
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(binary(), file_meta:uuid(), session:id(), file_meta:posix_permissions()) ->
    {FileId :: binary(), StorageId :: storage:id()}.
create_storage_file(SpaceId, FileUuid, SessId, Mode) ->
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),

    {ok, Path} = fslogic_path:gen_storage_path({uuid, FileUuid}),
    FileId0 = fslogic_utils:gen_storage_file_id({uuid, FileUuid}, Path, 0),
    LeafLess = fslogic_path:dirname(FileId0),

    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    FileCreatorFun = fun FileCreator(ToCreate, Version) ->
        SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, ToCreate),
        case storage_file_manager:create(SFMHandle1, Mode) of
            ok -> {ok, ToCreate};
            {error, eexist} ->
                NextVersion = Version + 1,
                NextFileId = fslogic_utils:gen_storage_file_id({uuid, FileUuid}, Path, NextVersion),
                case NextFileId of
                    ToCreate ->
                        ?error("Unable to generate different conflicted fileId: ~p vs ~p",
                            [{ToCreate, Version}, {NextFileId, NextVersion}]),
                        {error, create_loop_detected};
                    _ ->
                        FileCreator(NextFileId, NextVersion)
                end
        end
    end,

    {ok, FileId} = FileCreatorFun(FileId0, 0),

    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid,
        space_id = SpaceId},
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),

    {StorageId, FileId}.

%%--------------------------------------------------------------------
%% @doc
%% If given UserId is present in provider, then file owner is changes.
%% Otherwise, file is added to files awaiting owner change.
%% @end
%%--------------------------------------------------------------------
-spec chown_file(file_meta:uuid(), od_user:id(), od_space:id()) -> ok.
chown_file(FileUuid, UserId, SpaceId) ->
    case od_user:exists(UserId) of
        true ->
            files_to_chown:chown_file(FileUuid, UserId, SpaceId);
        false ->
            case files_to_chown:add(UserId, FileUuid) of
                {ok, _} ->
                    ok;
                AddAns ->
                    AddAns
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Copies file contents to another file on sfm level
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