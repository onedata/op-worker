%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @todo simplify rename logic here
%%% @doc
%%% Utility functions for storage file manager module.
%%% @end
%%%--------------------------------------------------------------------
-module(sfm_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/6,
    create_storage_file_if_not_exists/1, create_storage_file_location/2,
    create_delayed_storage_file/1, create_storage_file/2, delete_storage_file/2,
    delete_storage_file_without_location/2, delete_storage_dir/2,
    create_parent_dirs/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_file(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:posix_permissions()) -> ok | {error, Reason :: term()} | no_return().
chmod_storage_file(UserCtx, FileCtx, Mode) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case file_ctx:is_dir(FileCtx) of
        {true, _FileCtx2} ->
            ok;
        {false, FileCtx2} ->
            {SFMHandle, _} = storage_file_manager:new_handle(SessId, FileCtx2),
            case storage_file_manager:chmod(SFMHandle, Mode) of
                ok -> ok;
                {error, ?ENOENT} -> ok;
                {error, ?EROFS} -> {error, ?EROFS}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file(session:id(), od_space:id(), storage:doc(),
    file_meta:uuid(), helpers:file_id(), helpers:file_id()) -> ok | {error, term()}.
rename_storage_file(SessId, SpaceId, Storage, FileUuid, SourceFileId, TargetFileId) ->
    %create target dir
    TargetDir = filename:dirname(TargetFileId),
    TargetDirHandle = storage_file_manager:new_handle(?ROOT_SESS_ID,
        SpaceId, undefined, Storage, TargetDir, undefined),
    case storage_file_manager:mkdir(TargetDirHandle,
        ?AUTO_CREATED_PARENT_DIR_MODE, true)
    of
        ok ->
            ok;
        {error, ?EEXIST} ->
            ok
    end,

    SourceHandle = storage_file_manager:new_handle(SessId, SpaceId,
        FileUuid, Storage, SourceFileId, undefined),

    case SourceFileId =/= TargetFileId of
        true ->
            storage_file_manager:mv(SourceHandle, TargetFileId);
        false ->
            ok
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
            case file_ctx:get_local_file_location_doc(file_ctx:reset(FileCtx)) of
                {undefined, _} ->
                    {_, _FileCtx2} = create_storage_file_location(FileCtx, false),
                    ok;
                _ ->
                    ok
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Create storage file if it hasn't been created yet (it has been delayed)
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_storage_file(file_ctx:ctx()) -> file_ctx:ctx().
create_delayed_storage_file(FileCtx) ->
    {#document{
        key = FileLocationId,
        value = #file_location{storage_file_created = StorageFileCreated}
    }, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),

    case StorageFileCreated of
        false ->
            file_location:update(FileLocationId, fun
                (#file_location{storage_file_created = true}) ->
                    {error, already_created};
                (FileLocation = #file_location{storage_file_created = false}) ->
                    try
                        FileCtx3 = create_storage_file(user_ctx:new(?ROOT_SESS_ID), FileCtx2),
                        files_to_chown:chown_or_schedule_chowning(FileCtx3),
                        {ok, FileLocation#file_location{storage_file_created = true}}
                    catch
                        Error:Reason ->
                            ?error_stacktrace("Error during storage file creation: ~p:~p",
                                [Error, Reason]),
                            {error, {Error, Reason}}
                    end
            end),
            FileCtx2;
        true ->
            FileCtx2
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates file location of storage file
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_location(file_ctx:ctx(), StorageFileCreated :: boolean()) ->
    {#file_location{}, file_ctx:ctx()}.
create_storage_file_location(FileCtx, StorageFileCreated) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    Location = #file_location{
        provider_id = oneprovider:get_id(fail_with_throw),
        file_id = FileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = StorageFileCreated
    },
    LocId = file_location:local_id(FileUuid),
    case file_location:create(#document{
        key = LocId,
        value = Location
    }) of
        {ok, _LocId} ->
            FileCtx4 = file_ctx:add_file_location(FileCtx3, LocId),
            {Location, FileCtx4};
        {error, already_exists} ->
            {#document{value = FileLocation}, FileCtx4} =
                file_ctx:get_local_file_location_doc(FileCtx3),
            {FileLocation, FileCtx4}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create file_location and storage file.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(user_ctx:ctx(), file_ctx:ctx()) ->
    file_ctx:ctx().
create_storage_file(UserCtx, FileCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {#document{value = #file_meta{mode = Mode}}, FileCtx2} =
        file_ctx:get_file_doc(FileCtx),
    {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
    {ok, FinalCtx} = case storage_file_manager:create(SFMHandle, Mode) of
        {error, ?ENOENT} ->
            FileCtx4 = create_parent_dirs(FileCtx3),
            {storage_file_manager:create(SFMHandle, Mode), FileCtx4};
        {error, ?EEXIST} ->
            storage_file_manager:unlink(SFMHandle),
            {storage_file_manager:create(SFMHandle, Mode), FileCtx3};
        Other ->
            {Other, FileCtx3}
    end,
    FinalCtx.

%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    delete_storage_file_without_location(FileCtx, UserCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    file_location:delete(file_location:local_id(FileUuid)).

%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file_without_location(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_file_without_location(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {SFMHandle, _} = storage_file_manager:new_handle(SessId, FileCtx),
    storage_file_manager:unlink(SFMHandle).

%%--------------------------------------------------------------------
%% @doc
%% Removes directory from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_dir(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_dir(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {SFMHandle, _} = storage_file_manager:new_handle(SessId, FileCtx),
    storage_file_manager:rmdir(SFMHandle).

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
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, LeafLess, undefined),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok ->
            FileCtx3;
        {error, eexist} ->
            FileCtx3
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

