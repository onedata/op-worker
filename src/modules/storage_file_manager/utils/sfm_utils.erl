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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/6, create_storage_file_if_not_exists/1,
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
        {error, eexist} ->
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