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
    create_storage_file_location/3, create_delayed_storage_file/1,
    create_storage_file/2, delete_storage_file/2,
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
    } = FL, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),

    case StorageFileCreated of
        false ->
            ?info("cccc 1 ~p", [{FL, FileCtx2}]),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            file_location:critical_section(FileUuid, fun() ->
                case file_location:get(FileLocationId) of
                    {ok, #document{
                        value = #file_location{storage_file_created = true}
                    }} ->
                        FileCtx2;
                    {ok, _} ->
                        FileCtx3 = create_storage_file(
                            user_ctx:new(?ROOT_SESS_ID), FileCtx2),
                        files_to_chown:chown_or_schedule_chowning(FileCtx3),

                        {ok, #document{} = Doc} = file_location:update(FileLocationId, fun
                            (FileLocation = #file_location{storage_file_created = false}) ->
                                {ok, FileLocation#file_location{storage_file_created = true}}
                        end),
                        file_ctx:update_location_doc(FileCtx3, Doc)
                end
            end);
        true ->
            ?info("cccc 2 ~p", [{FL, FileCtx2}]),
            FileCtx2
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates file location of storage file
%% @equiv create_storage_file_location(FileCtx, StorageFileCreated, false)
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_location(file_ctx:ctx(), StorageFileCreated :: boolean()) ->
    {#file_location{}, file_ctx:ctx()}.
create_storage_file_location(FileCtx, StorageFileCreated) ->
    create_storage_file_location(FileCtx, StorageFileCreated, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates file location of storage file
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_location(file_ctx:ctx(), StorageFileCreated :: boolean(),
    GeneratedKey :: boolean()) -> {#file_location{}, file_ctx:ctx()}.
create_storage_file_location(FileCtx, StorageFileCreated, GeneratedKey) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = StorageFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = StorageFileCreated
    },
    LocId = file_location:local_id(FileUuid),
    case file_location:create(#document{
        key = LocId,
        value = Location
    }, GeneratedKey) of
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
            ?info("cccc 3 ~p", [FileCtx]),
            FileCtx4 = create_parent_dirs(FileCtx3),
            {storage_file_manager:create(SFMHandle, Mode), FileCtx4};
        {error, ?EEXIST} = Eexists ->
            ?info("cccc 4 ~p", [FileCtx]),
            case application:get_env(?APP_NAME, unlink_on_create, true) of
                true ->
                    storage_file_manager:unlink(SFMHandle),
                    {storage_file_manager:create(SFMHandle, Mode), FileCtx3};
                _ ->
                    Eexists
            end;
        Other ->
            ?info("cccc 5 ~p", [{Other, FileCtx}]),
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
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {Storage, FileCtx3} = file_ctx:get_storage_doc(FileCtx),
    {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, undefined),
    create_parent_dirs(ParentCtx, [], SpaceId, Storage),
    FileCtx4.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail recursive helper function of ?MODULE:create_parent_dirs/1
%% @end
%%-------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx(), [file_ctx:ctx()], od_space:id(), storage:doc()) -> ok.
create_parent_dirs(FileCtx, ChildrenDirCtxs, SpaceId, Storage) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            lists:foreach(fun(Ctx) ->
                create_dir(Ctx, SpaceId, Storage)
            end, [FileCtx | ChildrenDirCtxs]);
        false ->
            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, undefined),
            create_parent_dirs(ParentCtx, [FileCtx2 | ChildrenDirCtxs], SpaceId, Storage)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage with suitable mode and owner.
%% @end
%%-------------------------------------------------------------------
-spec create_dir(file_ctx:ctx(), od_space:id(), storage:doc()) -> file_ctx:ctx().
create_dir(FileCtx, SpaceId, Storage) ->
    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, FileId, undefined),
    try
        {#document{value = #file_meta{
            mode = Mode}}, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
        case file_ctx:is_space_dir_const(FileCtx3) of
            true ->
                mkdir_and_maybe_chown(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, FileCtx3, false);
            false ->
                mkdir_and_maybe_chown(SFMHandle0, Mode, FileCtx3, true)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Creating parent dir ~p failed due to ~p.
            Parent dir will be create with default mode.", [FileId, {Error, Reason}]),
            mkdir_and_maybe_chown(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, FileCtx, true)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage and chowns it if ShouldChown flag is set to
%% true.
%% @end
%%-------------------------------------------------------------------
-spec mkdir_and_maybe_chown(storage_file_manager:handle(), non_neg_integer(),
    file_ctx:ctx(), boolean()) -> any().
mkdir_and_maybe_chown(SFMHandle, Mode, FileCtx, ShouldChown) ->
    case storage_file_manager:mkdir(SFMHandle, Mode, false) of
        ok ->
            case ShouldChown of
                true ->
                    files_to_chown:chown_or_schedule_chowning(FileCtx);
                false ->
                    ok
            end,
            FileCtx;
        {error, eexist} ->
            FileCtx
    end.