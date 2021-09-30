%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for trash management.
%%% Trash is a special directory where files are moved as a result
%%% of fslogic #move_to_trash operation.
%%% Trash directory is created for each space, it has
%%% a predefined uuid (see fslogic_uuid) and a predefined name.
%%% Trash directory is child of a space directory.
%%%
%%% TODO VFS-7064 below paragraph will be true after adding link from space directory to trash in 21.02
%%% Each provider adds link from space to trash to its own file_meta_forest
%%% tree. The links are not listed as conflicting because file_meta_forest
%%% module detects that they point to the same uuid.
%%%
%%% Currently, deletion using trash is performed on directories via GUI
%%% and REST/CDMI interfaces.
%%%
%%% Directories moved to trash become direct children of the trash directory.
%%% Therefore, their names are suffixed with their uuid to avoid conflicts.
%%% @end
%%%-------------------------------------------------------------------
-module(trash).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([create/1, move_to_trash/2, schedule_deletion_from_trash/4]).


-define(NAME_UUID_SEPARATOR, "@@").
-define(NAME_IN_TRASH(FileName, FileUuid), <<FileName/binary, ?NAME_UUID_SEPARATOR, FileUuid/binary>>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(od_space:id()) -> ok.
create(SpaceId) ->
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    TrashDoc = file_meta:new_doc(fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
        ?TRASH_DIR_NAME, ?DIRECTORY_TYPE, ?DEFAULT_DIR_MODE, ?SPACE_OWNER_ID(SpaceId),
        SpaceUuid, SpaceId
    ),
    % TODO VFS-7064 use file_meta:create so that link to the trash directory will be added
    %  * remember to filter trash from list result in storage_import_deletion or replica_controller, tree_traverse, etc
    %  * maybe there should be option passed to file_meta_forest:list that would exclude trash from the result
    %% {ok, _} = file_meta:create({uuid, SpaceUuid}, TrashDoc),
    {ok, _} = file_meta:save(TrashDoc),
    ok.


-spec move_to_trash(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
move_to_trash(FileCtx, UserCtx) ->
    file_ctx:assert_not_special_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = files_tree:get_parent_guid_if_not_root_dir(FileCtx, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    FileCtx3 = add_deletion_marker_if_applicable(ParentUuid, FileCtx2),
    {Name, FileCtx4} = file_ctx:get_aliased_name(FileCtx3, UserCtx),
    {FileDoc, FileCtx5} = file_ctx:get_file_doc(FileCtx4),
    % files moved to trash are direct children of trash directory
    % they names are suffixed with Uuid to avoid conflicts
    TrashUuid = fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
    % TODO VFS-7133 save original parent after extending file_meta in 21.02 !!!
    file_qos:cleanup_reference_related_documents(FileCtx5),
    ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    NameInTrash = ?NAME_IN_TRASH(Name, Uuid),
    file_meta:rename(FileDoc, ParentUuid, TrashUuid, NameInTrash),
    ok = dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
    ok = paths_cache:invalidate_on_all_nodes(SpaceId),
    TrashGuid = file_id:pack_guid(TrashUuid, SpaceId),
    fslogic_event_emitter:emit_file_renamed_no_exclude(
        FileCtx5, ParentGuid, TrashGuid, NameInTrash, Name),
    FileCtx5.


%%--------------------------------------------------------------------
%% @doc
%% This function starts a tree_deletion_traverse on given subtree to
%% asynchronously remove the subtree from the trash.
%% @end
%%--------------------------------------------------------------------
-spec schedule_deletion_from_trash(file_ctx:ctx(), user_ctx:ctx(), boolean(), file_meta:uuid()) ->
    {ok, tree_deletion_traverse:id()} | {error, term()}.
schedule_deletion_from_trash(FileCtx, _UserCtx, EmitEvents, RootOriginalParentUuid) ->
    file_ctx:assert_not_special_const(FileCtx),
    % TODO VFS-7348 schedule deletion as user not by root
    case tree_deletion_traverse:start(FileCtx, user_ctx:new(?ROOT_USER_ID), EmitEvents, RootOriginalParentUuid) of
        {ok, TaskId} ->
            {ok, TaskId};
        {error, _} = Error ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error("Unable to start deletion of ~s from trash in space ~s due to ~p.", [Guid, SpaceId, Error]),
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec add_deletion_marker_if_applicable(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
add_deletion_marker_if_applicable(ParentUuid, FileCtx) ->
    case file_ctx:is_imported_storage(FileCtx) of
        {true, FileCtx2} ->
            case file_ctx:is_storage_file_created(FileCtx2) of
                {true, FileCtx3} -> deletion_marker:add(ParentUuid, FileCtx3);
                {false, FileCtx3} -> FileCtx3
            end;
        {false, FileCtx2} -> FileCtx2
    end.