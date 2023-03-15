%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing request for renaming files or
%%% directories
%%% @end
%%%-------------------------------------------------------------------
-module(rename_req).
-author("Mateusz Paciorek").
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([rename/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Renames file or directory
%% @end
%%--------------------------------------------------------------------
-spec rename(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    validate_target_name(TargetName),
    file_ctx:assert_not_special_const(SourceFileCtx),
    file_ctx:assert_not_trash_dir_const(TargetParentFileCtx, TargetName),
    SourceSpaceId = file_ctx:get_space_id_const(SourceFileCtx),
    TargetSpaceId = file_ctx:get_space_id_const(TargetParentFileCtx),
    case SourceSpaceId =:= TargetSpaceId of
        false ->
            % TODO VFS-6627 Handle interprovider move to RO storage
            rename_between_spaces(
                UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
        true ->
            TargetParentFileCtx2 = file_ctx:assert_not_readonly_storage(TargetParentFileCtx),
            rename_within_space(
                UserCtx, SourceFileCtx, TargetParentFileCtx2, TargetName)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file between spaces, the operation removes target if it is of the
%% same type as source, copies source into target, and removes source.
%% @end
%%--------------------------------------------------------------------
-spec rename_between_spaces(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename_between_spaces(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    SessId = user_ctx:get_session_id(UserCtx),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentFileCtx), % TODO VFS-7446 - rename vs hardlinks
    {SourceFileType, SourceFileCtx2} = get_type(SourceFileCtx),
    {TargetFileType, TargetGuid} =
        remotely_get_child_type(SessId, TargetParentGuid, TargetName),
    case {SourceFileType, TargetFileType} of
        {_, undefined} ->
            copy_and_remove(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName);
        {TheSameType, TheSameType} ->
            ok = lfm:unlink(SessId, ?FILE_REF(TargetGuid), false),
            copy_and_remove(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName);
        {?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE} ->
            throw(?EISDIR);
        {?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE} ->
            throw(?ENOTDIR)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Copies source file/dir into target dir (to different space) and then removes
%% source file/dir.
%% @end
%%--------------------------------------------------------------------
-spec copy_and_remove(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
copy_and_remove(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    SessId = user_ctx:get_session_id(UserCtx),
    SourceGuid = file_ctx:get_logical_guid_const(SourceFileCtx),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentFileCtx),
    case file_copy:copy(SessId, SourceGuid, TargetParentGuid, TargetName) of
        {ok, NewCopiedGuid, ChildEntries} ->
            case lfm:rm_recursive(SessId, ?FILE_REF(SourceGuid)) of
                ok ->
                    RenameChildEntries = lists:map(
                        fun({OldGuid, NewGuid, NewParentGuid, NewName}) ->
                            #file_renamed_entry{
                                old_guid = OldGuid,
                                new_guid = NewGuid,
                                new_parent_guid = NewParentGuid,
                                new_name = NewName
                            }
                        end, ChildEntries),

                    #fuse_response{
                        status = #status{code = ?OK},
                        fuse_response = #file_renamed{
                            new_guid = NewCopiedGuid,
                            child_entries = RenameChildEntries}
                    };
                {error, Code} ->
                    #fuse_response{status = #status{code = Code}}
            end;
        {error, Code} ->
            #fuse_response{status = #status{code = Code}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file or directory within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_within_space(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename_within_space(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    {SourceFileType, SourceFileCtx2} = get_type(SourceFileCtx),
    {TargetFileType, TargetFileCtx, TargetParentFileCtx2} =
        get_child_type(TargetParentFileCtx, TargetName, UserCtx),

    SourceEqualsTarget = ((TargetFileCtx =/= undefined)
        andalso (file_ctx:equals(SourceFileCtx2, TargetFileCtx))),

    {SourcePath, SourceFileCtx3} = file_ctx:get_canonical_path(SourceFileCtx2),
    {TargetPath, TargetParentFileCtx3} = file_ctx:get_canonical_path(TargetParentFileCtx2),

    SourceTokens = filepath_utils:split(SourcePath),
    TargetTokens = filepath_utils:split(TargetPath),
    TargetTokensBeg = lists:sublist(TargetTokens, length(SourceTokens)),
    MoveIntoItself = (TargetTokensBeg =:= SourceTokens),

    case {SourceEqualsTarget, MoveIntoItself} of
        {true, _} ->
            rename_into_itself(file_ctx:get_logical_guid_const(SourceFileCtx3));
        {false, true} ->
            #fuse_response{
                status = #status{code = ?EINVAL}
            };
        _ ->
            rename_into_different_place_within_space(
                UserCtx, SourceFileCtx3, TargetParentFileCtx3, TargetName,
                SourceFileType, TargetFileType, TargetFileCtx)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Source and target files are the same, returns success and does nothing more.
%% @end
%%--------------------------------------------------------------------
-spec rename_into_itself(FileGuid :: fslogic_worker:file_guid()) ->
    #fuse_response{}.
rename_into_itself(FileGuid) ->
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_renamed{
            new_guid = FileGuid,
            child_entries = []
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file into different place in the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_into_different_place_within_space(user_ctx:ctx(),
    SourceFileCtx :: file_ctx:ctx(), TargetParentFileCtx :: file_ctx:ctx(),
    TargetName :: file_meta:name(), SourceFileType :: file_meta:type(),
    TargetFileType :: file_meta:type() | undefined, TargetParentFileCtx :: file_ctx:ctx() | undefined) ->
    no_return() | #fuse_response{}.
rename_into_different_place_within_space(UserCtx, SourceFileCtx, TargetParentFileCtx,
    TargetName, SourceFileType, TargetFileType, TargetFileCtx) ->
    {Storage, SourceFileCtx2} = file_ctx:get_storage(SourceFileCtx),
    Helper = storage:get_helper(Storage),
    case helper:is_rename_supported(Helper) of
        true ->
            rename_into_different_place_within_posix_space(UserCtx, SourceFileCtx2,
                TargetParentFileCtx, TargetName, SourceFileType, TargetFileType,
                TargetFileCtx);
        _ ->
            rename_into_different_place_within_non_posix_space(UserCtx, SourceFileCtx2,
                TargetParentFileCtx, TargetName, SourceFileType, TargetFileType,
                TargetFileCtx)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file into different place in the same space. The space must be
%% supported with posix storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_into_different_place_within_posix_space(user_ctx:ctx(),
    SourceFileCtx :: file_ctx:ctx(), TargetParentFileCtx :: file_ctx:ctx(),
    TargetName :: file_meta:name(), SourceFileType :: file_meta:type(),
    TargetFileType :: file_meta:type() | undefined, TargetFileCtx :: file_ctx:ctx() | undefined) ->
    no_return() | #fuse_response{}.
rename_into_different_place_within_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, ?DIRECTORY_TYPE, undefined, _
) ->
    rename_dir(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
rename_into_different_place_within_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, ?REGULAR_FILE_TYPE, undefined, _
) ->
    rename_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
rename_into_different_place_within_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, ?DIRECTORY_TYPE, ?DIRECTORY_TYPE,
    TargetFileCtx
) ->
    #fuse_response{status = #status{code = ?OK}} =
        delete_req:delete(UserCtx, TargetFileCtx, false),
    rename_dir(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
rename_into_different_place_within_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, ?REGULAR_FILE_TYPE, ?REGULAR_FILE_TYPE, TargetFileCtx
) ->
    {#document{key = FileLocationId, value = #file_location{uuid = TargetFileUuid}}, _} =
        file_ctx:get_local_file_location_doc(TargetFileCtx),
    fslogic_location_cache:update_location(TargetFileUuid, FileLocationId, fun
        (FileLocation) -> {ok, FileLocation#file_location{storage_file_created = false}}
    end, false),
    #fuse_response{status = #status{code = ?OK}} =
        delete_req:delete(UserCtx, TargetFileCtx, false),
    rename_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
rename_into_different_place_within_posix_space(_, _, _, _,
    ?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE, _
) ->
    throw(?ENOTDIR);
rename_into_different_place_within_posix_space(_, _, _, _,
    ?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, _
) ->
    throw(?EISDIR).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file on storage with flat paths, i.e. only modifies
%% file_meta and optionally removes the target file, if TargetGuid
%% is not undefined (e.g. if target file already exists) it will be
%% removed first.
%% @end
%%--------------------------------------------------------------------
-spec rename_file_on_flat_storage(
    user_ctx:ctx(),
    SourceFileCtx :: file_ctx:ctx(),
    FileType :: file_meta:type(),
    TargetParentFileCtx :: file_ctx:ctx(),
    TargetName :: file_meta:name(),
    TargetGuid :: undefined | fslogic_worker:file_guid()
) ->
    #fuse_response{}.
rename_file_on_flat_storage(UserCtx, SourceFileCtx0, FileType, TargetParentFileCtx0, TargetName, TargetGuid) ->
    TargetParentAddPerm = case FileType of
        ?REGULAR_FILE_TYPE -> ?add_object_mask;
        ?DIRECTORY_TYPE -> ?add_subcontainer_mask
    end,

    {SourceFileParentCtx, SourceFileCtx1} = file_tree:get_parent(
        SourceFileCtx0, UserCtx
    ),
    SourceFileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, SourceFileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_mask)]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, SourceFileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
    ),
    TargetParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, TargetParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, TargetParentAddPerm)]
    ),
    rename_file_on_flat_storage_insecure(
        UserCtx, SourceFileCtx2,
        TargetParentFileCtx1, TargetName, TargetGuid
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file on storage with flat paths, i.e. only modifies
%% file_meta and optionally removes the target file, if TargetGuid
%% is not undefined, i.e. target file already exists, it will be removed
%% first. Does not check permissions.
%% @end
%%--------------------------------------------------------------------
-spec rename_file_on_flat_storage_insecure(UserCtx :: user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name(),
    TargetGuid :: undefined | fslogic_worker:file_guid()) -> #fuse_response{}.
rename_file_on_flat_storage_insecure(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName, TargetGuid) ->
    SourceGuid = file_ctx:get_logical_guid_const(SourceFileCtx),
    {ParentDoc, TargetParentFileCtx2} = file_ctx:get_file_doc(TargetParentFileCtx),
    {SourceDoc, SourceFileCtx2} = file_ctx:get_file_doc(SourceFileCtx),
    {SourceParentFileCtx, SourceFileCtx3} = file_tree:get_parent(SourceFileCtx2, UserCtx),
    {SourceParentDoc, SourceParentFileCtx2} = file_ctx:get_file_doc(SourceParentFileCtx),
    ok = case TargetGuid of
        undefined ->
            ok;
        _ ->
            SessId = user_ctx:get_session_id(UserCtx),
            ok = lfm:unlink(SessId, ?FILE_REF(TargetGuid), false)
    end,
    ok = file_meta:rename(SourceDoc, SourceParentDoc, ParentDoc, TargetName),
    maybe_unset_ignore_in_changes(SourceFileCtx3, SourceDoc, ParentDoc),
    on_successful_rename(UserCtx, SourceFileCtx3, SourceParentFileCtx2, TargetParentFileCtx2, TargetName),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_renamed{
            new_guid = SourceGuid,
            child_entries = []
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file into different place in the same space. The space does not have
%% a support from posix storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_into_different_place_within_non_posix_space(user_ctx:ctx(),
    SourceFileCtx :: file_ctx:ctx(), TargetParentFileCtx :: file_ctx:ctx(),
    TargetName :: file_meta:name(), SourceFileType :: file_meta:type(),
    TargetFileType :: file_meta:type(), TargetParentFileCtx :: file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
rename_into_different_place_within_non_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, SourceFileType, undefined, _
) ->
    {Storage, SourceFileCtx1} = file_ctx:get_storage(SourceFileCtx),
    Helper = storage:get_helper(Storage),
    StoragePathType = helper:get_storage_path_type(Helper),

    case StoragePathType of
        ?FLAT_STORAGE_PATH ->
            rename_file_on_flat_storage(UserCtx, SourceFileCtx1, SourceFileType,
                TargetParentFileCtx, TargetName, undefined
            );
        _ ->
            copy_and_remove(UserCtx, SourceFileCtx1, TargetParentFileCtx, TargetName)
    end;
rename_into_different_place_within_non_posix_space(UserCtx, SourceFileCtx,
    TargetParentFileCtx, TargetName, TheSameType, TheSameType, TargetFileCtx
) ->
    TargetGuid = file_ctx:get_logical_guid_const(TargetFileCtx),
    SessId = user_ctx:get_session_id(UserCtx),

    {Storage, SourceFileCtx1} = file_ctx:get_storage(SourceFileCtx),
    Helper = storage:get_helper(Storage),
    StoragePathType = helper:get_storage_path_type(Helper),

    case StoragePathType of
        ?FLAT_STORAGE_PATH ->
            rename_file_on_flat_storage(UserCtx, SourceFileCtx1, TheSameType,
                TargetParentFileCtx, TargetName, TargetGuid
            );
        _ ->
            ok = lfm:unlink(SessId, ?FILE_REF(TargetGuid), false),
            copy_and_remove(UserCtx, SourceFileCtx1, TargetParentFileCtx, TargetName)
    end;
rename_into_different_place_within_non_posix_space(_, _, _, _,
    ?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, _
) ->
    throw(?EISDIR);
rename_into_different_place_within_non_posix_space(_, _, _, _,
    ?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE, _
) ->
    throw(?ENOTDIR).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_file(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename_file(UserCtx, SourceFileCtx0, TargetParentFileCtx0, TargetName) ->
    {SourceFileParentCtx, SourceFileCtx1} = file_tree:get_parent(
        SourceFileCtx0, UserCtx
    ),
    SourceFileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, SourceFileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_mask)]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, SourceFileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
    ),
    TargetParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, TargetParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?add_object_mask)]
    ),
    rename_file_insecure(
        UserCtx, SourceFileCtx2, TargetParentFileCtx1, TargetName
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_dir(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename_dir(UserCtx, SourceFileCtx0, TargetParentFileCtx0, TargetName) ->
    {SourceFileParentCtx, SourceFileCtx1} = file_tree:get_parent(
        SourceFileCtx0, UserCtx
    ),
    SourceFileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, SourceFileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_mask)]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, SourceFileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
    ),
    TargetParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, TargetParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?add_subcontainer_mask)]
    ),
    rename_dir_insecure(
        UserCtx, SourceFileCtx2, TargetParentFileCtx1, TargetName
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_file_insecure(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_file_insecure(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    {SourceFileCtx3, TargetFileId, _} =
        rename_meta_and_storage_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName),
    replica_updater:rename(SourceFileCtx, TargetFileId),
    FileGuid = file_ctx:get_logical_guid_const(SourceFileCtx3),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_renamed{
            new_guid = FileGuid,
            child_entries = []
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames directory within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_dir_insecure(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_dir_insecure(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    {SourceFileCtx3, TargetFileId, UnsetAction} =
        rename_meta_and_storage_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName),
    ChildEntries = rename_child_locations(UserCtx, SourceFileCtx3, TargetFileId, UnsetAction),
    FileGuid = file_ctx:get_logical_guid_const(SourceFileCtx3),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_renamed{
            new_guid = FileGuid,
            child_entries = ChildEntries
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file_meta and file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_meta_and_storage_file(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    {TargetFileCtx :: file_ctx:ctx(), TargetFileId :: helpers:file_id(), unset | ignored}.
rename_meta_and_storage_file(UserCtx, SourceFileCtx0, TargetParentCtx0, TargetName) ->
    {SourceFileId, SourceFileCtx} = file_ctx:get_storage_file_id(SourceFileCtx0),
    {TargetParentFileId, TargetParentCtx} = file_ctx:get_storage_file_id(TargetParentCtx0),
    TargetFileId = filename:join(TargetParentFileId, TargetName),

    FileUuid = file_ctx:get_logical_uuid_const(SourceFileCtx),
    {ParentDoc, TargetParentCtx2} = file_ctx:get_file_doc(TargetParentCtx),
    {SourceDoc, SourceFileCtx2} = file_ctx:get_file_doc(SourceFileCtx),
    {SourceParentFileCtx, SourceFileCtx3} = file_tree:get_parent(SourceFileCtx2, UserCtx),
    {SourceParentDoc, SourceParentFileCtx2} = file_ctx:get_file_doc(SourceParentFileCtx),
    file_meta:rename(SourceDoc, SourceParentDoc, ParentDoc, TargetName),
    UnsetAction = maybe_unset_ignore_in_changes(SourceFileCtx3, SourceDoc, ParentDoc),

    SpaceId = file_ctx:get_space_id_const(SourceFileCtx3),
    paths_cache:invalidate_on_all_nodes(SpaceId),
    dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
    archive_recall_cache:invalidate_on_all_nodes(SpaceId),
    file_meta_sync_status_cache:invalidate_on_all_nodes(SpaceId),

    {Storage, SourceFileCtx4} = file_ctx:get_storage(SourceFileCtx3),
    Helper = storage:get_helper(Storage),
    StoragePathType = helper:get_storage_path_type(Helper),
    StorageId = storage:get_id(Storage),
    {IsStorageFileCreated, SourceFileCtx5} = file_ctx:is_storage_file_created(SourceFileCtx4),
    {IsDir, SourceFileCtx6} = file_ctx:is_dir(SourceFileCtx5),
    case StoragePathType =:= ?CANONICAL_STORAGE_PATH andalso IsStorageFileCreated of
        true ->
            ok = sd_utils:rename(UserCtx, SpaceId, StorageId, FileUuid, SourceFileId, TargetParentCtx2, TargetFileId),
            IsDir andalso (ok = dir_location:update_storage_file_id(FileUuid, TargetFileId));
        false ->
            ok
    end,
    on_successful_rename(UserCtx, SourceFileCtx6, SourceParentFileCtx2, TargetParentCtx2, TargetName),
    {SourceFileCtx6, TargetFileId, UnsetAction}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames location of all child files, returns rename result as a list.
%% @end
%%--------------------------------------------------------------------
-spec rename_child_locations(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    ParentStorageFileId :: helpers:file_id(), unset | ignored) -> [#file_renamed_entry{}].
rename_child_locations(UserCtx, ParentFileCtx, ParentStorageFileId, RootUnsetAction) ->
    ListOpts = #{tune_for_large_continuous_listing => true},
    rename_child_locations(UserCtx, ParentFileCtx, ParentStorageFileId, RootUnsetAction, ListOpts, []).


-spec rename_child_locations(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    ParentStorageFileId :: helpers:file_id(), unset | ignored, file_listing:options(), [#file_renamed_entry{}]) ->
    [#file_renamed_entry{}].
rename_child_locations(UserCtx, ParentFileCtx, ParentStorageFileId, RootUnsetAction, ListOpts, ChildEntries) ->
    ParentGuid = file_ctx:get_logical_guid_const(ParentFileCtx),
    {Children, ListingPaginationToken, ParentFileCtx2} = file_tree:list_children(ParentFileCtx, UserCtx, ListOpts),
    NewChildEntries = lists:flatten(lists:map(fun(ChildCtx) ->
        {ChildName, ChildCtx2} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
        ChildStorageFileId = filename:join(ParentStorageFileId, ChildName),
        {IsDir, ChildCtx3} = file_ctx:is_dir(ChildCtx2),
        maybe_unset_child_ignore_in_changes(RootUnsetAction, ChildCtx3),
        case IsDir of
            true ->
                rename_child_locations(UserCtx, ChildCtx3, ChildStorageFileId, RootUnsetAction);
            false ->
                ok = replica_updater:rename(ChildCtx3, ChildStorageFileId),
                ChildGuid = file_ctx:get_logical_guid_const(ChildCtx3),
                #file_renamed_entry{
                    new_name = ChildName,
                    new_parent_guid = ParentGuid,
                    new_guid = ChildGuid,
                    old_guid = ChildGuid
                }
        end
    end, Children)),
    AllChildEntries = ChildEntries ++ NewChildEntries,
    case file_listing:is_finished(ListingPaginationToken) of
        true ->
            AllChildEntries;
        false ->
            rename_child_locations(UserCtx, ParentFileCtx2, ParentStorageFileId, RootUnsetAction,
                ListOpts#{pagination_token => ListingPaginationToken}, AllChildEntries)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns type of file, together with updated file context.
%% @end
%%--------------------------------------------------------------------
-spec get_type(file_ctx:ctx()) -> {file_meta:type(), file_ctx:ctx()}.
get_type(FileCtx) ->
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            {?DIRECTORY_TYPE, FileCtx2};
        {false, FileCtx2} ->
            {?REGULAR_FILE_TYPE, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns type of child file with given name, together with child file context
%% and updated parent file context.
%% @end
%%--------------------------------------------------------------------
-spec get_child_type(ParentFileCtx :: file_ctx:ctx(), ChildName :: file_meta:name(), user_ctx:ctx()) ->
    {file_meta:type(), ChildFileCtx :: file_ctx:ctx(), ParentFileCtx :: file_ctx:ctx()} |
    {undefined, undefined, ParentFileCtx :: file_ctx:ctx()}.
get_child_type(ParentFileCtx, ChildName, UserCtx) ->
    try file_tree:get_child(ParentFileCtx, ChildName, UserCtx) of
        {ChildCtx, ParentFileCtx2} ->
            {ChildType, ChildCtx2} = get_type(ChildCtx),
            {ChildType, ChildCtx2, ParentFileCtx2}
    catch
        throw:?ENOENT ->
            {undefined, undefined, ParentFileCtx}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns type of child file with given name, may send requests to other
%% providers if the current one does not support file's space.
%% @end
%%--------------------------------------------------------------------
-spec remotely_get_child_type(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    ChildName :: file_meta:name()) ->
    {file_meta:type(), ChildGuid :: fslogic_worker:file_guid()} |
    {undefined, undefined}.
remotely_get_child_type(SessId, ParentGuid, ChildName) ->
    case lfm:get_child_attr(SessId, ParentGuid, ChildName) of
        {ok, #file_attr{type = Type, guid = ChildGuid}} ->
            {Type, ChildGuid};
        {error, ?ENOENT} ->
            {undefined, undefined}
    end.


%% @private
-spec on_successful_rename(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(), 
    SourceParentFileCtx :: file_ctx:ctx(), TargetParentFileCtx :: file_ctx:ctx(), 
    file_meta:name()) -> ok | no_return().
on_successful_rename(UserCtx, SourceFileCtx, SourceParentFileCtx, TargetParentFileCtx, TargetName) ->
    permissions_cache:invalidate(),
    case file_ctx:equals(SourceParentFileCtx, TargetParentFileCtx) of
        true ->
            ok;
        false ->
            {RenamedFileCtx, _} = file_tree:get_child(TargetParentFileCtx, TargetName, UserCtx),
            qos_bounded_cache:invalidate_on_all_nodes(file_ctx:get_space_id_const(RenamedFileCtx)),
            qos_logic:reconcile_qos(file_ctx:reset(RenamedFileCtx))
    end,
    {PrevName, SourceFileCtx2} = file_ctx:get_aliased_name(SourceFileCtx, UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(TargetParentFileCtx),
    CurrentTime = global_clock:timestamp_seconds(),
    ok = fslogic_times:update_mtime_ctime(SourceParentFileCtx, CurrentTime),
    ok = fslogic_times:update_mtime_ctime(TargetParentFileCtx, CurrentTime),
    ok = fslogic_times:update_ctime(SourceFileCtx2, CurrentTime, emit_event),
    ok = fslogic_event_emitter:emit_file_renamed_to_client(SourceFileCtx2, ParentGuid, TargetName, PrevName, UserCtx).


%% @private
-spec validate_target_name(file_meta:name()) -> ok | no_return().
validate_target_name(TargetName) ->
    case re:run(TargetName, <<"/">>, [{capture, none}]) of
        match -> throw(?EINVAL);
        nomatch -> ok
    end.


%% @private
-spec maybe_unset_ignore_in_changes(file_ctx:ctx(), file_meta:doc(), file_meta:doc()) -> unset | ignored.
maybe_unset_ignore_in_changes(
    SourceCtx,
    #document{ignore_in_changes = true} = _SourceDoc,
    #document{ignore_in_changes = false} = _ParentDoc
) ->
    % TODO - trzeba to zrobic rekursywnie na kazdym pliku w katalogu
    % TODO - zablokowac rename w druga strone (od public do local)
    unset_child_ignore_in_changes(SourceCtx),
    unset;
maybe_unset_ignore_in_changes(_, _, _) ->
    ignored.


%% @private
-spec maybe_unset_child_ignore_in_changes(unset | ignored, file_ctx:ctx()) -> ok.
maybe_unset_child_ignore_in_changes(unset = _RootUnsetAction, FileCtx) ->
    unset_child_ignore_in_changes(FileCtx);
maybe_unset_child_ignore_in_changes(ignored = _RootUnsetAction, _FileCtx) ->
    ok.


%% @private
-spec unset_child_ignore_in_changes(file_ctx:ctx()) -> ok.
unset_child_ignore_in_changes(FileCtx) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    {IsDir, _} = file_ctx:is_dir(FileCtx),
    file_meta:unset_ignore_in_changes(Uuid, IsDir),
    times:unset_ignore_in_changes(Uuid),
    case IsDir of
        true -> ok;
        false -> fslogic_location_cache:unset_ignore_in_changes(FileCtx)
    end.