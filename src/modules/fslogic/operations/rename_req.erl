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
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([rename/4]).

-define(LS_CHUNK_SIZE, 1000).

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
    case file_ctx:get_space_id_const(SourceFileCtx) =:= file_ctx:get_space_id_const(TargetParentFileCtx) of
        false ->
            rename_between_spaces(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
        true ->
            rename_within_space(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName)
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
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_between_spaces(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    SessId = user_ctx:get_session_id(UserCtx),
    TargetParentGuid = file_ctx:get_guid_const(TargetParentFileCtx),
    {SourceFileType, SourceFileCtx2} = get_type(SourceFileCtx),
    {TargetFileType, TargetGuid} = remotely_get_child_type(SessId, TargetParentGuid, TargetName),
    case file_ctx:get_guid_const(SourceFileCtx) =:= TargetGuid of
        true ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_renamed{
                    new_guid = TargetGuid,
                    child_entries = []
                }
            };
        false ->
            case {SourceFileType, TargetFileType} of
                {_, undefined} ->
                    copy_and_remove(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName);
                {TheSameType, TheSameType} ->
                    ok = logical_file_manager:unlink(SessId, {guid, TargetGuid}, false),
                    copy_and_remove(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName);
                {?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE} ->
                    throw(?EISDIR);
                {?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE} ->
                    throw(?ENOTDIR)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Copies source file/dir into target dir (to different space) and then removes
%% source file/dir.
%% @end
%%--------------------------------------------------------------------
-spec copy_and_remove(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
copy_and_remove(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    SessId = user_ctx:get_session_id(UserCtx),
    SourceGuid = file_ctx:get_guid_const(SourceFileCtx),
    TargetParentGuid = file_ctx:get_guid_const(TargetParentFileCtx),
    case copy_utils:copy(SessId, SourceGuid, TargetParentGuid, TargetName) of
        {ok, NewGuid, ChildEntries} ->
            case logical_file_manager:rm_recursive(SessId, {guid, SourceGuid}) of
                ok ->
                    RenameChildEntries = lists:map(fun({OldGuid, NewGuid, NewParentGuid, NewName}) ->
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
                            new_guid = NewGuid,
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
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_within_space(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    {SourceFileType, SourceFileCtx2} = get_type(SourceFileCtx),
    {TargetFileType, TargetFileCtx, TargetParentFileCtx2} = get_child_type(TargetParentFileCtx, TargetName, UserCtx),

    case TargetFileCtx =/= undefined andalso file_ctx:equals(SourceFileCtx2, TargetFileCtx) of
        true ->
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_renamed{
                    new_guid = file_ctx:get_guid_const(SourceFileCtx2),
                    child_entries = []
                }
            };
        false ->
            case {SourceFileType, TargetFileType} of
                {?DIRECTORY_TYPE, undefined} ->
                    rename_dir(UserCtx, SourceFileCtx2, TargetParentFileCtx2, TargetName);
                {?REGULAR_FILE_TYPE, undefined} ->
                    rename_file(UserCtx, SourceFileCtx2, TargetParentFileCtx2, TargetName);
                {?DIRECTORY_TYPE, ?DIRECTORY_TYPE} ->
                    #fuse_response{status = #status{code = ?OK}} =
                        delete_req:delete(UserCtx, TargetFileCtx, false),
                    rename_dir(UserCtx, SourceFileCtx2, TargetParentFileCtx2, TargetName);
                {?REGULAR_FILE_TYPE, ?REGULAR_FILE_TYPE} ->
                    #fuse_response{status = #status{code = ?OK}} =
                        delete_req:delete(UserCtx, TargetFileCtx, false),
                    rename_file(UserCtx, SourceFileCtx2, TargetParentFileCtx2, TargetName);
                {?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE} ->
                    throw(?ENOTDIR);
                {?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE} ->
                    throw(?EISDIR)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_file(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    check_permissions:execute(
        [traverse_ancestors, ?delete, {?delete_object, parent},
            {traverse_ancestors, 3}, {?traverse_container, 3}, {?add_object, 3}],
        [UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName],
        fun rename_file_insecure/4).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_dir(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_dir(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    check_permissions:execute(
        [traverse_ancestors, ?delete, {?delete_subcontainer, parent},
            {traverse_ancestors, 3}, {?traverse_container, 3}, {?add_subcontainer, 3}],
        [UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName],
        fun rename_dir_insecure/4).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames file within the same space.
%% @end
%%--------------------------------------------------------------------
-spec rename_file_insecure(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) -> no_return() | #fuse_response{}.
rename_file_insecure(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName) ->
    {SourceParentFileCtx, SourceFileCtx2} = file_ctx:get_parent(SourceFileCtx, UserCtx),
    {ok, SourceFileCtx3, TargetFileId} = rename_meta_and_storage_file(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName),
    replica_updater:rename(SourceFileCtx, TargetFileId),
    FileGuid = file_ctx:get_guid_const(SourceFileCtx3),
    update_parent_times(SourceParentFileCtx, TargetParentFileCtx),
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
    {SourceParentFileCtx, SourceFileCtx2} = file_ctx:get_parent(SourceFileCtx, UserCtx),
    {RenameAns, SourceFileCtx3, TargetFileId} = rename_meta_and_storage_file(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName),
    case RenameAns of
        ok -> ok;
        {error, ?ENOENT} -> ok
    end,
    ChildEntries = rename_child_locations(UserCtx, SourceFileCtx3, TargetFileId, 0),
    FileGuid = file_ctx:get_guid_const(SourceFileCtx3),
    update_parent_times(SourceParentFileCtx, TargetParentFileCtx),
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
    {ok | {error, term()}, TargetFileCtx :: file_ctx:ctx(),
        TargetFileId :: helpers:file_id()}.
rename_meta_and_storage_file(UserCtx, SourceFileCtx0, TargetParentFileCtx0, TargetName) ->
    {SourceFileId, SourceFileCtx} = file_ctx:get_storage_file_id(SourceFileCtx0),
    {TargetParentFileId, TargetParentFileCtx} = file_ctx:get_storage_file_id(TargetParentFileCtx0),
    TargetFileId = filename:join(TargetParentFileId, TargetName),

    FileUuid = file_ctx:get_uuid_const(SourceFileCtx),
    {ParentDoc, _TargetParentFileCtx2} = file_ctx:get_file_doc(TargetParentFileCtx),
    {SourceDoc, SourceFileCtx2} = file_ctx:get_file_doc(SourceFileCtx),
    {SourceParentFileCtx, _SourceFileCtx3} = file_ctx:get_parent(SourceFileCtx2, UserCtx),
    {SourceParentDoc, _SourceParentFileCtx2} = file_ctx:get_file_doc(SourceParentFileCtx),
    file_meta:rename(SourceDoc, SourceParentDoc, ParentDoc, TargetName),

    SpaceId = file_ctx:get_space_id_const(SourceFileCtx2),
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    Ans = sfm_utils:rename_storage_file(user_ctx:get_session_id(UserCtx), SpaceId, Storage, FileUuid, SourceFileId, TargetFileId),
    {Ans, SourceFileCtx2, TargetFileId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renames location of all child files, returns rename result as a list.
%% @end
%%--------------------------------------------------------------------
-spec rename_child_locations(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    ParentStorageFileId :: helpers:file_id(), Offset :: non_neg_integer()) ->
    [#file_renamed_entry{}].
rename_child_locations(UserCtx, ParentFileCtx, ParentStorageFileId, Offset) ->
    ParentGuid = file_ctx:get_guid_const(ParentFileCtx),
    case file_ctx:get_file_children(ParentFileCtx, UserCtx, Offset, ?LS_CHUNK_SIZE) of
        {[], FileCtx2} ->
            [];
        {Children, FileCtx2} ->
            ChildEntries =
                lists:flatten(lists:map(fun(ChildCtx) ->
                    {ChildName, ChildCtx2} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
                    ChildStorageFileId = filename:join(ParentStorageFileId, ChildName),
                    case file_ctx:is_dir(ChildCtx2) of
                        {true, ChildCtx3} ->
                            rename_child_locations(UserCtx, ChildCtx3, ChildStorageFileId, 0);
                        {false, ChildCtx3} ->
                            ok = replica_updater:rename(ChildCtx3, ChildStorageFileId),
                            ChildGuid = file_ctx:get_guid_const(ChildCtx3),
                            #file_renamed_entry{
                                new_name = ChildName,
                                new_parent_guid = ParentGuid,
                                new_guid = ChildGuid,
                                old_guid = ChildGuid
                            }
                    end
                end, Children)),
            ChildEntries ++ rename_child_locations(UserCtx, FileCtx2,
                ParentStorageFileId, Offset + length(Children))
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
    try file_ctx:get_child(ParentFileCtx, ChildName, UserCtx) of
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
    case logical_file_manager:get_child_attr(SessId, ParentGuid, ChildName) of
        {ok, #file_attr{type = Type, guid = ChildGuid}} ->
            {Type, ChildGuid};
        {error, ?ENOENT} ->
            {undefined, undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update mtime and ctime of parent files to the current time.
%% @end
%%--------------------------------------------------------------------
-spec update_parent_times(SourceParentFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx()) -> ok.
update_parent_times(SourceParentFileCtx, TargetParentFileCtx) ->
    CurrentTime = erlang:system_time(seconds),
    fslogic_times:update_mtime_ctime(SourceParentFileCtx, CurrentTime),
    fslogic_times:update_mtime_ctime(TargetParentFileCtx, CurrentTime).