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
            copy_and_remove(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
        true ->
            rename_within_space(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    case file_ctx:is_dir(SourceFileCtx) of
        {true, SourceFileCtx2} ->
            rename_dir(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName);
        {false, SourceFileCtx2} ->
            rename_file(UserCtx, SourceFileCtx2, TargetParentFileCtx, TargetName)
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
    {ok, SourceFileCtx2, TargetFileId} = rename_meta_and_storage_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName),
    replica_updater:rename(SourceFileCtx, TargetFileId),
    FileGuid = file_ctx:get_guid_const(SourceFileCtx2),
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
    {RenameAns, SourceFileCtx2, TargetFileId} = rename_meta_and_storage_file(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName),
    case RenameAns of
        ok -> ok;
        {error, ?ENOENT} -> ok
    end,
    ChildEntries = rename_child_locations(UserCtx, SourceFileCtx2, TargetFileId, 0),
    FileGuid = file_ctx:get_guid_const(SourceFileCtx2),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_renamed{
            new_guid = FileGuid,
            child_entries = ChildEntries
        }
    }.

%%--------------------------------------------------------------------
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
    {SourceDoc = #document{value = SourceMeta = #file_meta{
        name = SourceName
    }}, SourceFileCtx2} = file_ctx:get_file_doc(SourceFileCtx),
    TargetDoc = SourceDoc#document{value = SourceMeta#file_meta{name = TargetName}},
    {ok, _} = file_meta:update(FileUuid, #{name => TargetName}),
    {SourceParentFileCtx, _SourceFileCtx3} = file_ctx:get_parent(SourceFileCtx2, UserCtx),
    {SourceParentDoc, _SourceParentFileCtx2} = file_ctx:get_file_doc(SourceParentFileCtx),
    file_meta:rename(TargetDoc, SourceParentDoc, ParentDoc, SourceName, TargetName),

    SpaceId = file_ctx:get_space_id_const(SourceFileCtx2),
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    Ans = sfm_utils:rename_storage_file(user_ctx:get_session_id(UserCtx), SpaceId, Storage, FileUuid, SourceFileId, TargetFileId),
    {Ans, SourceFileCtx2, TargetFileId}.

%%--------------------------------------------------------------------
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