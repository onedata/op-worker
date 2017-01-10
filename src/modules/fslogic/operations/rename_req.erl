%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Request for renaming files or directories
%%% @end
%%%-------------------------------------------------------------------
-module(rename_req).
-author("Mateusz Paciorek").

%% TODO: VFS-2008
%% Add 'hint' for fslogic_storage:select_storage, to suggest using
%% source storage if possible to avoid copying
%% TODO: VFS-2009
%% Add rollback or any other means of rescuing from failed renaming

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([rename/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Transforms target path to required forms and executes renaming.
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    TargetParentFile :: file_info:file_info(), TargetName :: file_meta:name()) ->
    fslogic_worker:fuse_response().
rename(Ctx, SourceFile, TargetParentFile, TargetName) ->
    {CanonicalSourcePath, SourceFile2} = file_info:get_path(SourceFile),
    {_, CanonicalTargetPath} = get_logical_and_canonical_path_of_remote_file(Ctx, TargetParentFile, TargetName),

    case CanonicalSourcePath =:= CanonicalTargetPath of
        true ->
            {Guid, _SourceFile3} = file_info:get_guid(SourceFile2),
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_renamed{
                    new_uuid = Guid}};
        false ->
            rename(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Executes proper rename case to check permissions.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path(), TargetParentFile :: file_info:file_info(), TargetName :: file_meta:name()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?delete, 2}]).
rename(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName) ->
    case file_info:is_dir(SourceFile) of
        {true, SourceFile2} ->
            rename_dir(Ctx, SourceFile2, CanonicalTargetPath, TargetParentFile, TargetName);
        {false, SourceFile2} ->
            rename_file(Ctx, SourceFile2, CanonicalTargetPath, TargetParentFile, TargetName)
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames directory
%%--------------------------------------------------------------------
-spec rename_dir(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path(), TargetParentFile :: file_info:file_info(),
    TargetName :: file_meta:name()) -> fslogic_worker:fuse_response().
-check_permissions([{?delete_subcontainer, {parent, 2}}]).
rename_dir(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName) ->
    case check_dir_preconditions(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName) of
        ok ->
            rename_select(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName, ?DIRECTORY_TYPE);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames file
%%--------------------------------------------------------------------
-spec rename_file(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path(), TargetParentFile :: file_info:file_info(),
    TargetName :: file_meta:name()) -> fslogic_worker:fuse_response().
-check_permissions([{?delete_object, {parent, 2}}]).
rename_file(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName) ->
    case check_reg_preconditions(Ctx, TargetParentFile, TargetName) of
        ok ->
            rename_select(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName, ?REGULAR_FILE_TYPE);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming directory.
%%--------------------------------------------------------------------
-spec check_dir_preconditions(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path(), TargetParentFile :: file_info:file_info(), TargetName :: file_meta:name()) ->
    fslogic_worker:fuse_response().
check_dir_preconditions(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName) ->
    SessId = fslogic_context:get_session_id(Ctx),
    case moving_into_itself(SourceFile, CanonicalTargetPath) of
        true ->
            #fuse_response{status = #status{code = ?EINVAL}};
        false ->
            {Guid, _} = file_info:get_guid(TargetParentFile),
            case logical_file_manager:get_child_attr(SessId, Guid, TargetName) of
                {error, ?ENOENT} ->
                    ok;
                {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
                    ok;
                {ok, #file_attr{}} ->
                    #fuse_response{status = #status{code = ?ENOTDIR}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming regular file.
%%--------------------------------------------------------------------
-spec check_reg_preconditions(fslogic_context:ctx(),
    TargetParentFile :: file_info:file_info(), TargetName :: file_meta:name()) ->
    ok | fslogic_worker:fuse_response().
check_reg_preconditions(Ctx, TargetParentFile, TargetName) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {TargetParentGuid, _TargetParentFile2} = file_info:get_guid(TargetParentFile),
    case logical_file_manager:get_child_attr(SessId, TargetParentGuid, TargetName) of
        {error, ?ENOENT} ->
            ok;
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            #fuse_response{status = #status{code = ?EISDIR}};
        {ok, #file_attr{}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Checks if renamed entry is one of target path parents.
%%--------------------------------------------------------------------
-spec moving_into_itself(SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path()) -> boolean().
moving_into_itself(SourceFile, CanonicalTargetPath) ->
    {CanonicalSourcePath, _SourceFile2} = file_info:get_path(SourceFile),
    SourceTokens = fslogic_path:split(CanonicalSourcePath),
    TargetTokens = fslogic_path:split(CanonicalTargetPath),
    lists:prefix(SourceTokens, TargetTokens).

%%--------------------------------------------------------------------
%% @doc Selects proper rename function - trivial, inter-space or inter-provider.
%%--------------------------------------------------------------------
-spec rename_select(fslogic_context:ctx(), SourceFile :: file_info:file_info(),
    CanonicalTargetPath :: file_meta:path(), TargetParentFile :: file_info:file_info(),
    TargetName :: file_meta:name(), FileType :: file_meta:type()) ->
    fslogic_worker:fuse_response().
rename_select(Ctx, SourceFile, CanonicalTargetPath, TargetParentFile, TargetName, FileType) ->
    SourceSpaceId = file_info:get_space_id(SourceFile),
    TargetSpaceId = file_info:get_space_id(TargetParentFile),
    {LogicalTargetPath, _} = get_logical_and_canonical_path_of_remote_file(Ctx, TargetParentFile, TargetName),



    case SourceSpaceId =:= TargetSpaceId of
        true ->
            case FileType of
                ?REGULAR_FILE_TYPE ->
                    rename_file_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath);
                ?DIRECTORY_TYPE ->
                    rename_dir_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath)
            end;
        false ->
            #document{value = #od_user{}} = fslogic_context:get_user(Ctx),
            Auth = fslogic_context:get_auth(Ctx),
            UserId = fslogic_context:get_user_id(Ctx),
            TargetProvidersSet = get_supporting_providers(SourceSpaceId, Auth, UserId),
            SourceProvidersSet = get_supporting_providers(TargetSpaceId, Auth, UserId),
            CommonProvidersSet = ordsets:intersection(TargetProvidersSet, SourceProvidersSet),
            case ordsets:is_element(oneprovider:get_provider_id(), CommonProvidersSet) of
                true ->
                    case FileType of
                        ?REGULAR_FILE_TYPE ->
                            rename_file_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath);
                        ?DIRECTORY_TYPE ->
                            rename_dir_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath)
                    end;
                false ->
                    rename_interprovider(Ctx, SourceFile, LogicalTargetPath)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming regular file within one space.
%%--------------------------------------------------------------------
-spec rename_file_trivial(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_object, {parent, {path, 3}}}]).
rename_file_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    rename_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming directory within one space.
%%--------------------------------------------------------------------
-spec rename_dir_trivial(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    rename_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Renames file within one space.
%%--------------------------------------------------------------------
-spec rename_trivial(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
rename_trivial(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming regular file within one provider.
%%--------------------------------------------------------------------
-spec rename_file_interspace(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_object, {parent, {path, 3}}}]).
rename_file_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming directory within one provider.
%%--------------------------------------------------------------------
-spec rename_dir_interspace(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Renames file within one provider.
%%--------------------------------------------------------------------
-spec rename_interspace(fslogic_context:ctx(), file_info:file_info(),
    file_meta:path(), file_meta:path()) -> fslogic_worker:fuse_response().
rename_interspace(Ctx, SourceFile, CanonicalTargetPath, LogicalTargetPath) ->
    {SourceEntry, _File2} = file_info:get_uuid_entry(SourceFile), %todo pass file_info
    SessId = fslogic_context:get_session_id(Ctx),
    ok = ensure_deleted(SessId, LogicalTargetPath),
    {ok, SourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    {_, CanonicalTargetParentPath} = fslogic_path:basename_and_parent(CanonicalTargetPath),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(LogicalTargetPath),
    {ok, #document{key = SourceUUID} = SourceDoc} = file_meta:get(SourceEntry),
    UserId = fslogic_context:get_user_id(Ctx),
    SourceSpaceId = fslogic_spaces:get_space_id({uuid, SourceUUID}),
    TargetSpaceId = fslogic_spaces:get_space_id(Ctx, TargetParentPath),
    RenamedEntries = case SourceDoc of
        #document{value = #file_meta{type = ?DIRECTORY_TYPE}} ->
            %todo VFS-2813 support multi location , get all snapshots: VFS-1966
            SourceDirSnapshots = [SourceEntry],

            %% Quota
            Size = for_each_child_file(SourceEntry,
                fun
                    (#document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = File, AccSize) ->
                        Size = fslogic_blocks:get_file_size(File),
                        {AccSize + Size, undefined};
                    (_Dir, AccSize) ->
                        {AccSize, undefined}
                end,
                fun(_, AccSize, _) ->
                    AccSize
                end, 0),
            ok = space_quota:assert_write(TargetSpaceId, Size),

            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, CanonicalTargetPath})
                end, SourceDirSnapshots),

            {ok, UserId} = session:get_user_id(SessId),
            {ok, UpdatedSourceEntry} = file_meta:get(SourceUUID),

            for_each_child_file(UpdatedSourceEntry,
                fun
                    (#document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = File, Acc) ->
                        %todo VFS-2813 support multi location , get all snapshots: VFS-1966
                        FileSnapshots = [File],
                        lists:foreach(
                            fun(Snapshot) ->
                                maybe_sync_file(SessId, Snapshot, SourceSpaceId, TargetSpaceId),
                                ok = sfm_utils:rename_on_storage(Ctx, TargetSpaceId, Snapshot)
                            end, FileSnapshots),
                        {Acc, undefined};
                    (_Dir, Acc) ->
                        {Acc, undefined}
                end,
                fun(#document{key = Uuid} = Entry, Acc, _) ->
                    {ok, NewName} = file_meta:get_name(Entry),
                    NewParentUuid = fslogic_uuid:parent_uuid(Entry, UserId),
                    [{fslogic_uuid:uuid_to_guid(Uuid, SourceSpaceId),
                      fslogic_uuid:uuid_to_guid(Uuid, TargetSpaceId),
                      fslogic_uuid:uuid_to_guid(NewParentUuid, TargetSpaceId),
                      NewName} | Acc]
                end, []);

        #document{key = Uuid} = File ->
            SourcePathTokens = filename:split(SourcePath),
            TargetPathTokens = filename:split(CanonicalTargetPath),
            {ok, OldPath} = fslogic_path:gen_path(File, SessId),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),
            Size = fslogic_blocks:get_file_size(File),
            space_quota:assert_write(TargetSpaceId, Size),

            %todo VFS-2813 support multi location , get all snapshots: VFS-1966
            FileSnapshots = [File],
            lists:foreach(
                fun(Snapshot) ->
                    maybe_sync_file(SessId, Snapshot, SourceSpaceId, TargetSpaceId),
                    ok = file_meta:rename(Snapshot, {path, NewPath}),
                    ok = sfm_utils:rename_on_storage(Ctx, TargetSpaceId, Snapshot)
                end, FileSnapshots),

            {ok, NewName} = file_meta:get_name({uuid, Uuid}),
            NewParentUuid = fslogic_uuid:parent_uuid({uuid, Uuid}, UserId),
            [{fslogic_uuid:uuid_to_guid(Uuid, SourceSpaceId),
              fslogic_uuid:uuid_to_guid(Uuid, TargetSpaceId),
              fslogic_uuid:uuid_to_guid(NewParentUuid, TargetSpaceId),
              NewName}]
    end,

    ok = create_phantom_files(RenamedEntries, SourceSpaceId, TargetSpaceId),

    CurrTime = erlang:system_time(seconds),
    ok = fslogic_times:update_mtime_ctime(SourceParent, UserId, CurrTime),
    ok = fslogic_times:update_ctime({path, CanonicalTargetPath}, UserId, CurrTime),
    ok = fslogic_times:update_mtime_ctime({path, CanonicalTargetParentPath}, UserId, CurrTime),

    {#file_renamed_entry{new_uuid = NewGuid} = TopEntry, ChildEntries} = parse_renamed_entries(RenamedEntries),
    spawn(fun() ->
        fslogic_event:emit_file_renamed(TopEntry, ChildEntries, [SessId]) end),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_renamed{new_uuid = NewGuid, child_entries = ChildEntries}
    }.

%%--------------------------------------------------------------------
%% @doc Renames file moving it to another space supported by another provider.
%%--------------------------------------------------------------------
-spec rename_interprovider(fslogic_context:ctx(), file_info:file_info(), file_meta:path()) ->
    fslogic_worker:fuse_response().
rename_interprovider(Ctx, SourceFile, LogicalTargetPath) ->
    {SourceEntry, _SourceFile2} = file_info:get_uuid_entry(SourceFile), %todo remove and use file_info
    SessId = fslogic_context:get_session_id(Ctx),
    ok = ensure_deleted(SessId, LogicalTargetPath),

    {ok, SourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(LogicalTargetPath),
    SourcePathTokens = filename:split(SourcePath),
    TargetPathTokens = filename:split(LogicalTargetPath),
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),

    SourceSpaceId = fslogic_spaces:get_space_id({uuid, SourceUUID}),
    TargetSpaceId = fslogic_spaces:get_space_id(Ctx, TargetParentPath),

    RenamedEntries = for_each_child_file(SourceEntry,
        fun(#document{key = SourceUuid} = Doc, Acc) ->
            SourceGuid = fslogic_uuid:uuid_to_guid(SourceUuid),
            {ok, OldPath} = fslogic_path:gen_path(Doc, SessId),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),
            {ok, {ATime, CTime, MTime}} = times:get_or_default(SourceUUID),

            case Doc of
                #document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} ->
                    {ok, TargetGuid} = logical_file_manager:create(SessId, NewPath, 8#777),
                    ok = copy_file_contents(SessId, {guid, SourceGuid}, {guid, TargetGuid});

                #document{value = #file_meta{type = ?DIRECTORY_TYPE}} ->
                    {ok, TargetGuid} = logical_file_manager:mkdir(SessId, NewPath, 8#777)
            end,
            {Acc, {TargetGuid, NewPath, {ATime, CTime, MTime}}}
        end,
        fun(#document{key = SourceUuid, value = #file_meta{mode = Mode}}, Acc, {TargetGuid, NewPath, {ATime, CTime, MTime}}) ->
            SourceGuid = fslogic_uuid:uuid_to_guid(SourceUuid),
            ok = logical_file_manager:set_perms(SessId, {guid, TargetGuid}, Mode),
            ok = copy_file_attributes(SessId, {guid, SourceGuid}, {guid, TargetGuid}),
            ok = logical_file_manager:update_times(SessId, {guid, TargetGuid}, ATime, MTime, CTime),
            ok = logical_file_manager:unlink(SessId, {guid, SourceGuid}, false),
            {NewName, NewParentPath} = fslogic_path:basename_and_parent(NewPath),
            TargetParentGuid = fslogic_uuid:ensure_guid(Ctx, {path, NewParentPath}),
            [{SourceGuid, TargetGuid, TargetParentGuid, NewName} | Acc]
        end, []),

    ok = create_phantom_files(RenamedEntries, SourceSpaceId, TargetSpaceId),

    CurrTime = erlang:system_time(seconds),
    ok = fslogic_times:update_mtime_ctime(SourceParent, fslogic_context:get_user_id(Ctx), CurrTime),
    ok = logical_file_manager:update_times(SessId, {path, LogicalTargetPath}, undefined, undefined, CurrTime),
    ok = logical_file_manager:update_times(SessId, {path, TargetParentPath}, undefined, CurrTime, CurrTime),

    {#file_renamed_entry{new_uuid = NewGuid} = TopEntry, ChildEntries} = parse_renamed_entries(RenamedEntries),
    spawn(fun() ->
        fslogic_event:emit_file_renamed(TopEntry, ChildEntries, [SessId]) end),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_renamed{new_uuid = NewGuid, child_entries = ChildEntries}
    }.

%%--------------------------------------------------------------------
%% @doc Unlinks file if it exists.
%%--------------------------------------------------------------------
-spec ensure_deleted(session:id(), file_meta:path()) -> ok.
ensure_deleted(SessId, LogicalTargetPath) ->
    case logical_file_manager:stat(SessId, {path, LogicalTargetPath}) of
        {error, ?ENOENT} ->
            ok;
        {ok, #file_attr{}} ->
            ok = logical_file_manager:unlink(SessId, {path, LogicalTargetPath}, true)
    end.

%%--------------------------------------------------------------------
%% @doc Traverses files tree depth first, executing Pre function before
%% descending into children and executing Post function after returning
%% from children.
%% Data flow:
%% - Input accumulator passed as third argument is passed to Pre function which
%%   returns tuple {first intermediate accumulator, memorized value}.
%% - First intermediate accumulator is passed to all children recursive calls
%%   using foldl which returns second intermediate accumulator.
%% - Second intermediate accumulator and memorized value are passed to Post
%%   function which returns output accumulator.
%% - Output Accumulator is returned
%%--------------------------------------------------------------------
-spec for_each_child_file(Entry :: fslogic_worker:file(),
    PreFun :: fun((fslogic_worker:file(), AccIn :: term()) -> {AccInt1 :: term(), Mem :: term()}),
    PostFun :: fun((fslogic_worker:file(), AccInt2 :: term(), Mem :: term()) -> AccOut :: term()),
    AccIn :: term()) -> AccOut :: term().
for_each_child_file(Entry, PreFun, PostFun, AccIn) ->
    {ok, Doc} = file_meta:get(Entry),
    {AccInt1, Mem} = PreFun(Doc, AccIn),
    AccInt2 = case Doc of
        #document{value = #file_meta{type = ?DIRECTORY_TYPE}} ->
            {ok, ChildrenLinks} = list_all_children(Doc),
            lists:foldl(
                fun(#child_link{uuid = ChildUUID}, AccIn0) ->
                    for_each_child_file({uuid, ChildUUID}, PreFun, PostFun, AccIn0)
                end, AccInt1, ChildrenLinks);
        _ ->
            AccInt1
    end,
    PostFun(Doc, AccInt2, Mem).

%%--------------------------------------------------------------------
%% @doc Lists all children of given entry
%%--------------------------------------------------------------------
-spec list_all_children(fslogic_worker:file()) ->
    {ok, [#child_link{}]}.
list_all_children(Entry) ->
    {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    list_all_children(Entry, 0, ChunkSize, []).

-spec list_all_children(Entry :: fslogic_worker:file(),
    Offset :: non_neg_integer(), Count :: non_neg_integer(),
    AccIn :: [#child_link{}]) -> {ok, [#child_link{}]}.
list_all_children(Entry, Offset, Size, AccIn) ->
    {ok, ChildrenLinks} = file_meta:list_children(Entry, Offset, Size),
    case length(ChildrenLinks) of
        Size ->
            list_all_children(Entry, Offset + Size, Size, AccIn ++ ChildrenLinks);
        _ ->
            {ok, AccIn ++ ChildrenLinks}
    end.

%%--------------------------------------------------------------------
%% @doc Copies file attributes to another file
%%--------------------------------------------------------------------
-spec copy_file_attributes(session:id(), From :: fslogic_worker:file_guid_or_path(),
    To :: fslogic_worker:file_guid_or_path()) -> ok.
copy_file_attributes(SessId, From, To) ->
    case logical_file_manager:get_acl(SessId, From) of
        {ok, ACL} ->
            ok = logical_file_manager:set_acl(SessId, To, ACL);
        {error, ?ENOATTR} ->
            ok
    end,

    case logical_file_manager:get_mimetype(SessId, From) of
        {ok, Mimetype} ->
            ok = logical_file_manager:set_mimetype(SessId, To, Mimetype);
        {error, ?ENOATTR} ->
            ok
    end,

    case logical_file_manager:get_transfer_encoding(SessId, From) of
        {ok, TransferEncoding} ->
            ok = logical_file_manager:set_transfer_encoding(SessId, To, TransferEncoding);
        {error, ?ENOATTR} ->
            ok
    end,

    case logical_file_manager:get_cdmi_completion_status(SessId, From) of
        {ok, CompletionStatus} ->
            ok = logical_file_manager:set_cdmi_completion_status(SessId, To, CompletionStatus);
        {error, ?ENOATTR} ->
            ok
    end,

    {ok, XattrNames} = logical_file_manager:list_xattr(SessId, From, false, true),

    lists:foreach(
        fun
            (<<"cdmi_", _/binary>>) ->
                ok;
            (XattrName) ->
                {ok, Xattr} = logical_file_manager:get_xattr(SessId, From, XattrName, false),
                ok = logical_file_manager:set_xattr(SessId, To, Xattr)
        end, XattrNames
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc Copies file contents to another file on lfm level
%%--------------------------------------------------------------------
-spec copy_file_contents(session:id(), From :: fslogic_worker:file_guid_or_path(),
    To :: fslogic_worker:file_guid_or_path()) -> ok.
copy_file_contents(SessId, From, To) ->
    {ok, FromHandle} = logical_file_manager:open(SessId, From, read),
    {ok, ToHandle} = logical_file_manager:open(SessId, To, write),
    {ok, ChunkSize} = application:get_env(?APP_NAME, rename_file_chunk_size),
    {NewFromHandle, NewToHandle} = copy_file_contents(SessId, FromHandle, ToHandle, 0, ChunkSize),
    ok = logical_file_manager:release(NewFromHandle),
    ok = logical_file_manager:release(NewToHandle).

-spec copy_file_contents(session:id(), FromHandle :: logical_file_manager:handle(),
    ToHandle :: logical_file_manager:handle(), Offset :: non_neg_integer(),
    Size :: non_neg_integer()) ->
    {NewFromHandle :: logical_file_manager:handle(), NewToHandle :: logical_file_manager:handle()}.
copy_file_contents(SessId, FromHandle, ToHandle, Offset, Size) ->
    {ok, NewFromHandle, Data} = logical_file_manager:read(FromHandle, Offset, Size),
    DataSize = size(Data),
    {ok, NewToHandle, DataSize} = logical_file_manager:write(ToHandle, Offset, Data),
    case DataSize of
        0 ->
            logical_file_manager:fsync(NewToHandle),
            {NewFromHandle, NewToHandle};
        _ ->
            copy_file_contents(SessId, NewFromHandle, NewToHandle, Offset + DataSize, Size)
    end.

%%--------------------------------------------------------------------
%% @doc Returns list of ids of providers supporting
%%--------------------------------------------------------------------
-spec get_supporting_providers(SpaceUUID :: binary(),
    Auth :: oz_endpoint:auth(), UserId :: od_user:id()) -> [binary()].
get_supporting_providers(SpaceId, Auth, UserId) ->
    {ok, #document{value = #od_space{providers = Providers}}} =
        od_space:get_or_fetch(Auth, SpaceId, UserId),
    ordsets:from_list(Providers).

%%--------------------------------------------------------------------
%% @doc Converts list of entry tuples to records that can be sent or emitted
%%--------------------------------------------------------------------
-spec parse_renamed_entries([{OldUuid :: fslogic_worker:file_guid(),
    NewGuid :: fslogic_worker:file_guid(), NewParentGuid :: fslogic_worker:file_guid(),
    NewName :: file_meta:name()}]) ->
    {#file_renamed_entry{}, [#file_renamed_entry{}]}.
parse_renamed_entries([TopEntryRaw | ChildEntriesRaw]) ->
    {TopEntryOldGuid, TopEntryNewGuid, TopEntryNewParentGuid, TopEntryNewName} = TopEntryRaw,
    ChildEntries = lists:map(
        fun({OldGuid, NewGuid, NewParentGuid, NewName}) ->
            #file_renamed_entry{old_uuid = OldGuid, new_uuid = NewGuid,
                                new_parent_uuid = NewParentGuid, new_name = NewName}
        end, ChildEntriesRaw),
    {#file_renamed_entry{old_uuid = TopEntryOldGuid, new_uuid = TopEntryNewGuid,
        new_parent_uuid = TopEntryNewParentGuid, new_name = TopEntryNewName}, ChildEntries}.

%%--------------------------------------------------------------------
%% @doc Creates phantom file for each renamed entry if space has changed
%%--------------------------------------------------------------------
-spec create_phantom_files([{OldUuid :: fslogic_worker:file_guid(),
    NewGuid :: fslogic_worker:file_guid(), NewParentGuid :: fslogic_worker:file_guid(),
    NewName :: file_meta:name()}], OldSpaceId :: binary(), NewSpaceId :: binary()) ->
    ok.
create_phantom_files(Entries, OldSpaceId, NewSpaceId) ->
    case OldSpaceId =:= NewSpaceId of
        true ->
            ok;
        false ->
            lists:foreach(
                fun({OldGuid, NewGuid, _, _}) ->
                    file_meta:create_phantom_file(fslogic_uuid:guid_to_uuid(OldGuid),
                        OldSpaceId, NewGuid)
                end, Entries)
    end.

%%--------------------------------------------------------------------
%% @doc Requests file replication to current provider if
%% NewSpaceId is different than OldSpaceId
%%--------------------------------------------------------------------
-spec maybe_sync_file(SessId :: session:id(), Entry :: file_meta:entry(),
    OldSpaceId :: binary(), NewSpaceId :: binary()) ->
    ok.
maybe_sync_file(SessId, Entry, OldSpaceId, NewSpaceId) ->
    case OldSpaceId =:= NewSpaceId of
        false ->
            {ok, Uuid} = file_meta:to_uuid(Entry),
            logical_file_manager:replicate_file(
                SessId,
                {guid, fslogic_uuid:uuid_to_guid(Uuid, OldSpaceId)},
                oneprovider:get_provider_id()
            );
        true ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetch canonical path (starting with /SpaceId) of file, routing request to
%% provider that supports the space of file.
%% @end
%%--------------------------------------------------------------------
-spec get_logical_and_canonical_path_of_remote_file(fslogic_context:ctx(),
    TargetParentFile :: file_info:file_info(), TargetName :: file_meta:name()) ->
    {LogicalPath:: file_meta:path(), CanonicalPath :: file_meta:path()}.
get_logical_and_canonical_path_of_remote_file(Ctx, TargetParentFile, TargetName) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {Guid, _TargetParentFile2} = file_info:get_guid(TargetParentFile),
    {ok, LogicalTargetParentPath} = logical_file_manager:get_file_path(SessId, Guid),
    case fslogic_path:tokenize_skipping_dots(LogicalTargetParentPath) of
        {ok, [<<"/">>]} ->
            throw(?EPERM);
        {ok, [<<"/">>, _SpaceName | Rest]} ->
            LogicalPath = filename:join(LogicalTargetParentPath, TargetName),
            SpaceId = file_info:get_space_id(TargetParentFile),
            CanonicalPath = filename:join([<<"/">>, SpaceId | Rest] ++ [TargetName]),
            {LogicalPath, CanonicalPath}
    end.