%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_rename).
-author("Mateusz Paciorek").

-define(CHUNK_SIZE, 100).

%% TODO future work:
%% 1. If any provider supporting old path does not support new path -
%%    get his changes, other providers should update their locations
%% 2. Add 'hint' for fslogic_storage:select_storage, to suggest using
%%    source storage if possible to avoid copying
%% 3. Add rollback or any other means of rescuing from failed renaming

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([rename/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Transforms target path to required forms.
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
rename(CTX, SourceEntry, LogicalTargetPath) ->
    {ok, Tokens} = fslogic_path:verify_file_path(LogicalTargetPath),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, CanonicalTargetPath} = file_meta:gen_path(CanonicalFileEntry),
    rename(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Executes proper rename case to check permissions.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {traverse_ancestors, {path, 3}}, {?delete, 2}]).
rename(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    ?debug("Renaming file ~p to ~p...", [SourceEntry, CanonicalTargetPath]),
    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = FileDoc} ->
            rename_dir(CTX, FileDoc, CanonicalTargetPath, LogicalTargetPath);
        {ok, FileDoc} ->
            rename_file(CTX, FileDoc, CanonicalTargetPath, LogicalTargetPath)
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames directory
%%--------------------------------------------------------------------
-spec rename_dir(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    case check_dir_preconditions(CTX, SourceEntry, CanonicalTargetPath) of
        #fuse_response{status = #status{code = ?OK}} ->
            rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames file
%%--------------------------------------------------------------------
-spec rename_file(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_object, {parent, 2}}, {?add_object, {parent, {path, 3}}}]).
rename_file(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    case check_reg_preconditions(CTX, SourceEntry, CanonicalTargetPath) of
        #fuse_response{status = #status{code = ?OK}} ->
            rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming directory.
%%--------------------------------------------------------------------
-spec check_dir_preconditions(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
check_dir_preconditions(CTX, SourceEntry, CanonicalTargetPath) ->
    case file_meta:exists({path, CanonicalTargetPath}) of
        false ->
            #fuse_response{status = #status{code = ?OK}};
        true ->
            case file_meta:get({path, CanonicalTargetPath}) of
                {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = TargetDoc} ->
                    case moving_into_itself(SourceEntry, CanonicalTargetPath) of
                        true ->
                            #fuse_response{status = #status{code = ?EINVAL}};
                        false ->
                            fslogic_req_generic:delete(CTX, TargetDoc)
                    end;
                {ok, _TargetDoc} ->
                    #fuse_response{status = #status{code = ?ENOTDIR}}
            end
    end.


%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming regular file.
%%--------------------------------------------------------------------
-spec check_reg_preconditions(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
check_reg_preconditions(CTX, _SourceEntry, CanonicalTargetPath) ->
    case file_meta:exists({path, CanonicalTargetPath}) of
        false ->
            #fuse_response{status = #status{code = ?OK}};
        true ->
            case file_meta:get({path, CanonicalTargetPath}) of
                {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                    #fuse_response{status = #status{code = ?EISDIR}};
                {ok, TargetDoc} ->
                    fslogic_req_generic:delete(CTX, TargetDoc)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks if renamed entry is one of target path parents.
%%--------------------------------------------------------------------
-spec moving_into_itself(SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path()) ->
    boolean().
moving_into_itself(SourceEntry, CanonicalTargetPath) ->
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),
    {_, ParentPath} = fslogic_path:basename_and_parent(CanonicalTargetPath),
    {ok, {_, ParentUUIDs}} = file_meta:resolve_path(ParentPath),
    lists:any(fun(ParentUUID) -> ParentUUID =:= SourceUUID end, ParentUUIDs).

%%--------------------------------------------------------------------
%% @doc Selects proper rename function - trivial, inter-space or inter-provider.
%%--------------------------------------------------------------------
-spec rename_select(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    {_, TargetParentPath} = fslogic_path:basename_and_parent(CanonicalTargetPath),
    SourceSpaceUUID = get_space_uuid(CTX, SourceEntry),
    TargetSpaceUUID = get_space_uuid(CTX, {path, TargetParentPath}),

    case SourceSpaceUUID =:= TargetSpaceUUID of
        true ->
            rename_trivial(CTX, SourceEntry, LogicalTargetPath);
        false ->
            TargetSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(TargetSpaceUUID),
            {ok, TargetProviderIds} = oz_spaces:get_providers(provider, TargetSpaceId),
            TargetProvidersSet = ordsets:from_list(TargetProviderIds),

            SourceSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SourceSpaceUUID),
            {ok, SourceProviderIds} = oz_spaces:get_providers(provider, SourceSpaceId),
            SourceProvidersSet = ordsets:from_list(SourceProviderIds),

            CommonProvidersSet = ordsets:intersection([TargetProvidersSet, SourceProvidersSet]),
            case ordsets:is_element(oneprovider:get_provider_id(), CommonProvidersSet) of
                true ->
                    rename_interspace(CTX, SourceEntry, LogicalTargetPath);
                false ->
                    rename_interprovider(CTX, SourceEntry, LogicalTargetPath)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Renames file within one space.
%%--------------------------------------------------------------------
-spec rename_trivial(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
rename_trivial(CTX, SourceEntry, LogicalTargetPath) ->
    rename_interspace(CTX, SourceEntry, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Renames file within one provider.
%%--------------------------------------------------------------------
-spec rename_interspace(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
rename_interspace(CTX, SourceEntry, LogicalTargetPath) ->
    {ok, SourcePath} = file_meta:gen_path(SourceEntry),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    CanonicalTargetPath = get_canonical_path(CTX, LogicalTargetPath),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(CanonicalTargetPath),
    SourceSpaceUUID = get_space_uuid(CTX, SourceEntry),
    TargetSpaceUUID = get_space_uuid(CTX, {path, TargetParentPath}),

    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
            %% TODO: get all snapshots:
            SourceDirSnapshots = [SourceEntry],
            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, CanonicalTargetPath})
                end, SourceDirSnapshots),

            for_each_child_file(SourceEntry,
                fun
                    (#document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = File) ->
                        {ok, NewPath} = file_meta:gen_path(File),

                        %% TODO: get all snapshots:
                        FileSnapshots = [File],
                        lists:foreach(
                            fun(Snapshot) ->
                                ok = rename_on_storage(CTX, SourceSpaceUUID, TargetSpaceUUID, Snapshot, NewPath)
                            end, FileSnapshots);
                    (_Dir) ->
                        ok
                end,
                fun(_) ->
                    ok
                end),

            CurrTime = erlang:system_time(seconds),
            ok = update_ctime({path, CanonicalTargetPath}, CurrTime),
            ok = update_mtime_ctime(SourceParent, CurrTime),
            ok = update_mtime_ctime({path, TargetParentPath}, CurrTime);

        {ok, File} ->
            SourcePathTokens = filename:split(SourcePath),
            TargetPathTokens = filename:split(CanonicalTargetPath),
            {ok, OldPath} = file_meta:gen_path(File),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),

            %% TODO: get all snapshots:
            FileSnapshots = [File],
            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, NewPath}),
                    ok = rename_on_storage(CTX, SourceSpaceUUID, TargetSpaceUUID, Snapshot, NewPath)
                end, FileSnapshots),

            CurrTime = erlang:system_time(seconds),
            ok = update_ctime({path, CanonicalTargetPath}, CurrTime),
            ok = update_mtime_ctime(SourceParent, CurrTime)
    end,

    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc Renames file moving it to another space supported by another provider.
%%--------------------------------------------------------------------
-spec rename_interprovider(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
rename_interprovider(#fslogic_ctx{session_id = SessId}, SourceEntry, LogicalTargetPath) ->
    {ok, SourcePath} = file_meta:gen_path(SourceEntry),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    SourcePathTokens = filename:split(SourcePath),
    TargetPathTokens = filename:split(LogicalTargetPath),

    for_each_child_file(SourceEntry,
        fun(Entry) ->
            {ok, OldPath} = file_meta:gen_path(Entry),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),

            case Entry of
                #document{key = SourceUuid, value = #file_meta{type = ?REGULAR_FILE_TYPE, mode = Mode}} ->
                    {ok, TargetUuid} = logical_file_manager:create(SessId, NewPath, Mode),
                    ok = copy_file_contents(SessId, {uuid, SourceUuid}, {uuid, TargetUuid}),
                    ok = copy_file_attributes(SessId, {uuid, SourceUuid}, {uuid, TargetUuid}),
                    ok = logical_file_manager:unlink(SessId, {uuid, SourceUuid});

                #document{key = SourceUuid, value = #file_meta{type = ?DIRECTORY_TYPE, mode = Mode}} ->
                    {ok, TargetUuid} = logical_file_manager:mkdir(SessId, NewPath, Mode),
                    ok = copy_file_attributes(SessId, {uuid, SourceUuid}, {uuid, TargetUuid})
            end
        end,
        fun(Entry) ->
            case Entry of
                #document{key = DirUuid, value = #file_meta{type = ?DIRECTORY_TYPE}} ->
                    ok = logical_file_manager:rmdir(SessId, {uuid, DirUuid});

                _File ->
                    ok
            end
        end),

    CurrTime = erlang:system_time(seconds),
    ok = update_mtime_ctime(SourceParent, CurrTime),
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc returns space UUID for given entry
%%--------------------------------------------------------------------
-spec get_space_uuid(fslogic_worker:ctx(), fslogic_worker:file()) ->
    binary().
get_space_uuid(CTX, Entry) ->
    UserId = fslogic_context:get_user_id(CTX),
    {ok, Doc} = file_meta:get(Entry),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(Doc, UserId),
    SpaceUUID.

%%--------------------------------------------------------------------
%% @doc Renames file on storage and all its locations.
%%--------------------------------------------------------------------
-spec rename_on_storage(fslogic_worker:ctx(), SourceSpaceUUID :: binary(),
    TargetSpaceUUID :: binary(), SourceEntry :: file_meta:entry(),
    TargetPath :: file_meta:path()) -> ok.
rename_on_storage(CTX, SourceSpaceUUID, TargetSpaceUUID, SourceEntry, TargetPath) ->
    #fslogic_ctx{session_id = SessId} = CTX,
    {ok, #document{key = SourceUUID, value = #file_meta{mode = Mode}}} = file_meta:get(SourceEntry),

    lists:foreach(
        fun(LocationDoc) ->
            #document{value = #file_location{storage_id = SourceStorageId,
                file_id = SourceFileId}} = LocationDoc,
            {ok, SourceStorage} = storage:get(SourceStorageId),

            TargetSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(TargetSpaceUUID),
            {ok, #document{key = TargetStorageId}} = fslogic_storage:select_storage(TargetSpaceId),
            TargetFileId = fslogic_utils:gen_storage_file_id({uuid, SourceUUID}),
            {ok, TargetStorage} = storage:get(TargetStorageId),

            TargetDir = fslogic_path:dirname(TargetFileId),
            TargetDirHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, TargetSpaceUUID, undefined, TargetStorage, TargetDir),
            case storage_file_manager:mkdir(TargetDirHandle, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
                ok ->
                    ok;
                {error, eexist} ->
                    ok
            end,

            SourceHandle = storage_file_manager:new_handle(SessId, SourceSpaceUUID, SourceUUID, SourceStorage, SourceFileId),
            ok = case storage_file_manager:link(SourceHandle, TargetFileId) of
                ok ->
                    ok = storage_file_manager:unlink(SourceHandle),
                    ok;
                Error ->
                    SourceRootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SourceSpaceUUID, SourceUUID, SourceStorage, SourceFileId),
                    TargetHandle = storage_file_manager:new_handle(SessId, TargetSpaceUUID, SourceUUID, TargetStorage, TargetFileId),
                    case storage_file_manager:mv(SourceRootHandle, TargetFileId) of
                        ok ->
                            StorageType = fslogic_utils:get_storage_type(TargetStorageId),
                            #posix_user_ctx{gid = GID} = fslogic_storage:get_posix_user_ctx(StorageType, SessId, TargetSpaceUUID),
                            ok = storage_file_manager:chown(TargetHandle, -1, GID),
                            ok;
                        Error ->
                            ok = storage_file_manager:create(TargetHandle, Mode, true),
                            ok = copy_file_contents_sfm(SourceHandle, TargetHandle),
                            ok = storage_file_manager:unlink(SourceHandle)
                    end
            end,

            ok = update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId)

        end, fslogic_utils:get_local_file_locations(SourceEntry)),
    ok = fslogic_req_generic:chmod_storage_files(CTX, SourceEntry, Mode),
    ok.

%%--------------------------------------------------------------------
%% @doc Updates file location.
%%--------------------------------------------------------------------
-spec update_location(LocationDoc :: datastore:document(),
    TargetFileId :: helpers:file(), TargetSpaceUUID :: binary(),
    TargetStorageId :: storage:id()) -> ok.
update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId) ->
    #document{key = LocationId,
        value = #file_location{blocks = Blocks}} = LocationDoc,
    UpdatedBlocks = lists:map(
        fun(Block) ->
            Block#file_block{file_id = TargetFileId, storage_id = TargetStorageId}
        end, Blocks),
    file_location:update(LocationId, #{
        file_id => TargetFileId,
        space_id => TargetSpaceUUID,
        storage_id => TargetStorageId,
        blocks => UpdatedBlocks
    }),
    ok.

%%--------------------------------------------------------------------
%% @doc Traverses files tree depth first, executing Pre function before
%% descending into children and executing Post function after returning
%% from children
%%--------------------------------------------------------------------
-spec for_each_child_file(Entry :: fslogic_worker:file(),
    PreFun :: fun((fslogic_worker:file()) -> term()),
    PostFun :: fun((fslogic_worker:file()) -> term())) -> ok.
for_each_child_file(Entry, PreFun, PostFun) ->
    {ok, Doc} = file_meta:get(Entry),
    PreFun(Doc),
    case Doc of
        #document{value = #file_meta{type = ?DIRECTORY_TYPE}} ->
            {ok, ChildrenLinks} = list_all_children(Doc),
            lists:foreach(
                fun(#child_link{uuid = ChildUUID}) ->
                    for_each_child_file({uuid, ChildUUID}, PreFun, PostFun)
                end, ChildrenLinks);
        _ ->
            ok
    end,
    PostFun(Doc),
    ok.

%%--------------------------------------------------------------------
%% @doc Lists all children of given entry
%%--------------------------------------------------------------------
-spec list_all_children(Entry :: fslogic_worker:file()) ->
    {ok, [#child_link{}]}.
list_all_children(Entry) ->
    list_all_children(Entry, 0, 100, []).

-spec list_all_children(Entry :: fslogic_worker:file(),
    Offset :: non_neg_integer(), Count :: non_neg_integer(),
    AccIn :: [#child_link{}]) -> {ok, [#child_link{}]}.
list_all_children(Entry, Offset, Size, AccIn) ->
    {ok, ChildrenLinks} = file_meta:list_children(Entry, Offset, Size),
    case length(ChildrenLinks) of
        Size ->
            list_all_children(Entry, Offset+Size, Size, AccIn ++ ChildrenLinks);
        _ ->
            {ok, AccIn ++ ChildrenLinks}
    end.

%%--------------------------------------------------------------------
%% @doc Copies file attributes to another file
%%--------------------------------------------------------------------
-spec copy_file_attributes(session:id(), From :: file_meta:uuid_or_path(),
    To :: file_meta:uuid_or_path()) -> ok.
copy_file_attributes(SessId, From, To) ->
    {ok, #document{value = #file_meta{atime = Atime, mtime = Mtime, ctime = Ctime}}} = file_meta:get(From),
    ok = logical_file_manager:update_times(SessId, To, Atime, Mtime, Ctime),

    case logical_file_manager:get_acl(SessId, From) of
        {ok, ACL} ->
            ok = logical_file_manager:set_acl(SessId, To, ACL);
        {error, enoattr} ->
            ok
    end,

    case logical_file_manager:get_mimetype(SessId, From) of
        {ok, Mimetype} ->
            ok = logical_file_manager:set_mimetype(SessId, To, Mimetype);
        {error, enoattr} ->
            ok
    end,

    case logical_file_manager:get_transfer_encoding(SessId, From) of
        {ok, TransferEncoding} ->
            ok = logical_file_manager:set_transfer_encoding(SessId, To, TransferEncoding);
        {error, enoattr} ->
            ok
    end,

    case logical_file_manager:get_cdmi_completion_status(SessId, From) of
        {ok, CompletionStatus} ->
            ok = logical_file_manager:set_cdmi_completion_status(SessId, To, CompletionStatus);
        {error, enoattr} ->
            ok
    end,

    {ok, XattrNames} = logical_file_manager:list_xattr(SessId, From),

    lists:foreach(
        fun
            (<<"cdmi_", _/binary>>) ->
                ok;
            (XattrName) ->
                {ok, Xattr} = logical_file_manager:get_xattr(SessId, From, XattrName),
                ok = logical_file_manager:set_xattr(SessId, To, Xattr)
        end, XattrNames
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc Copies file contents to another file on lfm level
%%--------------------------------------------------------------------
-spec copy_file_contents(session:id(), From :: file_meta:uuid_or_path(),
    To :: file_meta:uuid_or_path()) -> ok.
copy_file_contents(SessId, From, To) ->
    {ok, FromHandle} = logical_file_manager:open(SessId, From, read),
    {ok, ToHandle} = logical_file_manager:open(SessId, To, write),
    copy_file_contents(SessId, FromHandle, ToHandle, 0, ?CHUNK_SIZE).

-spec copy_file_contents(session:id(), FromHandle :: logical_file_manager:handle(),
    ToHandle :: logical_file_manager:handle(), Offset :: non_neg_integer(),
    Size :: non_neg_integer()) -> ok.
copy_file_contents(SessId, FromHandle, ToHandle, Offset, Size) ->
    {ok, NewFromHandle, Data} = logical_file_manager:read(FromHandle, Offset, Size),
    DataSize = size(Data),
    {ok, NewToHandle, DataSize} = logical_file_manager:write(ToHandle, Offset, Data),
    case DataSize of
        Size ->
            copy_file_contents(SessId, NewFromHandle, NewToHandle, Offset+Size, Size);
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Copies file contents to another file on sfm level
%%--------------------------------------------------------------------
-spec copy_file_contents_sfm(HandleFrom :: storage_file_manager:handle(),
    HandleTo :: storage_file_manager:handle()) -> ok.
copy_file_contents_sfm(FromHandle, ToHandle) ->
    {ok, OpenFromHandle} = storage_file_manager:open(FromHandle, read),
    {ok, OpenToHandle} = storage_file_manager:open(ToHandle, write),
    copy_file_contents_sfm(OpenFromHandle, OpenToHandle, 0, ?CHUNK_SIZE).

-spec copy_file_contents_sfm(HandleFrom :: storage_file_manager:handle(),
    HandleTo :: storage_file_manager:handle(), Offset :: non_neg_integer(),
    Size :: non_neg_integer()) -> ok.
copy_file_contents_sfm(FromHandle, ToHandle, Offset, Size) ->
    {ok, Data} = storage_file_manager:read(FromHandle, Offset, Size),
    DataSize = size(Data),
    {ok, DataSize} = storage_file_manager:write(ToHandle, Offset, Data),
    case DataSize of
        Size ->
            copy_file_contents_sfm(FromHandle, ToHandle, Offset+Size, Size);
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Returns canonical form of path
%%--------------------------------------------------------------------
-spec get_canonical_path(fslogic_worker:ctx(), file_meta:path()) ->
    file_meta:path().
get_canonical_path(CTX, Path) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, CanonicalPath} = file_meta:gen_path(CanonicalFileEntry),
    CanonicalPath.


%%--------------------------------------------------------------------
%% @doc Updates entry ctime
%%--------------------------------------------------------------------
-spec update_ctime(fslogic_worker:file(), Time :: file_meta:time()) -> ok.
update_ctime(Entry, Time) ->
    {ok, #document{value = Meta} = Doc} = file_meta:get(Entry),
    {ok, _} = file_meta:update(Doc, #{ctime => Time}),

    spawn(
        fun() ->
            fslogic_event:emit_file_sizeless_attrs_update(
                Doc#document{value = Meta#file_meta{ctime = Time}})
        end),
    ok.

%%--------------------------------------------------------------------
%% @doc Updates entry mtime and ctime
%%--------------------------------------------------------------------
-spec update_mtime_ctime(fslogic_worker:file(), Time :: file_meta:time()) -> ok.
update_mtime_ctime(Entry, Time) ->
    {ok, #document{value = Meta} = Doc} = file_meta:get(Entry),
    {ok, _} = file_meta:update(Doc, #{mtime => Time, ctime => Time}),

    spawn(
        fun() ->
            fslogic_event:emit_file_sizeless_attrs_update(
                Doc#document{value = Meta#file_meta{
                    mtime = Time, ctime = Time
                }})
        end),
    ok.