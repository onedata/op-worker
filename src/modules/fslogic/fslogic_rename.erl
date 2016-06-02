%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Files renaming functions
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_rename).
-author("Mateusz Paciorek").

-define(CHUNK_SIZE_ENV_KEY, rename_file_chunk_size).

%% TODO: VFS-2008
%% Add 'hint' for fslogic_storage:select_storage, to suggest using
%% source storage if possible to avoid copying
%% TODO: VFS-2009
%% Add rollback or any other means of rescuing from failed renaming
%% TODO: VFS-2010
%% If any provider supporting old path does not support new path -
%% get his changes, other providers should update their locations

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([rename/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Transforms target path to required forms and executes renaming.
%% @end
%%--------------------------------------------------------------------
-spec rename(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
rename(#fslogic_ctx{session_id = SessId} = CTX, SourceEntry, LogicalTargetPath) ->
    {ok, LogicalSourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    CanonicalSourcePath = logical_path_to_canonical(CTX, LogicalSourcePath),
    CanonicalTargetPath = logical_path_to_canonical(CTX, LogicalTargetPath),
    case CanonicalSourcePath =:= CanonicalTargetPath of
        true ->
            #fuse_response{status = #status{code = ?OK}};
        false ->
            case rename(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) of
                ok -> #fuse_response{status = #status{code = ?OK}};
                {error, Code} -> #fuse_response{status = #status{code = Code}}
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Executes proper rename case to check permissions.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?delete, 2}]).
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
-spec rename_dir(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{?delete_subcontainer, {parent, 2}}]).
rename_dir(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    case check_dir_preconditions(CTX, SourceEntry, LogicalTargetPath) of
        ok ->
            rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath, ?DIRECTORY_TYPE);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames file
%%--------------------------------------------------------------------
-spec rename_file(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{?delete_object, {parent, 2}}]).
rename_file(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    case check_reg_preconditions(CTX, LogicalTargetPath) of
        ok ->
            rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath, ?REGULAR_FILE_TYPE);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming directory.
%%--------------------------------------------------------------------
-spec check_dir_preconditions(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> ok | logical_file_manager:error_reply().
check_dir_preconditions(#fslogic_ctx{session_id = SessId} = CTX, SourceEntry, LogicalTargetPath) ->
    case moving_into_itself(CTX, SourceEntry, LogicalTargetPath) of
        true ->
            {error, ?EINVAL};
        false ->
            case logical_file_manager:stat(SessId, {path, LogicalTargetPath}) of
                {error, ?ENOENT} ->
                    ok;
                {ok, #file_attr{type = Type}} ->
                    case Type of
                        ?DIRECTORY_TYPE ->
                            ok;
                        _ ->
                            {error, ?ENOTDIR}
                    end
            end
    end.


%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming regular file.
%%--------------------------------------------------------------------
-spec check_reg_preconditions(fslogic_worker:ctx(),
    file_meta:path()) -> ok | logical_file_manager:error_reply().
check_reg_preconditions(#fslogic_ctx{session_id = SessId}, LogicalTargetPath) ->
    case logical_file_manager:stat(SessId, {path, LogicalTargetPath}) of
        {error, ?ENOENT} ->
            ok;
        {ok, #file_attr{type = Type}} ->
            case Type of
                ?DIRECTORY_TYPE ->
                    {error, ?EISDIR};
                _ ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks if renamed entry is one of target path parents.
%%--------------------------------------------------------------------
-spec moving_into_itself(CTX :: fslogic_worker:ctx(),
    SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) -> boolean().
moving_into_itself(#fslogic_ctx{session_id = SessId} = CTX, SourceEntry, LogicalTargetPath) ->
    {ok, LogicalSourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    CanonicalSourcePath = logical_path_to_canonical(CTX, LogicalSourcePath),
    CanonicalTargetPath = logical_path_to_canonical(CTX, LogicalTargetPath),
    SourceTokens = fslogic_path:split(CanonicalSourcePath),
    TargetTokens = fslogic_path:split(CanonicalTargetPath),
    lists:prefix(SourceTokens, TargetTokens).

%%--------------------------------------------------------------------
%% @doc Selects proper rename function - trivial, inter-space or inter-provider.
%%--------------------------------------------------------------------
-spec rename_select(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    CanonicalTargetPath :: file_meta:path(),
    LogicalTargetPath :: file_meta:path(), FileType :: file_meta:type()) ->
    ok | logical_file_manager:error_reply().
rename_select(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath, FileType) ->
    {_, TargetParentPath} = fslogic_path:basename_and_parent(LogicalTargetPath),
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),
    SourceSpaceId = fslogic_spaces:get_space_id(SourceUUID),
    TargetSpaceId = fslogic_spaces:get_space_id(CTX, TargetParentPath),

    case SourceSpaceId =:= TargetSpaceId of
        true ->
            case FileType of
                ?REGULAR_FILE_TYPE ->
                    rename_file_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath);
                ?DIRECTORY_TYPE ->
                    rename_dir_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath)
            end;
        false ->
            #fslogic_ctx{session_id = SessId, session = #session{}} = CTX,
            {ok, #auth{macaroon = Macaroon, disch_macaroons = DMacaroons}} =
                session:get_auth(SessId),
            Client = {user, {Macaroon, DMacaroons}},
            {ok, #user_details{id = UserId}} = oz_users:get_details(Client),

            TargetProvidersSet = get_supporting_providers(SourceSpaceId, Client, UserId),
            SourceProvidersSet = get_supporting_providers(TargetSpaceId, Client, UserId),
            CommonProvidersSet = ordsets:intersection(TargetProvidersSet, SourceProvidersSet),
            case ordsets:is_element(oneprovider:get_provider_id(), CommonProvidersSet) of
                true ->
                    case FileType of
                        ?REGULAR_FILE_TYPE ->
                            rename_file_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath);
                        ?DIRECTORY_TYPE ->
                            rename_dir_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath)
                    end;
                false ->
                    rename_interprovider(CTX, SourceEntry, LogicalTargetPath)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming regular file within one space.
%%--------------------------------------------------------------------
-spec rename_file_trivial(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_object, {parent, {path, 3}}}]).
rename_file_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    rename_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming directory within one space.
%%--------------------------------------------------------------------
-spec rename_dir_trivial(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    rename_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Renames file within one space.
%%--------------------------------------------------------------------
-spec rename_trivial(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
rename_trivial(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).


%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming regular file within one provider.
%%--------------------------------------------------------------------
-spec rename_file_interspace(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_object, {parent, {path, 3}}}]).
rename_file_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Checks permissions before renaming directory within one provider.
%%--------------------------------------------------------------------
-spec rename_dir_interspace(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, {path, 3}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    rename_interspace(CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath).

%%--------------------------------------------------------------------
%% @doc Renames file within one provider.
%%--------------------------------------------------------------------
-spec rename_interspace(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path(), file_meta:path()) -> ok | logical_file_manager:error_reply().
rename_interspace(#fslogic_ctx{session_id = SessId} = CTX, SourceEntry, CanonicalTargetPath, LogicalTargetPath) ->
    ok = ensure_deleted(SessId, LogicalTargetPath),

    {ok, SourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    {_, CanonicalTargetParentPath} = fslogic_path:basename_and_parent(CanonicalTargetPath),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(LogicalTargetPath),
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),
    SourceSpaceId = fslogic_spaces:get_space_id(SourceUUID),
    TargetSpaceId = fslogic_spaces:get_space_id(CTX, TargetParentPath),

    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
            %% TODO: get all snapshots: TODO: VFS-1966
            SourceDirSnapshots = [SourceEntry],

            %% Quota
            Host = self(),
            for_each_child_file(SourceEntry,
                fun
                    (#document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = File) ->
                        Size = fslogic_blocks:get_file_size(File),
                        Host ! {child_size, Size};
                    (_Dir) ->
                        ok
                end,
                fun(_, _) ->
                    ok
                end),
            GetSize =
                fun CollectAllSizes(Acc) ->
                    receive
                        {child_size, Size} -> CollectAllSizes(Acc + Size)
                    after 0 ->
                        Acc
                    end
                end,
            Size = GetSize(0),
            ok = space_quota:assert_write(TargetSpaceId, Size),

            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, CanonicalTargetPath})
                end, SourceDirSnapshots),

            for_each_child_file(SourceEntry,
                fun
                    (#document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = File) ->
                        %% TODO: get all snapshots: VFS-1965
                        FileSnapshots = [File],
                        lists:foreach(
                            fun(Snapshot) ->
                                ok = rename_on_storage(CTX, SourceSpaceId, TargetSpaceId, Snapshot)
                            end, FileSnapshots);
                    (_Dir) ->
                        ok
                end,
                fun(_, _) ->
                    ok
                end);

        {ok, File} ->
            SourcePathTokens = filename:split(SourcePath),
            TargetPathTokens = filename:split(CanonicalTargetPath),
            {ok, OldPath} = fslogic_path:gen_path(File, SessId),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),
            Size = fslogic_blocks:get_file_size(File),
            space_quota:assert_write(TargetSpaceId, Size),

            %% TODO: get all snapshots: VFS-1965
            FileSnapshots = [File],
            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, NewPath}),
                    ok = rename_on_storage(CTX, SourceSpaceId, TargetSpaceId, Snapshot)
                end, FileSnapshots)
    end,

    UserId = fslogic_context:get_user_id(CTX),
    CurrTime = erlang:system_time(seconds),
    ok = fslogic_times:update_mtime_ctime(SourceParent, UserId, CurrTime),
    ok = fslogic_times:update_ctime({path, CanonicalTargetPath}, UserId, CurrTime),
    ok = fslogic_times:update_mtime_ctime({path, CanonicalTargetParentPath}, UserId, CurrTime),
    ok.

%%--------------------------------------------------------------------
%% @doc Renames file moving it to another space supported by another provider.
%%--------------------------------------------------------------------
-spec rename_interprovider(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> ok | logical_file_manager:error_reply().
rename_interprovider(#fslogic_ctx{session_id = SessId} = CTX, SourceEntry, LogicalTargetPath) ->
    ok = ensure_deleted(SessId, LogicalTargetPath),

    {ok, SourcePath} = fslogic_path:gen_path(SourceEntry, SessId),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(LogicalTargetPath),
    SourcePathTokens = filename:split(SourcePath),
    TargetPathTokens = filename:split(LogicalTargetPath),

    for_each_child_file(SourceEntry,
        fun(#document{key = SourceUuid} = Doc) ->
            SourceGuid = fslogic_uuid:to_file_guid(SourceUuid),
            {ok, OldPath} = fslogic_path:gen_path(Doc, SessId),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),

            case Doc of
                #document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} ->
                    {ok, TargetGuid} = logical_file_manager:create(SessId, NewPath, 8#777),
                    ok = copy_file_contents(SessId, {guid, SourceGuid}, {guid, TargetGuid});

                #document{value = #file_meta{type = ?DIRECTORY_TYPE}} ->
                    {ok, TargetGuid} = logical_file_manager:mkdir(SessId, NewPath, 8#777)
            end,
            {guid, TargetGuid}
        end,
        fun(#document{key = SourceUuid, value = #file_meta{atime = ATime,
            mtime = MTime, ctime = CTime, mode = Mode}}, Target) ->
            SourceGuid = fslogic_uuid:to_file_guid(SourceUuid),
            ok = logical_file_manager:set_perms(SessId, Target, Mode),
            ok = copy_file_attributes(SessId, {guid, SourceGuid}, Target),
            ok = logical_file_manager:update_times(SessId, Target, ATime, MTime, CTime),
            ok = logical_file_manager:unlink(SessId, {guid, SourceGuid})
        end),

    CurrTime = erlang:system_time(seconds),
    ok = fslogic_times:update_mtime_ctime(SourceParent, fslogic_context:get_user_id(CTX), CurrTime),
    ok = logical_file_manager:update_times(SessId, {path, LogicalTargetPath}, undefined, undefined, CurrTime),
    ok = logical_file_manager:update_times(SessId, {path, TargetParentPath}, undefined, CurrTime, CurrTime),
    ok.

%%--------------------------------------------------------------------
%% @doc Renames file on storage and all its locations.
%%--------------------------------------------------------------------
-spec rename_on_storage(CTX :: fslogic_worker:ctx(), SourceSpaceId :: binary(),
    TargetSpaceId :: binary(), SourceEntry :: file_meta:entry()) -> ok.
rename_on_storage(CTX, SourceSpaceId, TargetSpaceId, SourceEntry) ->
    #fslogic_ctx{session_id = SessId, session = #session{identity = #identity{user_id = UserId}}} = CTX,
    {ok, #document{key = SourceUUID, value = #file_meta{mode = Mode}}} = file_meta:get(SourceEntry),
    SourceSpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SourceSpaceId),
    TargetSpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(TargetSpaceId),

    lists:foreach(
        fun(LocationDoc) ->
            #document{value = #file_location{storage_id = SourceStorageId,
                file_id = SourceFileId}} = LocationDoc,
            {ok, SourceStorage} = storage:get(SourceStorageId),

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
                    ok = storage_file_manager:unlink(SourceHandle);
                Error ->
                    SourceRootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SourceSpaceUUID, SourceUUID, SourceStorage, SourceFileId),
                    case storage_file_manager:mv(SourceRootHandle, TargetFileId) of
                        ok ->
                            TargetRootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, TargetSpaceUUID, SourceUUID, TargetStorage, TargetFileId),
                            ok = storage_file_manager:chown(TargetRootHandle, UserId, TargetSpaceId);
                        Error ->
                            TargetHandle = storage_file_manager:new_handle(SessId, TargetSpaceUUID, SourceUUID, TargetStorage, TargetFileId),
                            ok = storage_file_manager:create(TargetHandle, Mode, true),
                            ok = copy_file_contents_sfm(SourceHandle, TargetHandle),
                            ok = storage_file_manager:unlink(SourceHandle)
                    end
            end,

            ok = update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId)

        end, fslogic_utils:get_local_file_locations(SourceEntry)),
    ok = fslogic_req_generic:chmod_storage_files(
        CTX#fslogic_ctx{session_id = ?ROOT_SESS_ID, session = ?ROOT_SESS},
        SourceEntry,
        Mode
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc Updates file location.
%%--------------------------------------------------------------------
-spec update_location(LocationDoc :: datastore:document(),
    TargetFileId :: helpers:file(), TargetSpaceUUID :: binary(),
    TargetStorageId :: storage:id()) -> ok.
update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId) ->
    TargetSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(TargetSpaceUUID),
    #document{value = #file_location{uuid = FileUUID, blocks = Blocks} = Location} = LocationDoc,
    UpdatedBlocks = lists:map(
        fun(Block) ->
            Block#file_block{file_id = TargetFileId, storage_id = TargetStorageId}
        end, Blocks),
    {ok, _} = datastore:run_synchronized(file_location, FileUUID,
        fun() ->
            file_location:save(LocationDoc#document{value = Location#file_location{
                file_id = TargetFileId,
                space_id = TargetSpaceId,
                storage_id = TargetStorageId,
                blocks = UpdatedBlocks
            }})
        end),
    ok.

%%--------------------------------------------------------------------
%% @doc Unlinks file if it exists.
%%--------------------------------------------------------------------
-spec ensure_deleted(session:id(), file_meta:path()) -> ok.
ensure_deleted(SessId, LogicalTargetPath) ->
    case logical_file_manager:stat(SessId, {path, LogicalTargetPath}) of
        {error, ?ENOENT} ->
            ok;
        {ok, #file_attr{}} ->
            ok = logical_file_manager:unlink(SessId, {path, LogicalTargetPath})
    end.

%%--------------------------------------------------------------------
%% @doc Traverses files tree depth first, executing Pre function before
%% descending into children and executing Post function after returning
%% from children. Value returned from Pre function will be passed to Post
%% function for the same file doc.
%%--------------------------------------------------------------------
-spec for_each_child_file(Entry :: fslogic_worker:file(),
    PreFun :: fun((fslogic_worker:file()) -> term()),
    PostFun :: fun((fslogic_worker:file(), term()) -> term())) -> ok.
for_each_child_file(Entry, PreFun, PostFun) ->
    {ok, Doc} = file_meta:get(Entry),
    Mem = PreFun(Doc),
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
    PostFun(Doc, Mem),
    ok.

%%--------------------------------------------------------------------
%% @doc Lists all children of given entry
%%--------------------------------------------------------------------
-spec list_all_children(Entry :: fslogic_worker:file()) ->
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
            list_all_children(Entry, Offset+Size, Size, AccIn ++ ChildrenLinks);
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
-spec copy_file_contents(session:id(), From :: fslogic_worker:file_guid_or_path(),
    To :: fslogic_worker:file_guid_or_path()) -> ok.
copy_file_contents(SessId, From, To) ->
    {ok, FromHandle} = logical_file_manager:open(SessId, From, read),
    {ok, ToHandle} = logical_file_manager:open(SessId, To, write),
    {ok, ChunkSize} = application:get_env(?APP_NAME, ?CHUNK_SIZE_ENV_KEY),
    copy_file_contents(SessId, FromHandle, ToHandle, 0, ChunkSize).

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
            logical_file_manager:fsync(ToHandle),
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
    {ok, ChunkSize} = application:get_env(?APP_NAME, ?CHUNK_SIZE_ENV_KEY),
    copy_file_contents_sfm(OpenFromHandle, OpenToHandle, 0, ChunkSize).

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
%% @doc Returns list of ids of providers supporting
%%--------------------------------------------------------------------
-spec get_supporting_providers(SpaceUUID :: binary(),
    Client :: oz_endpoint:client(), UserId :: onedata_user:id()) -> [binary()].
get_supporting_providers(SpaceId, Client, UserId) ->
    {ok, #document{value = #space_info{providers = Providers}}} =
        space_info:get_or_fetch(Client, SpaceId, UserId),
    ordsets:from_list(Providers).


%%--------------------------------------------------------------------
%% @doc Converts Logical file path to canonical file path
%%--------------------------------------------------------------------
-spec logical_path_to_canonical(CTX :: fslogic_worker:ctx(),
    LogicalPath :: file_meta:path()) -> file_meta:path().
logical_path_to_canonical(#fslogic_ctx{session_id = SessId} = CTX, LogicalPath) ->
    {ok, Tokens} = fslogic_path:verify_file_path(LogicalPath),
    CanonicalEntry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, CanonicalPath} = fslogic_path:gen_path(CanonicalEntry, SessId),
    CanonicalPath.