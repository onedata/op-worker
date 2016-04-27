%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic generic (both for regular and special files) request handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/events/types.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% Keys of special cdmi attrs
-define(MIMETYPE_XATTR_NAME, <<"cdmi_mimetype">>).
-define(TRANSFER_ENCODING_XATTR_NAME, <<"cdmi_valuetransferencoding">>).
-define(CDMI_COMPLETION_STATUS_XATTR_NAME, <<"cdmi_completion_status">>).

%% API
-export([chmod/3, get_file_attr/2, delete/2, rename/3, update_times/5,
    get_xattr/3, set_xattr/3, remove_xattr/3, list_xattr/2,
    get_acl/2, set_acl/3, remove_acl/2, get_transfer_encoding/2,
    set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Changes file's access times.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec update_times(fslogic_worker:ctx(), File :: fslogic_worker:file(),
                   ATime :: file_meta:time(), MTime :: file_meta:time(), CTime :: file_meta:time()) -> #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
update_times(#fslogic_ctx{session_id = SessId}, FileEntry, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) -> is_integer(Value) end, UpdateMap),
    {ok, _} = file_meta:update(FileEntry, UpdateMap1),

    spawn(fun() -> fslogic_event:emit_file_attr_update(FileEntry, [SessId]) end),

    #fuse_response{status = #status{code = ?OK}}.



%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chmod(fslogic_worker:ctx(), File :: fslogic_worker:file(), Perms :: fslogic_worker:posix_permissions()) ->
                   #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {owner, 2}]).
chmod(CTX, FileEntry, Mode) ->
    chmod_storage_files(CTX, FileEntry, Mode),

    % remove acl
    {ok, FileUuid} = file_meta:to_uuid(FileEntry),
    xattr:delete_by_name(FileUuid, ?ACL_XATTR_NAME),
    {ok, _} = file_meta:update(FileEntry, #{mode => Mode}),

    fslogic_times:update_mtime_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
    spawn(
        fun() ->
            fslogic_event:emit_permission_changed(FileUuid)
        end),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Changes file owner.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chown(fslogic_worker:ctx(), File :: fslogic_worker:file(), UserId :: onedata_user:id()) ->
                   #fuse_response{} | no_return().
-check_permissions([{?write_owner, 2}]).
chown(_, _File, _UserId) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.


%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(Ctx :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
                           FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_file_attr(#fslogic_ctx{session_id = SessId} = CTX, File) ->
    ?debug("Get attr for file entry: ~p", [File]),
    case file_meta:get(File) of
        {ok, #document{key = UUID, value = #file_meta{
            type = Type, mode = Mode, atime = ATime, mtime = MTime,
            ctime = CTime, uid = UserID, name = Name}} = FileDoc} ->
            Size = fslogic_blocks:get_file_size(File),

            #posix_user_ctx{gid = GID, uid = UID} = try
                {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),
                StorageId = luma_utils:get_storage_id(SpaceUUID),
                StorageType = luma_utils:get_storage_type(StorageId),
                fslogic_storage:get_posix_user_ctx(StorageType, SessId, SpaceUUID)
            catch
                throw:{not_a_space, _} -> ?ROOT_POSIX_CTX
            end,
            FinalUID = case  session:get(SessId) of
                {ok, #document{value = #session{identity = #identity{user_id = UserID}}}} ->
                    UID;
                _ ->
                    luma_utils:gen_storage_uid(UserID)
            end,
            #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
                gid = GID,
                uuid = UUID, type = Type, mode = Mode, atime = ATime, mtime = MTime,
                ctime = CTime, uid = FinalUID, size = Size, name = Name
            }};
        {error, {not_found, _}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @doc Deletes file.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec delete(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
                         FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
delete(CTX, File) ->
    FuseResponse = case file_meta:get(File) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = FileDoc} ->
            delete_dir(CTX, FileDoc);
        {ok, FileDoc} ->
            delete_file(CTX, FileDoc)
    end,
    case FuseResponse of
        #fuse_response{status = #status{code = ?OK}} ->
            {uuid, UUID} = fslogic_uuid:ensure_uuid(CTX, File),
            fslogic_event:emit_file_removal(UUID);
        _ ->
            ok
    end,
    FuseResponse.



%%--------------------------------------------------------------------
%% @doc Renames file/dir.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(), TargetPath :: file_meta:path()) ->
                         #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {traverse_ancestors, {path, 3}}, {?delete, 2}]).
rename(CTX, SourceEntry, TargetPath) ->
    ?debug("Renaming file ~p to ~p...", [SourceEntry, TargetPath]),
    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = FileDoc} ->
            rename_dir(CTX, FileDoc, TargetPath);
        {ok, FileDoc} ->
            rename_file(CTX, FileDoc, TargetPath)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_xattr(_CTX, _, <<"cdmi_", _/binary>>) -> throw(?EPERM);
get_xattr(_CTX, {uuid, FileUuid}, XattrName) ->
    case xattr:get_by_name(FileUuid, XattrName) of
        {ok, #document{value = Xattr}} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = Xattr};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}};
        {error, {not_found, xattr}} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, #xattr{}) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_xattr(_CTX, _, #xattr{name = <<"cdmi_", _/binary>>}) -> throw(?EPERM);
set_xattr(CTX, {uuid, FileUuid} = FileEntry, Xattr) ->
    case xattr:save(FileUuid, Xattr) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_xattr(CTX, {uuid, FileUuid} = FileEntry, XattrName) ->
    case xattr:delete_by_name(FileUuid, XattrName) of
        ok ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
list_xattr(_CTX, {uuid, FileUuid}) ->
    case xattr:list(FileUuid) of
        {ok, List} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = #xattr_list{names = List}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Get access control list of file.
%%--------------------------------------------------------------------
-spec get_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_acl, 2}]).
get_acl(_CTX, {uuid, FileUuid})  ->
    case xattr:get_by_name(FileUuid, ?ACL_XATTR_NAME) of
        {ok, #document{value = #xattr{value = Val}}} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = #acl{value = Val}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}};
        {error, {not_found, xattr}} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets access control list of file.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, #acl{}) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
set_acl(CTX, {uuid, FileUuid} = FileEntry, #acl{value = Val}) ->
    case xattr:save(FileUuid, #xattr{name = ?ACL_XATTR_NAME, value = Val}) of
        {ok, _} ->
            ok = chmod_storage_files(
                CTX#fslogic_ctx{session_id = ?ROOT_SESS_ID, session = ?ROOT_SESS},
                {uuid, FileUuid}, 8#000
            ),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Removes access control list of file.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
remove_acl(CTX, {uuid, FileUuid} = FileEntry) ->
    case xattr:delete_by_name(FileUuid, ?ACL_XATTR_NAME) of
        ok ->
            {ok, #document{value = #file_meta{mode = Mode}}} = file_meta:get({uuid, FileUuid}),
            ok = chmod_storage_files(
                CTX#fslogic_ctx{session_id = ?ROOT_SESS_ID, session = ?ROOT_SESS},
                {uuid, FileUuid}, Mode
            ),
            ok = fslogic_event:emit_permission_changed(FileUuid),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    {ok, xattr:transfer_encoding()} | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_transfer_encoding(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?TRANSFER_ENCODING_XATTR_NAME) of
        {ok, #document{value = #xattr{value = Val}}} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = #transfer_encoding{value = Val}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}};
        {error, {not_found, xattr}} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:transfer_encoding()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_transfer_encoding(CTX, {uuid, FileUuid} = FileEntry, Encoding) ->
    case xattr:save(FileUuid, #xattr{name = ?TRANSFER_ENCODING_XATTR_NAME, value = Encoding}) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.
%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    {ok, xattr:cdmi_completion_status()} | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_cdmi_completion_status(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?CDMI_COMPLETION_STATUS_XATTR_NAME) of
        {ok, #document{value = #xattr{value = Val}}} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = #cdmi_completion_status{value = Val}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}};
        {error, {not_found, xattr}} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:cdmi_completion_status()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_cdmi_completion_status(_CTX, {uuid, FileUuid}, CompletionStatus) ->
    case xattr:save(FileUuid, #xattr{name = ?CDMI_COMPLETION_STATUS_XATTR_NAME, value = CompletionStatus}) of
        {ok, _} ->
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.
%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    {ok, xattr:mimetype()} | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_mimetype(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?MIMETYPE_XATTR_NAME) of
        {ok, #document{value = #xattr{value = Val}}} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = #mimetype{value = Val}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}};
        {error, {not_found, xattr}} ->
            #fuse_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:mimetype()) ->
    ok | logical_file_manager:error_reply().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_mimetype(CTX, {uuid, FileUuid} = FileEntry, Mimetype) ->
    case xattr:save(FileUuid, #xattr{name = ?MIMETYPE_XATTR_NAME, value = Mimetype}) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}};
        {error, {not_found, file_meta}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv delete_impl(CTX, File) with permission check
%%--------------------------------------------------------------------
-spec delete_dir(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?delete, 2}]).
delete_dir(CTX, File) ->
    delete_impl(CTX, File).

%%--------------------------------------------------------------------
%% @equiv delete_impl(CTX, File) with permission check
%%--------------------------------------------------------------------
-spec delete_file(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{?delete_object, {parent, 2}}, {?delete, 2}]).
delete_file(CTX, File) ->
    delete_impl(CTX, File).

%%--------------------------------------------------------------------
%% @doc Deletes file or directory
%%--------------------------------------------------------------------
-spec delete_impl(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    FuseResponse :: #fuse_response{} | no_return().
delete_impl(CTX = #fslogic_ctx{session_id = SessId}, File) ->
    {ok, #document{key = FileUUID, value = #file_meta{type = Type}} = FileDoc} = file_meta:get(File),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),
    {ok, FileChildren} =
        case Type of
            ?DIRECTORY_TYPE ->
                file_meta:list_children(FileDoc, 0, 1);
            ?REGULAR_FILE_TYPE ->
                #document{value = #file_location{} = Location} = fslogic_utils:get_local_file_location(File),
                ToDelete = fslogic_utils:get_local_storage_file_locations(Location),
                Results =
                    lists:map( %% @todo: run this via task manager
                        fun({StorageId, FileId}) ->
                            case storage:get(StorageId) of
                                {ok, Storage} ->
                                    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FileId),
                                    case storage_file_manager:unlink(SFMHandle) of
                                        ok -> ok;
                                        {error, Reason1} ->
                                            {{StorageId, FileId}, {error, Reason1}}
                                    end ;
                                {error, Reason2} ->
                                    {{StorageId, FileId}, {error, Reason2}}
                            end
                        end, ToDelete),
                case Results -- [ok] of
                    [] -> ok;
                    Errors ->
                        lists:foreach(
                            fun({{SID0, FID0}, {error, Reason0}}) ->
                                ?error("Cannot unlink file ~p from storage ~p due to: ~p", [FID0, SID0, Reason0])
                            end, Errors)
                end,
                {ok, []};
            _ ->
                {ok, []}
        end,
    case length(FileChildren) of
        0 ->
            {ok, ParentDoc} = file_meta:get_parent(FileDoc),
            fslogic_times:update_mtime_ctime(ParentDoc, fslogic_context:get_user_id(CTX)),
            ok = file_meta:delete(FileDoc),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames directory
%%--------------------------------------------------------------------
-spec rename_dir(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir(CTX, SourceEntry, TargetPath) ->
    case moving_into_itself(SourceEntry, TargetPath) of
        true ->
            #fuse_response{status = #status{code = ?EINVAL}};
        false ->
            case file_meta:exists({path, TargetPath}) of
                false ->
                    rename_impl(CTX, SourceEntry, TargetPath);
                true ->
                    case file_meta:get({path, TargetPath}) of
                        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = TargetDoc} ->
                            case delete_impl(CTX, TargetDoc) of
                                #fuse_response{status = #status{code = ?OK}} ->
                                    rename_impl(CTX, SourceEntry, TargetPath);
                                NotOK ->
                                    NotOK
                            end;
                        {ok, _TargetDoc} ->
                            #fuse_response{status = #status{code = ?ENOTDIR}}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames file
%%--------------------------------------------------------------------
-spec rename_file(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_object, {parent, 2}}, {?add_object, {parent, {path, 3}}}]).
rename_file(CTX, SourceEntry, TargetPath) ->
    case file_meta:exists({path, TargetPath}) of
        false ->
            rename_impl(CTX, SourceEntry, TargetPath);
        true ->
            case file_meta:get({path, TargetPath}) of
                {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                    #fuse_response{status = #status{code = ?EISDIR}};
                {ok, TargetDoc} ->
                    case delete_impl(CTX, TargetDoc) of
                        #fuse_response{status = #status{code = ?OK}} ->
                            rename_impl(CTX, SourceEntry, TargetPath);
                        NotOK ->
                            NotOK
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc Renames file_meta doc.
%%--------------------------------------------------------------------
-spec rename_impl(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
rename_impl(CTX, SourceEntry, TargetPath) ->
    ok = file_meta:rename(SourceEntry, {path, TargetPath}),
    {ok, FileDoc} = file_meta:get({path, TargetPath}),
    {ok, ParentDoc} = file_meta:get_parent({path, TargetPath}),

    fslogic_times:update_ctime(FileDoc, fslogic_context:get_user_id(CTX)),
    fslogic_times:update_mtime_ctime(ParentDoc, fslogic_context:get_user_id(CTX)),

    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_files(fslogic_worker:ctx(), file_meta:entry(), file_meta:posix_permissions()) ->
    ok | no_return().
chmod_storage_files(CTX = #fslogic_ctx{session_id = SessId}, FileEntry, Mode) ->
    case file_meta:get(FileEntry) of
        {ok, #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}} = FileDoc} ->
            {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),
            Results = lists:map(
                fun({SID, FID} = Loc) ->
                    {ok, Storage} = storage:get(SID),
                    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),
                    {Loc, storage_file_manager:chmod(SFMHandle, Mode)}
                end, fslogic_utils:get_local_storage_file_locations(FileEntry)),

            case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
                [] -> ok;
                Errors ->
                    [?error("Unable to chmod [FileId: ~p] [StoragId: ~p] to mode ~p due to: ~p", [FID, SID, Mode, Reason])
                        || {{SID, FID}, {error, Reason}} <- Errors],
                    throw(?EAGAIN)
            end;
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Checks if renamed entry is one of target path parents.
%% @end
%%--------------------------------------------------------------------
-spec moving_into_itself(SourceEntry :: fslogic_worker:file(), TargetPath :: file_meta:path()) ->
    boolean().
moving_into_itself(SourceEntry, TargetPath) ->
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),
    {_, ParentPath} = fslogic_path:basename_and_parent(TargetPath),
    {ok, {_, ParentUUIDs}} = file_meta:resolve_path(ParentPath),
    lists:any(fun(ParentUUID) -> ParentUUID =:= SourceUUID end, ParentUUIDs).
