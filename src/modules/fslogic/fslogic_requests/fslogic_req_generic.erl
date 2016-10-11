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
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/events/types.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([chmod/3, get_file_attr/2, delete/3, update_times/5,
    get_xattr/4, set_xattr/3, remove_xattr/3, list_xattr/4,
    get_acl/2, set_acl/3, remove_acl/2, get_transfer_encoding/2,
    set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3,
    get_file_path/2, chmod_storage_files/3, replicate_file/3,
    get_metadata/5, set_metadata/5, remove_metadata/3,
    check_perms/3, create_share/3, remove_share/2]).
%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Translates given file's UUID to absolute path.
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
get_file_path(Ctx, {uuid, FileUUID}) ->
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_path{value = fslogic_uuid:uuid_to_path(Ctx, FileUUID)}
    }.


%%--------------------------------------------------------------------
%% @doc Changes file's access times.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec update_times(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {{owner, 'or', ?write_attributes}, 2}]).
update_times(CTX, FileEntry, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) ->
        is_integer(Value) end, UpdateMap),
    fslogic_times:update_times_and_emit(FileEntry, UpdateMap1, fslogic_context:get_user_id(CTX)),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chmod(fslogic_worker:ctx(), File :: fslogic_worker:file(), Perms :: fslogic_worker:posix_permissions()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {owner, 2}]).
chmod(CTX, File, Mode) ->
    chmod_storage_files(CTX, File, Mode),

    % remove acl
    {ok, FileUuid} = file_meta:to_uuid(File),
    xattr:delete_by_name(FileUuid, ?ACL_KEY),
    {ok, _} = file_meta:update(File, #{mode => Mode}),
    ok = permissions_cache:invalidate_permissions_cache(),

    fslogic_times:update_ctime(File, fslogic_context:get_user_id(CTX)),
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
-spec chown(fslogic_worker:ctx(), File :: fslogic_worker:file(), UserId :: od_user:id()) ->
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
get_file_attr(#fslogic_ctx{session_id = SessId, share_id = ShareId} = CTX, File) ->
    ?debug("Get attr for file entry: ~p", [File]),
    case file_meta:get(File) of
        {ok, #document{key = UUID, value = #file_meta{
            type = Type, mode = Mode, provider_id = ProviderId, uid = UserID,
            name = Name, shares = Shares}} = FileDoc} ->


            {#posix_user_ctx{gid = GID, uid = UID}, SpaceId} = try
                {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),
                SId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
                StorageId = luma_utils:get_storage_id(SpaceUUID),
                StorageType = luma_utils:get_storage_type(StorageId),
                {fslogic_storage:get_posix_user_ctx(StorageType, SessId, SpaceUUID), SId}
            catch
                % TODO (VFS-2024) - repair decoding and change to throw:{not_a_space, _} -> ?ROOT_POSIX_CTX
                _:_ -> {?ROOT_POSIX_CTX, undefined}
            end,

            Size = fslogic_blocks:get_file_size(FileDoc),

            FinalUID = case session:get(SessId) of
                {ok, #document{value = #session{identity = #user_identity{user_id = UserID}}}} ->
                    UID;
                _ ->
                    luma_utils:gen_storage_uid(UserID)
            end,

            {ok, {ATime, CTime, MTime}} = times:get_or_default(UUID),

            #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
                gid = GID,
                uuid = fslogic_uuid:uuid_to_share_guid(UUID, SpaceId, ShareId),
                type = Type, mode = Mode, atime = ATime, mtime = MTime,
                ctime = CTime, uid = FinalUID, size = Size, name = Name, provider_id = ProviderId,
                shares = Shares
            }};
        {error, {not_found, _}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @doc Deletes file.
%% For best performance use following arg types: document -> uuid -> path
%% If parameter Silent is true, file_removal_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete(fslogic_worker:ctx(), File :: fslogic_worker:file(), Silent :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
delete(CTX, File, Silent) ->
    case file_meta:get(File) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = FileDoc} ->
            delete_dir(CTX, FileDoc, Silent);
        {ok, FileDoc} ->
            delete_file(CTX, FileDoc, Silent)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name(), boolean()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
%%get_xattr(_CTX, _, <<?CDMI_PREFIX_STR, _/binary>>, _) -> throw(?EPERM); %todo fix eperm in attributes/ GET
%%get_xattr(_CTX, _, <<?ONEDATA_PREFIX_STR, _/binary>>, _) -> throw(?EPERM);
get_xattr(_CTX, {uuid, FileUuid}, XattrName, Inherited) ->
    case xattr:get_by_name(FileUuid, XattrName, Inherited) of
        {ok, XattrValue} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = XattrName, value = XattrValue}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, #xattr{}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_xattr(_CTX, _, #xattr{name = <<?CDMI_PREFIX_STR, _/binary>>}) -> throw(?EPERM);
set_xattr(_CTX, _, #xattr{name = <<?ONEDATA_PREFIX_STR, _/binary>>}) -> throw(?EPERM);
set_xattr(CTX, {uuid, FileUuid} = FileEntry, #xattr{name = XattrName, value = XattrValue}) ->
    case xattr:save(FileUuid, XattrName, XattrValue) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_xattr(CTX, {uuid, FileUuid} = FileEntry, XattrName) ->
    case xattr:delete_by_name(FileUuid, XattrName) of
        ok ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, boolean(), boolean()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
list_xattr(_CTX, {uuid, FileUuid}, Inherited, ShowInternal) ->
    case xattr:list(FileUuid, Inherited) of
        {ok, XattrList} ->
            FilteredXattrList = case ShowInternal of
                true ->
                    XattrList;
                false ->
                    lists:filter(fun(Key) ->
                        not lists:any(
                            fun(InternalPrefix) ->
                                str_utils:binary_starts_with(Key, InternalPrefix)
                            end, ?METADATA_INTERNAL_PREFIXES)
                    end, XattrList)
            end,
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr_list{names = FilteredXattrList}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Get access control list of file.
%%--------------------------------------------------------------------
-spec get_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_acl, 2}]).
get_acl(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?ACL_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets access control list of file.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}, #acl{}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
set_acl(CTX, {uuid, FileUuid} = FileEntry, #acl{value = Val}) ->
    case xattr:save(FileUuid, ?ACL_KEY, Val) of
        {ok, _} ->
            ok = permissions_cache:invalidate_permissions_cache(),
            ok = chmod_storage_files(
                CTX#fslogic_ctx{session_id = ?ROOT_SESS_ID, session = ?ROOT_SESS},
                {uuid, FileUuid}, 8#000
            ),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Removes access control list of file.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_worker:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
remove_acl(CTX, {uuid, FileUuid} = FileEntry) ->
    case xattr:delete_by_name(FileUuid, ?ACL_KEY) of
        ok ->
            ok = permissions_cache:invalidate_permissions_cache(),
            {ok, #document{value = #file_meta{mode = Mode}}} = file_meta:get({uuid, FileUuid}),
            ok = chmod_storage_files(
                CTX#fslogic_ctx{session_id = ?ROOT_SESS_ID, session = ?ROOT_SESS},
                {uuid, FileUuid}, Mode
            ),
            ok = fslogic_event:emit_permission_changed(FileUuid),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_transfer_encoding(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?TRANSFER_ENCODING_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:transfer_encoding()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_transfer_encoding(CTX, {uuid, FileUuid} = FileEntry, Encoding) ->
    case xattr:save(FileUuid, ?TRANSFER_ENCODING_KEY, Encoding) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.
%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_cdmi_completion_status(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?CDMI_COMPLETION_STATUS_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #cdmi_completion_status{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:cdmi_completion_status()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_cdmi_completion_status(_CTX, {uuid, FileUuid}, CompletionStatus) ->
    case xattr:save(FileUuid, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus) of
        {ok, _} ->
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.
%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_mimetype(_CTX, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?MIMETYPE_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    xattr:mimetype()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_mimetype(CTX, {uuid, FileUuid} = FileEntry, Mimetype) ->
    case xattr:save(FileUuid, ?MIMETYPE_KEY, Mimetype) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(CTX)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @equiv replicate_file(Ctx, {uuid, Uuid}, Block, 0)
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block()) ->
    #provider_response{}.
replicate_file(Ctx, {uuid, Uuid}, Block) ->
    replicate_file(Ctx, {uuid, Uuid}, Block, 0).

%%--------------------------------------------------------------------
%% @doc
%% Replicate given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_worker:ctx(), {uuid, file_meta:uuid()},
    fslogic_blocks:block(), non_neg_integer()) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
replicate_file(Ctx, {uuid, Uuid}, Block, Offset) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_meta:get({uuid, Uuid}) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
            case fslogic_req_special:read_dir(Ctx, {uuid, Uuid}, Offset, Chunk) of
                #fuse_response{fuse_response = #file_children{child_links = ChildLinks}}
                    when length(ChildLinks) < Chunk ->
                    utils:pforeach(
                        fun(#child_link{uuid = ChildGuid}) ->
                            replicate_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ChildGuid)}, Block)
                        end, ChildLinks),
                    #provider_response{status = #status{code = ?OK}};
                #fuse_response{fuse_response = #file_children{child_links = ChildLinks}} ->
                    utils:pforeach(
                        fun(#child_link{uuid = ChildGuid}) ->
                            replicate_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ChildGuid)}, Block)
                        end, ChildLinks),
                    replicate_file(Ctx, {uuid, Uuid}, Block, Offset + Chunk);
                Other ->
                    Other
            end;
        {ok, _} ->
            #fuse_response{status = Status} = fslogic_req_regular:synchronize_block(Ctx, {uuid, Uuid}, Block, false),
            #provider_response{status = Status}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Get metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(session:id(), {uuid, file_meta:uuid()}, custom_metadata:type(),
    custom_metadata:filter(), boolean()) -> {ok, custom_metadata:value()}.
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_metadata(_CTX, {uuid, FileUuid}, json, Names, Inherited) ->
    case custom_metadata:get_json_metadata(FileUuid, Names, Inherited) of
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = json, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata(_CTX, {uuid, FileUuid}, rdf, _, _) ->
    case custom_metadata:get_rdf_metadata(FileUuid) of
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = rdf, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Set metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(session:id(), {uuid, file_meta:uuid()}, custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter()) -> ok.
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_metadata(_CTX, {uuid, FileUuid}, json, Value, Names) ->
    {ok, _} = custom_metadata:set_json_metadata(FileUuid, Value, Names),
    #provider_response{status = #status{code = ?OK}};
set_metadata(_CTX, {uuid, FileUuid}, rdf, Value, _) ->
    {ok, _} = custom_metadata:set_rdf_metadata(FileUuid, Value),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Remove metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(session:id(), {uuid, file_meta:uuid()}, custom_metadata:type()) -> ok.
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_metadata(_CTX, {uuid, FileUuid}, json) ->
    ok = custom_metadata:remove_json_metadata(FileUuid),
    #provider_response{status = #status{code = ?OK}};
remove_metadata(_CTX, {uuid, FileUuid}, rdf) ->
    ok = custom_metadata:remove_rdf_metadata(FileUuid),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check given permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, fslogic_worker:open_flags()) ->
    #provider_response{}.
check_perms(Ctx, Uuid, read) ->
    check_perms_read(Ctx, Uuid);
check_perms(Ctx, Uuid, write) ->
    check_perms_write(Ctx, Uuid);
check_perms(Ctx, Uuid, rdwr) ->
    check_perms_rdwr(Ctx, Uuid).

%%--------------------------------------------------------------------
%% @doc
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec create_share(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, od_share:name()) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
create_share(Ctx = #fslogic_ctx{space_id = SpaceId}, {uuid, FileUuid}, Name) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, Auth} = session:get_auth(SessId),
    ShareId = datastore_utils:gen_uuid(),
    ShareGuid = fslogic_uuid:uuid_to_share_guid(FileUuid, SpaceId, ShareId),
    {ok, _} = share_logic:create(Auth, ShareId, Name, SpaceId, ShareGuid),
    {ok, _} = file_meta:add_share(FileUuid, ShareId),
    #provider_response{status = #status{code = ?OK}, provider_response = #share{share_id = ShareId, share_file_uuid = ShareGuid}}.

%%--------------------------------------------------------------------
%% @doc
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec remove_share(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
remove_share(Ctx = #fslogic_ctx{share_id = ShareId}, {uuid, FileUuid}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, Auth} = session:get_auth(SessId),

    ok = share_logic:delete(Auth, ShareId),
    {ok, _} = file_meta:remove_share(FileUuid, ShareId),
    ok = permissions_cache:invalidate_permissions_cache(),

    #provider_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check read permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_read(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
check_perms_read(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check write permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_write(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
check_perms_write(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check rdwr permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_rdwr(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
check_perms_rdwr(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @equiv delete_impl(CTX, File, Silent) with permission check
%%--------------------------------------------------------------------
-spec delete_dir(fslogic_worker:ctx(), File :: fslogic_worker:file(), Silent :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?delete, 2}, {?list_container, 2}]).
delete_dir(CTX, File, Silent) ->
    delete_impl(CTX, File, Silent).

%%--------------------------------------------------------------------
%% @equiv delete_impl(CTX, File, Silent) with permission check
%%--------------------------------------------------------------------
-spec delete_file(fslogic_worker:ctx(), File :: fslogic_worker:file(), Silent :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{?delete_object, {parent, 2}}, {?delete, 2}]).
delete_file(CTX, File, Silent) ->
    delete_impl(CTX, File, Silent).

%%--------------------------------------------------------------------
%% @doc
%% Deletes file or directory
%% If parameter Silent is true, file_removal_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete_impl(fslogic_worker:ctx(), File :: fslogic_worker:file(), Silent :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
delete_impl(CTX, File, Silent) ->
    {ok, #document{key = FileUUID, value = #file_meta{type = Type}} = FileDoc} = file_meta:get(File),

    {ok, FileChildren} =
        case Type of
            ?DIRECTORY_TYPE ->
                file_meta:list_children(FileDoc, 0, 1);
            _ ->
                {ok, []}
        end,
    case length(FileChildren) of
        0 ->
            ok = worker_proxy:call(file_deletion_worker,
                {fslogic_deletion_request, CTX, FileUUID, Silent}),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_files(fslogic_worker:ctx(), file_meta:entry(), file_meta:posix_permissions()) ->
    ok | no_return().
chmod_storage_files(CTX = #fslogic_ctx{session_id = SessId}, File, Mode) ->
    case file_meta:get(File) of
        {ok, #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}} = FileDoc} ->
            {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),
            Results = lists:map(
                fun({SID, FID} = Loc) ->
                    {ok, Storage} = storage:get(SID),
                    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),
                    {Loc, storage_file_manager:chmod(SFMHandle, Mode)}
                end, fslogic_utils:get_local_storage_file_locations(File)),

            case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
                [] -> ok;
                Errors ->
                    [?error("Unable to chmod [FileId: ~p] [StoragId: ~p] to mode ~p due to: ~p", [FID, SID, Mode, Reason])
                        || {{SID, FID}, {error, Reason}} <- Errors],

                    throw(?EAGAIN)
            end;
        _ -> ok
    end.
