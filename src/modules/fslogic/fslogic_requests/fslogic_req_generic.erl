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
-export([chmod/3, update_times/5,
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
-spec get_file_path(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
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
-spec update_times(fslogic_context:ctx(), File :: fslogic_worker:file(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {{owner, 'or', ?write_attributes}, 2}]).
update_times(Ctx, FileEntry, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) ->
        is_integer(Value) end, UpdateMap),
    fslogic_times:update_times_and_emit(FileEntry, UpdateMap1, fslogic_context:get_user_id(Ctx)),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chmod(fslogic_context:ctx(), File :: fslogic_worker:file(), Perms :: fslogic_worker:posix_permissions()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {owner, 2}]).
chmod(Ctx, File, Mode) ->
    chmod_storage_files(Ctx, File, Mode),

    % remove acl
    {ok, FileUuid} = file_meta:to_uuid(File),
    xattr:delete_by_name(FileUuid, ?ACL_KEY),
    {ok, _} = file_meta:update({uuid, FileUuid}, #{mode => Mode}),
    ok = permissions_cache:invalidate_permissions_cache(file_meta, FileUuid),

    fslogic_times:update_ctime(File, fslogic_context:get_user_id(Ctx)),
    fslogic_event:emit_file_perm_changed(FileUuid),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Changes file owner.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chown(fslogic_context:ctx(), File :: fslogic_worker:file(), UserId :: od_user:id()) ->
    #fuse_response{} | no_return().
-check_permissions([{?write_owner, 2}]).
chown(_, _File, _UserId) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name(), boolean()) ->
    #provider_response{} | no_return().
get_xattr(Ctx, FileEntry, ?ACL_KEY, _Inherited) ->
    case get_acl(Ctx, FileEntry) of
        #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = Acl}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?ACL_KEY, value = fslogic_acl:from_acl_to_json_format(Acl)}};
        Other ->
            Other
    end;
get_xattr(Ctx, FileEntry, ?MIMETYPE_KEY, _Inherited) ->
    case get_mimetype(Ctx, FileEntry) of
        #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Mimetype}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?MIMETYPE_KEY, value = Mimetype}};
        Other ->
            Other
    end;
get_xattr(Ctx, FileEntry, ?TRANSFER_ENCODING_KEY, _Inherited) ->
    case get_transfer_encoding(Ctx, FileEntry) of
        #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Encoding}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}};
        Other ->
            Other
    end;
get_xattr(Ctx, FileEntry, ?CDMI_COMPLETION_STATUS_KEY, _Inherited) ->
    case get_cdmi_completion_status(Ctx, FileEntry) of
        #provider_response{status = #status{code = ?OK}, provider_response = #cdmi_completion_status{value = Completion}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}};
        Other ->
            Other
    end;
get_xattr(Ctx, FileEntry, ?JSON_METADATA_KEY, Inherited) ->
    case get_metadata(Ctx, FileEntry, json, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = JsonTerm}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?JSON_METADATA_KEY, value = JsonTerm}};
        Other ->
            Other
    end;
get_xattr(Ctx, FileEntry, ?RDF_METADATA_KEY, Inherited) ->
    case get_metadata(Ctx, FileEntry, rdf, [], Inherited) of
        #provider_response{status = #status{code = ?OK}, provider_response = #metadata{value = Rdf}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #xattr{name = ?RDF_METADATA_KEY, value = Rdf}};
        Other ->
            Other
    end;
get_xattr(_Ctx, _, <<?CDMI_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(_Ctx, _, <<?ONEDATA_PREFIX_STR, _/binary>>, _) ->
    throw(?EPERM);
get_xattr(Ctx, FileEntry, XattrName, Inherited) ->
    get_custom_xatttr(Ctx, FileEntry, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @doc
%% Decides if xattr is normal or internal, and routes request to specific function
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, #xattr{}) ->
    #provider_response{} | no_return().
set_xattr(Ctx, FileEntry, #xattr{name = ?ACL_KEY, value = Acl}) ->
    set_acl(Ctx, FileEntry, #acl{value = fslogic_acl:from_json_format_to_acl(Acl)});
set_xattr(Ctx, FileEntry, #xattr{name = ?MIMETYPE_KEY, value = Mimetype}) ->
    set_mimetype(Ctx, FileEntry, Mimetype);
set_xattr(Ctx, FileEntry, #xattr{name = ?TRANSFER_ENCODING_KEY, value = Encoding}) ->
    set_transfer_encoding(Ctx, FileEntry, Encoding);
set_xattr(Ctx, FileEntry, #xattr{name = ?CDMI_COMPLETION_STATUS_KEY, value = Completion}) ->
    set_cdmi_completion_status(Ctx, FileEntry, Completion);
set_xattr(Ctx, FileEntry, #xattr{name = ?JSON_METADATA_KEY, value = Json}) ->
    set_metadata(Ctx, FileEntry, json, Json, []);
set_xattr(Ctx, FileEntry, #xattr{name = ?RDF_METADATA_KEY, value = Rdf}) ->
    set_metadata(Ctx, FileEntry, rdf, Rdf, []);
set_xattr(_Ctx, _, #xattr{name = <<?CDMI_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(_Ctx, _, #xattr{name = <<?ONEDATA_PREFIX_STR, _/binary>>}) ->
    throw(?EPERM);
set_xattr(Ctx, FileEntry, Xattr) ->
    set_custom_xattr(Ctx, FileEntry, Xattr).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_xattr(Ctx, {uuid, FileUuid} = FileEntry, XattrName) ->
    case xattr:delete_by_name(FileUuid, XattrName) of
        ok ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes' keys of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, boolean(), boolean()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
list_xattr(_Ctx, {uuid, FileUuid}, Inherited, ShowInternal) ->
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
-spec get_acl(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_acl, 2}]).
get_acl(_Ctx, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?ACL_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = fslogic_acl:from_json_format_to_acl(Val)}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets access control list of file.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, #acl{}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
set_acl(Ctx, {uuid, FileUuid} = FileEntry, #acl{value = Val}) ->
    case xattr:save(FileUuid, ?ACL_KEY, fslogic_acl:from_acl_to_json_format(Val)) of
        {ok, _} ->
            ok = permissions_cache:invalidate_permissions_cache(custom_metadata, FileUuid),
            ok = chmod_storage_files(
                fslogic_context:set_session_id(Ctx, ?ROOT_SESS_ID),
                {uuid, FileUuid}, 8#000
            ),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Removes access control list of file.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
remove_acl(Ctx, {uuid, FileUuid} = FileEntry) ->
    case xattr:delete_by_name(FileUuid, ?ACL_KEY) of
        ok ->
            ok = permissions_cache:invalidate_permissions_cache(custom_metadata, FileUuid),
            {ok, #document{value = #file_meta{mode = Mode}}} = file_meta:get({uuid, FileUuid}),
            ok = chmod_storage_files(
                fslogic_context:set_session_id(Ctx, ?ROOT_SESS_ID),
                {uuid, FileUuid}, Mode
            ),
            ok = fslogic_event:emit_file_perm_changed(FileUuid),
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_transfer_encoding(_Ctx, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?TRANSFER_ENCODING_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(fslogic_context:ctx(), {uuid, file_meta:uuid()},
    xattr:transfer_encoding()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_transfer_encoding(Ctx, {uuid, FileUuid} = FileEntry, Encoding) ->
    case xattr:save(FileUuid, ?TRANSFER_ENCODING_KEY, Encoding) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
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
-spec get_cdmi_completion_status(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_cdmi_completion_status(_Ctx, {uuid, FileUuid}) ->
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
-spec set_cdmi_completion_status(fslogic_context:ctx(), {uuid, file_meta:uuid()},
    xattr:cdmi_completion_status()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_cdmi_completion_status(_Ctx, {uuid, FileUuid}, CompletionStatus) ->
    case xattr:save(FileUuid, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus) of
        {ok, _} ->
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.
%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_mimetype(_Ctx, {uuid, FileUuid}) ->
    case xattr:get_by_name(FileUuid, ?MIMETYPE_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(fslogic_context:ctx(), {uuid, file_meta:uuid()},
    xattr:mimetype()) -> #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_mimetype(Ctx, {uuid, FileUuid} = FileEntry, Mimetype) ->
    case xattr:save(FileUuid, ?MIMETYPE_KEY, Mimetype) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @equiv replicate_file(Ctx, {uuid, Uuid}, Block, 0)
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block()) ->
    #provider_response{}.
replicate_file(Ctx, {uuid, Uuid}, Block) ->
    replicate_file(Ctx, {uuid, Uuid}, Block, 0).

%%--------------------------------------------------------------------
%% @doc
%% Replicate given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), {uuid, file_meta:uuid()},
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
    custom_metadata:filter(), boolean()) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_metadata(_Ctx, {uuid, FileUuid}, json, Names, Inherited) ->
    case custom_metadata:get_json_metadata(FileUuid, Names, Inherited) of
        {ok, Meta} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #metadata{type = json, value = Meta}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end;
get_metadata(_Ctx, {uuid, FileUuid}, rdf, _, _) ->
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
    custom_metadata:value(), custom_metadata:filter()) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_metadata(_Ctx, {uuid, FileUuid}, json, Value, Names) ->
    {ok, _} = custom_metadata:set_json_metadata(FileUuid, Value, Names),
    #provider_response{status = #status{code = ?OK}};
set_metadata(_Ctx, {uuid, FileUuid}, rdf, Value, _) ->
    {ok, _} = custom_metadata:set_rdf_metadata(FileUuid, Value),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Remove metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(session:id(), {uuid, file_meta:uuid()}, custom_metadata:type()) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
remove_metadata(_Ctx, {uuid, FileUuid}, json) ->
    ok = custom_metadata:remove_json_metadata(FileUuid),
    #provider_response{status = #status{code = ?OK}};
remove_metadata(_Ctx, {uuid, FileUuid}, rdf) ->
    ok = custom_metadata:remove_rdf_metadata(FileUuid),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check given permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(fslogic_context:ctx(), {uuid, file_meta:uuid()}, fslogic_worker:open_flag()) ->
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
-spec create_share(fslogic_context:ctx(), {uuid, file_meta:uuid()}, od_share:name()) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
create_share(Ctx, {uuid, FileUuid}, Name) ->
    SpaceId = fslogic_context:get_space_id(Ctx),
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
-spec remove_share(fslogic_context:ctx(), {uuid, file_meta:uuid()}) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
remove_share(Ctx, {uuid, FileUuid}) ->
    Auth = fslogic_context:get_auth(Ctx),
    ShareId = file_info:get_share_id(Ctx),

    ok = share_logic:delete(Auth, ShareId),
    {ok, _} = file_meta:remove_share(FileUuid, ShareId),
    ok = permissions_cache:invalidate_permissions_cache(file_meta, FileUuid),

    #provider_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_custom_xatttr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, xattr:name(), boolean()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_metadata, 2}]).
get_custom_xatttr(_Ctx, {uuid, FileUuid}, XattrName, Inherited) ->
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
-spec set_custom_xattr(fslogic_context:ctx(), {uuid, Uuid :: file_meta:uuid()}, #xattr{}) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_metadata, 2}]).
set_custom_xattr(Ctx, {uuid, FileUuid} = FileEntry, #xattr{name = XattrName, value = XattrValue}) ->
    case xattr:save(FileUuid, XattrName, XattrValue) of
        {ok, _} ->
            fslogic_times:update_ctime(FileEntry, fslogic_context:get_user_id(Ctx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check read permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_read(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
check_perms_read(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check write permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_write(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
check_perms_write(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check rdwr permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_rdwr(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
check_perms_rdwr(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_files(fslogic_context:ctx(), file_meta:entry(), file_meta:posix_permissions()) ->
    ok | no_return().
chmod_storage_files(Ctx, File, Mode) ->
    SessId = fslogic_context:get_session_id(Ctx),
    case file_meta:get(File) of
        {ok, #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}} = FileDoc} ->
            {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(Ctx)),
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
