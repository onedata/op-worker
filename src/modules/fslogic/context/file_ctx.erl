%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque type storing information about file, working as a cache.
%%% Its lifetime is limited by the time of request.
%%% Once the record is created via new_* function and fslogic_worker has
%%% determined that the request can be handled locally - all of the functions
%%% in this module should work (for remote files the operations are limited to
%%% getting space_id). If effort of computing something is significant,
%%% the value is cached and the further calls will use it. Therefore all of the
%%% functions return updated version of context together with the result.
%%% @end
%%%--------------------------------------------------------------------
-module(file_ctx).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% Record definition
-record(file_ctx, {
    canonical_path :: undefined | path(),
    guid :: undefined | guid(),
    file_doc :: undefined | file_meta:doc(),
    parent :: undefined | ctx(),
    storage_file_id :: undefined | helpers:file_id(),
    space_name :: undefined | od_space:name() | od_space:alias(),
    storage_posix_user_context :: undefined | luma:posix_user_ctx(),
    times :: undefined | times:times(),
    file_name :: undefined | file_meta:name(),
    storage_doc :: undefined | storage:doc(),
    local_file_location_doc :: undefined | file_location:doc(),
    location_ids :: undefined | [file_location:id()],
    acl :: undefined | acl:acl()
}).

-type path() :: file_meta:path().
-type guid() :: fslogic_worker:file_guid().
-type ctx() :: #file_ctx{}.

%% Functions creating context and filling its data
-export([new_by_logical_path/2, new_by_canonical_path/2, new_by_guid/1]).
-export([fill_guid/1]).

%% Functions that do not modify context
-export([get_share_id_const/1, get_space_id_const/1, get_space_dir_uuid_const/1,
    get_guid_const/1, get_uuid_entry_const/1]).
-export([is_file_ctx_const/1, is_space_dir_const/1, is_user_root_dir_const/2,
    is_root_dir_const/1, has_acl_const/1, file_exists_const/1]).

%% Functions modifying context
-export([get_canonical_path/1, get_file_doc/1, get_parent/2, get_storage_file_id/1,
    get_aliased_name/2, get_posix_storage_user_context/2, get_times/1,
    get_parent_guid/2, get_child/3, get_file_children/4, get_logical_path/2,
    get_storage_doc/1, get_local_file_location_doc/1, get_file_location_ids/1, get_acl/1]).
-export([is_dir/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file logical path.
%% @end
%%--------------------------------------------------------------------
-spec new_by_logical_path(user_ctx:ctx(), path()) -> ctx().
new_by_logical_path(UserCtx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case session:is_special(user_ctx:get_session_id(UserCtx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context of special session."
            " You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    UserId = user_ctx:get_user_id(UserCtx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(fslogic_uuid:user_root_dir_uuid(UserId)),
                    #file_ctx{canonical_path = filename:join(Tokens), guid = UserRootDirGuid};
                [<<"/">>, SpaceName | Rest] ->
                    #document{value = #od_user{space_aliases = Spaces}} = user_ctx:get_user(UserCtx),
                    case lists:keyfind(SpaceName, 2, Spaces) of
                        false ->
                            throw(?ENOENT);
                        {SpaceId, SpaceName} ->
                            #file_ctx{canonical_path = filename:join([<<"/">>, SpaceId | Rest]), space_name = SpaceName}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file canonical path.
%% @end
%%--------------------------------------------------------------------
-spec new_by_canonical_path(user_ctx:ctx(), path()) -> ctx().
new_by_canonical_path(UserCtx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case session:is_special(user_ctx:get_session_id(UserCtx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context of special session."
            " You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    UserId = user_ctx:get_user_id(UserCtx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(fslogic_uuid:user_root_dir_uuid(UserId)),
                    #file_ctx{canonical_path = filename:join(Tokens), guid = UserRootDirGuid};
                [<<"/">>, SpaceId | Rest] ->
                    fill_guid(#file_ctx{canonical_path = filename:join([<<"/">>, SpaceId | Rest])})
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(guid()) -> ctx().
new_by_guid(Guid) when Guid =/= undefined ->
    #file_ctx{guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Fills GUID in file context record. This function is called when we know
%% that the file is locally supported, to ensure that file context contains GUID
%% in function later on, to simplify logic.
%% @end
%%--------------------------------------------------------------------
-spec fill_guid
    (ctx()) -> ctx();
    (undefined) -> undefined.
fill_guid(undefined) ->
    undefined;
fill_guid(FileCtx = #file_ctx{guid = undefined, canonical_path = Path}) ->
    {ok, Uuid} = file_meta:to_uuid({path, Path}),
    SpaceId = get_space_id_const(FileCtx),
    Guid = fslogic_uuid:uuid_to_guid(Uuid, SpaceId),
    FileCtx#file_ctx{guid = Guid};
fill_guid(FileCtx) ->
    FileCtx.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's share ID.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id_const(user_ctx:ctx()) -> od_share:id() | undefined.
get_share_id_const(#file_ctx{guid = undefined}) ->
    undefined;
get_share_id_const(#file_ctx{guid = Guid}) ->
    {_FileUuid, _SpaceId, ShareId} = fslogic_uuid:unpack_share_guid(Guid),
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id() | undefined.
get_space_id_const(#file_ctx{guid = undefined, canonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, SpaceId | _] ->
            SpaceId;
        _ ->
            undefined
    end;
get_space_id_const(#file_ctx{guid = Guid}) ->
    fslogic_uuid:guid_to_space_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space dir UUID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_dir_uuid_const(ctx()) -> file_meta:uuid().
get_space_dir_uuid_const(FileCtx) ->
    SpaceId = get_space_id_const(FileCtx),
    fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec get_guid_const(ctx()) -> fslogic_worker:file_guid().
get_guid_const(#file_ctx{guid = Guid}) ->
    Guid.

%%--------------------------------------------------------------------
%% @todo remove this function and pass file info wherever possible
%% @doc
%% Returns file UUID entry.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid_entry_const(ctx()) -> {uuid, file_meta:uuid()}.
get_uuid_entry_const(FileCtx) ->
    Guid = get_guid_const(FileCtx),
    {uuid, fslogic_uuid:guid_to_uuid(Guid)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's canonical path (starting with "/SpaceId/...").
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_path(ctx()) -> {path(), ctx()}.
get_canonical_path(FileCtx = #file_ctx{canonical_path = undefined}) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {<<"/">>, FileCtx#file_ctx{canonical_path = <<"/">>}};
        false ->
            Guid = get_guid_const(FileCtx),
            Uuid = fslogic_uuid:guid_to_uuid(Guid),
            LogicalPath = fslogic_uuid:uuid_to_path(?ROOT_SESS_ID, Uuid),
            {ok, [<<"/">>, _SpaceName | Rest]} = fslogic_path:tokenize_skipping_dots(LogicalPath),
            SpaceId = get_space_id_const(FileCtx),
            CanonicalPath = filename:join([<<"/">>, SpaceId | Rest]),
            {CanonicalPath, FileCtx#file_ctx{canonical_path = CanonicalPath}}
    end;
get_canonical_path(#file_ctx{canonical_path = Path}) ->
    Path.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's logical path (starting with "/SpaceName/...", or "/SpaceAlias/...").
%% @end
%%--------------------------------------------------------------------
-spec get_logical_path(ctx(), user_ctx:ctx()) ->
    {file_meta:path(), ctx()}.
get_logical_path(FileCtx, UserCtx) ->
    case get_canonical_path(FileCtx) of
        {<<"/">>, FileCtx2} ->
            {<<"/">>, FileCtx2};
        {Path, FileCtx2} ->
            {SpaceName, FileCtx3} = get_space_name(FileCtx2, UserCtx),
            {ok, [<<"/">>, _SpaceId | Rest]} = fslogic_path:tokenize_skipping_dots(Path),
            LogicalPath = filename:join([<<"/">>, SpaceName | Rest]),
            {LogicalPath, FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's file_meta document.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc(ctx()) -> {file_meta:doc(), ctx()}.
get_file_doc(FileCtx = #file_ctx{file_doc = undefined}) ->
    Guid = get_guid_const(FileCtx),
    {ok, FileDoc} =  file_meta:get({uuid, fslogic_uuid:guid_to_uuid(Guid)}),
    {FileDoc, FileCtx#file_ctx{file_doc = FileDoc}};
get_file_doc(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent's file context.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(ctx(), undefined | od_user:id()) -> {ParentFileCtx :: ctx(), NewFileCtx :: ctx()}.
get_parent(FileCtx = #file_ctx{parent = undefined}, UserId) -> %todo use user_ctx
    {Doc, FileCtx2} = get_file_doc(FileCtx),
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
    ParentGuid =
        case fslogic_uuid:is_root_dir(ParentUuid) of
            true ->
                case ParentUuid =:= ?ROOT_DIR_UUID %todo use user_ctx:is_root/guest
                    andalso UserId =/= undefined
                    andalso UserId =/= ?ROOT_USER_ID
                    andalso UserId =/= ?GUEST_USER_ID
                of
                    true ->
                        fslogic_uuid:user_root_dir_guid(fslogic_uuid:user_root_dir_uuid(UserId));
                    _ ->
                        fslogic_uuid:uuid_to_guid(ParentUuid, undefined)
                end;
            false ->
                SpaceId = get_space_id_const(FileCtx2),
                case get_share_id_const(FileCtx2) of
                    undefined ->
                        fslogic_uuid:uuid_to_guid(ParentUuid, SpaceId);
                    ShareId ->
                        fslogic_uuid:uuid_to_share_guid(ParentUuid, SpaceId, ShareId)
                end
        end,
    Parent = new_by_guid(ParentGuid),
    {Parent, FileCtx2#file_ctx{parent = Parent}};
get_parent(FileCtx = #file_ctx{parent = Parent}, _UserId) ->
    {Parent, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns GUID of parent or undefined when the file is a root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid(ctx(), undefined | od_user:id()) -> {fslogic_worker:file_guid(), ctx()}.
get_parent_guid(FileCtx, UserId) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {undefined, FileCtx};
        false ->
            {ParentFile, NewFile} = get_parent(FileCtx, UserId),
            ParentGuid = get_guid_const(ParentFile),
            {ParentGuid, NewFile}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage file ID (the ID of file on storage. In case of posix it is
%% its path on storage).
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(ctx()) -> {StorageFileId :: helpers:file(), ctx()}.
get_storage_file_id(FileCtx) ->
    FileEntry = get_uuid_entry_const(FileCtx),
    FileId = fslogic_utils:gen_storage_file_id(FileEntry), %todo TL do not use this util function, as it it overcomplicated
    {FileId, FileCtx#file_ctx{storage_file_id = FileId}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns name (or user alias) of the space where the file is located.
%% @end
%%--------------------------------------------------------------------
-spec get_space_name(ctx(), user_ctx:ctx()) ->
    {od_space:name() | od_space:alias(), ctx()} | no_return().
get_space_name(FileCtx = #file_ctx{space_name = undefined}, UserCtx) ->
    SpaceId = get_space_id_const(FileCtx),
    #document{value = #od_user{space_aliases = Spaces}} = user_ctx:get_user(UserCtx),

    case lists:keyfind(SpaceId, 1, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, SpaceName} ->
            {SpaceName, FileCtx#file_ctx{space_name = SpaceName}}
    end;
get_space_name(FileCtx = #file_ctx{space_name = SpaceName}, _Ctx) ->
    {SpaceName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns name of the file (if the file represents space dir, returns user's space alias).
%% @end
%%--------------------------------------------------------------------
-spec get_aliased_name(ctx(), user_ctx:ctx()) ->
    {file_meta:name(), ctx()} | no_return().
get_aliased_name(FileCtx = #file_ctx{file_name = undefined}, UserCtx) ->
    SessionIsNotSpecial = (not session:is_special(user_ctx:get_session_id(UserCtx))),
    case is_space_dir_const(FileCtx) andalso SessionIsNotSpecial of
        false ->
            {#document{value = #file_meta{name = Name}}, FileCtx2} = get_file_doc(FileCtx),
            {Name, FileCtx2#file_ctx{file_name = Name}};
        true ->
            {Name, FileCtx2} = get_space_name(FileCtx, UserCtx),
            {Name, FileCtx2#file_ctx{file_name = Name}}
    end;
get_aliased_name(FileCtx = #file_ctx{file_name = FileName}, _UserCtx) ->
    {FileName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns posix storage user context, holding UID and GID of file on posix storage.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_storage_user_context(ctx(), user_ctx:ctx()) ->
    {luma:posix_user_ctx(), ctx()}.
get_posix_storage_user_context(FileCtx, UserId) ->
    IsSpaceDir = is_space_dir_const(FileCtx),
    IsUserRootDir = is_root_dir_const(FileCtx),
    SpaceId = get_space_id_const(FileCtx),
    UserCtx = case IsSpaceDir orelse IsUserRootDir of
        true -> luma:get_posix_user_ctx(?ROOT_USER_ID, SpaceId);
        false -> luma:get_posix_user_ctx(UserId, SpaceId)
    end,
    {UserCtx, FileCtx#file_ctx{storage_posix_user_context = UserCtx}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's atime, ctime and mtime.
%% @end
%%--------------------------------------------------------------------
-spec get_times(ctx()) -> {times:times(), ctx()}.
get_times(FileCtx) ->
    {uuid, FileUuid} = get_uuid_entry_const(FileCtx),
    {ok, Times} = times:get_or_default(FileUuid),
    {Times, FileCtx#file_ctx{times = Times}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns child of the file with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child(ctx(), file_meta:name(), od_user:id()) ->
    {ChildFile :: ctx(), NewFile :: ctx()} | no_return().
get_child(FileCtx, Name, UserId) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {ok, #document{value = #od_user{space_aliases = Spaces}}} = od_user:get(UserId),
            case lists:keyfind(Name, 2, Spaces) of
                {SpaceId, _} ->
                    Child = new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
                    {Child, FileCtx};
                false -> throw(?ENOENT)
            end;
        _ ->
            SpaceId = get_space_id_const(FileCtx),
            {FileDoc, FileCtx2} = get_file_doc(FileCtx),
            case file_meta:resolve_path(FileDoc, <<"/", Name/binary>>) of
                {ok, {ChildDoc, _}} ->
                    ShareId = get_share_id_const(FileCtx2),
                    Child = new_child_by_doc(ChildDoc, SpaceId, ShareId),
                    {Child, FileCtx2};
                {error, {not_found, _}} ->
                    throw(?ENOENT)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of file children.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(), Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit) ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            #document{value = #od_user{space_aliases = Spaces}} = user_ctx:get_user(UserCtx),
            case Offset < length(Spaces) of
                true ->
                    SpacesChunk = lists:sublist(Spaces, Offset + 1, Limit),
                    Children = lists:map(fun({SpaceId, SpaceName}) ->
                        SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                        new_child_by_uuid(SpaceDirUuid, SpaceName, SpaceId, undefined)
                    end, SpacesChunk),
                    {Children, FileCtx};
                false ->
                    {[], FileCtx}
            end;
        false ->
            {FileDoc = #document{}, FileCtx2} = get_file_doc(FileCtx),
            {ok, ChildrenLinks} = file_meta:list_children(FileDoc, Offset, Limit),
            SpaceId = get_space_id_const(FileCtx2),
            ShareId = get_share_id_const(FileCtx2),
            Children =
                lists:map(fun(#child_link{name = Name, uuid = Uuid}) ->
                    new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
                end, ChildrenLinks),
            {Children, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage document of file's space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_doc(ctx()) -> {storage:doc(), ctx()}.
get_storage_doc(FileCtx = #file_ctx{storage_doc = undefined}) ->
    SpaceId = get_space_id_const(FileCtx),
    {ok, StorageDoc} = fslogic_storage:select_storage(SpaceId),
    {StorageDoc, FileCtx#file_ctx{storage_doc = StorageDoc}};
get_storage_doc(FileCtx = #file_ctx{storage_doc = StorageDoc}) ->
    {StorageDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location for file.
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx()) ->
    {file_location:doc(), ctx()}.
get_local_file_location_doc(FileCtx = #file_ctx{local_file_location_doc = undefined}) ->
    FileEntry = get_uuid_entry_const(FileCtx),
    LocalLocation = fslogic_utils:get_local_file_location(FileEntry),
    {LocalLocation, FileCtx#file_ctx{local_file_location_doc = LocalLocation}};
get_local_file_location_doc(FileCtx = #file_ctx{local_file_location_doc = Doc}) ->
    {Doc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file location IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_ids(ctx()) ->
    {[file_location:id()], ctx()}.
get_file_location_ids(FileCtx = #file_ctx{location_ids = undefined}) ->
    {FileDoc, FileCtx2} = get_file_doc(FileCtx),
    {ok, Locations} = file_meta:get_locations(FileDoc),
    {Locations, FileCtx2#file_ctx{location_ids = Locations}};
get_file_location_ids(FileCtx = #file_ctx{location_ids = Locations}) ->
    {Locations, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(ctx()) -> {undefined | acl:acl(), ctx()}.
get_acl(FileCtx = #file_ctx{acl = undefined}) ->
    {uuid, FileUuid} = get_uuid_entry_const(FileCtx),
    Acl = acl:get(FileUuid),
    {Acl, FileCtx#file_ctx{acl = Acl}};
get_acl(FileCtx = #file_ctx{acl = Acl}) ->
    {Acl, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if given argument contains file context record.
%% @end
%%--------------------------------------------------------------------
-spec is_file_ctx_const(ctx() | term()) -> boolean().
is_file_ctx_const(#file_ctx{}) ->
    true;
is_file_ctx_const(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is a space root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir_const(ctx()) -> boolean().
is_space_dir_const(#file_ctx{guid = undefined, canonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, _SpaceId] ->
            true;
        _ ->
            false
    end;
is_space_dir_const(#file_ctx{guid = Guid}) ->
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(Guid))),
    is_binary(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir_const(ctx(), user_ctx:ctx()) -> boolean().
is_user_root_dir_const(#file_ctx{canonical_path = <<"/">>}, _Ctx) ->
    true;
is_user_root_dir_const(#file_ctx{guid = Guid, canonical_path = undefined}, Ctx) ->
    UserId = user_ctx:get_user_id(Ctx),
    UserRootDirUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    UserRootDirUuid == fslogic_uuid:guid_to_uuid(Guid);
is_user_root_dir_const(#file_ctx{}, _Ctx) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is a root dir (any user root).
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir_const(ctx()) -> boolean().
is_root_dir_const(#file_ctx{canonical_path = <<"/">>}) ->
    true;
is_root_dir_const(#file_ctx{guid = Guid, canonical_path = undefined}) ->
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    fslogic_uuid:is_root_dir(Uuid);
is_root_dir_const(#file_ctx{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has Access Control List defined.
%% @end
%%--------------------------------------------------------------------
-spec has_acl_const(ctx()) -> boolean().
has_acl_const(FileCtx = #file_ctx{acl = undefined}) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    acl:exists(FileUuid);
has_acl_const(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file exists.
%% @end
%%--------------------------------------------------------------------
-spec file_exists_const(ctx()) -> boolean().
file_exists_const(FileCtx = #file_ctx{file_doc = undefined}) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    file_meta:exists(FileUuid);
file_exists_const(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is a directory.
%% @end
%%--------------------------------------------------------------------
-spec is_dir(ctx()) -> {boolean(), ctx()}.
is_dir(FileCtx) ->
    {#document{value = #file_meta{type = Type}}, FileCtx2} = get_file_doc(FileCtx),
    {Type =:= ?DIRECTORY_TYPE, FileCtx2}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new file context using file's GUID, and file name.
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_uuid(file_meta:uuid(), file_meta:name(), od_space:id(), undefined | od_share:id()) -> ctx().
new_child_by_uuid(Uuid, Name, SpaceId, ShareId) ->
    #file_ctx{guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId), file_name = Name}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new file context using file's guid.
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_doc(file_meta:doc(), od_space:id(), undefined | od_share:id()) -> ctx().
new_child_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId, ShareId) ->
    Guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
    #file_ctx{file_doc = Doc, guid = Guid}.
