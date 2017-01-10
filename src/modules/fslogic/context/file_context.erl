%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque type storing informations about file.
%%% Once the record is created via new_* function and fslogic_worker has
%%% determined that the request can be handled locally - all of the functions
%%% in this module should work. If not - please report it.
%%% @end
%%%--------------------------------------------------------------------
-module(file_context).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% Record definition
-record(file_context, {
    cannonical_path :: undefined | path(),
    guid :: undefined | guid(),
    space_dir_doc :: undefined | file_meta:doc(),
    file_doc :: undefined | file_meta:doc() | {error, term()},
    parent :: undefined | ctx(),
    storage_file_id :: undefined | helpers:file_id(),
    space_name :: undefined | od_space:name() | od_space:alias(),
    storage_posix_user_context :: undefined | luma:posix_user_ctx(),
    times :: undefined | times:times(),
    file_name :: undefined | file_meta:name(),
    storage_doc :: undefined | space_storage:doc(),
    local_file_location_doc :: undefined | file_location:doc(),
    location_ids :: undefined | [file_location:id()]
}).

-type path() :: file_meta:path().
-type guid() :: fslogic_worker:file_guid().
-type ctx() :: #file_context{}.

%% API
-export([new_by_path/2, new_by_guid/1]).

-export([fill_guid/1]).

-export([get_share_id/1, get_path/1, get_space_id/1, get_space_dir_uuid/1,
    get_guid/1, get_file_doc/1, get_parent/2, get_storage_file_id/1,
    get_aliased_name/2, get_posix_storage_user_context/2, get_times/1, get_parent_guid/2,
    get_child/3, get_file_children/4, get_logical_path/2, get_uuid_entry/1,
    get_storage_doc/1, get_local_file_location_doc/1, get_file_location_ids/1]).

-export([is_file_context/1, is_space_dir/1, is_user_root_dir/2, is_root_dir/1, is_dir/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create new file_context using file's path
%% @end
%%--------------------------------------------------------------------
-spec new_by_path(user_context:ctx(), path()) -> ctx().
new_by_path(Ctx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case session:is_special(user_context:get_session_id(Ctx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context
of special session. You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    UserId = user_context:get_user_id(Ctx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(fslogic_uuid:user_root_dir_uuid(UserId)),
                    #file_context{cannonical_path = filename:join(Tokens), guid = UserRootDirGuid};
                [<<"/">>, SpaceName | Rest] ->
                    #document{value = #od_user{space_aliases = Spaces}} = user_context:get_user(Ctx),
                    case lists:keyfind(SpaceName, 2, Spaces) of
                        false ->
                            throw(?ENOENT);
                        {SpaceId, SpaceName} ->
                            #file_context{cannonical_path = filename:join([<<"/">>, SpaceId | Rest]), space_name = SpaceName}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create new file_context using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(guid()) -> ctx().
new_by_guid(Guid) when Guid =/= undefined ->
    #file_context{guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Fill guid field in file context record.
%% @end
%%--------------------------------------------------------------------
-spec fill_guid(ctx()) -> ctx().
fill_guid(FileCtx = #file_context{guid = undefined, cannonical_path = Path}) ->
    {ok, Uuid} = file_meta:to_uuid({path, Path}),
    SpaceId = get_space_id(FileCtx),
    Guid = fslogic_uuid:uuid_to_guid(Uuid, SpaceId),
    FileCtx#file_context{guid = Guid};
fill_guid(FileCtx) ->
    FileCtx.

%%--------------------------------------------------------------------
%% @doc
%% Get file's share_id.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(user_context:ctx()) -> od_share:id() | undefined.
get_share_id(#file_context{guid = undefined}) ->
    undefined;
get_share_id(#file_context{guid = Guid}) ->
    {_FileUuid, _SpaceId, ShareId} = fslogic_uuid:unpack_share_guid(Guid),
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Get file's cannonical path (starting with "/SpaceId/...."
%% @end
%%--------------------------------------------------------------------
-spec get_path(ctx()) -> {path(), ctx()}.
get_path(FileCtx = #file_context{cannonical_path = undefined}) ->
    case is_root_dir(FileCtx) of
        true ->
            {<<"/">>, FileCtx#file_context{cannonical_path = <<"/">>}};
        false ->
            Guid = get_guid(FileCtx),
            Uuid = fslogic_uuid:guid_to_uuid(Guid),
            LogicalPath = fslogic_uuid:uuid_to_path(?ROOT_SESS_ID, Uuid),
            {ok, [<<"/">>, _SpaceName | Rest]} = fslogic_path:tokenize_skipping_dots(LogicalPath),
            SpaceId = get_space_id(FileCtx),
            CanonicalPath = filename:join([<<"/">>, SpaceId | Rest]),
            {CanonicalPath, FileCtx#file_context{cannonical_path = CanonicalPath}}
    end;
get_path(#file_context{cannonical_path = Path}) ->
    Path.

%%--------------------------------------------------------------------
%% @doc
%% Get file's logical path (starting with "/SpaceName/...."
%% @end
%%--------------------------------------------------------------------
-spec get_logical_path(ctx(), user_context:ctx()) ->
    {file_meta:path(), ctx()}.
get_logical_path(FileCtx, Ctx) ->
    case get_path(FileCtx) of
        {<<"/">>, FileCtx2} ->
            {<<"/">>, FileCtx2};
        {Path, FileCtx2} ->
            {SpaceName, FileCtx3} = get_space_name(FileCtx2, Ctx),
            {ok, [<<"/">>, _SpaceId | Rest]} = fslogic_path:tokenize_skipping_dots(Path),
            LogicalPath = filename:join([<<"/">>, SpaceName | Rest]),
            {LogicalPath, FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceId
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(ctx()) -> od_space:id() | undefined.
get_space_id(#file_context{space_dir_doc = #document{key = SpaceUuid}}) ->
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid);
get_space_id(#file_context{guid = undefined, cannonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, SpaceId | _] ->
            SpaceId;
        _ ->
            undefined
    end;
get_space_id(#file_context{guid = Guid}) ->
    fslogic_uuid:guid_to_space_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceDir uuid
%% @end
%%--------------------------------------------------------------------
-spec get_space_dir_uuid(ctx()) -> file_meta:uuid().
get_space_dir_uuid(#file_context{space_dir_doc = #document{key = SpaceUuid}}) ->
    SpaceUuid;
get_space_dir_uuid(FileCtx) ->
    fslogic_uuid:spaceid_to_space_dir_uuid(get_space_id(FileCtx)).

%%--------------------------------------------------------------------
%% @doc
%% Get file's guid
%% @end
%%--------------------------------------------------------------------
-spec get_guid(ctx()) -> fslogic_worker:file_guid().
get_guid(#file_context{guid = Guid}) ->
    Guid.

%%--------------------------------------------------------------------
%% @doc
%% Get file's file_meta document.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc(ctx()) -> {file_meta:doc() | {error, term()}, ctx()}.
get_file_doc(FileCtx = #file_context{file_doc = undefined}) ->
    Guid = get_guid(FileCtx),
    case file_meta:get({uuid, fslogic_uuid:guid_to_uuid(Guid)}) of
        {ok, FileDoc} ->
            {FileDoc, FileCtx#file_context{file_doc = FileDoc}};
        Error ->
            {Error, FileCtx#file_context{file_doc = Error}}
    end;
get_file_doc(FileCtx = #file_context{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get parent's file_context.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(ctx(), undefined | od_user:id()) -> {ParentFileCtx :: ctx(), NewFileCtx :: ctx()}.
get_parent(FileCtx = #file_context{parent = undefined}, UserId) ->
    {Doc, FileCtx2} = file_context:get_file_doc(FileCtx),
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
    ParentGuid =
        case fslogic_uuid:is_root_dir(ParentUuid) of
            true ->
                case ParentUuid =:= ?ROOT_DIR_UUID andalso UserId =/= undefined of
                    true ->
                        fslogic_uuid:user_root_dir_guid(fslogic_uuid:user_root_dir_uuid(UserId));
                    _ ->
                        fslogic_uuid:uuid_to_guid(ParentUuid, undefined)
                end;
            false ->
                SpaceId = file_context:get_space_id(FileCtx2),
                case get_share_id(FileCtx2) of
                    undefined ->
                        fslogic_uuid:uuid_to_guid(ParentUuid, SpaceId);
                    ShareId ->
                        fslogic_uuid:uuid_to_share_guid(ParentUuid, SpaceId, ShareId)
                end
        end,
    Parent = file_context:new_by_guid(ParentGuid),
    {Parent, FileCtx2#file_context{parent = Parent}};
get_parent(FileCtx = #file_context{parent = Parent}, _UserId) ->
    {Parent, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get Guid of parent, returns undefined when the file is a root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid(ctx(), undefined | od_user:id()) -> {fslogic_worker:file_guid(), ctx()}.
get_parent_guid(FileCtx, UserId) ->
    case file_context:is_root_dir(FileCtx) of
        true ->
            {undefined, FileCtx};
        false ->
            {ParentFile, NewFile} = file_context:get_parent(FileCtx, UserId),
            ParentGuid = file_context:get_guid(ParentFile),
            {ParentGuid, NewFile}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get storage file id (the id of file on storage. In case of posix it is its path on storage)
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(ctx()) -> {StorageFileId :: helpers:file(), ctx()}.
get_storage_file_id(FileCtx) ->
    FileEntry = get_uuid_entry(FileCtx),
    FileId = fslogic_utils:gen_storage_file_id(FileEntry), %todo TL do not use this util function, as it it overcomplicated
    {FileId, FileCtx#file_context{storage_file_id = FileId}}.

%%--------------------------------------------------------------------
%% @doc
%% Get name (or user alias) of the space where the file is located
%% @end
%%--------------------------------------------------------------------
-spec get_space_name(ctx(), user_context:ctx()) ->
    {od_space:name() | od_space:alias(), ctx()}.
get_space_name(FileCtx = #file_context{space_name = undefined}, Ctx) ->
    SpaceId = get_space_id(FileCtx),
    #document{value = #od_user{space_aliases = Spaces}} = user_context:get_user(Ctx),

    case lists:keyfind(SpaceId, 1, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, SpaceName} ->
            {SpaceName, FileCtx#file_context{space_name = SpaceName}}
    end;
get_space_name(FileCtx = #file_context{space_name = SpaceName}, _Ctx) ->
    {SpaceName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get name of file (if the file represents space dir, returns user's space alias)
%% @end
%%--------------------------------------------------------------------
-spec get_aliased_name(ctx(), user_context:ctx()) ->
    {file_meta:name(), ctx()}.
get_aliased_name(FileCtx = #file_context{file_name = undefined}, Ctx) ->
    SessionIsNotSpecial = (not session:is_special(user_context:get_session_id(Ctx))),
    case is_space_dir(FileCtx) andalso SessionIsNotSpecial of
        false ->
            case get_file_doc(FileCtx) of
                {#document{value = #file_meta{name = Name}}, FileCtx2} ->
                    {Name, FileCtx2#file_context{file_name = Name}};
                ErrorResponse ->
                    ErrorResponse
            end;
        true ->
            {Name, FileCtx2} = get_space_name(FileCtx, Ctx),
            {Name, FileCtx2#file_context{file_name = Name}}
    end;
get_aliased_name(FileCtx = #file_context{file_name = FileName}, _Ctx) ->
    {FileName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get posix storage user context, holding uid and gid of file on posix storage.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_storage_user_context(ctx(), user_context:ctx()) ->
    {luma:posix_user_ctx(), ctx()}.
get_posix_storage_user_context(FileCtx, UserId) ->
    IsSpaceDir = is_space_dir(FileCtx),
    IsUserRootDir = is_root_dir(FileCtx),
    SpaceId = get_space_id(FileCtx),
    UserCtx = case IsSpaceDir orelse IsUserRootDir of
        true -> luma:get_posix_user_ctx(?ROOT_USER_ID, SpaceId);
        false -> luma:get_posix_user_ctx(UserId, SpaceId)
    end,
    {UserCtx, FileCtx#file_context{storage_posix_user_context = UserCtx}}.

%%--------------------------------------------------------------------
%% @doc
%% Get file atime, ctime and mtime
%% @end
%%--------------------------------------------------------------------
-spec get_times(ctx()) -> {times:times() | {error, term()}, ctx()}.
get_times(FileCtx) ->
    {uuid, FileUuid} = file_context:get_uuid_entry(FileCtx),
    case times:get_or_default(FileUuid) of
        {ok, Times} ->
            {Times, FileCtx#file_context{times = Times}};
        Error ->
            {Error, FileCtx}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get child of the file
%% @end
%%--------------------------------------------------------------------
-spec get_child(ctx(), file_meta:name(), od_user:id()) ->
    {ChildFile :: ctx(), NewFile :: ctx()}.
get_child(FileCtx, Name, UserId) ->
    case is_root_dir(FileCtx) of
        true ->
            {ok, #document{value = #od_user{space_aliases = Spaces}}} = od_user:get(UserId),
            case lists:keyfind(Name, 2, Spaces) of
                {SpaceId, _} ->
                    Child = file_context:new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
                    {Child, FileCtx};
                false -> throw(?ENOENT)
            end;
        _ ->
            SpaceId = file_context:get_space_id(FileCtx),
            {FileDoc, FileCtx2} = file_context:get_file_doc(FileCtx),
            case file_meta:resolve_path(FileDoc, <<"/", Name/binary>>) of
                {ok, {ChildDoc, _}} ->
                    ShareId = get_share_id(FileCtx2),
                    Child = new_child_by_doc(ChildDoc, SpaceId, ShareId),
                    {Child, FileCtx2};
                {error, {not_found, _}} ->
                    throw(?ENOENT)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get list of file children.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_context:ctx(), Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    {Children :: [ctx()] | {error, term()}, NewFileCtx :: ctx()}.
get_file_children(FileCtx, Ctx, Offset, Limit) ->
    case is_user_root_dir(FileCtx, Ctx) of
        true ->
            #document{value = #od_user{space_aliases = Spaces}} = user_context:get_user(Ctx),

            Children =
                case Offset < length(Spaces) of
                    true ->
                        SpacesChunk = lists:sublist(Spaces, Offset + 1, Limit),
                        lists:map(fun({SpaceId, SpaceName}) ->
                            SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                            new_child_by_uuid(SpaceDirUuid, SpaceName, SpaceId, undefined)
                        end, SpacesChunk);
                    false ->
                        []
                end,
            {Children, FileCtx};
        false ->
            {FileDoc = #document{}, FileCtx2} = get_file_doc(FileCtx),
            case file_meta:list_children(FileDoc, Offset, Limit) of
                {ok, ChildrenLinks} ->
                    SpaceId = get_space_id(FileCtx2),
                    ShareId = get_share_id(FileCtx2),
                    Children =
                        lists:map(fun(#child_link{name = Name, uuid = Uuid}) ->
                            new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
                        end, ChildrenLinks),
                    {Children, FileCtx2};
                Error ->
                    {Error, FileCtx2}
            end
    end.

%%--------------------------------------------------------------------
%% @todo remove this function and pass file info wherever possible
%% @doc
%% Get file uuid entry
%% @end
%%--------------------------------------------------------------------
-spec get_uuid_entry(ctx()) -> {uuid, file_meta:uuid()}.
get_uuid_entry(FileCtx) ->
    Guid = get_guid(FileCtx),
    {uuid, fslogic_uuid:guid_to_uuid(Guid)}.

%%--------------------------------------------------------------------
%% @doc
%% Get storage document fo file's space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_doc(ctx()) -> {space_storage:doc(), ctx()}.
get_storage_doc(FileCtx = #file_context{storage_doc = undefined}) ->
    SpaceId = get_space_id(FileCtx),
    {ok, StorageDoc} = fslogic_storage:select_storage(SpaceId),
    {StorageDoc, FileCtx#file_context{storage_doc = StorageDoc}};
get_storage_doc(FileCtx = #file_context{storage_doc = StorageDoc}) ->
    {StorageDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get local file location for file
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx()) ->
    {file_location:doc(), ctx()}.
get_local_file_location_doc(FileCtx = #file_context{local_file_location_doc = undefined}) ->
    FileEntry = get_uuid_entry(FileCtx),
    LocalLocation = fslogic_utils:get_local_file_location(FileEntry),
    {LocalLocation, FileCtx#file_context{local_file_location_doc = LocalLocation}};
get_local_file_location_doc(FileCtx = #file_context{local_file_location_doc = Doc}) ->
    {Doc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Get file location ids
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_ids(ctx()) ->
    {[file_location:id()], ctx()}.
get_file_location_ids(FileCtx = #file_context{location_ids = undefined}) ->
    {FileDoc, FileCtx2} = get_file_doc(FileCtx),
    {ok, Locations} = file_meta:get_locations(FileDoc),
    {Locations, FileCtx2#file_context{location_ids = Locations}};
get_file_location_ids(FileCtx = #file_context{location_ids = Locations}) ->
    {Locations, FileCtx}.



%%--------------------------------------------------------------------
%% @doc
%% Check if given argument contains file_context record
%% @end
%%--------------------------------------------------------------------
-spec is_file_context(ctx() | term()) -> boolean().
is_file_context(#file_context{}) ->
    true;
is_file_context(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a space root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir(ctx()) -> boolean().
is_space_dir(#file_context{guid = undefined, cannonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, _SpaceId] ->
            true;
        _ ->
            false
    end;
is_space_dir(#file_context{guid = Guid}) ->
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(Guid))),
    is_binary(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Check if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir(ctx(), user_context:ctx()) -> boolean().
is_user_root_dir(#file_context{cannonical_path = <<"/">>}, _Ctx) ->
    true;
is_user_root_dir(#file_context{guid = Guid, cannonical_path = undefined}, Ctx) ->
    UserId = user_context:get_user_id(Ctx),
    UserRootDirUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    UserRootDirUuid == fslogic_uuid:guid_to_uuid(Guid);
is_user_root_dir(#file_context{}, _Ctx) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a root dir (any user root).
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir(ctx()) -> boolean().
is_root_dir(#file_context{cannonical_path = <<"/">>}) ->
    true;
is_root_dir(#file_context{guid = Guid, cannonical_path = undefined}) ->
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    fslogic_uuid:is_root_dir(Uuid);
is_root_dir(#file_context{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a root dir (any user root).
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
%% Create new file_context using file's guid, and file name
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_uuid(file_meta:uuid(), file_meta:name(), od_space:id(), undefined | od_share:id()) -> ctx().
new_child_by_uuid(Uuid, Name, SpaceId, ShareId) ->
    #file_context{guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId), file_name = Name}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create new file_context using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_doc(file_meta:doc(), od_space:id(), undefined | od_share:id()) -> ctx().
new_child_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId, ShareId) ->
    Guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
    #file_context{file_doc = Doc, guid = Guid}.
