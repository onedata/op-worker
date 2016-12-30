%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque type storing informations about file.
%%% @end
%%%--------------------------------------------------------------------
-module(file_info).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% Record definition
-record(file_info, {
    path :: undefined | path(),
    guid :: undefined | guid(),
    space_dir_doc :: undefined | file_meta:doc(),
    file_doc :: undefined | file_meta:doc() | {error, term()},
    parent :: undefined | file_info(),
    storage_file_id :: undefined | helpers:file(),
    space_name :: od_space:name() | od_space:alias(),
    storage_posix_user_context :: undefined | #posix_user_ctx{},
    times :: undefined | times:time(),
    file_name :: file_meta:name()
}).

-type path() :: file_meta:path().
-type guid() :: fslogic_worker:file_guid().
-type file_info() :: #file_info{}.

%% API
-export([new_by_path/2, new_by_guid/1]).
-export([get_share_id/1, get_path/1, get_space_id/1, get_space_dir_uuid/1,
    get_guid/1, get_file_doc/1, get_parent/2, get_storage_file_id/1,
    get_aliased_name/2, get_storage_user_context/2, get_times/1, get_parent_guid/2,
    get_child/3, get_file_children/4]).
-export([is_file_info/1, is_space_dir/1, is_user_root_dir/2, is_root_dir/1, is_dir/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create new file_info using file's path
%% @end
%%--------------------------------------------------------------------
-spec new_by_path(fslogic_context:ctx(), path()) -> {NewCtx :: fslogic_context:ctx(), file_info()}.
new_by_path(Ctx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case session:is_special(fslogic_context:get_session_id(Ctx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context
of special session. You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    {UserRootDirUuid, NewCtx} = fslogic_context:get_user_root_dir_uuid(Ctx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserRootDirUuid),
                    {NewCtx, #file_info{path = filename:join(Tokens), guid = UserRootDirGuid}};
                [<<"/">>, SpaceName | Rest] ->
                    {#document{value = #od_user{space_aliases = Spaces}}, Ctx2} =
                        fslogic_context:get_user(Ctx),

                    case lists:keyfind(SpaceName, 2, Spaces) of
                        false ->
                            throw(?ENOENT);
                        {SpaceId, SpaceName} ->
                            {Ctx2, #file_info{path = filename:join([<<"/">>, SpaceId | Rest]), space_name = SpaceName}}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create new file_info using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(guid()) -> file_info().
new_by_guid(Guid) when Guid =/= undefined ->
    #file_info{guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Get file's share_id.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(fslogic_context:ctx()) -> od_share:id() | undefined.
get_share_id(#file_info{guid = undefined}) ->
    undefined;
get_share_id(#file_info{guid = Guid}) ->
    case fslogic_uuid:unpack_share_guid(Guid) of
        {_FileUuid, undefined, ShareId} ->
            ShareId;
        {_, _SpaceId, ShareId} ->
            ShareId
    end;
get_share_id(Ctx) -> %todo TL return share_id cached in file_info
    fslogic_context:get_share_id(Ctx).

%%--------------------------------------------------------------------
%% @doc
%% Get file's cannonical path (starting with "/SpaceId/....
%% @end
%%--------------------------------------------------------------------
-spec get_path(file_info()) -> path().
get_path(#file_info{path = Path}) ->
    Path.

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceId
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(file_info()) -> od_space:id().
get_space_id(#file_info{space_dir_doc = #document{key = SpaceUuid}}) ->
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid);
get_space_id(#file_info{guid = undefined, path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, SpaceId | _] ->
            SpaceId;
        _ ->
            undefined
    end;
get_space_id(#file_info{guid = Guid}) ->
    fslogic_uuid:guid_to_space_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceDir uuid
%% @end
%%--------------------------------------------------------------------
-spec get_space_dir_uuid(file_info()) -> file_meta:uuid().
get_space_dir_uuid(#file_info{space_dir_doc = #document{key = SpaceUuid}}) ->
    SpaceUuid;
get_space_dir_uuid(FileInfo) ->
    fslogic_uuid:spaceid_to_space_dir_uuid(get_space_id(FileInfo)).

%%--------------------------------------------------------------------
%% @doc
%% Get file's guid
%% @end
%%--------------------------------------------------------------------
-spec get_guid(file_info()) -> {fslogic_worker:file_guid(), file_info()}.
get_guid(FileInfo = #file_info{guid = undefined, path = Path}) ->
    {ok, Uuid} = file_meta:to_uuid({path, Path}),
    SpaceId = get_space_id(FileInfo),
    Guid = fslogic_uuid:uuid_to_guid(Uuid, SpaceId),
    {Guid, FileInfo#file_info{guid = Guid}};
get_guid(FileInfo = #file_info{guid = Guid}) ->
    {Guid, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get file's file_meta document.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc(file_info()) -> {file_meta:doc() | {error, term()}, file_info()}.
get_file_doc(FileInfo = #file_info{file_doc = undefined}) ->
    {Guid, NewFileInfo} = get_guid(FileInfo),
    case file_meta:get({uuid, fslogic_uuid:guid_to_uuid(Guid)}) of
        {ok, FileDoc} ->
            {FileDoc, NewFileInfo#file_info{file_doc = FileDoc}};
        Error ->
            {Error, NewFileInfo#file_info{file_doc = Error}}
    end;
get_file_doc(FileInfo = #file_info{file_doc = FileDoc}) ->
    {FileDoc, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get parent's file_info.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(file_info(), undefined | od_user:id()) -> {ParentFileInfo :: file_info(), NewFileInfo :: file_info()}.
get_parent(FileInfo = #file_info{parent = undefined}, UserId) ->
    {Doc, NewFileInfo} = file_info:get_file_doc(FileInfo),
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
                SpaceId = file_info:get_space_id(NewFileInfo),
                fslogic_uuid:uuid_to_guid(ParentUuid, SpaceId)
        end,
    Parent = file_info:new_by_guid(ParentGuid),
    {Parent, NewFileInfo#file_info{parent = Parent}};
get_parent(FileInfo = #file_info{parent = Parent}, _UserId) ->
    {Parent, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get Guid of parent, returns undefined when the file is a root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid(file_info(), undefined | od_user:id()) -> {fslogic_worker:file_guid(), file_info()}.
get_parent_guid(FileInfo, UserId) ->
    case file_info:is_root_dir(FileInfo) of
        true ->
            {undefined, FileInfo};
        false ->
            {ParentFile, NewFile} = file_info:get_parent(FileInfo, UserId),
            {ParentGuid, _} = file_info:get_guid(ParentFile),
            {ParentGuid, NewFile}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get storage file id (the id of file on storage. In case of posix it is its path on storage)
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(file_info()) -> {StorageFileId :: helpers:file(), file_info()}.
get_storage_file_id(FileInfo) ->
    {Guid, NewFileInfo} = get_guid(FileInfo),
    FileId = fslogic_utils:gen_storage_file_id({uuid, fslogic_uuid:guid_to_uuid(Guid)}), %todo TL do not use this util function, as it it overcomplicated
    {FileId, NewFileInfo#file_info{storage_file_id = FileId}}.

%%--------------------------------------------------------------------
%% @doc
%% Get name (or user alias) of the space where the file is located
%% @end
%%--------------------------------------------------------------------
-spec get_space_name(file_info(), fslogic_context:ctx()) ->
    {od_space:name() | od_space:alias(), fslogic_context:ctx(), file_info()}.
get_space_name(FileInfo = #file_info{space_name = undefined}, Ctx) ->
    SpaceId = get_space_id(FileInfo),
    {#document{value = #od_user{space_aliases = Spaces}}, Ctx2} =
        fslogic_context:get_user(Ctx),

    case lists:keyfind(SpaceId, 1, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, SpaceName} ->
            {SpaceName, Ctx2, FileInfo#file_info{space_name = SpaceName}}
    end;
get_space_name(FileInfo = #file_info{space_name = SpaceName}, Ctx) ->
    {SpaceName, Ctx, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get name of file (if the file represents space dir, returns user's space alias)
%% @end
%%--------------------------------------------------------------------
-spec get_aliased_name(file_info(), fslogic_context:ctx()) ->
    {file_meta:name(), fslogic_context:ctx(), file_info()}.
get_aliased_name(FileInfo = #file_info{file_name = undefined}, Ctx) ->
    SessionIsNotSpecial = (not session:is_special(fslogic_context:get_session_id(Ctx))),
    case is_space_dir(FileInfo) andalso SessionIsNotSpecial of
        false ->
            case get_file_doc(FileInfo) of
                {#document{value = #file_meta{name = Name}}, FileInfo2} ->
                    {Name, Ctx, FileInfo2#file_info{file_name = Name}};
                ErrorResponse ->
                    ErrorResponse
            end;
        true ->
            {Name, Ctx2, FileInfo2} = get_space_name(FileInfo, Ctx),
            {Name, Ctx2, FileInfo2#file_info{file_name = Name}}
    end;
get_aliased_name(FileInfo = #file_info{file_name = FileName}, Ctx) ->
    {FileName, Ctx, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get posix storage user context, holding uid and gid of file on posix storage.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_user_context(file_info(), fslogic_context:ctx()) ->
    {#posix_user_ctx{}, fslogic_context:ctx(), file_info()}.
get_storage_user_context(FileInfo, Ctx) ->
    IsSpaceDir = is_space_dir(FileInfo),
    IsUserRootDir = is_root_dir(FileInfo),
    case IsSpaceDir orelse IsUserRootDir of
        true ->
            {?ROOT_POSIX_CTX, FileInfo#file_info{storage_posix_user_context = ?ROOT_POSIX_CTX}};
        false ->
            SpaceDirUuid = get_space_dir_uuid(FileInfo),
            StorageId = luma_utils:get_storage_id(SpaceDirUuid), %todo use SpaceId in luma instead of SpaceDirUuid
            StorageType = luma_utils:get_storage_type(StorageId),
            SessId = fslogic_context:get_session_id(Ctx),
            StorageContext = fslogic_storage:get_posix_user_ctx(StorageType, SessId, SpaceDirUuid),
            {StorageContext, FileInfo#file_info{storage_posix_user_context = StorageContext}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get file atime, ctime and mtime
%% @end
%%--------------------------------------------------------------------
-spec get_times(file_info()) -> {times:time(), file_info()}.
get_times(FileInfo) ->
    {Guid, FileInfo2} = file_info:get_guid(FileInfo),
    case times:get_or_default(fslogic_uuid:guid_to_uuid(Guid)) of
        {ok, Times} ->
            {Times, FileInfo2#file_info{times = Times}};
        Error ->
            {Error, FileInfo2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get child of the file
%% @end
%%--------------------------------------------------------------------
-spec get_child(file_info(), file_meta:name(), od_user:id()) ->
    {ChildFile :: file_info(), NewFile :: file_info()}.
get_child(FileInfo, Name, UserId) ->
    case is_root_dir(FileInfo) of
        true ->
            {ok, #document{value = #od_user{space_aliases = Spaces}}} = od_user:get(UserId),
            case lists:keyfind(Name, 2, Spaces) of
                {SpaceId, _} ->
                    Child = file_info:new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
                    {Child, FileInfo};
                false -> throw(?ENOENT)
            end;
        _ ->
            SpaceId = file_info:get_space_id(FileInfo),
            {FileDoc, FileInfo2} = file_info:get_file_doc(FileInfo),
            case file_meta:resolve_path(FileDoc, <<"/", Name/binary>>) of
                {ok, {ChildDoc, _}} ->
                    ShareId = get_share_id(FileInfo2),
                    Child = new_child_by_doc(ChildDoc, SpaceId, ShareId),
                    {Child, FileInfo2};
                {error, {not_found, _}} ->
                    throw(?ENOENT)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get list of file children.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(file_info(), fslogic_context:ctx(), Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    {Children :: [file_info()], NewFileInfo :: file_info()}.
get_file_children(FileInfo, Ctx, Offset, Limit) ->
    case is_user_root_dir(FileInfo, Ctx) of
        {true, Ctx2} ->
            {#document{value = #od_user{space_aliases = Spaces}}, Ctx3} =
                fslogic_context:get_user(Ctx2),

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
            {Children, Ctx3, FileInfo};
        {false, Ctx2} ->
            {FileDoc = #document{}, FileInfo2} = get_file_doc(FileInfo),
            case file_meta:list_children(FileDoc, Offset, Limit) of
                {ok, ChildrenLinks} ->
                    SpaceId = get_space_id(FileInfo2),
                    ShareId = get_share_id(FileInfo2),
                    Children =
                        lists:map(fun(#child_link{name = Name, uuid = Uuid}) ->
                            new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
                        end, ChildrenLinks),
                    {Children, Ctx2, FileInfo2};
                Error ->
                    {Error, Ctx2, FileInfo2}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if given argument contains file_info record
%% @end
%%--------------------------------------------------------------------
-spec is_file_info(file_info()) -> boolean().
is_file_info(#file_info{}) ->
    true;
is_file_info(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a space root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir(file_info()) -> boolean().
is_space_dir(#file_info{guid = undefined, path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, _SpaceId] ->
            true;
        _ ->
            false
    end;
is_space_dir(#file_info{guid = Guid})->
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(Guid))),
    is_binary(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Check if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir(file_info(), fslogic_context:ctx()) -> {boolean(), NewCtx :: fslogic_context:ctx()}.
is_user_root_dir(#file_info{path = <<"/">>}, Ctx) ->
    {true, Ctx};
is_user_root_dir(#file_info{guid = Guid, path = undefined}, Ctx) ->
    {UserRootDirUuid, NewCtx} = fslogic_context:get_user_root_dir_uuid(Ctx),
    {UserRootDirUuid == fslogic_uuid:guid_to_uuid(Guid), NewCtx};
is_user_root_dir(#file_info{}, Ctx) ->
    {false, Ctx}.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a root dir (any user root).
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir(file_info()) -> boolean().
is_root_dir(#file_info{path = <<"/">>}) ->
    true;
is_root_dir(#file_info{guid = Guid, path = undefined}) ->
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    fslogic_uuid:is_root_dir(Uuid);
is_root_dir(#file_info{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a root dir (any user root).
%% @end
%%--------------------------------------------------------------------
-spec is_dir(file_info()) -> boolean().
is_dir(FileInfo) ->
    {#document{value = #file_meta{type = Type}}, FileInfo2} = get_file_doc(FileInfo),
    {Type =:= ?DIRECTORY_TYPE, FileInfo2}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create new file_info using file's guid, and file name
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_uuid(file_meta:uuid(), file_meta:name(), od_space:id(), undefined | od_share:id()) -> file_info().
new_child_by_uuid(Uuid, Name, SpaceId, ShareId) ->
    #file_info{guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId), file_name = Name}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create new file_info using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_child_by_doc(file_meta:doc(), od_space:id(), undefined | od_share:id()) -> file_info().
new_child_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId, ShareId) ->
    Guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
    #file_info{file_doc = Doc, guid = Guid}.
