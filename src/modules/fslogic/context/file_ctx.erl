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
%%% If effort of computing something is significant,
%%% the value is cached and the further calls will use it. Therefore some of the
%%% functions (those without '_const' suffix) return updated version of context
%%% together with the result.
%%%
%%% The context can be created using:
%%% - logical path (/UserSpaceName/...), and user context
%%% - canonical path (/SpaceId/...)
%%% - Guid
%%% - FileDoc, SpaceId, ShareId (can be undefined if file is not in a share context)
%%% - file_partial_ctx
%%% @end
%%%--------------------------------------------------------------------
-module(file_ctx).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-record(file_ctx, {
    canonical_path :: undefined | file_meta:path(),
    uuid_based_path :: undefined | file_meta:uuid_based_path(),
    guid :: fslogic_worker:file_guid(),
    file_doc :: undefined | file_meta:doc(),
    parent :: undefined | ctx(),
    storage_file_id :: undefined | helpers:file_id(),
    space_name :: undefined | od_space:name(),
    display_credentials :: undefined | luma:display_credentials(),
    times :: undefined | times:times(),
    file_name :: undefined | file_meta:name(),
    storage :: undefined | storage:data(),
    file_location_ids :: undefined | [file_location:id()],
    is_dir :: undefined | boolean(),
    is_imported_storage :: undefined | boolean()
}).

-type ctx() :: #file_ctx{}.
-export_type([ctx/0]).

%% Functions creating context and filling its data
-export([new_by_canonical_path/2, new_by_guid/1, new_by_doc/3, new_root_ctx/0]).
-export([reset/1, new_by_partial_context/1, add_file_location/2, set_file_id/2,
    set_is_dir/2]).

%% Functions that do not modify context
-export([get_share_id_const/1, get_space_id_const/1, get_space_dir_uuid_const/1,
    get_guid_const/1, get_uuid_const/1, get_dir_location_doc_const/1
]).
-export([is_file_ctx_const/1, is_space_dir_const/1, is_user_root_dir_const/2,
    is_root_dir_const/1, file_exists_const/1, file_exists_or_is_deleted/1,
    is_in_user_space_const/2]).
-export([equals/2]).
-export([assert_not_readonly_target_storage_const/2]).

%% Functions that do not modify context but does not have _const suffix and return context.
% TODO VFS-6119 missing _const suffix in function name
-export([get_local_file_location_doc/1, get_local_file_location_doc/2]).

%% Functions modifying context
-export([get_canonical_path/1, get_canonical_path_tokens/1, get_uuid_based_path/1, get_file_doc/1,
    get_file_doc_including_deleted/1, get_parent/2, get_and_check_parent/2,
    get_storage_file_id/1, get_storage_file_id/2,
    get_new_storage_file_id/1, get_aliased_name/2,
    get_display_credentials/1, get_times/1,
    get_parent_guid/2, get_child/3,
    get_file_children/4, get_file_children/5, get_file_children/6, get_file_children_whitelisted/5,
    get_logical_path/2,
    get_storage_id/1, get_storage/1, get_file_location_with_filled_gaps/1,
    get_file_location_with_filled_gaps/2, fill_location_gaps/4, fill_location_gaps/5,
    get_or_create_local_file_location_doc/1, get_or_create_local_file_location_doc/2,
    get_or_create_local_regular_file_location_doc/3,
    get_file_location_ids/1, get_file_location_docs/1, get_file_location_docs/2,
    get_active_perms_type/2, get_acl/1, get_mode/1, get_child_canonical_path/2, get_file_size/1,
    get_replication_status_and_size/1, get_file_size_from_remote_locations/1, get_owner/1,
    get_local_storage_file_size/1, get_and_cache_file_doc_including_deleted/1]).
-export([is_dir/1, is_imported_storage/1, is_storage_file_created/1, is_readonly_storage/1]).
-export([assert_not_readonly_storage/1, assert_file_exists/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new root directory file context.
%% @end
%%--------------------------------------------------------------------
-spec new_root_ctx() -> ctx().
new_root_ctx() ->
    new_by_guid(fslogic_uuid:root_dir_guid()).

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file canonical path.
%% @end
%%--------------------------------------------------------------------
-spec new_by_canonical_path(user_ctx:ctx(), file_meta:path()) -> ctx().
new_by_canonical_path(UserCtx, Path) ->
    {FileCtx, _} = new_by_partial_context(
        file_partial_ctx:new_by_canonical_path(UserCtx, Path)),
    FileCtx.

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(fslogic_worker:file_guid()) -> ctx().
new_by_guid(Guid) ->
    #file_ctx{guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec new_by_doc(file_meta:doc(), od_space:id(), undefined | od_share:id()) -> ctx().
new_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId, ShareId) ->
    Guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
    #file_ctx{file_doc = Doc, guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Converts partial file context into file context. Which means that the function
%% fills GUID in file context record. This function is called when we know
%% that the file is locally supported.
%% @end
%%--------------------------------------------------------------------
-spec new_by_partial_context(file_partial_ctx:ctx() | ctx()) ->
    {ctx(), od_space:id() | undefined}.
new_by_partial_context(FileCtx = #file_ctx{}) ->
    {FileCtx, get_space_id_const(FileCtx)};
new_by_partial_context(FilePartialCtx) ->
    {CanonicalPath, FilePartialCtx2} = file_partial_ctx:get_canonical_path(FilePartialCtx),
    {ok, FileDoc} = fslogic_path:resolve(CanonicalPath),
    SpaceId = file_partial_ctx:get_space_id_const(FilePartialCtx2),
    {new_by_doc(FileDoc, SpaceId, undefined), SpaceId}.

%%--------------------------------------------------------------------
%% @doc
%% Resets all cached data besides GUID.
%% @end
%%--------------------------------------------------------------------
-spec reset(ctx()) -> ctx().
reset(FileCtx) ->
    new_by_guid(get_guid_const(FileCtx)).

%%--------------------------------------------------------------------
%% @doc
%% Adds file location to context record
%% @end
%%--------------------------------------------------------------------
-spec add_file_location(ctx(), file_location:id()) -> ctx().
add_file_location(FileCtx = #file_ctx{file_location_ids = undefined}, _LocationId) ->
    FileCtx;
add_file_location(FileCtx = #file_ctx{file_location_ids = Locations}, LocationId) ->
    FileCtx#file_ctx{
        file_location_ids = [LocationId | Locations]
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets file_id in context record
%% @end
%%--------------------------------------------------------------------
-spec set_file_id(ctx(), helpers:file_id()) -> ctx().
set_file_id(FileCtx, FileId) ->
    FileCtx#file_ctx{storage_file_id = FileId}.

%%--------------------------------------------------------------------
%% @doc
%% Sets is_dir flag in context record
%% @end
%%--------------------------------------------------------------------
-spec set_is_dir(ctx(), boolean()) -> ctx().
set_is_dir(FileCtx, IsDir) ->
    FileCtx#file_ctx{is_dir = IsDir}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's share ID.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id_const(ctx()) -> od_share:id() | undefined.
get_share_id_const(#file_ctx{guid = Guid}) ->
    {_FileUuid, _SpaceId, ShareId} = file_id:unpack_share_guid(Guid),
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id().
get_space_id_const(#file_ctx{guid = Guid}) ->
    file_id:guid_to_space_id(Guid).

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
%% @todo remove this function and pass file_ctx wherever possible
%% @doc
%% Returns file UUID entry.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid_const(ctx()) -> file_meta:uuid().
get_uuid_const(FileCtx) ->
    Guid = get_guid_const(FileCtx),
    file_id:guid_to_uuid(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's canonical path (starting with "/SpaceId/...").
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_path(ctx()) -> {file_meta:path(), ctx()}.
get_canonical_path(FileCtx = #file_ctx{canonical_path = undefined}) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {<<"/">>, FileCtx#file_ctx{canonical_path = <<"/">>}};
        false ->
            {Path, FileCtx2} = resolve_canonical_path_tokens(FileCtx),
            CanonicalPath = filename:join(Path),
            {CanonicalPath, FileCtx2#file_ctx{canonical_path = CanonicalPath}}
    end;
get_canonical_path(FileCtx = #file_ctx{canonical_path = Path}) ->
    {Path, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's canonical path tokens (starting with "/", "SpaceId/", ...).
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_path_tokens(ctx()) -> {[file_meta:name()], ctx()}.
get_canonical_path_tokens(FileCtx = #file_ctx{canonical_path = undefined}) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {[<<"/">>], FileCtx#file_ctx{canonical_path = <<"/">>}};
        false ->
            {CanonicalPathTokens, FileCtx2} = resolve_canonical_path_tokens(FileCtx),
            CanonicalPath = filename:join(CanonicalPathTokens),
            {CanonicalPathTokens,
                FileCtx2#file_ctx{canonical_path = CanonicalPath}}
    end;
get_canonical_path_tokens(FileCtx = #file_ctx{canonical_path = Path}) ->
    {fslogic_path:split(Path), FileCtx}.


-spec get_uuid_based_path(ctx()) -> {file_meta:uuid_based_path(), ctx()}.
get_uuid_based_path(FileCtx = #file_ctx{uuid_based_path = undefined}) ->
    {UuidPathTokens, FileCtx2} = resolve_uuid_based_path_tokens(FileCtx),
    UuidPath = filename:join(UuidPathTokens),
    {UuidPath, FileCtx2#file_ctx{uuid_based_path = UuidPath}};
get_uuid_based_path(FileCtx = #file_ctx{uuid_based_path = UuidPath}) ->
    {UuidPath, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's logical path (starting with "/SpaceName/...").
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
            {ok, [<<"/">>, _SpaceId | Rest]} = fslogic_path:split_skipping_dots(Path),
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
    {ok, FileDoc} = file_meta:get({uuid, file_id:guid_to_uuid(Guid)}),
    {FileDoc, FileCtx#file_ctx{file_doc = FileDoc}};
get_file_doc(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether space is supported by imported storage.
%% @end
%%--------------------------------------------------------------------
-spec is_imported_storage(ctx()) -> {boolean(), ctx()}.
is_imported_storage(FileCtx = #file_ctx{is_imported_storage = undefined}) ->
    {StorageId, FileCtx2} = get_storage_id(FileCtx),
    ImportedStorage = storage:is_imported(StorageId),
    {ImportedStorage, FileCtx2#file_ctx{is_imported_storage = ImportedStorage}};
is_imported_storage(FileCtx = #file_ctx{is_imported_storage = ImportedStorage}) ->
    {ImportedStorage, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's file_meta document even if its marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc_including_deleted(ctx()) -> {file_meta:doc(), ctx()}.
get_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    {ok, Doc} = file_meta:get_including_deleted(FileUuid),
    case file_meta:is_deleted(Doc) of
        false ->
            {Doc, FileCtx#file_ctx{file_doc = Doc}};
        true ->
            {Doc, FileCtx}
    end;
get_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's file_meta document even if its marked as deleted. If
%% document does not exists returns error
%% @end
%%--------------------------------------------------------------------
-spec get_and_cache_file_doc_including_deleted(ctx()) ->
    {file_meta:doc(), ctx()} | {error, term()}.
get_and_cache_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    case file_meta:get_including_deleted(FileUuid) of
        {ok, Doc} ->
            {Doc, FileCtx#file_ctx{file_doc = Doc}};
        Error ->
            Error
    end;
get_and_cache_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent's file context. In case of user root dir and share root
%% dir/file returns the same file_ctx. Therefore, to check if given
%% file_ctx points to root dir (either user root dir or share root) it is
%% enough to call this function and compare returned parent ctx's guid
%% with its own.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(ctx(), user_ctx:ctx() | undefined) ->
    {ParentFileCtx :: ctx(), NewFileCtx :: ctx()}.
get_parent(FileCtx = #file_ctx{guid = Guid, parent = undefined}, UserCtx) ->
    {FileUuid, SpaceId, ShareId} = file_id:unpack_share_guid(Guid),
    {Doc, FileCtx2} = get_file_doc_including_deleted(FileCtx),
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),

    IsShareRootFile = case ShareId of
        undefined ->
            false;
        _ ->
            % ShareId is added to file_meta.shares only for directly shared
            % files/directories and not their children
            lists:member(ShareId, Doc#document.value#file_meta.shares)
    end,

    Parent = case {fslogic_uuid:is_root_dir_uuid(ParentUuid), IsShareRootFile} of
        {true, false} ->
            case ParentUuid =:= ?GLOBAL_ROOT_DIR_UUID
                andalso UserCtx =/= undefined
                andalso user_ctx:is_normal_user(UserCtx)
            of
                true ->
                    case is_user_root_dir_const(FileCtx2, UserCtx) of
                        true ->
                            FileCtx2;
                        false ->
                            UserId = user_ctx:get_user_id(UserCtx),
                            new_by_guid(fslogic_uuid:user_root_dir_guid(UserId))
                    end;
                _ ->
                    new_by_guid(fslogic_uuid:root_dir_guid())
            end;
        {true, true} ->
            case fslogic_uuid:is_space_dir_uuid(FileUuid) of
                true ->
                    FileCtx2;
                false ->
                    % userRootDir and globalRootDir can not be shared
                    throw(?EINVAL)
            end;
        {false, false} ->
            new_by_guid(file_id:pack_share_guid(ParentUuid, SpaceId, ShareId));
        {false, true} ->
            FileCtx2
    end,
    {Parent, FileCtx2#file_ctx{parent = Parent}};
get_parent(FileCtx = #file_ctx{parent = Parent}, _UserCtx) ->
    {Parent, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns 'undefined' if file is root file (either userRootDir or share root)
%% or proper ParentCtx otherwise.
%% @end
%%--------------------------------------------------------------------
-spec get_and_check_parent(ctx(), user_ctx:ctx() | undefined) ->
    {ParentFileCtx :: undefined | ctx(), NewFileCtx :: ctx()}.
get_and_check_parent(FileCtx0, UserCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),

    case file_ctx:get_guid_const(ParentCtx) of
        FileGuid ->
            % root dir/share root file -> there are no parents
            {undefined, FileCtx1};
        _ ->
            {ParentCtx, FileCtx1}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns GUID of parent or undefined when the file is a root/share root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid(ctx(), user_ctx:ctx() | undefined) -> {fslogic_worker:file_guid() | undefined, ctx()}.
get_parent_guid(#file_ctx{guid = FileGuid} = FileCtx, UserCtx) ->
    {ParentCtx, FileCtx2} = get_parent(FileCtx, UserCtx),
    case get_guid_const(ParentCtx) of
        FileGuid ->
            {undefined, FileCtx2};
        ParentGuid ->
            {ParentGuid, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_storage_file_id(FileCtx, true).
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(ctx()) -> {StorageFileId :: helpers:file_id(), ctx()}.
get_storage_file_id(FileCtx) ->
    get_storage_file_id(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns storage file ID (the ID of file on storage). If file ID does not
%% exists it can be generated (depending on second argument).
%% Storage file Id depends on the storage file mapping setting, currently
%% 2 options are supported:
%%   - canonical - which is POSIX-style file mapping including complete
%%                 directory path
%%   - flat - which provides a 3 level tree based on the FileUuid, enabling
%%            efficient rename operations without copying objects on the
%%            storage
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(ctx(), boolean()) ->
    {StorageFileId :: helpers:file_id() | undefined, ctx()}.
get_storage_file_id(FileCtx0 = #file_ctx{storage_file_id = undefined}, Generate) ->
    case is_root_dir_const(FileCtx0) of
        true ->
            StorageFileId = ?DIRECTORY_SEPARATOR_BINARY,
            {StorageFileId, FileCtx0#file_ctx{storage_file_id = StorageFileId}};
        false ->
            case get_local_file_location_doc(FileCtx0, false) of
                {#document{value = #file_location{file_id = ID, storage_file_created = SFC}}, FileCtx}
                    when ID =/= undefined andalso (SFC or Generate) ->
                    {ID, FileCtx};
                {_, FileCtx} ->
                    % Check if id should be generated
                    {Continue, FileCtx2} = case Generate of
                        true -> {true, FileCtx};
                        _ -> is_dir(FileCtx)
                    end,
                    case Continue of
                        true ->
                            get_new_storage_file_id(FileCtx2);
                        _ ->
                            {undefined, FileCtx2}
                    end
            end
    end;
get_storage_file_id(FileCtx = #file_ctx{storage_file_id = StorageFileId}, _) ->
    {StorageFileId, FileCtx}.

-spec get_new_storage_file_id(ctx()) -> {helpers:file_id(), ctx()}.
get_new_storage_file_id(FileCtx) ->
    {Storage, FileCtx2} = get_storage(FileCtx),
    Helper = storage:get_helper(Storage),
    case helper:get_storage_path_type(Helper) of
        ?FLAT_STORAGE_PATH ->
            {FileId, FileCtx3} = storage_file_id:flat(FileCtx2),
            % TODO - do not get_canonical_path (fix acceptance tests before)
            {_, FileCtx4} = get_canonical_path(FileCtx3),
            {FileId, FileCtx4#file_ctx{storage_file_id = FileId}};
        ?CANONICAL_STORAGE_PATH ->
            {StorageFileId, FileCtx3} = storage_file_id:canonical(FileCtx2),
            {StorageFileId, FileCtx3#file_ctx{storage_file_id = StorageFileId}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns name of the space where the file is located.
%% @end
%%--------------------------------------------------------------------
-spec get_space_name(ctx(), user_ctx:ctx()) ->
    {od_space:name(), ctx()} | no_return().
get_space_name(FileCtx = #file_ctx{space_name = undefined}, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    SpaceId = get_space_id_const(FileCtx),
    case space_logic:get_name(SessionId, SpaceId) of
        {ok, SpaceName} ->
            {SpaceName, FileCtx#file_ctx{space_name = SpaceName}};
        ?ERROR_FORBIDDEN when SessionId == ?ROOT_SESS_ID ->
            % Fetching space name from oz as provider is forbidden if provider
            % doesn't support space. Such requests are made e.g. when executing
            % file_meta:setup_onedata_user (all user space dirs, supported or not,
            % are created). To handle this special case SpaceId is returned instead.
            {SpaceId, FileCtx#file_ctx{space_name = SpaceId}};
        {error, _} ->
            throw(?ENOENT)
    end;
get_space_name(FileCtx = #file_ctx{space_name = SpaceName}, _Ctx) ->
    {SpaceName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns name of the file (if the file represents space dir, returns space name).
%% @end
%%--------------------------------------------------------------------
-spec get_aliased_name(ctx(), user_ctx:ctx() | undefined) ->
    {file_meta:name(), ctx()} | no_return().
get_aliased_name(FileCtx = #file_ctx{file_name = undefined}, UserCtx) ->
    FileGuid = get_guid_const(FileCtx),

    case is_space_dir_const(FileCtx) andalso UserCtx =/= undefined of
        true ->
            {Name, FileCtx2} = case user_ctx:is_guest(UserCtx) andalso file_id:is_share_guid(FileGuid) of
                true ->
                    % Special case for guest user - get space name with provider auth
                    get_space_name(FileCtx, user_ctx:new(?ROOT_SESS_ID));
                false ->
                    get_space_name(FileCtx, UserCtx)
            end,
            {Name, FileCtx2#file_ctx{file_name = Name}};
        false ->
            {#document{value = #file_meta{name = Name}}, FileCtx2} = get_file_doc_including_deleted(FileCtx),
            {Name, FileCtx2}
    end;
get_aliased_name(FileCtx = #file_ctx{file_name = FileName}, _UserCtx) ->
    {FileName, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns POSIX compatible display credentials.
%% @end
%%--------------------------------------------------------------------
-spec get_display_credentials(ctx()) -> {luma:display_credentials(), ctx()}.
get_display_credentials(FileCtx = #file_ctx{display_credentials = undefined}) ->
    SpaceId = get_space_id_const(FileCtx),
    {FileMetaDoc, FileCtx2} = get_file_doc_including_deleted(FileCtx),
    OwnerId = file_meta:get_owner(FileMetaDoc),
    {Storage, FileCtx3} = get_storage(FileCtx2),
    case luma:map_to_display_credentials(OwnerId, SpaceId, Storage) of
        {ok, DisplayCredentials = {Uid, Gid}} ->
            case Storage =:= undefined of
                true ->
                    {DisplayCredentials, FileCtx3#file_ctx{display_credentials = DisplayCredentials}};
                false ->
                    {SyncedGid, FileCtx4} = get_synced_gid(FileCtx3),
                    % if SyncedGid =/= undefined override display Gid
                    FinalGid = utils:ensure_defined(SyncedGid, Gid),
                    FinalDisplayCredentials = {Uid, FinalGid},
                    {FinalDisplayCredentials, FileCtx4#file_ctx{display_credentials = FinalDisplayCredentials}}
            end;
        {error, not_found} ->
            {error, ?EACCES}
    end;
get_display_credentials(FileCtx = #file_ctx{display_credentials = DisplayCredentials}) ->
    {DisplayCredentials, FileCtx}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file's atime, ctime and mtime.
%% @end
%%--------------------------------------------------------------------
-spec get_times(ctx()) -> {times:times(), ctx()}.
get_times(FileCtx = #file_ctx{times = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    {ok, Times} = times:get_or_default(FileUuid),
    {Times, FileCtx#file_ctx{times = Times}};
get_times(
    FileCtx = #file_ctx{times = Times}) ->
    {Times, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns child of the file with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child(ctx(), file_meta:name(), user_ctx:ctx()) ->
    {ChildFile :: ctx(), NewFile :: ctx()} | no_return().
get_child(FileCtx, Name, UserCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            UserDoc = user_ctx:get_user(UserCtx),
            SessionId = user_ctx:get_session_id(UserCtx),
            case user_logic:get_space_by_name(SessionId, UserDoc, Name) of
                {true, SpaceId} ->
                    Child = new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
                    {Child, FileCtx};
                false ->
                    case user_ctx:is_root(UserCtx) of
                        true ->
                            Child = new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(Name)),
                            {Child, FileCtx};
                        _ ->
                            throw(?ENOENT)
                    end
            end;
        _ ->
            SpaceId = get_space_id_const(FileCtx),
            {FileDoc, FileCtx2} = get_file_doc(FileCtx),
            case fslogic_path:resolve(FileDoc, <<"/", Name/binary>>) of
                {ok, ChildDoc} ->
                    ShareId = get_share_id_const(FileCtx2),
                    Child = new_by_doc(ChildDoc, SpaceId, ShareId),
                    {Child, FileCtx2};
                {error, not_found} ->
                    throw(?ENOENT)
            end
    end.

%%%-------------------------------------------------------------------
%%% @doc
%%% Returns CanonicalPath for child of ParentCtx with given FileName.
%%% @end
%%%-------------------------------------------------------------------
-spec get_child_canonical_path(ctx(), file_meta:name()) ->
    {file_meta:name(), NewParenCtx :: ctx()}.
get_child_canonical_path(ParentCtx, FileName) ->
    {ParentPath, ParentCtx2} = get_canonical_path(ParentCtx),
    CanonicalPath = filename:join(ParentPath, FileName),
    {CanonicalPath, ParentCtx2}.


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_file_children(FileCtx, UserCtx, Offset, Limit, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit()
) ->
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit) ->
    get_file_children(FileCtx, UserCtx, Offset, Limit, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_file_children(FileCtx, UserCtx, Offset, Limit, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | datastore_links_iter:token()
) ->
    {Children :: [ctx()], NewToken :: datastore_links_iter:token(), NewFileCtx :: ctx()} |
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit, Token) ->
    get_file_children(FileCtx, UserCtx, Offset, Limit, Token, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of directory children.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | datastore_links_iter:token(),
    StartId :: undefined | file_meta:name()
) ->
    {Children :: [ctx()], NewToken :: datastore_links_iter:token(), NewFileCtx :: ctx()} |
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit, Token, StartId) ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {list_user_spaces(UserCtx, Offset, Limit, undefined), FileCtx};
        false ->
            {FileDoc = #document{value = #file_meta{
                type = FileType
            }}, FileCtx2} = get_file_doc(FileCtx),
            SpaceId = get_space_id_const(FileCtx2),
            ShareId = get_share_id_const(FileCtx2),
            case FileType of
                ?DIRECTORY_TYPE ->
                    MapFun = fun(#child_link_uuid{name = Name, uuid = Uuid}) ->
                        new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
                    end,

                    case file_meta:list_children(FileDoc, Offset, Limit, Token, StartId) of
                        {ok, ChildrenLinks, #{token := Token2}} ->
                            {lists:map(MapFun, ChildrenLinks), Token2, FileCtx2};
                        {ok, ChildrenLinks, _} ->
                            {lists:map(MapFun, ChildrenLinks), FileCtx2}
                    end;
                _ ->
                    % In case of listing regular file - return it
                    {[FileCtx2], FileCtx2}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of directory children bounded by specified AllowedChildren.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children_whitelisted(ctx(), user_ctx:ctx(),
    NonNegOffset :: file_meta:non_neg_offset(),
    Limit :: file_meta:limit(),
    ChildrenWhiteList :: [file_meta:name()]
) ->
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children_whitelisted(
    FileCtx, UserCtx, NonNegOffset, Limit, ChildrenWhiteList
) when NonNegOffset >= 0 ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {list_user_spaces(UserCtx, NonNegOffset, Limit, ChildrenWhiteList), FileCtx};
        false ->
            {FileDoc = #document{}, FileCtx2} = get_file_doc(FileCtx),
            SpaceId = get_space_id_const(FileCtx2),
            ShareId = get_share_id_const(FileCtx2),

            {ok, ChildrenLinks} = file_meta:list_children_whitelisted(
                FileDoc, NonNegOffset, Limit, ChildrenWhiteList
            ),
            ChildrenCtxs = lists:map(fun(#child_link_uuid{name = Name, uuid = Uuid}) ->
                new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
            end, ChildrenLinks),
            {ChildrenCtxs, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage id.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(ctx()) -> {storage:id(), ctx()}.
get_storage_id(FileCtx) ->
    {Storage, FileCtx2} = get_storage(FileCtx),
    {storage:get_id(Storage), FileCtx2}.

%%--------------------------------------------------------------------
%% @doc
%% Returns record of storage supporting space in which file was created.
%% @end
%%--------------------------------------------------------------------
-spec get_storage(ctx()) -> {storage:data() | undefined, ctx()}.
get_storage(FileCtx = #file_ctx{storage = undefined}) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            {undefined, FileCtx};
        false ->
            SpaceId = get_space_id_const(FileCtx),
            case space_logic:get_local_storage_ids(SpaceId) of
                {ok, []} ->
                    {undefined, FileCtx};
                {ok, [StorageId | _]} ->
                    Storage2 = case storage:get(StorageId) of
                        {ok, Storage} -> Storage;
                        {error, not_found} -> undefined
                    end,
                    {Storage2, FileCtx#file_ctx{storage = Storage2}};
                {error, _} ->
                    {undefined, FileCtx}
            end
    end;
get_storage(FileCtx = #file_ctx{storage = Storage}) ->
    {Storage, FileCtx}.


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_file_location_with_filled_gaps(FileCtx, undefined)
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_with_filled_gaps(ctx()) -> {#file_location{}, ctx()}.
get_file_location_with_filled_gaps(FileCtx) ->
    get_file_location_with_filled_gaps(FileCtx, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_with_filled_gaps(ctx(),
    fslogic_blocks:blocks() | fslogic_blocks:block() | undefined) ->
    {#file_location{}, ctx()}.
get_file_location_with_filled_gaps(FileCtx, ReqRange)
    when is_list(ReqRange) orelse ReqRange == undefined ->
    % get locations
    {Locations, FileCtx2} = get_file_location_docs(FileCtx),
    {FileLocationDoc, FileCtx3} =
        get_or_create_local_file_location_doc(FileCtx2),
    {fill_location_gaps(ReqRange, FileLocationDoc, Locations,
        get_uuid_const(FileCtx3)), FileCtx3};
get_file_location_with_filled_gaps(FileCtx, ReqRange) ->
    get_file_location_with_filled_gaps(FileCtx, [ReqRange]).

%%--------------------------------------------------------------------
%% @doc
%% @equiv fill_location_gaps/5 but checks requested range and gets local blocks first.
%% @end
%%--------------------------------------------------------------------
-spec fill_location_gaps(fslogic_blocks:blocks() | undefined, file_location:doc(),
    [file_location:doc()], file_location:id()) -> #file_location{}.
fill_location_gaps(ReqRange0, #document{value = FileLocation = #file_location{
    size = Size}} = FileLocationDoc, Locations, Uuid) ->
    ReqRange = utils:ensure_defined(ReqRange0, [#file_block{offset = 0, size = Size}]),
    Blocks = fslogic_location_cache:get_blocks(FileLocationDoc,
        #{overlapping_blocks => ReqRange}),

    fill_location_gaps(ReqRange, FileLocation, Blocks, Locations, Uuid).

%%--------------------------------------------------------------------
%% @doc
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid.
%% @end
%%--------------------------------------------------------------------
-spec fill_location_gaps(fslogic_blocks:blocks(), file_location:record(), fslogic_blocks:blocks(),
    [file_location:doc()], file_location:id()) -> #file_location{}.
fill_location_gaps(ReqRange, FileLocation, LocalBlocks, Locations, Uuid) ->
    % find gaps
    AllRanges = lists:foldl(
        fun(Doc, Acc) ->
            fslogic_blocks:merge(Acc, fslogic_location_cache:get_blocks(Doc,
                #{overlapping_blocks => ReqRange}))
        end, [], Locations),
    Gaps = fslogic_blocks:consolidate(
        fslogic_blocks:invalidate(ReqRange, AllRanges)
    ),
    BlocksWithFilledGaps = fslogic_blocks:merge(LocalBlocks, Gaps),

    % fill gaps transform uid and emit
    fslogic_location_cache:set_final_blocks(FileLocation#file_location{
        uuid = Uuid}, BlocksWithFilledGaps).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_create_local_file_location_doc(FileCtx, true)
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_file_location_doc(FileCtx) ->
    get_or_create_local_file_location_doc(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_file_location_doc(ctx(), fslogic_location_cache:get_doc_opts()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_file_location_doc(FileCtx, GetDocOpts) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            {undefined, FileCtx2};
        {false, FileCtx2} ->
            get_or_create_local_regular_file_location_doc(FileCtx2,
                GetDocOpts, true)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_local_file_location_doc(FileCtx, true)
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_local_file_location_doc(FileCtx) ->
    % TODO VFS-6119 missing _const suffix in function name
    get_local_file_location_doc(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx(), fslogic_location_cache:get_doc_opts()) ->
    {file_location:doc() | undefined, ctx()}.
get_local_file_location_doc(FileCtx, GetDocOpts) ->
    % TODO VFS-6119 missing _const suffix in function name
    FileUuid = get_uuid_const(FileCtx),
    case fslogic_location_cache:get_local_location(FileUuid, GetDocOpts) of
        {ok, Location} ->
            {Location, FileCtx};
        {error, not_found} ->
            {undefined, FileCtx}
    end.

-spec get_dir_location_doc_const(ctx()) -> dir_location:doc() | undefined.
get_dir_location_doc_const(FileCtx) ->
    FileUuid = get_uuid_const(FileCtx),
    case dir_location:get(FileUuid) of
        {ok, Location} ->
            Location;
        {error, not_found} ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file location IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_ids(ctx()) ->
    {[file_location:id()], ctx()}.
get_file_location_ids(FileCtx = #file_ctx{file_location_ids = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    {ok, Locations} = file_meta:get_locations_by_uuid(FileUuid),
    {Locations, FileCtx#file_ctx{file_location_ids = Locations}};
get_file_location_ids(FileCtx = #file_ctx{file_location_ids = Locations}) ->
    {Locations, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file location docs.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_docs(ctx()) ->
    {[file_location:doc()], ctx()}.
% TODO VFS-4412 - export as _const function
get_file_location_docs(FileCtx) ->
    get_file_location_docs(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_file_location_docs(FileCtx, GetLocationOpts, true)
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_docs(ctx(), fslogic_location_cache:get_doc_opts()) ->
    {[file_location:doc()], ctx()}.
% TODO VFS-4412 - export as _const function
get_file_location_docs(FileCtx = #file_ctx{}, GetLocationOpts) ->
    get_file_location_docs(FileCtx, GetLocationOpts, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns file location docs.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_docs(ctx(), fslogic_location_cache:get_doc_opts(), boolean()) ->
    {[file_location:doc()], ctx()}.
get_file_location_docs(FileCtx = #file_ctx{}, GetLocationOpts, IncludeLocal) ->
    {LocationIds0, FileCtx2} = get_file_location_ids(FileCtx),
    FileUuid = get_uuid_const(FileCtx),
    LocationIds = case IncludeLocal of
        true -> LocationIds0;
        _ -> LocationIds0 -- [file_location:local_id(FileUuid)]
    end,
    LocationDocs = lists:filtermap(fun(LocId) ->
        case fslogic_location_cache:get_location(LocId, FileUuid,
            GetLocationOpts) of
            {ok, Location} ->
                {true, Location};
            _Error ->
                false
        end
    end, LocationIds),
    {LocationDocs, FileCtx2}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file active permissions type, that is info which permissions
%% are taken into account when checking authorization (acl if it is defined
%% or posix otherwise).
%% @end
%%--------------------------------------------------------------------
-spec get_active_perms_type(ctx(), include_deleted | ignore_deleted) -> {file_meta:permissions_type(), ctx()}.
get_active_perms_type(FileCtx = #file_ctx{file_doc = undefined}, GetMode) ->
    {Doc, FileCtx2} = case GetMode of
        include_deleted -> get_file_doc_including_deleted(FileCtx);
        _ -> get_file_doc(FileCtx)
    end,
    get_active_perms_type_from_doc(Doc, FileCtx2);
get_active_perms_type(FileCtx = #file_ctx{file_doc = Doc}, _) ->
    get_active_perms_type_from_doc(Doc, FileCtx).

-spec get_active_perms_type_from_doc(file_meta:doc(), ctx()) -> {file_meta:permissions_type(), ctx()}.
get_active_perms_type_from_doc(#document{value = #file_meta{acl = []}}, FileCtx) ->
    {posix, FileCtx};
get_active_perms_type_from_doc(#document{value = #file_meta{}}, FileCtx) ->
    {acl, FileCtx}.


-spec is_storage_file_created(ctx()) -> {boolean(), ctx()}.
is_storage_file_created(FileCtx) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            case get_dir_location_doc_const(FileCtx2) of
                undefined ->
                    {false, FileCtx2};
                DirLocation ->
                    {dir_location:is_storage_file_created(DirLocation), FileCtx2}
            end;
        {false, FileCtx2} ->
            case get_local_file_location_doc(FileCtx2, false) of
                {undefined, _} ->
                    {false, FileCtx2};
                {FileLocation, _} ->
                    {file_location:is_storage_file_created(FileLocation), FileCtx2}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns file Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(ctx()) -> {acl:acl(), ctx()}.
get_acl(FileCtx = #file_ctx{file_doc = #document{
    value = #file_meta{acl = Acl}
}}) ->
    {Acl, FileCtx};
get_acl(FileCtx) ->
    {_, FileCtx2} = get_file_doc(FileCtx),
    get_acl(FileCtx2).

%%--------------------------------------------------------------------
%% @doc
%% Returns file Posix Mode.
%% @end
%%--------------------------------------------------------------------
-spec get_mode(ctx()) -> {file_meta:posix_permissions(), ctx()}.
get_mode(FileCtx = #file_ctx{file_doc = #document{
    value = #file_meta{mode = Mode}
}}) ->
    {Mode, FileCtx};
get_mode(FileCtx) ->
    {_, FileCtx2} = get_file_doc(FileCtx),
    get_mode(FileCtx2).

%%--------------------------------------------------------------------
%% @doc
%% Returns size of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_size(ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), ctx()}.
get_file_size(FileCtx) ->
    case get_local_file_location_doc(FileCtx, false) of
        {#document{
            value = #file_location{
                size = undefined
            }
        }, FileCtx2} ->
            {FL, _} = get_local_file_location_doc(FileCtx, true),
            {fslogic_blocks:upper(fslogic_location_cache:get_blocks(FL)), FileCtx2};
        {#document{value = #file_location{size = Size}}, FileCtx2} ->
            {Size, FileCtx2};
        {undefined, FileCtx2} ->
            get_file_size_from_remote_locations(FileCtx2)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns information if file is fully replicated and size of file.
%% @end
%%--------------------------------------------------------------------
-spec get_replication_status_and_size(ctx() | file_meta:uuid()) ->
    {FullyReplicated :: boolean(), Size :: non_neg_integer(), ctx()}.
get_replication_status_and_size(FileCtx) ->
    case get_local_file_location_doc(FileCtx, {blocks_num, 2}) of
        {#document{value = #file_location{size = SizeInDoc}} = FLDoc, FileCtx2} ->
            Size = case SizeInDoc of
                undefined ->
                    {FLDocWithBlocks, _} = get_local_file_location_doc(FileCtx2, true),
                    fslogic_blocks:upper(fslogic_location_cache:get_blocks(FLDocWithBlocks));
                _ ->
                    SizeInDoc
            end,

            case fslogic_location_cache:get_blocks(FLDoc, #{count => 2}) of
                [#file_block{offset = 0, size = Size}] -> {true, Size, FileCtx2};
                [] when Size =:= 0 -> {true, Size, FileCtx2};
                _ -> {false, Size, FileCtx2}
            end;
        {undefined, FileCtx2} ->
            {RemoteSize, FileCtx3} = get_file_size_from_remote_locations(FileCtx2),
            {RemoteSize =:= 0, RemoteSize, FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns size of file on local storage
%% @end
%%--------------------------------------------------------------------
-spec get_local_storage_file_size(ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), ctx()}.
get_local_storage_file_size(FileCtx) ->
    FileUuid = get_uuid_const(FileCtx),
    LocalLocationId = file_location:local_id(FileUuid),
    {fslogic_location_cache:get_location_size(LocalLocationId, FileUuid), FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns id of file's owner.
%% @end
%%--------------------------------------------------------------------
-spec get_owner(ctx()) -> {od_user:id() | undefined, ctx()}.
get_owner(FileCtx = #file_ctx{
    file_doc = #document{
        value = #file_meta{owner = OwnerId}
    }}) ->
    {OwnerId, FileCtx};
get_owner(FileCtx) ->
    {_, FileCtx2} = get_file_doc(FileCtx),
    get_owner(FileCtx2).


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
is_space_dir_const(#file_ctx{guid = Guid}) ->
    fslogic_uuid:is_space_dir_guid(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir_const(ctx(), user_ctx:ctx()) -> boolean().
is_user_root_dir_const(#file_ctx{canonical_path = <<"/">>}, _UserCtx) ->
    true;
is_user_root_dir_const(#file_ctx{guid = Guid, canonical_path = undefined}, UserCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    UserRootDirUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    UserRootDirUuid == file_id:guid_to_uuid(Guid);
is_user_root_dir_const(#file_ctx{}, _UserCtx) ->
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
    Uuid = file_id:guid_to_uuid(Guid),
    fslogic_uuid:is_root_dir_uuid(Uuid);
is_root_dir_const(#file_ctx{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file exists.
%% @end
%%--------------------------------------------------------------------
-spec file_exists_const(ctx()) -> boolean().
file_exists_const(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    file_meta:exists(FileUuid);
file_exists_const(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file exists. Returns 'deleted' if files was created and then deleted.
%% @end
%%--------------------------------------------------------------------
-spec file_exists_or_is_deleted(ctx()) -> {?FILE_EXISTS | ?FILE_DELETED | ?FILE_NEVER_EXISTED, ctx()}.
file_exists_or_is_deleted(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    case file_meta:get_including_deleted(FileUuid) of
        {ok, Doc} ->
            case {Doc#document.value#file_meta.deleted, Doc#document.deleted} of
                {false, false} ->
                    {?FILE_EXISTS, FileCtx#file_ctx{file_doc = Doc}};
                _ ->
                    {?FILE_DELETED, FileCtx}
            end;
        {error, not_found} ->
            {?FILE_NEVER_EXISTED, FileCtx}
    end;
file_exists_or_is_deleted(FileCtx) ->
    {?FILE_EXISTS, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is located in space accessible by user.
%% @end
%%--------------------------------------------------------------------
-spec is_in_user_space_const(ctx(), user_ctx:ctx()) -> boolean().
is_in_user_space_const(FileCtx, UserCtx) ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            true;
        false ->
            SpaceId = get_space_id_const(FileCtx),
            user_logic:has_eff_space(user_ctx:get_user(UserCtx), SpaceId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns true if contexts point to the file with the same GUID.
%% @end
%%--------------------------------------------------------------------
-spec equals(ctx(), ctx()) -> boolean().
equals(FileCtx1, FileCtx2) ->
    get_guid_const(FileCtx1) =:= get_guid_const(FileCtx2).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is a directory.
%% @end
%%--------------------------------------------------------------------
-spec is_dir(ctx()) -> {boolean(), ctx()}.
is_dir(FileCtx = #file_ctx{is_dir = undefined}) ->
    {#document{value = #file_meta{type = Type}}, FileCtx2} =
        get_file_doc_including_deleted(FileCtx),
    IsDir = Type =:= ?DIRECTORY_TYPE,
    {IsDir, FileCtx2#file_ctx{is_dir = IsDir}};
is_dir(FileCtx = #file_ctx{is_dir = IsDir}) ->
    {IsDir, FileCtx}.

-spec is_readonly_storage(ctx()) -> {boolean(), ctx()}.
is_readonly_storage(FileCtx) ->
    {StorageId, FileCtx2} = get_storage_id(FileCtx),
    SpaceId = get_space_id_const(FileCtx2),
    IsReadonly = storage:is_storage_readonly(StorageId, SpaceId),
    {IsReadonly, FileCtx2}.

-spec assert_not_readonly_storage(ctx()) -> ctx().
assert_not_readonly_storage(FileCtx) ->
    case is_readonly_storage(FileCtx) of
        {true, _} -> throw(?EROFS);
        {false, FileCtx2} -> FileCtx2
    end.

-spec assert_not_readonly_target_storage_const(ctx(), od_provider:id()) -> ok.
assert_not_readonly_target_storage_const(FileCtx, TargetProviderId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_logic:has_readonly_support_from(SpaceId, TargetProviderId) of
        true -> throw(?EROFS);
        false -> ok
    end.

-spec assert_file_exists(ctx()) -> ctx() | no_return().
assert_file_exists(FileCtx0) ->
    % If file doesn't exists (or was deleted) fetching doc will fail,
    % {badmatch, {error, not_found}} will propagate up and fslogic_worker will
    % translate it to ?ENOENT
    {#document{}, FileCtx1} = file_ctx:get_file_doc(FileCtx0),
    FileCtx1.

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
    #file_ctx{guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId), file_name = Name}.

-spec resolve_canonical_path_tokens(ctx()) -> {[file_meta:name()], ctx()}.
resolve_canonical_path_tokens(FileCtx) ->
    resolve_and_cache_path(FileCtx, name).

-spec resolve_uuid_based_path_tokens(ctx()) -> {[file_meta:uuid()], ctx()}.
resolve_uuid_based_path_tokens(FileCtx) ->
    resolve_and_cache_path(FileCtx, uuid).

-spec resolve_and_cache_path(ctx(), name | uuid) -> {[file_meta:uuid() | file_meta:name()], ctx()}.
resolve_and_cache_path(FileCtx, Type) ->
    Callback = fun([#document{key = Uuid, value = #file_meta{name = Name}, scope = SpaceId}, ParentValue, CalculationInfo]) ->
        case fslogic_uuid:is_root_dir_uuid(Uuid) of
            true ->
                {ok, [<<"/">>], CalculationInfo};
            false ->
                case fslogic_uuid:is_space_dir_uuid(Uuid) of
                    true ->
                        {ok, [<<"/">>, SpaceId], CalculationInfo};
                    false ->
                        NameOrUuid = case Type of
                            uuid -> Uuid;
                            name -> Name
                        end,
                        {ok, ParentValue ++ [NameOrUuid], CalculationInfo}
                end
        end
    end,

    {#document{key = Uuid, value = #file_meta{type = FileType, name = Filename}, scope = SpaceId} = Doc, FileCtx2} =
        get_file_doc_including_deleted(FileCtx),
    {FilenameOrUuid, CacheName} = case Type of
        name -> {Filename, location_and_link_utils:get_canonical_paths_cache_name(SpaceId)};
        uuid -> {Uuid, location_and_link_utils:get_uuid_based_paths_cache_name(SpaceId)}
    end,
    case FileType of
        ?DIRECTORY_TYPE ->
            {ok, Path, _} = effective_value:get_or_calculate(CacheName, Doc, Callback),
            {Path, FileCtx2};
        _ ->
            {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
            {ok, ParentDoc} = file_meta:get_including_deleted(ParentUuid),
            {ok, Path, _} = effective_value:get_or_calculate(CacheName, ParentDoc, Callback),
            {Path ++ [FilenameOrUuid], FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns local file location doc, creates it if it's not present
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_regular_file_location_doc(ctx(), fslogic_location_cache:get_doc_opts(),
    boolean()) -> {file_location:doc() | undefined, ctx()}.
get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, true) ->
    case get_local_file_location_doc(FileCtx, GetDocOpts) of
        {undefined, FileCtx2} ->
            get_or_create_local_regular_file_location_doc(FileCtx2, GetDocOpts, false);
        {Location, FileCtx2} ->
            {Location, FileCtx2}
    end;
get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, _CheckLocationExists) ->
    case location_and_link_utils:get_new_file_location_doc(FileCtx, false, false) of
        {{ok, _}, FileCtx2} ->
            {LocationDocs, FileCtx3} = get_file_location_docs(FileCtx2, true, false),
            lists:foreach(fun(ChangedLocation) ->
                replica_dbsync_hook:on_file_location_change(FileCtx3, ChangedLocation)
            end, LocationDocs),
            get_local_file_location_doc(FileCtx3, GetDocOpts);
        {{error, already_exists}, FileCtx2} ->
            get_local_file_location_doc(FileCtx2, GetDocOpts)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file. File size is calculated from remote locations.
%% @end
%%--------------------------------------------------------------------
-spec get_file_size_from_remote_locations(ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), ctx()}.
get_file_size_from_remote_locations(FileCtx) ->
    {LocationDocs, FileCtx2} = get_file_location_docs(FileCtx, true, false),
    case LocationDocs of
        [] ->
            {0, FileCtx2};
        [First | DocsTail] ->
            ChocenDoc = lists:foldl(fun(
                New = #document{value = #file_location{
                    version_vector = NewVV
                }},
                Current = #document{value = #file_location{
                    version_vector = CurrentVV
                }}
            ) ->
                case version_vector:compare(CurrentVV, NewVV) of
                    identical -> Current;
                    greater -> Current;
                    lesser -> New;
                    concurrent -> New
                end
            end, First, DocsTail),

            case ChocenDoc of
                #document{
                    value = #file_location{
                        size = undefined
                    }
                } = FL ->
                    {fslogic_blocks:upper(fslogic_location_cache:get_blocks(FL)), FileCtx2};
                #document{value = #file_location{size = Size}} ->
                    {Size, FileCtx2}
            end
    end.

%% @private
-spec list_user_spaces(user_ctx:ctx(), file_meta:offset(), file_meta:limit(),
    SpaceWhiteList :: undefined | [od_space:id()]) -> Children :: [ctx()].
list_user_spaces(UserCtx, Offset, Limit, SpaceWhiteList) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    #document{value = #od_user{eff_spaces = UserEffSpaces}} = user_ctx:get_user(UserCtx),

    FilteredSpaces = case SpaceWhiteList of
        undefined ->
            UserEffSpaces;
        _ ->
            lists:filter(fun(Space) ->
                lists:member(Space, SpaceWhiteList)
            end, UserEffSpaces)
    end,
    case Offset < length(FilteredSpaces) of
        true ->
            SpacesChunk = lists:sublist(
                lists:sort(lists:map(fun(SpaceId) ->
                    {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
                    {SpaceName, SpaceId}
                end, FilteredSpaces)),
                Offset + 1,
                Limit
            ),
            lists:map(fun({SpaceName, SpaceId}) ->
                SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                new_child_by_uuid(SpaceDirUuid, SpaceName, SpaceId, undefined)
            end, SpacesChunk);
        false ->
            []
    end.

-spec get_synced_gid(ctx()) -> {luma:gid() | undefined, ctx()}.
get_synced_gid(FileCtx) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            {get_dir_synced_gid_const(FileCtx), FileCtx2};
        {false, FileCtx2} ->
            {get_file_synced_gid_const(FileCtx2), FileCtx2}
    end.

-spec get_file_synced_gid_const(ctx()) -> luma:gid() | undefined.
get_file_synced_gid_const(FileCtx) ->
    case get_local_file_location_doc(FileCtx, skip_local_blocks) of
        {undefined, _} ->
            undefined;
        {FileLocation, _} ->
            file_location:get_synced_gid(FileLocation)
    end.

-spec get_dir_synced_gid_const(ctx()) -> luma:gid() | undefined.
get_dir_synced_gid_const(FileCtx) ->
    case get_dir_location_doc_const(FileCtx) of
        undefined ->
            undefined;
        DirLocation ->
            dir_location:get_synced_gid(DirLocation)
    end.
