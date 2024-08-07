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
%%%
%%% Note: always consider usage of ctx based on referenced guid
%%% (see ensure_based_on_referenced_guid/1).
%%% @end
%%%--------------------------------------------------------------------
-module(file_ctx).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-record(file_ctx, {
    canonical_path :: undefined | file_meta:path(),
    uuid_based_path :: undefined | file_meta:uuid_based_path(),
    guid :: fslogic_worker:file_guid(),
    uuid :: file_meta:uuid(),
    share_id :: undefined | od_share:id(),
    space_id :: od_space:id(),
    file_doc :: undefined | file_meta:doc(),
    parent :: undefined | ctx(),
    storage_file_id :: undefined | helpers:file_id(),
    space_name :: undefined | od_space:name(),
    display_credentials :: undefined | luma:display_credentials(),
    times :: undefined | #{times_api:times_type() => times:time()},
    file_name :: undefined | file_meta:name(),
    storage :: undefined | storage:data(),
    file_location_ids :: undefined | [file_location:id()],
    is_dir :: undefined | boolean(),
    is_imported_storage :: undefined | boolean(),
    path_before_deletion :: undefined | file_meta:path()
}).

-type ctx() :: #file_ctx{}.
-type file_size_summary() :: [{virtual | storage:id(), non_neg_integer()}].
-export_type([ctx/0, file_size_summary/0]).

%% Functions creating context and filling its data
-export([new_by_canonical_path/2, new_by_guid/1,
    new_by_uuid/2, new_by_uuid/3, new_by_uuid/4,
    new_by_doc/2, new_by_doc/3, new_root_ctx/0]).
-export([reset/1, new_by_partial_context/1, set_file_location/2, set_file_id/2,
    set_file_doc/2, set_is_dir/2, set_path_before_deletion/2, ensure_based_on_referenced_guid/1]).

%% Functions that do not modify context
-export([get_share_id_const/1, get_space_id_const/1, get_space_dir_uuid_const/1,
    get_logical_guid_const/1, get_referenced_guid_const/1, get_logical_uuid_const/1, get_referenced_uuid_const/1,
    is_link_const/1, get_dir_location_doc_const/1, list_references_const/1, list_references_ctx_const/1, count_references_const/1
]).
-export([is_file_ctx_const/1, is_space_dir_const/1, is_trash_dir_const/1, is_trash_dir_const/2,
    is_tmp_dir_const/1, is_tmp_dir_const/2, is_share_root_dir_const/1, is_symlink_const/1, is_special_const/1,
    is_user_root_dir_const/2, is_root_dir_const/1, file_exists_const/1, file_exists_or_is_deleted/1,
    is_in_user_space_const/2, assert_not_special_const/1, assert_is_dir/1, assert_not_dir/1, get_type/1, 
    get_effective_type/1, assert_not_trash_dir_const/1, assert_not_trash_dir_const/2,
    assert_not_trash_or_tmp_dir_const/1, assert_not_trash_or_tmp_dir_const/2,
    assert_synchronization_enabled/1, assert_synchronization_disabled/1]).
-export([equals/2]).
-export([assert_not_readonly_target_storage_const/2]).
-export([get_local_file_location_doc_const/1, get_local_file_location_doc_const/2]).

%% Functions modifying context
-export([
    get_canonical_path/1, get_uuid_based_path/1, get_file_doc/1, is_synchronization_enabled/1,
    get_file_doc_including_deleted/1, get_and_cache_file_doc_including_deleted/1,
    get_cached_parent_const/1, cache_parent/2, cache_name/2,
    get_storage_file_id/1, get_storage_file_id/2,
    get_new_storage_file_id/1, get_aliased_name/2,
    get_display_credentials/1, get_times/1, get_times/2,
    get_logical_path/2, get_path_before_deletion/1,
    get_storage_id/1, get_storage/1, get_file_location_with_filled_gaps/1,
    get_file_location_with_filled_gaps/2,
    get_or_create_local_file_location_doc/1, get_or_create_local_file_location_doc/2,
    get_or_create_local_regular_file_location_doc/3, get_or_create_local_regular_file_location_doc/4,
    get_file_location_ids/1, get_file_location_docs/1, get_file_location_docs/2,
    get_active_perms_type/2, get_acl/1, get_mode/1, get_file_size/1, prepare_file_size_summary/2,
    get_file_size_from_remote_locations/1, get_owner/1,
    get_local_storage_file_size/1, ensure_synced/1
]).
-export([is_dir/1, is_imported_storage/1, is_storage_file_created/1, is_readonly_storage/1]).
-export([assert_not_readonly_storage/1, assert_file_exists/1, assert_smaller_than_provider_support_size/2]).


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
    new_by_guid(fslogic_file_id:root_dir_guid()).

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
    {Uuid, SpaceId, ShareId} = file_id:unpack_share_guid(Guid),
    #file_ctx{guid = Guid, uuid = Uuid, space_id = SpaceId, share_id = ShareId}.

-spec new_by_uuid(file_meta:uuid(), od_space:id()) -> ctx().
new_by_uuid(Uuid, SpaceId) ->
    #file_ctx{guid = file_id:pack_guid(Uuid, SpaceId), uuid = Uuid, space_id = SpaceId}.

-spec new_by_uuid(file_meta:uuid(), od_space:id(), undefined | od_share:id()) -> ctx().
new_by_uuid(Uuid, SpaceId, ShareId) ->
    #file_ctx{guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
        uuid = Uuid, space_id = SpaceId, share_id = ShareId}.

-spec new_by_uuid(file_meta:uuid(), od_space:id(), undefined | od_share:id(), file_meta:name()) -> ctx().
new_by_uuid(Uuid, SpaceId, ShareId, Name) ->
    (new_by_uuid(Uuid, SpaceId, ShareId))#file_ctx{file_name = Name}.

-spec new_by_doc(file_meta:doc(), od_space:id()) -> ctx().
new_by_doc(Doc, SpaceId) ->
    new_by_doc(Doc, SpaceId, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates new file context using file's file_meta doc.
%% @end
%%--------------------------------------------------------------------
-spec new_by_doc(file_meta:doc(), od_space:id(), undefined | od_share:id()) -> ctx().
new_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId, ShareId) ->
    Guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
    #file_ctx{file_doc = Doc, guid = Guid, uuid = Uuid, space_id = SpaceId, share_id = ShareId}.

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
    {ok, FileDoc} = canonical_path:resolve(CanonicalPath),
    SpaceId = file_partial_ctx:get_space_id_const(FilePartialCtx2),
    {new_by_doc(FileDoc, SpaceId), SpaceId}.

%%--------------------------------------------------------------------
%% @doc
%% Resets all cached data besides GUID.
%% @end
%%--------------------------------------------------------------------
-spec reset(ctx()) -> ctx().
reset(#file_ctx{guid = Guid, uuid = Uuid, space_id = SpaceId, share_id = ShareId}) ->
    #file_ctx{guid = Guid, uuid = Uuid, space_id = SpaceId, share_id = ShareId}.

-spec set_file_location(ctx(), file_location:id()) -> ctx().
set_file_location(FileCtx = #file_ctx{file_location_ids = undefined}, _LocationId) ->
    FileCtx;
set_file_location(FileCtx = #file_ctx{file_location_ids = Locations}, LocationId) ->
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
%% Sets dataset path in context record
%% @end
%%--------------------------------------------------------------------
-spec set_path_before_deletion(ctx(), file_meta:path()) -> ctx().
set_path_before_deletion(FileCtx, PathBeforeDeletion) ->
    FileCtx#file_ctx{path_before_deletion = PathBeforeDeletion}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's share ID.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id_const(ctx()) -> od_share:id() | undefined.
get_share_id_const(#file_ctx{share_id = ShareId}) ->
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id().
get_space_id_const(#file_ctx{space_id = SpaceId}) ->
    SpaceId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space dir UUID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_dir_uuid_const(ctx()) -> file_meta:uuid().
get_space_dir_uuid_const(FileCtx) ->
    SpaceId = get_space_id_const(FileCtx),
    fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec get_logical_guid_const(ctx()) -> fslogic_worker:file_guid().
get_logical_guid_const(#file_ctx{guid = Guid}) ->
    Guid.

-spec get_referenced_guid_const(ctx()) -> fslogic_worker:file_guid().
get_referenced_guid_const(FileCtx) ->
    Uuid = get_logical_uuid_const(FileCtx),
    case fslogic_file_id:ensure_referenced_uuid(Uuid) of
        Uuid -> get_logical_guid_const(FileCtx);
        ReferencedUuid -> file_id:pack_guid(ReferencedUuid, get_space_id_const(FileCtx))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file UUID entry.
%% @end
%%--------------------------------------------------------------------
-spec get_logical_uuid_const(ctx()) -> file_meta:uuid().
get_logical_uuid_const(#file_ctx{uuid = Uuid}) ->
    Uuid.

-spec get_referenced_uuid_const(ctx()) -> file_meta:uuid().
get_referenced_uuid_const(FileCtx) ->
    fslogic_file_id:ensure_referenced_uuid(get_logical_uuid_const(FileCtx)).

-spec is_link_const(ctx()) -> boolean().
is_link_const(FileCtx) ->
    fslogic_file_id:is_link_uuid(get_logical_uuid_const(FileCtx)).

%%--------------------------------------------------------------------
%% @doc Creates new ctx if referenced uuid (see fslogic_file_id:ensure_referenced_uuid/1) is not
%% equal to file uuid (ctx has been created for link and it is replaced with ctx of target file).
%% @end
%%--------------------------------------------------------------------
-spec ensure_based_on_referenced_guid(ctx()) -> ctx().
ensure_based_on_referenced_guid(FileCtx) ->
    Uuid = get_logical_uuid_const(FileCtx),
    case fslogic_file_id:ensure_referenced_uuid(Uuid) of
        Uuid -> FileCtx;
        ReferencedUuid -> new_by_uuid(ReferencedUuid, get_space_id_const(FileCtx))
    end.

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
            {CanonicalPath, FileCtx2} = resolve_and_cache_path(FileCtx, ?CANONICAL_PATH),
            {CanonicalPath, FileCtx2#file_ctx{canonical_path = CanonicalPath}}
    end;
get_canonical_path(FileCtx = #file_ctx{canonical_path = Path}) ->
    {Path, FileCtx}.


-spec get_uuid_based_path(ctx()) -> {file_meta:uuid_based_path(), ctx()}.
get_uuid_based_path(FileCtx = #file_ctx{uuid_based_path = undefined}) ->
    {UuidBasedPath, FileCtx2} = resolve_and_cache_path(FileCtx, ?UUID_BASED_PATH),
    {UuidBasedPath, FileCtx2#file_ctx{uuid_based_path = UuidBasedPath}};
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
            {ok, [<<"/">>, _SpaceId | Rest]} = filepath_utils:split_and_skip_dots(Path),
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
    Uuid = get_logical_uuid_const(FileCtx),
    {ok, FileDoc} = case fslogic_file_id:is_share_root_dir_uuid(Uuid) of
        true -> get_share_root_dir_doc(FileCtx, false);
        false -> file_meta:get({uuid, Uuid})
    end,
    {FileDoc, FileCtx#file_ctx{file_doc = FileDoc}};
get_file_doc(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.

-spec set_file_doc(ctx(), file_meta:doc()) -> ctx().
set_file_doc(FileCtx, NewDoc) ->
    FileCtx#file_ctx{file_doc = NewDoc}.

-spec is_synchronization_enabled(ctx()) -> {boolean(), ctx()}.
is_synchronization_enabled(FileCtx) ->
    try
        {#document{ignore_in_changes = Ignore}, FileCtx2} = get_file_doc_including_deleted(FileCtx),
        {not Ignore, FileCtx2}
    catch
        _:Error ->
            case datastore_runner:normalize_error(Error) of
                not_found -> {true, FileCtx}; % doc is not found so it must be due to synchronization or
                                              % import (imported are sync enabled)
                _ -> throw(Error)
            end
    end.

-spec list_references_const(ctx()) -> {ok, [file_meta:uuid()]} | {error, term()}.
list_references_const(FileCtx) ->
    % TODO VFS-7444 - Investigate possibility to cache hardlink references in file_ctx
    FileUuid = get_referenced_uuid_const(FileCtx),
    file_meta_hardlinks:list_references(FileUuid).

-spec list_references_ctx_const(ctx()) -> {ok, [ctx()]} | {error, term()}.
list_references_ctx_const(FileCtx) ->
    case list_references_const(FileCtx) of
        {ok, References} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx),
            {ok, lists:map(
                fun (FileUuid) when FileUuid == LogicalUuid -> FileCtx;
                    (FileUuid) -> file_ctx:new_by_uuid(FileUuid, SpaceId)
                end,
                References
            )};
        {error, _} = Error ->
            Error
    end.

-spec count_references_const(ctx()) -> {ok, non_neg_integer()} | {error, term()}.
count_references_const(FileCtx) ->
    FileUuid = get_referenced_uuid_const(FileCtx),
    file_meta_hardlinks:count_references(FileUuid).

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
    FileUuid = get_logical_uuid_const(FileCtx),
    {ok, Doc} = case fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true -> get_share_root_dir_doc(FileCtx, true);
        false -> file_meta:get_including_deleted(FileUuid)
    end,
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
    FileUuid = get_logical_uuid_const(FileCtx),
    Result = case fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true -> get_share_root_dir_doc(FileCtx, true);
        false -> file_meta:get_including_deleted(FileUuid)
    end,
    case Result of
        {ok, Doc} ->
            {Doc, FileCtx#file_ctx{file_doc = Doc}};
        Error ->
            Error
    end;
get_and_cache_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = FileDoc}) ->
    {FileDoc, FileCtx}.


%% @private
-spec get_share_root_dir_doc(ctx(), IncludingDeleted :: boolean()) ->
    {ok, file_meta:doc()} | {error, not_found}.
get_share_root_dir_doc(FileCtx, IncludingDeleted) ->
    ShareDirUuid = get_logical_uuid_const(FileCtx),
    SpaceId = get_space_id_const(FileCtx),

    #document{
        value = #file_meta{
            deleted = IsDeleted
        }
    } = ShareRootDirDoc = file_meta:new_share_root_dir_doc(ShareDirUuid, SpaceId),

    case {IsDeleted, IncludingDeleted} of
        {true, false} ->
            ?ERROR_NOT_FOUND;
        _ ->
            {ok, ShareRootDirDoc}
    end.


-spec get_cached_parent_const(ctx()) -> undefined | ctx().
get_cached_parent_const(#file_ctx{parent = Parent}) ->
    Parent.


-spec cache_parent(ctx(), ctx()) -> ctx().
cache_parent(ParentCtx, FileCtx) ->
    FileCtx#file_ctx{parent = ParentCtx}.


-spec cache_name(file_meta:name(), ctx()) -> ctx().
cache_name(FileName, FileCtx) ->
    FileCtx#file_ctx{file_name = FileName}.


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
            StorageFileId = <<?DIRECTORY_SEPARATOR>>,
            {StorageFileId, FileCtx0#file_ctx{storage_file_id = StorageFileId}};
        false ->
            {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
            case IsDir of
                true ->
                    StorageFileIdOrUndefined = case get_dir_location_doc_const(FileCtx1) of
                        undefined ->
                            undefined;
                        DirLocation ->
                            dir_location:get_storage_file_id(DirLocation)
                    end,
                    case StorageFileIdOrUndefined of
                        undefined ->
                            get_new_storage_file_id(FileCtx1);
                        StorageFileId ->
                            {StorageFileId, FileCtx1#file_ctx{storage_file_id = StorageFileId}}
                    end;
                false ->
                    case get_local_file_location_doc_const(FileCtx1, false) of
                        #document{
                            value = #file_location{file_id = StorageFileId, storage_file_created = SFC}
                        }
                            when StorageFileId =/= undefined
                            andalso (SFC or Generate)
                        ->
                            {StorageFileId, FileCtx1#file_ctx{storage_file_id = StorageFileId}};
                        _ ->
                            % Check if id should be generated
                            case Generate of
                                true ->
                                    get_new_storage_file_id(FileCtx1);
                                _ ->
                                    {undefined, FileCtx1}
                            end
                    end
            end
    end;
get_storage_file_id(FileCtx = #file_ctx{storage_file_id = StorageFileId}, _) ->
    {StorageFileId, FileCtx}.

-spec get_new_storage_file_id(ctx()) -> {helpers:file_id(), ctx()}.
get_new_storage_file_id(FileCtx) ->
    ReferencedUuidBasedFileCtx = ensure_based_on_referenced_guid(FileCtx),
    {Storage, ReferencedUuidBasedFileCtx2} = get_storage(ReferencedUuidBasedFileCtx),
    Helper = storage:get_helper(Storage),
    SpaceId = file_ctx:get_space_id_const(ReferencedUuidBasedFileCtx2),
    {CanonicalPath, ReferencedUuidBasedFileCtx3} = file_ctx:get_canonical_path(ReferencedUuidBasedFileCtx2),
    case helper:get_storage_path_type(Helper) of
        ?FLAT_STORAGE_PATH ->
            FileUuid = file_ctx:get_logical_uuid_const(ReferencedUuidBasedFileCtx3),
            StorageFileId = storage_file_id:flat(CanonicalPath, FileUuid, SpaceId, Storage),
            FinalCtx = return_newer_if_equals(ReferencedUuidBasedFileCtx3, FileCtx),
            {StorageFileId, FinalCtx#file_ctx{storage_file_id = StorageFileId}};
        ?CANONICAL_STORAGE_PATH ->
            StorageFileId = storage_file_id:canonical(CanonicalPath, SpaceId, Storage),
            FinalCtx = return_newer_if_equals(ReferencedUuidBasedFileCtx3, FileCtx),
            {StorageFileId, FinalCtx#file_ctx{storage_file_id = StorageFileId}}
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
            % file_meta:ensure_space_docs_exist (all user space dirs, supported or not,
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
    FileGuid = get_logical_guid_const(FileCtx),

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
    ReferencedFileCtx = ensure_based_on_referenced_guid(FileCtx),
    SpaceId = get_space_id_const(ReferencedFileCtx),
    {FileMetaDoc, ReferencedFileCtx2} = get_file_doc_including_deleted(ReferencedFileCtx),
    OwnerId = file_meta:get_owner(FileMetaDoc),
    {Storage, ReferencedFileCtx3} = get_storage(ReferencedFileCtx2),
    case luma:map_to_display_credentials(OwnerId, SpaceId, Storage) of
        {ok, DisplayCredentials = {Uid, Gid}} ->
            case Storage =:= undefined of
                true ->
                    FinalCtx = return_newer_if_equals(ReferencedFileCtx3, FileCtx),
                    {DisplayCredentials, FinalCtx#file_ctx{display_credentials = DisplayCredentials}};
                false ->
                    {SyncedGid, ReferencedFileCtx4} = get_synced_gid(ReferencedFileCtx3),
                    % if SyncedGid =/= undefined override display Gid
                    FinalGid = utils:ensure_defined(SyncedGid, Gid),
                    FinalDisplayCredentials = {Uid, FinalGid},
                    FinalCtx = return_newer_if_equals(ReferencedFileCtx4, FileCtx),
                    {FinalDisplayCredentials, FinalCtx#file_ctx{display_credentials = FinalDisplayCredentials}}
            end;
        {error, not_found} ->
            {error, ?EACCES}
    end;
get_display_credentials(FileCtx = #file_ctx{display_credentials = DisplayCredentials}) ->
    {DisplayCredentials, FileCtx}.


-spec get_times(ctx()) -> {times:record(), ctx()}.
get_times(FileCtx) ->
    get_times(FileCtx, [?attr_atime, ?attr_mtime, ?attr_ctime]).


-spec get_times(ctx(), [times_api:times_type()]) -> {times:record(), ctx()}.
get_times(FileCtx = #file_ctx{times = undefined}, RequestedTimes) ->
    FileUuid = get_logical_uuid_const(FileCtx),
    Times = case fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true ->
            % Share root dir is virtual directory which does not have documents
            % like `file_meta` or `times` - in such case get times of share root
            % file
            ShareId = fslogic_file_id:share_root_dir_uuid_to_shareid(FileUuid),
            {ok, #document{
                value = #od_share{
                    root_file = RootFileShareGuid
                }
            }} = share_logic:get(?ROOT_SESS_ID, ShareId),

            RootFileGuid = file_id:share_guid_to_guid(RootFileShareGuid),
            {RootFileTimes, _} = get_times(new_by_guid(RootFileGuid), RequestedTimes),
            RootFileTimes;
        false ->
            times_api:get(FileCtx, RequestedTimes)
    end,
    {Times, FileCtx#file_ctx{times = times_record_to_map(Times)}};
get_times(FileCtx = #file_ctx{times = TimesMap}, RequestedTimes) ->
    case lists_utils:intersect(RequestedTimes, maps:keys(TimesMap)) of
        RequestedTimes ->
            {map_to_times_record(TimesMap), FileCtx};
        NotAllTimes ->
            MissingTimes = RequestedTimes -- NotAllTimes,
            MissingTimesMap = times_record_to_map(times_api:get(FileCtx, MissingTimes)),
            FinalMap = maps:merge(TimesMap, MissingTimesMap),
            {map_to_times_record(FinalMap), FileCtx#file_ctx{times = FinalMap}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns storage id.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(ctx()) -> {storage:id() | undefined, ctx()}.
get_storage_id(FileCtx) ->
    case get_storage(FileCtx) of
        {undefined, FileCtx2} ->
            % can happen for user root dir or space dir accessed via provider proxy
            {undefined, FileCtx2};
        {Storage, FileCtx2} ->
            {storage:get_id(Storage), FileCtx2}
    end.

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
            case space_logic:get_local_storages(SpaceId) of
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
-spec get_file_location_with_filled_gaps(ctx()) -> {#file_location{} | undefined, ctx()}.
get_file_location_with_filled_gaps(FileCtx) ->
    get_file_location_with_filled_gaps(FileCtx, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid. Returns undefined if location is deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_with_filled_gaps(ctx(),
    fslogic_blocks:blocks() | fslogic_blocks:block() | undefined) ->
    {#file_location{} | undefined, ctx()}.
get_file_location_with_filled_gaps(FileCtx, ReqRange)
    when is_list(ReqRange) orelse ReqRange == undefined ->
    % get locations
    {Locations, FileCtx2} = get_file_location_docs(set_is_dir(FileCtx, false)),
    case get_or_create_local_file_location_doc(FileCtx2) of
        {#document{deleted = true}, FileCtx3} ->
            {undefined, FileCtx3};
        {FileLocationDoc, FileCtx3} ->
            {fslogic_location:get_local_blocks_and_fill_location_gaps(ReqRange, FileLocationDoc, Locations,
                get_logical_uuid_const(FileCtx3)), FileCtx3}
    end;
get_file_location_with_filled_gaps(FileCtx, ReqRange) ->
    get_file_location_with_filled_gaps(FileCtx, [ReqRange]).

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
-spec get_local_file_location_doc_const(ctx()) ->
    file_location:doc() | undefined.
get_local_file_location_doc_const(FileCtx) ->
    get_local_file_location_doc_const(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc_const(ctx(), fslogic_location_cache:get_doc_opts()) ->
    file_location:doc() | undefined.
get_local_file_location_doc_const(FileCtx, GetDocOpts) ->
    FileUuid = get_logical_uuid_const(FileCtx),
    case fslogic_location_cache:get_local_location(FileUuid, GetDocOpts) of
        {ok, Location} ->
            Location;
        {error, not_found} ->
            undefined
    end.

-spec get_dir_location_doc_const(ctx()) -> dir_location:doc() | undefined.
get_dir_location_doc_const(FileCtx) ->
    FileUuid = get_logical_uuid_const(FileCtx),
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
    FileUuid = get_logical_uuid_const(FileCtx),
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
    FileUuid = get_logical_uuid_const(FileCtx),
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
            case get_local_file_location_doc_const(FileCtx2, false) of
                undefined ->
                    {false, FileCtx2};
                FileLocation ->
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
    case get_local_file_location_doc_const(FileCtx, false) of
        #document{value = #file_location{size = undefined}} ->
            FL = get_local_file_location_doc_const(FileCtx, true),
            {fslogic_blocks:upper(fslogic_location_cache:get_blocks(FL)), FileCtx};
        #document{value = #file_location{size = Size}} ->
            {Size, FileCtx};
        undefined ->
            get_file_size_from_remote_locations(FileCtx)
    end.


-spec prepare_file_size_summary(ctx(), create_missing_location | throw_on_missing_location) ->
    {file_size_summary(), ctx()} | no_return().
prepare_file_size_summary(FileCtx, create_missing_location) ->
    {LocationDoc, FileCtx2} = get_or_create_local_regular_file_location_doc(FileCtx, true, true),
    {prepare_file_size_summary_from_existing_doc(LocationDoc), FileCtx2};
prepare_file_size_summary(FileCtx, throw_on_missing_location) ->
    case get_local_file_location_doc_const(FileCtx, true) of
        undefined ->
            throw({error, file_location_missing});
        LocationDoc ->
            {prepare_file_size_summary_from_existing_doc(LocationDoc), FileCtx}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns size of file on local storage
%% @end
%%--------------------------------------------------------------------
-spec get_local_storage_file_size(ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), ctx()}.
get_local_storage_file_size(FileCtx) ->
    FileUuid = get_logical_uuid_const(FileCtx),
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


-spec is_space_dir_const(ctx()) -> boolean().
is_space_dir_const(#file_ctx{guid = Guid}) ->
    fslogic_file_id:is_space_dir_guid(Guid).


-spec is_trash_dir_const(ctx()) -> boolean().
is_trash_dir_const(#file_ctx{guid = Guid}) ->
    fslogic_file_id:is_trash_dir_guid(Guid).


-spec is_trash_dir_const(ctx(), file_meta:name()) -> boolean().
is_trash_dir_const(ParentCtx, Name) ->
    file_ctx:is_space_dir_const(ParentCtx)
        andalso (Name =:= ?TRASH_DIR_NAME).


-spec is_tmp_dir_const(ctx()) -> boolean().
is_tmp_dir_const(#file_ctx{guid = Guid}) ->
    fslogic_file_id:is_tmp_dir_guid(Guid).


-spec is_tmp_dir_const(ctx(), file_meta:name()) -> boolean().
is_tmp_dir_const(ParentCtx, Name) ->
    file_ctx:is_space_dir_const(ParentCtx)
        andalso (Name =:= ?TMP_DIR_NAME).


-spec is_share_root_dir_const(ctx()) -> boolean().
is_share_root_dir_const(#file_ctx{guid = Guid}) ->
    fslogic_file_id:is_share_root_dir_guid(Guid).


-spec is_symlink_const(ctx()) -> boolean().
is_symlink_const(FileCtx) ->
    fslogic_file_id:is_symlink_uuid(get_logical_uuid_const(FileCtx)).


-spec is_special_const(ctx()) -> boolean().
is_special_const(#file_ctx{guid = Guid}) ->
    fslogic_file_id:is_special_guid(Guid).


-spec assert_not_special_const(ctx()) -> ok.
assert_not_special_const(FileCtx) ->
    case is_special_const(FileCtx) of
        true -> throw(?EPERM);
        false -> ok
    end.


-spec assert_not_trash_dir_const(file_ctx:ctx()) -> ok.
assert_not_trash_dir_const(FileCtx) ->
    case is_trash_dir_const(FileCtx) of
        true -> throw(?EPERM);
        false -> ok
    end.


-spec assert_not_trash_dir_const(file_ctx:ctx(), file_meta:name()) -> ok.
assert_not_trash_dir_const(ParentCtx, Name) ->
    case is_trash_dir_const(ParentCtx, Name) of
        true -> throw(?EPERM);
        false -> ok
    end.


-spec assert_not_trash_or_tmp_dir_const(file_ctx:ctx()) -> ok.
assert_not_trash_or_tmp_dir_const(FileCtx) ->
    case is_trash_dir_const(FileCtx) orelse is_tmp_dir_const(FileCtx) of
        true -> throw(?EPERM);
        false -> ok
    end.


-spec assert_not_trash_or_tmp_dir_const(file_ctx:ctx(), file_meta:name()) -> ok.
assert_not_trash_or_tmp_dir_const(ParentCtx, Name) ->
    case is_trash_dir_const(ParentCtx, Name) orelse is_tmp_dir_const(ParentCtx, Name) of
        true -> throw(?EPERM);
        false -> ok
    end.


-spec assert_synchronization_enabled(ctx()) -> ctx().
assert_synchronization_enabled(FileCtx) ->
    case is_synchronization_enabled(FileCtx) of
        {false, _} -> throw(?EINVAL);
        {true, FileCtx2} -> FileCtx2
    end.


-spec assert_synchronization_disabled(ctx()) -> ctx().
assert_synchronization_disabled(FileCtx) ->
    case is_synchronization_enabled(FileCtx) of
        {true, _} -> throw(?EINVAL);
        {false, FileCtx2} -> FileCtx2
    end.


%% @TODO VFS-12025 - find better place for this function, it does not belong here
-spec ensure_synced(ctx()) -> ctx().
ensure_synced(FileCtx) ->
    Uuid = get_logical_uuid_const(FileCtx),
    times:ensure_synced(Uuid),
    custom_metadata:ensure_synced(Uuid),
    case file_meta:ensure_synced(Uuid) of
        {ok, #document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = Doc} ->
            FileCtx2 = FileCtx#file_ctx{file_doc = Doc},
            fslogic_location_cache:ensure_synced(FileCtx2),
            FileCtx2;
        {ok, Doc} ->
            FileCtx#file_ctx{file_doc = Doc};
        {error, not_found} ->
            FileCtx
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir_const(ctx(), user_ctx:ctx()) -> boolean().
is_user_root_dir_const(#file_ctx{canonical_path = <<"/">>}, _UserCtx) ->
    true;
is_user_root_dir_const(#file_ctx{uuid = Uuid, canonical_path = undefined}, UserCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    UserRootDirUuid = fslogic_file_id:user_root_dir_uuid(UserId),
    UserRootDirUuid == Uuid;
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
    fslogic_file_id:is_root_dir_guid(Guid);
is_root_dir_const(#file_ctx{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file exists.
%% @end
%%--------------------------------------------------------------------
-spec file_exists_const(ctx()) -> boolean().
file_exists_const(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_logical_uuid_const(FileCtx),

    case fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true ->
            ShareId = fslogic_file_id:share_root_dir_uuid_to_shareid(FileUuid),

            case share_logic:get(?ROOT_SESS_ID, ShareId) of
                {ok, _} -> true;
                ?ERROR_NOT_FOUND -> false
            end;
        false ->
            file_meta:exists(FileUuid)
    end;
file_exists_const(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file exists. Returns 'deleted' if files was created and then deleted.
%% @end
%%--------------------------------------------------------------------
-spec file_exists_or_is_deleted(ctx()) -> {?FILE_EXISTS | ?FILE_DELETED | ?FILE_NEVER_EXISTED, ctx()}.
file_exists_or_is_deleted(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_logical_uuid_const(FileCtx),
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
    get_logical_guid_const(FileCtx1) =:= get_logical_guid_const(FileCtx2).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether passed contexts are associated with the same file.
%% If true, returns NewerFileCtx else returns DefaultFileCtx.
%% @end
%%--------------------------------------------------------------------
-spec return_newer_if_equals(ctx(), ctx()) -> ctx().
return_newer_if_equals(NewerFileCtx, DefaultFileCtx) ->
    case equals(NewerFileCtx, DefaultFileCtx) of
        true -> NewerFileCtx;
        false -> DefaultFileCtx
    end.


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


-spec assert_is_dir(ctx()) -> ctx().
assert_is_dir(FileCtx) ->
    case is_dir(FileCtx) of
        {false, _} -> throw(?ENOTDIR);
        {true, FileCtx2} -> FileCtx2
    end.

-spec assert_not_dir(ctx()) -> ctx().
assert_not_dir(FileCtx) ->
    case is_dir(FileCtx) of
        {true, _} -> throw(?EISDIR);
        {false, FileCtx2} -> FileCtx2
    end.


-spec get_type(ctx()) -> {onedata_file:type(), ctx()}.
get_type(FileCtx) ->
    get_type(FileCtx, fun file_meta:get_type/1).


-spec get_effective_type(ctx()) -> {onedata_file:type(), ctx()}.
get_effective_type(FileCtx) ->
    get_type(FileCtx, fun file_meta:get_effective_type/1).


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


-spec assert_smaller_than_provider_support_size(ctx(), od_provider:id()) -> ctx().
assert_smaller_than_provider_support_size(FileCtx, ProviderId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_logic:get_support_size(SpaceId, ProviderId) of
        {ok, Size} ->
            {FileSize, FileCtx2} = file_ctx:get_file_size(FileCtx),
            case FileSize =< Size of
                true -> FileCtx2;
                false -> throw(?ENOSPC)
            end;
        {error, _} = Error ->
            throw(Error)
    end.


-spec assert_file_exists(ctx()) -> ctx() | no_return().
assert_file_exists(FileCtx0) ->
    % If file doesn't exists (or was deleted) fetching doc will fail,
    % {badmatch, {error, not_found}} will propagate up and fslogic_worker will
    % translate it to ?ENOENT
    {#document{}, FileCtx1} = file_ctx:get_file_doc(FileCtx0),
    FileCtx1.


-spec get_path_before_deletion(ctx()) -> file_meta:path() | undefined.
get_path_before_deletion(#file_ctx{path_before_deletion = PathBeforeDeletion}) ->
    PathBeforeDeletion.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec resolve_and_cache_path(ctx(), file_meta:path_type()) -> {file_meta:uuid() | file_meta:name(), ctx()}.
resolve_and_cache_path(FileCtx, PathType) ->
    try get_file_doc_including_deleted(FileCtx) of
        {#document{
            key = Uuid,
            value = #file_meta{
                type = FileType,
                name = Filename
            },
            scope = SpaceId
        } = Doc, FileCtx2} ->
            case FileType of
                ?DIRECTORY_TYPE ->
                    case paths_cache:get(SpaceId, Doc, PathType) of
                        {ok, Path} ->
                            {Path, FileCtx2};
                        {error, ?MISSING_FILE_META(_MissingUuid)} = Error ->
                            throw(Error)
                    end;
                _ ->
                    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
                    case file_meta:get_including_deleted(ParentUuid) of
                        {ok, ParentDoc} ->
                            case paths_cache:get(SpaceId, ParentDoc, PathType) of
                                {ok, Path} ->
                                    FilenameOrUuid = case PathType of
                                        ?CANONICAL_PATH -> Filename;
                                        ?UUID_BASED_PATH -> Uuid
                                    end,
                                    {filename:join(Path, FilenameOrUuid), FileCtx2};
                                {error, ?MISSING_FILE_META(_MissingUuid)} = Error ->
                                    throw(Error)
                            end;
                        {error, not_found} ->
                            throw({error, ?MISSING_FILE_META(ParentUuid)})
                    end
            end
    catch
        _:{badmatch, {error, not_found}} ->
            throw({error, ?MISSING_FILE_META(file_ctx:get_logical_uuid_const(FileCtx))})
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns local file location doc, creates it if it's not present
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_regular_file_location_doc(ctx(), fslogic_location_cache:get_doc_opts(),
    boolean()) -> {file_location:doc(), ctx()}.
get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, CheckLocationExists) ->
    get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, CheckLocationExists, 0).

-spec get_or_create_local_regular_file_location_doc(ctx(), fslogic_location_cache:get_doc_opts(),
    boolean(), non_neg_integer()) -> {file_location:doc(), ctx()}.
get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, true, QoSCheckSizeLimit) ->
    case get_local_file_location_doc_const(FileCtx, GetDocOpts) of
        undefined ->
            get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, false, QoSCheckSizeLimit);
        Location ->
            {Location, FileCtx}
    end;
get_or_create_local_regular_file_location_doc(FileCtx, GetDocOpts, _CheckLocationExists, QoSCheckSizeLimit) ->
    case fslogic_location:create_doc(FileCtx, false, false, QoSCheckSizeLimit) of
        {{ok, _}, FileCtx2} ->
            {LocationDocs, FileCtx3} = get_file_location_docs(FileCtx2, true, false),
            lists:foreach(fun(ChangedLocation) ->
                replica_dbsync_hook:on_file_location_change(FileCtx3, ChangedLocation, QoSCheckSizeLimit)
            end, LocationDocs),
            {get_local_file_location_doc_const(FileCtx3, GetDocOpts), FileCtx3};
        {{error, already_exists}, FileCtx2} ->
            % Possible race with file deletion - get including deleted
            {ok, Location} = fslogic_location_cache:get_local_location_including_deleted(FileCtx2, GetDocOpts),
            {Location, FileCtx2}
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
            ChosenDoc = lists:foldl(fun(
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

            case ChosenDoc of
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
-spec get_synced_gid(ctx()) -> {luma:gid() | undefined, ctx()}.
get_synced_gid(FileCtx) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            {get_dir_synced_gid_const(FileCtx), FileCtx2};
        {false, FileCtx2} ->
            {get_file_synced_gid_const(FileCtx2), FileCtx2}
    end.

%% @private
-spec get_file_synced_gid_const(ctx()) -> luma:gid() | undefined.
get_file_synced_gid_const(FileCtx) ->
    case get_local_file_location_doc_const(FileCtx, skip_local_blocks) of
        undefined ->
            undefined;
        FileLocation ->
            file_location:get_synced_gid(FileLocation)
    end.

%% @private
-spec get_dir_synced_gid_const(ctx()) -> luma:gid() | undefined.
get_dir_synced_gid_const(FileCtx) ->
    case get_dir_location_doc_const(FileCtx) of
        undefined ->
            undefined;
        DirLocation ->
            dir_location:get_synced_gid(DirLocation)
    end.


%% @private
-spec get_type(ctx(), fun((file_meta:doc()) -> onedata_file:type())) -> {onedata_file:type(), ctx()}.
get_type(FileCtx = #file_ctx{is_dir = true}, _) ->
    {?DIRECTORY_TYPE, FileCtx};
get_type(FileCtx, FileMetaFun) ->
    {#document{value = FileMeta}, FileCtx2} =
        get_file_doc_including_deleted(FileCtx),
    Type = FileMetaFun(FileMeta),
    IsDir = Type =:= ?DIRECTORY_TYPE,
    {Type, FileCtx2#file_ctx{is_dir = IsDir}}.


%% @private
-spec prepare_file_size_summary_from_existing_doc(file_location:doc()) -> file_size_summary().
prepare_file_size_summary_from_existing_doc(LocationDoc) ->
    case LocationDoc of
        #document{value = #file_location{size = undefined, storage_id = StorageId}} ->
            Blocks = fslogic_location_cache:get_blocks(LocationDoc),
            Size = fslogic_blocks:upper(Blocks),
            [{virtual, Size}, {StorageId, file_location:count_bytes(Blocks)}];
        #document{value = #file_location{size = Size, storage_id = StorageId}} ->
            [{virtual, Size}, {StorageId, file_location:count_bytes(fslogic_location_cache:get_blocks(LocationDoc))}]
    end.


%% @private
-spec times_record_to_map(times:record()) -> #{times_api:times_type() => times:time()}.
times_record_to_map(#times{creation_time = CreationTime, atime = ATime, mtime = MTime, ctime = CTime}) ->
    lists:foldl(fun
        ({_Time, 0}, Acc) -> Acc;
        ({Time, Value}, Acc) -> Acc#{Time => Value}
    end, #{}, [
        {?attr_creation_time, CreationTime},
        {?attr_atime, ATime},
        {?attr_mtime, MTime},
        {?attr_ctime, CTime}
    ]).


%% @private
-spec map_to_times_record(#{times_api:times_type() => times:time()}) -> times:record().
map_to_times_record(Map) ->
    #times{
        creation_time = maps:get(?attr_creation_time, Map, 0),
        atime = maps:get(?attr_atime, Map, 0),
        mtime = maps:get(?attr_mtime, Map, 0),
        ctime = maps:get(?attr_ctime, Map, 0)
    }.
