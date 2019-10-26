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
    guid :: fslogic_worker:file_guid(),
    file_doc :: undefined | file_meta:doc(),
    parent :: undefined | ctx(),
    storage_file_id :: undefined | helpers:file_id(),
    space_name :: undefined | od_space:name(),
    storage_posix_user_context :: undefined | luma:posix_user_ctx(),
    times :: undefined | times:times(),
    file_name :: undefined | file_meta:name(),
    storage_doc :: undefined | storage:doc(),
    file_location_ids :: undefined | [file_location:id()],
    is_dir :: undefined | boolean(),
    is_import_on :: undefined | boolean(),
    extended_direct_io = false :: boolean(),
    storage_path_type :: undefined | helpers:storage_path_type(),
    mounted_in_root :: undefined | boolean(),
    storage_sync_info :: undefined | storage_sync_info:doc()
}).

-type ctx() :: #file_ctx{}.
-export_type([ctx/0]).

%% Functions creating context and filling its data
-export([new_by_canonical_path/2, new_by_guid/1, new_by_doc/3, new_root_ctx/0]).
-export([reset/1, new_by_partial_context/1, add_file_location/2, set_file_id/2,
    set_is_dir/2, set_extended_direct_io/2]).

%% Functions that do not modify context
-export([get_share_id_const/1, get_space_id_const/1, get_space_dir_uuid_const/1,
    get_guid_const/1, get_uuid_const/1, get_extended_direct_io_const/1,
    set_storage_path_type/2, get_storage_path_type_const/1, get_mounted_in_root/1,
    get_canonical_path_tokens/1]).
-export([is_file_ctx_const/1, is_space_dir_const/1, is_user_root_dir_const/2,
    is_root_dir_const/1, file_exists_const/1, is_in_user_space_const/2]).
-export([equals/2]).

%% Functions modifying context
-export([get_canonical_path/1, get_file_doc/1,
    get_file_doc_including_deleted/1, get_parent/2, get_ancestors_guids/1,
    get_storage_file_id/1, get_storage_file_id/2,
    get_new_storage_file_id/1, get_aliased_name/2, get_posix_storage_user_context/2, get_times/1,
    get_parent_guid/2, get_child/3,
    get_file_children/4, get_file_children/5, get_file_children/6,
    get_logical_path/2,
    get_storage_id/1, get_storage_doc/1, get_file_location_with_filled_gaps/1,
    get_file_location_with_filled_gaps/2, fill_location_gaps/4,
    get_or_create_local_file_location_doc/1, get_local_file_location_doc/1,
    get_local_file_location_doc/2, get_or_create_local_file_location_doc/2, get_or_create_local_regular_file_location_doc/3,
    get_file_location_ids/1, get_file_location_docs/1, get_file_location_docs/2,
    get_active_perms_type/1, get_acl/1, get_mode/1, get_raw_storage_path/1, get_child_canonical_path/2,
    get_file_size/1, get_file_size_from_remote_locations/1, get_owner/1, get_group_owner/1, get_local_storage_file_size/1,
    is_import_on/1, get_and_cache_file_doc_including_deleted/1, get_dir_location_doc/1,
    get_storage_sync_info/1]).
-export([is_dir/1]).

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
    new_by_guid(file_id:pack_guid(?ROOT_DIR_UUID, undefined)).

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
%% Sets extended_direct_io field in context record
%% @end
%%--------------------------------------------------------------------
-spec set_extended_direct_io(ctx(), boolean()) -> ctx().
set_extended_direct_io(FileCtx, Created) ->
    FileCtx#file_ctx{extended_direct_io = Created}.

%%--------------------------------------------------------------------
%% @doc
%% Returns value of extended_direct_io field.
%% @end
%%--------------------------------------------------------------------
-spec get_extended_direct_io_const(ctx()) -> boolean().
get_extended_direct_io_const(#file_ctx{extended_direct_io = Ans}) ->
    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Sets storage_path_type field in context record
%% @end
%%--------------------------------------------------------------------
-spec set_storage_path_type(ctx(), helpers:storage_path_type()) -> ctx().
set_storage_path_type(FileCtx, StoragePathType) ->
    FileCtx#file_ctx{storage_path_type = StoragePathType}.

%%--------------------------------------------------------------------
%% @doc
%% Returns value of storage_path_type field.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_path_type_const(ctx()) -> helpers:storage_path_type().
get_storage_path_type_const(#file_ctx{storage_path_type = StoragePathType}) ->
    StoragePathType.

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
-spec get_space_id_const(ctx()) -> od_space:id() | undefined.
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
            {Path, FileCtx2} = generate_canonical_path(FileCtx),
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
            {CanonicalPathTokens, FileCtx2} = generate_canonical_path(FileCtx),
            CanonicalPath = filename:join(CanonicalPathTokens),
            {CanonicalPathTokens,
                FileCtx2#file_ctx{canonical_path = CanonicalPath}}
    end;
get_canonical_path_tokens(FileCtx = #file_ctx{canonical_path = Path}) ->
    {fslogic_path:split(Path), FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's uuid path (i.e "/SpaceId/A/B/C/FileUuid").
%% If canonical_path entry is undefined, it will be set to canonical
%% path for compatibility with other modules, but the returned path
%% will be flat.
%% @end
%%--------------------------------------------------------------------
-spec get_flat_path_const(ctx()) -> file_meta:path().
get_flat_path_const(FileCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            <<"/">>;
        false ->
            filename:join(generate_flat_path(FileCtx))
    end.

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
%% Returns if space is mounted in root.
%% @end
%%--------------------------------------------------------------------
-spec get_mounted_in_root(ctx()) -> {boolean(), ctx()}.
get_mounted_in_root(FileCtx = #file_ctx{mounted_in_root = undefined}) ->
    SpaceId = get_space_id_const(FileCtx),
    {ok, SS} = space_storage:get(SpaceId),
    InRootIDs = space_storage:get_mounted_in_root(SS),
    {StorageDoc, FileCtx2} = get_storage_doc(FileCtx),
    MiR = lists:member(StorageDoc#document.key, InRootIDs),
    {MiR, FileCtx2#file_ctx{mounted_in_root = MiR}};
get_mounted_in_root(FileCtx = #file_ctx{mounted_in_root = MiR}) ->
    {MiR, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's file_meta document even if its marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc_including_deleted(ctx()) -> {file_meta:doc(), ctx()}.
get_file_doc_including_deleted(FileCtx = #file_ctx{file_doc = undefined}) ->
    FileUuid = get_uuid_const(FileCtx),
    {ok, Doc} = file_meta:get_including_deleted(FileUuid),
    case {Doc#document.value#file_meta.deleted, Doc#document.deleted} of
        {false, false} ->
            {Doc, FileCtx#file_ctx{file_doc = Doc}};
        _ ->
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
%% Returns parent's file context.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(ctx(), user_ctx:ctx() | undefined) ->
    {ParentFileCtx :: ctx(), NewFileCtx :: ctx()}.
get_parent(FileCtx = #file_ctx{parent = undefined}, UserCtx) ->
    {Doc, FileCtx2} = get_file_doc_including_deleted(FileCtx),
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
    ParentGuid =
        case fslogic_uuid:is_root_dir_uuid(ParentUuid) of
            true ->
                case ParentUuid =:= ?ROOT_DIR_UUID
                    andalso UserCtx =/= undefined
                    andalso user_ctx:is_normal_user(UserCtx)
                of
                    true ->
                        UserId = user_ctx:get_user_id(UserCtx),
                        fslogic_uuid:user_root_dir_guid(UserId);
                    _ ->
                        file_id:pack_guid(ParentUuid, undefined)
                end;
            false ->
                SpaceId = get_space_id_const(FileCtx2),
                case get_share_id_const(FileCtx2) of
                    undefined ->
                        file_id:pack_guid(ParentUuid, SpaceId);
                    ShareId ->
                        file_id:pack_share_guid(ParentUuid, SpaceId, ShareId)
                end
        end,
    Parent = new_by_guid(ParentGuid),
    {Parent, FileCtx2#file_ctx{parent = Parent}};
get_parent(FileCtx = #file_ctx{parent = Parent}, _UserCtx) ->
    {Parent, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's ancestors.
%% @end
%%--------------------------------------------------------------------
-spec get_ancestors_guids(ctx()) -> {[file_id:file_guid()], ctx()}.
get_ancestors_guids(FileCtx0) ->
    case is_root_dir_const(FileCtx0) of
        true ->
            {[], FileCtx0};
        false ->
            {ParentCtx0, FileCtx1} = file_ctx:get_parent(FileCtx0, undefined),
            {AncestorsGuids, ParentCtx1} = get_ancestors_guids(ParentCtx0),
            FileCtx2 = FileCtx1#file_ctx{parent = ParentCtx1},
            {[get_guid_const(ParentCtx1) | AncestorsGuids], FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns GUID of parent or undefined when the file is a root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid(ctx(), user_ctx:ctx()) -> {fslogic_worker:file_guid(), ctx()}.
get_parent_guid(FileCtx, UserCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            {undefined, FileCtx};
        false ->
            {ParentFile, FileCtx2} = get_parent(FileCtx, UserCtx),
            ParentGuid = get_guid_const(ParentFile),
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
    case get_local_file_location_doc(FileCtx0, false) of
        {#document{value = #file_location{file_id = ID, storage_file_created = SFC}}, FileCtx}
            when ID =/= undefined, SFC or Generate ->
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
    end;
get_storage_file_id(FileCtx = #file_ctx{storage_file_id = StorageFileId}, _) ->
    {StorageFileId, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Generates and returns storage file ID (the ID of file on storage).
%% @end
%%--------------------------------------------------------------------
-spec get_new_storage_file_id(ctx()) -> {StorageFileId :: helpers:file_id(), ctx()}.
get_new_storage_file_id(FileCtx) ->
    {StorageDoc, _} = file_ctx:get_storage_doc(FileCtx),
    #document{value = #storage{helpers
    = [#helper{storage_path_type = StoragePathType} | _]}} = StorageDoc,
    case StoragePathType of
        ?FLAT_STORAGE_PATH ->
            FileId = get_flat_path_const(FileCtx),
            % TODO - do not get_canonical_path (fix acceptance tests before)
            {_, FileCtx2} = get_canonical_path(FileCtx),
            {FileId, FileCtx2#file_ctx{storage_file_id = FileId}};
        ?CANONICAL_STORAGE_PATH ->
            {FileIdTokens, FileCtx2} = get_canonical_path_tokens(FileCtx),
            {MiR, FileCtx3} = get_mounted_in_root(FileCtx2),
            FileId = case {MiR, FileIdTokens} of
                {true, [Root, _SpaceID | Path]} ->
                    filename:join([Root | Path]);
                _ ->
                    filename:join(FileIdTokens)
            end,

            {FileId, FileCtx3#file_ctx{storage_file_id = FileId}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns raw path on storage.
%% @end
%%--------------------------------------------------------------------
-spec get_raw_storage_path(ctx()) -> {StorageFileId :: helpers:file_id(), ctx()}.
get_raw_storage_path(FileCtx = #file_ctx{is_import_on = true}) ->
    {FileId, FileCtx2} = get_storage_file_id(FileCtx),
    SpaceId = get_space_id_const(FileCtx2),
    {StorageId, FileCtx3} = get_storage_id(FileCtx2),
    {filename_mapping:to_storage_path(SpaceId, StorageId, FileId), FileCtx3};
get_raw_storage_path(FileCtx = #file_ctx{is_import_on = false}) ->
    {FileId, FileCtx2} = get_storage_file_id(FileCtx),
    {FileId, FileCtx2};
get_raw_storage_path(FileCtx) ->
    {_, FileCtx2} = is_import_on(FileCtx),
    get_raw_storage_path(FileCtx2).

%%--------------------------------------------------------------------
%% @doc
%% Returns information about import settings in space when file is stored.
%% @end
%%--------------------------------------------------------------------
-spec is_import_on(ctx()) -> {boolean(), ctx()}.
is_import_on(FileCtx = #file_ctx{is_import_on = undefined}) ->
    SpaceId = get_space_id_const(FileCtx),
    IsOn = space_strategies:is_import_on(SpaceId),
    {IsOn, FileCtx#file_ctx{is_import_on = IsOn}};
is_import_on(FileCtx = #file_ctx{is_import_on = IsOn}) ->
    {IsOn, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns name of the space where the file is located.
%% @end
%%--------------------------------------------------------------------
-spec get_space_name(ctx(), user_ctx:ctx()) ->
    {od_space:name(), ctx()} | no_return().
get_space_name(FileCtx = #file_ctx{space_name = undefined}, UserCtx) ->
    UserDoc = user_ctx:get_user(UserCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    SpaceId = get_space_id_const(FileCtx),
    case user_logic:has_eff_space(UserDoc, SpaceId) of
        false ->
            throw(?ENOENT);
        true ->
            {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
            {SpaceName, FileCtx#file_ctx{space_name = SpaceName}}
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
    case is_space_dir_const(FileCtx)
        andalso UserCtx =/= undefined
        andalso (not session_utils:is_special(user_ctx:get_session_id(UserCtx))) of
        false ->
            {#document{value = #file_meta{name = Name}}, FileCtx2} = get_file_doc_including_deleted(FileCtx),
            {Name, FileCtx2};
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
get_posix_storage_user_context(
    FileCtx = #file_ctx{storage_posix_user_context = undefined}, UserCtx) ->
    IsSpaceDir = is_space_dir_const(FileCtx),
    IsUserRootDir = is_root_dir_const(FileCtx),
    SpaceId = get_space_id_const(FileCtx),
    NewUserCtx = case IsSpaceDir orelse IsUserRootDir of
        true ->
            FileCtx2 = FileCtx,
            luma:get_posix_user_ctx(?ROOT_SESS_ID, ?ROOT_USER_ID, SpaceId);
        false ->
            {#document{
                value = #file_meta{
                    owner = OwnerId,
                    group_owner = GroupOwnerId
            }}, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
            SessionId = user_ctx:get_session_id(UserCtx),
            luma:get_posix_user_ctx(SessionId, OwnerId, GroupOwnerId, SpaceId)
    end,
    {NewUserCtx, FileCtx2#file_ctx{storage_posix_user_context = NewUserCtx}};
get_posix_storage_user_context(
    FileCtx = #file_ctx{storage_posix_user_context = UserCtx}, _UserCtx) ->
    {UserCtx, FileCtx}.

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
    {ParentPath, ParentCtx2} = file_ctx:get_canonical_path(ParentCtx),
    CanonicalPath = filename:join(ParentPath, FileName),
    {CanonicalPath, ParentCtx2}.


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_file_children(FileCtx, UserCtx, Offset, Limit, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(),
    Offset :: integer(), Limit :: non_neg_integer()
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
    Offset :: integer(),
    Limit :: non_neg_integer(),
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
    Offset :: integer(),
    Limit :: non_neg_integer(),
    Token :: undefined | datastore_links_iter:token(),
    StartId :: undefined | file_meta:name()
) ->
    {Children :: [ctx()], NewToken :: datastore_links_iter:token(), NewFileCtx :: ctx()} |
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit, Token, StartId) ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {list_user_spaces(UserCtx, Offset, Limit), FileCtx};
        false ->
            {FileDoc = #document{}, FileCtx2} = get_file_doc(FileCtx),
            SpaceId = get_space_id_const(FileCtx2),
            ShareId = get_share_id_const(FileCtx2),
            MapFun = fun(#child_link_uuid{name = Name, uuid = Uuid}) ->
                new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
            end,

            case file_meta:list_children(FileDoc, Offset, Limit, Token, StartId) of
                {ok, ChildrenLinks, #{token := Token2}} ->
                    {lists:map(MapFun, ChildrenLinks), Token2, FileCtx2};
                {ok, ChildrenLinks, _} ->
                    {lists:map(MapFun, ChildrenLinks), FileCtx2}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage id.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(ctx()) -> {storage:id(), ctx()}.
get_storage_id(FileCtx) ->
    {#document{key = StorageId}, FileCtx2} = get_storage_doc(FileCtx),
    {StorageId, FileCtx2}.

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
    {Locations, FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    {FileLocationDoc, FileCtx3} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx2),
    {fill_location_gaps(ReqRange, FileLocationDoc, Locations,
        file_ctx:get_uuid_const(FileCtx3)), FileCtx3};
get_file_location_with_filled_gaps(FileCtx, ReqRange) ->
    get_file_location_with_filled_gaps(FileCtx, [ReqRange]).

%%--------------------------------------------------------------------
%% @doc
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid.
%% @end
%%--------------------------------------------------------------------
-spec fill_location_gaps(fslogic_blocks:blocks() | undefined, file_location:doc(),
    [file_location:doc()], file_location:id()) -> #file_location{}.
fill_location_gaps(ReqRange0, #document{value = FileLocation = #file_location{
    size = Size}} = FileLocationDoc, Locations, Uuid) ->
    ReqRange = utils:ensure_defined(ReqRange0, undefined,
        [#file_block{offset = 0, size = Size}]),
    Blocks = fslogic_location_cache:get_blocks(FileLocationDoc,
        #{overlapping_blocks => ReqRange}),

    % find gaps
    AllRanges = lists:foldl(
        fun(Doc, Acc) ->
            fslogic_blocks:merge(Acc, fslogic_location_cache:get_blocks(Doc,
                #{overlapping_blocks => ReqRange}))
        end, [], Locations),
    ExtendedRequestedRange = lists:map(fun(RequestedRange) ->
        case RequestedRange of
            #file_block{offset = O, size = S} when O + S < Size ->
                RequestedRange#file_block{size = Size - O};
            _ -> RequestedRange
        end
    end, ReqRange),
    Gaps = fslogic_blocks:consolidate(
        fslogic_blocks:invalidate(ExtendedRequestedRange, AllRanges)
    ),
    BlocksWithFilledGaps = fslogic_blocks:merge(Blocks, Gaps),

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
-spec get_or_create_local_file_location_doc(ctx(), boolean()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_file_location_doc(FileCtx, IncludeBlocks) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            {undefined, FileCtx2};
        {false, FileCtx2} ->
            get_or_create_local_regular_file_location_doc(FileCtx2,
                IncludeBlocks, true)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_local_file_location_doc(FileCtx, true)
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_local_file_location_doc(FileCtx) ->
    get_local_file_location_doc(FileCtx, true).

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx(), boolean()) ->
    {file_location:doc() | undefined, ctx()}.
get_local_file_location_doc(FileCtx, IncludeBlocks) ->
    FileUuid = get_uuid_const(FileCtx),
    LocalLocationId = file_location:local_id(FileUuid),
    case fslogic_location_cache:get_location(LocalLocationId, FileUuid,
        IncludeBlocks) of
        {ok, Location} ->
            {Location, FileCtx};
        {error, not_found} ->
            {undefined, FileCtx}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns local dir location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_dir_location_doc(ctx()) ->
    {dir_location:doc() | undefined, ctx()}.
get_dir_location_doc(FileCtx) ->
    FileUuid = get_uuid_const(FileCtx),
    case dir_location:get(FileUuid) of
        {ok, Location} ->
            {Location, FileCtx};
        {error, not_found} ->
            {undefined, FileCtx}
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
-spec get_active_perms_type(ctx()) -> {file_meta:permissions_type(), ctx()}.
get_active_perms_type(FileCtx = #file_ctx{file_doc = #document{
    value = #file_meta{acl = []}
}}) ->
    {posix, FileCtx};
get_active_perms_type(FileCtx = #file_ctx{file_doc = #document{
    value = #file_meta{}
}}) ->
    {acl, FileCtx};
get_active_perms_type(FileCtx) ->
    {_, FileCtx2} = get_file_doc(FileCtx),
    get_active_perms_type(FileCtx2).

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
-spec get_file_size(file_ctx:ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), file_ctx:ctx()}.
get_file_size(FileCtx) ->
    case file_ctx:get_local_file_location_doc(FileCtx, false) of
        {#document{
            value = #file_location{
                size = undefined
            }
        } = FL, FileCtx2} ->
            {#document{
                value = #file_location{
                    size = undefined
                }
            } = FL, _} = file_ctx:get_local_file_location_doc(FileCtx, true),
            {fslogic_blocks:upper(fslogic_location_cache:get_blocks(FL)), FileCtx2};
        {#document{value = #file_location{size = Size}}, FileCtx2} ->
            {Size, FileCtx2};
        {undefined, FileCtx2} ->
            get_file_size_from_remote_locations(FileCtx2)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns size of file on local storage
%% @end
%%--------------------------------------------------------------------
-spec get_local_storage_file_size(file_ctx:ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), file_ctx:ctx()}.
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
%% Returns id of file's group_owner.
%% @end
%%--------------------------------------------------------------------
-spec get_group_owner(ctx()) -> {od_group:id() | undefined, ctx()}.
get_group_owner(FileCtx = #file_ctx{
    file_doc = #document{
        value = #file_meta{group_owner = GroupOwnerId}
    }}) ->
    {GroupOwnerId, FileCtx};
get_group_owner(FileCtx) ->
    {_, FileCtx2} = get_file_doc(FileCtx),
    get_group_owner(FileCtx2).

%%-------------------------------------------------------------------
%% @doc
%% Returns local #storage_sync_info document for given FileCtx.
%% @end
%%-------------------------------------------------------------------
-spec get_storage_sync_info(ctx()) -> {storage_sync_info:doc() | undefined, ctx()}.
get_storage_sync_info(FileCtx = #file_ctx{storage_sync_info = undefined}) ->
    {FilePath, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    SpaceId = get_space_id_const(FileCtx2),
    case storage_sync_info:get(FilePath, SpaceId) of
        {error, _Reason} ->
            {undefined, FileCtx2};
        {ok, StorageSyncInfo} ->
            {StorageSyncInfo, FileCtx2#file_ctx{storage_sync_info = StorageSyncInfo}}
    end;
get_storage_sync_info(FileCtx = #file_ctx{storage_sync_info = StorageSyncInfo}) ->
    {StorageSyncInfo, FileCtx}.


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
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(file_id:guid_to_uuid(Guid))),
    is_binary(SpaceId).

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
%% Checks if file is located in space accessible by user.
%% @end
%%--------------------------------------------------------------------
-spec is_in_user_space_const(ctx(), user_ctx:ctx()) -> boolean().
is_in_user_space_const(FileCtx, UserCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            true;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates canonical path
%% @end
%%--------------------------------------------------------------------
-spec generate_canonical_path(ctx()) -> {[file_meta:name()], ctx()}.
generate_canonical_path(FileCtx) ->
    Callback = fun([#document{key = Uuid, value = #file_meta{name = Name}}, ParentValue, CalculationInfo]) ->
        case fslogic_uuid:is_root_dir_uuid(Uuid) of
            true ->
                {ok, [<<"/">>], CalculationInfo};
            false ->
                SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(Uuid)),
                case is_binary(SpaceId) of
                    true ->
                        {ok, [<<"/">>, SpaceId], CalculationInfo};
                    false ->
                        {ok, ParentValue ++ [Name], CalculationInfo}
                end
        end
    end,
    {#document{value = #file_meta{name = FileName, type = FileType}, scope = Space} = Doc, FileCtx2} =
        get_file_doc_including_deleted(FileCtx),
    CacheName = location_and_link_utils:get_cannonical_paths_cache_name(Space),
    case FileType of
        ?DIRECTORY_TYPE ->
            {ok, Path, _} = effective_value:get_or_calculate(CacheName, Doc, Callback),
            {Path, FileCtx2};
        _ ->
            {ok, ParentDoc} = file_meta:get_parent(Doc),
            {ok, Path, _} = effective_value:get_or_calculate(CacheName, ParentDoc, Callback),
            {Path ++ [FileName], FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates Uuid based flat path. Uuid based path is for storages, which
%% do not have POSIX style file paths (e.g. object stores) and
%% do not provide rename on files on the storage without necessity
%% to copy and delete.
%% The paths have a flat 3-level tree namespace based on the first characters,
%% e.g. "/SpaceId/A/B/C/ABCyasd7321r5ssasdd7asdsafdfvsd"
%% @end
%%--------------------------------------------------------------------
-spec generate_flat_path(ctx()) -> [file_meta:name()].
generate_flat_path(FileCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            [<<"/">>];
        false ->
            {_, FileCtx2} = get_parent(FileCtx, undefined),
            SpaceId = get_space_id_const(FileCtx2),
            case is_space_dir_const(FileCtx2) of
                true ->
                    [<<"/">>, SpaceId];
                false ->
                    FileUuid = get_uuid_const(FileCtx2),
                    FileUuidStr = binary_to_list(FileUuid),
                    case length(FileUuidStr) > 3 of
                        true ->
                            [<<"/">>, SpaceId,
                              list_to_binary(string:substr(FileUuidStr, 1, 1)),
                              list_to_binary(string:substr(FileUuidStr, 2, 1)),
                              list_to_binary(string:substr(FileUuidStr, 3, 1)),
                              FileUuid];
                        false ->
                            [<<"/">>, SpaceId, <<"other">>, FileUuid]
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns local file location doc, creates it if it's not present
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_regular_file_location_doc(ctx(), boolean(), boolean()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_regular_file_location_doc(FileCtx, IncludeBlocks, true) ->
    case get_local_file_location_doc(FileCtx, IncludeBlocks) of
        {undefined, FileCtx2} ->
            get_or_create_local_regular_file_location_doc(FileCtx2, IncludeBlocks, false);
        {Location, FileCtx2} ->
            {Location, FileCtx2}
    end;
get_or_create_local_regular_file_location_doc(FileCtx, _IncludeBlocks, _CheckLocationExists) ->
    {CreatedLocation, FileCtx2, New} =
        location_and_link_utils:get_new_file_location_doc(FileCtx, false, false),
    FileCtx4 = case New of
        true ->
            {LocationDocs, FileCtx3} = get_file_location_docs(FileCtx2, true, false),
            lists:foreach(fun(ChangedLocation) ->
                replica_dbsync_hook:on_file_location_change(FileCtx3, ChangedLocation)
            end, LocationDocs),
            FileCtx3;
        _ ->
            FileCtx2
    end,
    FileUuid = get_uuid_const(FileCtx),
    {
        #document{
            key = file_location:local_id(FileUuid),
            value = CreatedLocation
        },
        FileCtx4
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file. File size is calculated from remote locations.
%% @end
%%--------------------------------------------------------------------
-spec get_file_size_from_remote_locations(file_ctx:ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), file_ctx:ctx()}.
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
-spec list_user_spaces(user_ctx:ctx(), Offset :: non_neg_integer(),
    Limit :: non_neg_integer()) -> Children :: [ctx()].
list_user_spaces(UserCtx, Offset, Limit) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    #document{value = #od_user{eff_spaces = Spaces}} = user_ctx:get_user(UserCtx),
    case Offset < length(Spaces) of
        true ->
            SpacesChunk = lists:sublist(Spaces, Offset + 1, Limit),
            Children = lists:map(fun(SpaceId) ->
                {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
                SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                new_child_by_uuid(SpaceDirUuid, SpaceName, SpaceId, undefined)
            end, SpacesChunk),
            Children;
        false ->
            []
    end.
