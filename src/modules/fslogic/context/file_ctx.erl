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
-include_lib("ctool/include/posix/errors.hrl").
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
    file_location_docs :: undefined | [file_location:doc()],
    file_location_ids :: undefined | [file_location:id()],
    acl :: undefined | acl:acl(),
    is_dir :: undefined | boolean(),
    is_import_on :: undefined | boolean(),
    extended_direct_io = false :: boolean(),
    storage_path_type :: helpers:storage_path_type(),
    mounted_in_root :: undefined | boolean()
}).

-type ctx() :: #file_ctx{}.

%% Functions creating context and filling its data
-export([new_by_canonical_path/2, new_by_guid/1, new_by_doc/3, new_root_ctx/0]).
-export([reset/1, new_by_partial_context/1, add_file_location/2, set_file_id/2,
    set_is_dir/2, set_extended_direct_io/2, update_location_doc/2]).

%% Functions that do not modify context
-export([get_share_id_const/1, get_space_id_const/1, get_space_dir_uuid_const/1,
    get_guid_const/1, get_uuid_const/1, get_extended_direct_io_const/1,
    set_storage_path_type/2, get_storage_path_type_const/1]).
-export([is_file_ctx_const/1, is_space_dir_const/1, is_user_root_dir_const/2,
    is_root_dir_const/1, has_acl_const/1, file_exists_const/1, is_in_user_space_const/2]).
-export([equals/2]).

%% Functions modifying context
-export([get_canonical_path/1, get_file_doc/1,
    get_file_doc_including_deleted/1, get_parent/2, get_storage_file_id/1,
    get_aliased_name/2, get_posix_storage_user_context/2, get_times/1,
    get_parent_guid/2, get_child/3, get_file_children/4, get_logical_path/2,
    get_storage_id/1, get_storage_doc/1, get_file_location_with_filled_gaps/2,
    get_or_create_local_file_location_doc/1, get_local_file_location_doc/1,
    get_file_location_ids/1, get_file_location_docs/1, get_acl/1,
    get_raw_storage_path/1, get_child_canonical_path/2, get_file_size/1,
    get_owner/1, get_group_owner/1, get_local_storage_file_size/1,
    is_import_on/1, get_and_cache_file_doc_including_deleted/1]).
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
    new_by_guid(fslogic_uuid:uuid_to_guid(?ROOT_DIR_UUID, undefined)).

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
    Guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
    #file_ctx{file_doc = Doc, guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Converts partial file context into file context. Which means that the function
%% fills GUID in file context record. This function is called when we know
%% that the file is locally supported.
%% @end
%%--------------------------------------------------------------------
-spec new_by_partial_context(file_partial_ctx:ctx()) ->
    {file_partial_ctx:ctx(), od_space:id() | undefined}.
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
        file_location_ids = [LocationId | Locations],
        file_location_docs = undefined
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
-spec get_share_id_const(user_ctx:ctx()) -> od_share:id() | undefined.
get_share_id_const(#file_ctx{guid = Guid}) ->
    {_FileUuid, _SpaceId, ShareId} = fslogic_uuid:unpack_share_guid(Guid),
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id() | undefined.
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
%% @todo remove this function and pass file_ctx wherever possible
%% @doc
%% Returns file UUID entry.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid_const(ctx()) -> file_meta:uuid().
get_uuid_const(FileCtx) ->
    Guid = get_guid_const(FileCtx),
    fslogic_uuid:guid_to_uuid(Guid).

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
            CanonicalPath = filename:join(generate_canonical_path(FileCtx)),
            {CanonicalPath, FileCtx#file_ctx{canonical_path = CanonicalPath}}
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
            CanonicalPathTokens = generate_canonical_path(FileCtx),
            CanonicalPath = filename:join(CanonicalPathTokens),
            {CanonicalPathTokens,
                FileCtx#file_ctx{canonical_path = CanonicalPath}}
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
    {ok, FileDoc} = file_meta:get({uuid, fslogic_uuid:guid_to_uuid(Guid)}),
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
    {file_meta:doc(), ctx()} | datastore:get_error().
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
get_parent(FileCtx = #file_ctx{parent = Parent}, _UserCtx) ->
    {Parent, FileCtx}.


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
%% Returns storage file ID (the ID of file on storage.
%% Storage file Id depends on the storage file mapping setting, currently
%% 2 options are supported:
%%   - canonical - which is POSIX-style file mapping including complete
%%                 directory path
%%   - flat - which provides a 3 level tree based on the FileUuid, enabling
%%            efficient rename operations without copying objects on the
%%            storage
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(ctx()) -> {StorageFileId :: helpers:file_id(), ctx()}.
get_storage_file_id(FileCtx0 = #file_ctx{storage_file_id = undefined}) ->
    case get_local_file_location_doc(FileCtx0) of
        {#document{value = #file_location{file_id = ID}}, FileCtx}
            when ID =/= undefined ->
            {ID, FileCtx};
        {_, FileCtx} ->
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
            end
    end;
get_storage_file_id(FileCtx = #file_ctx{storage_file_id = StorageFileId}) ->
    {StorageFileId, FileCtx}.

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
        andalso (not session:is_special(user_ctx:get_session_id(UserCtx))) of
        false ->
            get_name_of_nonspace_file(FileCtx);
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
%% Returns list of file children.
%% @end
%%--------------------------------------------------------------------
-spec get_file_children(ctx(), user_ctx:ctx(), Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    {Children :: [ctx()], NewFileCtx :: ctx()}.
get_file_children(FileCtx, UserCtx, Offset, Limit) ->
    case is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
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
                lists:map(fun(#child_link_uuid{name = Name, uuid = Uuid}) ->
                    new_child_by_uuid(Uuid, Name, SpaceId, ShareId)
                end, ChildrenLinks),
            {Children, FileCtx2}
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
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_with_filled_gaps(ctx(), fslogic_blocks:block() | undefined) ->
    {#file_location{}, ctx()}.
get_file_location_with_filled_gaps(FileCtx, ReqRange) ->
    % get locations
    {Locations, FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    {#document{value = FileLocation = #file_location{
        blocks = Blocks,
        size = Size
    }
    }, FileCtx3} = file_ctx:get_or_create_local_file_location_doc(FileCtx2),

    % find gaps
    AllRanges = lists:foldl(
        fun(#document{value = #file_location{blocks = _Blocks}}, Acc) ->
            fslogic_blocks:merge(Acc, _Blocks)
        end, [], Locations),
    RequestedRange = utils:ensure_defined(ReqRange, undefined, #file_block{offset = 0, size = Size}),
    ExtendedRequestedRange = case RequestedRange of
        #file_block{offset = O, size = S} when O + S < Size ->
            RequestedRange#file_block{size = Size - O};
        _ -> RequestedRange
    end,
    Gaps = fslogic_blocks:consolidate(
        fslogic_blocks:invalidate([ExtendedRequestedRange], AllRanges)
    ),
    BlocksWithFilledGaps = fslogic_blocks:merge(Blocks, Gaps),

    % fill gaps transform uid and emit
    {FileLocation#file_location{
        uuid = file_ctx:get_uuid_const(FileCtx),
        blocks = BlocksWithFilledGaps
    }, FileCtx3}.

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_file_location_doc(FileCtx) ->
    case is_dir(FileCtx) of
        {true, FileCtx2} ->
            {undefined, FileCtx2};
        {false, FileCtx2} ->
            get_or_create_local_regular_file_location_doc(FileCtx2)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns local file location doc.
%% @end
%%--------------------------------------------------------------------
-spec get_local_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_local_file_location_doc(FileCtx) ->
    FileUuid = get_uuid_const(FileCtx),
    LocalLocationId = file_location:local_id(FileUuid),
    case file_location:get(LocalLocationId) of
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
get_file_location_docs(FileCtx = #file_ctx{file_location_docs = undefined}) ->
    {LocationIds, FileCtx2} = get_file_location_ids(FileCtx),
    LocationDocs = lists:filtermap(fun(LocId) ->
        case file_location:get(LocId) of
            {ok, Location} ->
                {true, Location};
            _Error ->
                false
        end
    end, LocationIds),
    {LocationDocs, FileCtx2#file_ctx{file_location_docs = LocationDocs}};
get_file_location_docs(FileCtx = #file_ctx{file_location_docs = LocationDocs}) ->
    {LocationDocs, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Updates file location docs.
%% @end
%%--------------------------------------------------------------------
-spec update_location_doc(ctx(), file_location:doc()) -> ctx().
update_location_doc(#file_ctx{file_location_docs = undefined} = FileCtx, _Doc) ->
    FileCtx;
update_location_doc(#file_ctx{file_location_docs = LocationDocs} = FileCtx,
    #document{key = FileLocationId} = Doc) ->
    LocationDocs2 = lists:map(fun(#document{key = LocId} = LocDoc) ->
        case LocId of
            FileLocationId -> Doc;
            _ -> LocDoc
        end
    end, LocationDocs),
    FileCtx#file_ctx{file_location_docs = LocationDocs2}.

%%--------------------------------------------------------------------
%% @doc
%% Returns file Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(ctx()) -> {undefined | acl:acl(), ctx()}.
get_acl(FileCtx = #file_ctx{acl = undefined}) ->
    Acl = acl:get(FileCtx),
    {Acl, FileCtx#file_ctx{acl = Acl}};
get_acl(FileCtx = #file_ctx{acl = Acl}) ->
    {Acl, FileCtx}.

%%--------------------------------------------------------------------
%% @doc
%% Returns size of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_size(file_ctx:ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), file_ctx:ctx()}.
get_file_size(FileCtx) ->
    case file_ctx:get_local_file_location_doc(FileCtx) of
        {#document{
            value = #file_location{
                size = undefined,
                blocks = Blocks
            }
        }, FileCtx2} ->
            {fslogic_blocks:upper(Blocks), FileCtx2};
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
    case file_ctx:get_local_file_location_doc(FileCtx) of
        {#document{
            value = #file_location{
                blocks = Blocks
            }
        }, FileCtx2} ->
            {fslogic_blocks:size(Blocks), FileCtx2};
        {undefined, FileCtx2} ->
            {0, FileCtx2}
    end.

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
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(Guid))),
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
    UserRootDirUuid == fslogic_uuid:guid_to_uuid(Guid);
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
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    fslogic_uuid:is_root_dir_uuid(Uuid);
is_root_dir_const(#file_ctx{}) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has Access Control List defined.
%% @end
%%--------------------------------------------------------------------
-spec has_acl_const(ctx()) -> boolean().
has_acl_const(FileCtx = #file_ctx{acl = undefined}) ->
    acl:exists(FileCtx);
has_acl_const(_) ->
    true.

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
    #file_ctx{guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId), file_name = Name}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates canonical path
%% @end
%%--------------------------------------------------------------------
-spec generate_canonical_path(ctx()) -> [file_meta:name()].
generate_canonical_path(FileCtx) ->
    case is_root_dir_const(FileCtx) of
        true ->
            [<<"/">>];
        false ->
            {ParentCtx, FileCtx2} = get_parent(FileCtx, undefined),
            case is_space_dir_const(FileCtx2) of
                true ->
                    SpaceId = get_space_id_const(FileCtx2),
                    generate_canonical_path(ParentCtx) ++ [SpaceId];
                false ->
                    {Name, _FileCtx3} = get_name_of_nonspace_file(FileCtx2),
                    generate_canonical_path(ParentCtx) ++ [Name]
            end
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
%% Returns name of file that is not a space.
%% @end
%%--------------------------------------------------------------------
-spec get_name_of_nonspace_file(ctx()) -> {file_meta:name(), ctx()} | no_return().
get_name_of_nonspace_file(FileCtx = #file_ctx{file_name = undefined}) ->
    {#document{value = #file_meta{name = Name}}, FileCtx2} = get_file_doc_including_deleted(FileCtx),
    {Name, FileCtx2#file_ctx{file_name = Name}};
get_name_of_nonspace_file(FileCtx = #file_ctx{file_name = FileName}) ->
    {FileName, FileCtx}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns local file location doc, creates it if it's not present
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_local_regular_file_location_doc(ctx()) ->
    {file_location:doc() | undefined, ctx()}.
get_or_create_local_regular_file_location_doc(FileCtx) ->
    case get_local_file_location_doc(FileCtx) of
        {undefined, FileCtx2} ->
            {CreatedLocation, FileCtx3} =
                sfm_utils:create_storage_file_location(FileCtx2, false),
            {LocationDocs, FileCtx4} = get_file_location_docs(FileCtx3),
            lists:map(fun(ChangedLocation) ->
                replica_dbsync_hook:on_file_location_change(FileCtx4, ChangedLocation)
            end, LocationDocs),
            FileUuid = get_uuid_const(FileCtx),

            {
                #document{
                    key = file_location:local_id(FileUuid),
                    value = CreatedLocation
                },
                FileCtx4
            };
        {Location, FileCtx2} ->
            {Location, FileCtx2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file. File size is calculated from remote locations.
%% @end
%%--------------------------------------------------------------------
-spec get_file_size_from_remote_locations(file_ctx:ctx() | file_meta:uuid()) ->
    {Size :: non_neg_integer(), file_ctx:ctx()}.
get_file_size_from_remote_locations(FileCtx) ->
    {LocationDocs, FileCtx2} = get_file_location_docs(FileCtx),
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
                        size = undefined,
                        blocks = Blocks
                    }
                } ->
                    {fslogic_blocks:upper(Blocks), FileCtx2};
                #document{value = #file_location{size = Size}} ->
                    {Size, FileCtx2}
            end
    end.
