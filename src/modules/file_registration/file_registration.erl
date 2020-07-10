%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements mechanism that allows to register regular
%%% files located on external storage resources.
%%% TODO VFS-6508 abstract mechanisms used by sync and file_registration
%%% TODO VFS-6509 handle conflicts of file_registration and creation of files via LFM
%%% TODO VFS-6512 add new type of storage that enables registration of files
%%% TODO VFS-6513 add tests of error handling for registering files
%%% @end
%%%-------------------------------------------------------------------
-module(file_registration).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register/6, create_missing_directory/3]).

-type spec() :: json_utils:json_map().
%% structure of spec() map is described below
%% #{
%%       % required fields
%%       <<"spaceId">> => od_space:id(),
%%       <<"storageId">> => storage:id(),
%%       <<"storageFileId">> => helpers:file_id(),
%%       <<"destinationPath">> => file_meta:path(),
%%
%%       % optional fields
%%       <<"size">> => non_neg_integer(),
%%       <<"mode">> => non_neg_integer(),
%%       <<"atime">> => non_neg_integer(),
%%       <<"mtime">> => non_neg_integer(),
%%       <<"ctime">> => non_neg_integer(),
%%       <<"uid">> => non_neg_integer(),
%%       <<"gid">> => non_neg_integer(),
%%       <<"verifyExistence">> => boolean(),
%%       <<"xattrs">> => json_utils:json_map()
%% }

-define(DEFAULT_STAT, begin
    __T = time_utils:system_time_seconds(),
    #statbuf{
        % st_size is not set intentionally as we cannot assume default size
        st_mtime = __T,
        st_atime = __T,
        st_ctime = __T,
        st_mode = ?DEFAULT_FILE_MODE,
        st_uid = ?ROOT_UID,
        st_gid = ?ROOT_GID
    }
end).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec register(session:id(), od_space:id(), file_meta:path(), storage:id(), helpers:file_id(), spec()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
register(SessionId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec) ->
    try
        FileName = filename:basename(DestinationPath),
        StorageFileId2 = normalize_storage_file_id(StorageId, StorageFileId),
        StorageFileCtx = storage_file_ctx:new(StorageFileId2, FileName, SpaceId, StorageId),
        StorageFileCtx2 = maybe_verify_existence(StorageFileCtx, Spec),
        StorageFileCtx3 = prepare_stat(StorageFileCtx2, Spec),
        UserCtx = user_ctx:new(SessionId),
        CanonicalPath = destination_path_to_canonical_path(SpaceId, DestinationPath),
        FilePartialCtx = file_partial_ctx:new_by_canonical_path(UserCtx, CanonicalPath),
        DirectParentCtx = ensure_all_parents_exist_and_are_dirs(FilePartialCtx, UserCtx),
        % TODO VFS-6508 do not use TraverseInfo as its associated with storage_traverse
        {_, FileCtx, _} = storage_sync_engine:sync_file(StorageFileCtx3, #{
                parent_ctx => DirectParentCtx,
                space_storage_file_id => storage_file_id:space_dir_id(SpaceId, StorageId),
                iterator_type => storage_traverse:get_iterator(StorageId),
                is_posix_storage => storage:is_posix_compatible(StorageId),
                sync_acl => false
            }),
        case FileCtx =/= undefined of
            true ->
                set_xattrs(UserCtx, FileCtx, maps:get(<<"xattrs">>, Spec, #{})),
                {ok, file_ctx:get_guid_const(FileCtx)};
            false ->
                % TODO VFS-6508 find better error
                % this can happen if sync mechanisms decide not to synchronize file
                ?error("Skipped registration of file ~s located on storage ~s in space ~s under path ~s.",
                    [StorageFileId2, StorageId, SpaceId, DestinationPath]),
                {error, ?ENOENT}
        end
    catch
        throw:?ENOTSUP ->
            ?error_stacktrace(
                "Failed registration of file ~s located on storage ~s in space ~s under path ~s.~n"
                "stat (or equivalent) operation is not supported by the storage.",
                [StorageFileId, StorageId, SpaceId, DestinationPath]),
            ?ERROR_STAT_OPERATION_NOT_SUPPORTED(StorageId);
        Error:Reason ->
            ?error_stacktrace(
                "Failed registration of file ~s located on storage ~s in space ~s under path ~s.~n"
                "Operation failed due to ~p:~p",
                [StorageFileId, StorageId, SpaceId, DestinationPath, Error, Reason]),
            {error, Reason}
    end.


-spec create_missing_directory(file_ctx:ctx(), file_meta:name(), od_user:id()) -> {ok, file_ctx:ctx()}.
create_missing_directory(ParentCtx, DirName, UserId) ->
    SpaceId = file_ctx:get_space_id_const(ParentCtx),
    FileUuid = datastore_key:new(),
    ok = dir_location:mark_dir_synced_from_storage(FileUuid, undefined),
    {ok, DirCtx} = create_missing_directory_file_meta(FileUuid, ParentCtx, DirName, SpaceId, UserId),
    CurrentTime = time_utils:system_time_seconds(),
    times:save(FileUuid, SpaceId, CurrentTime, CurrentTime, CurrentTime),
    {ok, DirCtx}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec normalize_storage_file_id(storage:id(), helpers:file_id()) -> helpers:file_id().
normalize_storage_file_id(StorageId, StorageFileId) ->
    Helper = storage:get_helper(StorageId),
    case helper:get_name(Helper) of
        ?XROOTD_HELPER_NAME ->
            normalize_xrootd_storage_file_id(Helper, StorageFileId);
        _ ->
            StorageFileId
    end.

-spec normalize_xrootd_storage_file_id(helpers:helper(), helpers:file_id()) -> helpers:file_id().
normalize_xrootd_storage_file_id(Helper, StorageFileId) ->
    case is_url(StorageFileId) of
        true ->
            Args = helper:get_args(Helper),
            HelperUrl = maps:get(<<"url">>, Args),
            HelperUrlSize = byte_size(HelperUrl),
            case binary:match(StorageFileId, HelperUrl) of
                {0, HelperUrlSize} ->
                    % StorageFileId is prefixed with HelperUrl, we can strip it
                    binary:part(StorageFileId, {HelperUrlSize, byte_size(StorageFileId) - HelperUrlSize});
                _ ->
                    throw(?ERROR_BAD_VALUE_IDENTIFIER(StorageFileId))
            end;
        false ->
            StorageFileId
    end.

-spec is_url(binary()) -> boolean().
is_url(<<"root://", _/binary>>) -> true;
is_url(<<"http://", _/binary>>) -> true;
is_url(<<"https://", _/binary>>) -> true;
is_url(_) -> false.


-spec ensure_all_parents_exist_and_are_dirs(file_partial_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
ensure_all_parents_exist_and_are_dirs(FilePartialCtx, UserCtx) ->
    {ParentCtx, _} = file_partial_ctx:get_parent(FilePartialCtx, UserCtx),
    ensure_all_parents_exist_and_are_dirs(ParentCtx, UserCtx, []).

-spec ensure_all_parents_exist_and_are_dirs(file_partial_ctx:ctx(), user_ctx:ctx(), [file_partial_ctx:ctx()]) ->
    file_ctx:ctx().
ensure_all_parents_exist_and_are_dirs(PartialCtx, UserCtx, ChildrenPartialCtxs) ->
    try
        {FileCtx, _} = file_ctx:new_by_partial_context(PartialCtx),
        case file_ctx:is_dir(FileCtx) of
            {true, FileCtx2} ->
                % first directory on the path that exists in db
                create_missing_directories(FileCtx2, ChildrenPartialCtxs, user_ctx:get_user_id(UserCtx));
            {false, _} ->
                {error, ?ENOTDIR}
        end
    catch
        error:{badmatch, {error, not_found}} ->
            {ParentPartialCtx, PartialCtx2} = file_partial_ctx:get_parent(PartialCtx, UserCtx),
            ensure_all_parents_exist_and_are_dirs(ParentPartialCtx, UserCtx, [PartialCtx2 | ChildrenPartialCtxs])
    end.

-spec create_missing_directories(file_ctx:ctx(), [file_partial_ctx:ctx()], od_user:id()) -> file_ctx:ctx().
create_missing_directories(ParentCtx, [], _UserId) ->
    ParentCtx;
create_missing_directories(ParentCtx, [DirectChildPartialCtx | Rest], UserId) ->
    {CanonicalPath, _} = file_partial_ctx:get_canonical_path(DirectChildPartialCtx),
    DirectChildName = filename:basename(CanonicalPath),
    {ok, DirectChildCtx} = create_missing_directory(ParentCtx, DirectChildName, UserId),
    create_missing_directories(DirectChildCtx, Rest, UserId).


-spec create_missing_directory_file_meta(file_meta:uuid(), file_ctx:ctx(), file_meta:name(),
    od_space:id(), od_user:id()) -> {ok, file_ctx:ctx()}.
create_missing_directory_file_meta(FileUuid, ParentCtx, FileName, SpaceId, UserId) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    FileMetaDoc = file_meta:new_doc(FileUuid, FileName, ?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS,
        UserId, ParentUuid, SpaceId),
    {ok, FinalDoc} = case file_meta:create({uuid, ParentUuid}, FileMetaDoc) of
        {error, already_exists} ->
            % TODO VFS-6509
            % there was race with other process creating missing parent or with lfm
            % In case it is conflict with lfm we should create file with IMPORTED suffix
            {ok, FileUuid2} = link_utils:try_to_resolve_child_link(FileName, ParentCtx),
            file_meta:get(FileUuid2);
        {ok, _} ->
            {ok, FileMetaDoc}
    end,
    FileCtx = file_ctx:new_by_doc(FinalDoc, SpaceId, undefined),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []),
    {ok, FileCtx}.

-spec destination_path_to_canonical_path(od_space:id(), file_meta:path()) -> file_meta:path().
destination_path_to_canonical_path(SpaceId, DestinationPath) ->
    fslogic_path:join([?DIRECTORY_SEPARATOR_BINARY, SpaceId, DestinationPath]).

-spec maybe_verify_existence(storage_file_ctx:ctx(), spec()) -> storage_file_ctx:ctx().
maybe_verify_existence(StorageFileCtx, Spec) ->
    case maps:get(<<"verifyExistence">>, Spec, true) of
        true ->
            {_, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
            StorageFileCtx2;
        false ->
            StorageFileCtx
    end.

-spec prepare_stat(storage_file_ctx:ctx(), spec()) -> storage_file_ctx:ctx().
prepare_stat(StorageFileCtx, Spec) ->
    Stat = #statbuf{
        st_size = maps:get(<<"size">>, Spec, undefined),
        st_mtime = maps:get(<<"mtime">>, Spec, undefined),
        st_atime = maps:get(<<"atime">>, Spec, undefined),
        st_ctime = maps:get(<<"ctime">>, Spec, undefined),
        st_mode = maps:get(<<"mode">>, Spec, undefined),
        st_uid = maps:get(<<"uid">>, Spec, undefined),
        st_gid = maps:get(<<"gid">>, Spec, undefined)
    },
    fill_in_missing_stat_fields(StorageFileCtx, Stat).

-spec set_xattrs(user_ctx:ctx(), file_ctx:ctx(), json_utils:json_map()) -> ok.
set_xattrs(UserCtx, FileCtx, Xattrs) ->
    maps:fold(fun(K, V, _) ->
        xattr_req:set_xattr(UserCtx, FileCtx, #xattr{name = K, value = V}, false, false)
    end, undefined, Xattrs),
    ok.

-spec fill_in_missing_stat_fields(storage_file_ctx:ctx(), helpers:stat()) -> storage_file_ctx:ctx().
fill_in_missing_stat_fields(StorageFileCtx, Stat = #statbuf{
    st_size = Size,
    st_mtime = Mtime,
    st_atime = Atime,
    st_ctime = Ctime,
    st_mode = Mode,
    st_uid = Uid,
    st_gid = Gid
}) when Size =/= undefined
    andalso Mtime =/= undefined
    andalso Atime =/= undefined
    andalso Ctime =/= undefined
    andalso Mode =/= undefined
    andalso Uid =/= undefined
    andalso Gid =/= undefined
->
    % all fields were passed by user
    storage_file_ctx:set_stat(StorageFileCtx, Stat);
fill_in_missing_stat_fields(StorageFileCtx, UserDefinedStat = #statbuf{st_size = Size}) when Size =/= undefined ->
    % perform stat on storage to fill missing values
    % size is defined, other missing values can be filled with predefined defaults
    % even if stat operation is not supported
    {Stat2, StorageFileCtx2} = stat_or_return_defaults(StorageFileCtx),
    FinalStat = ensure_stat_defined(UserDefinedStat, Stat2),
    storage_file_ctx:set_stat(StorageFileCtx2, FinalStat);
fill_in_missing_stat_fields(StorageFileCtx, UserDefinedStat) ->
    % if stat operation is not supported, below call will throw ?ENOTSUP
    {Stat2, StorageFileCtx2} = stat_or_throw_missing_size(StorageFileCtx),
    FinalStat = ensure_stat_defined(UserDefinedStat, Stat2),
    storage_file_ctx:set_stat(StorageFileCtx2, FinalStat).

-spec stat_or_throw_missing_size(storage_file_ctx:ctx()) ->
    {helpers:stat(), storage_file_ctx:ctx()}.
stat_or_throw_missing_size(StorageFileCtx) ->
    try
        storage_file_ctx:stat(StorageFileCtx)
    catch
        throw:?ENOTSUP ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(<<"size">>));
        Error:Reason ->
            erlang:Error(Reason)
    end.

-spec stat_or_return_defaults(storage_file_ctx:ctx()) ->
    {helpers:stat(), storage_file_ctx:ctx()}.
stat_or_return_defaults(StorageFileCtx) ->
    try
        storage_file_ctx:stat(StorageFileCtx)
    catch
        throw:?ENOTSUP ->
            {?DEFAULT_STAT, StorageFileCtx};
        Error:Reason ->
            erlang:Error(Reason)
    end.

-spec ensure_stat_defined(helpers:stat(), helpers:stat()) -> helpers:stat().
ensure_stat_defined(Stat, DefaultStat) ->
    #statbuf{
        st_size = utils:ensure_defined(Stat#statbuf.st_size, DefaultStat#statbuf.st_size),
        st_mtime = utils:ensure_defined(Stat#statbuf.st_mtime, DefaultStat#statbuf.st_mtime),
        st_atime = utils:ensure_defined(Stat#statbuf.st_atime, DefaultStat#statbuf.st_atime),
        st_ctime = utils:ensure_defined(Stat#statbuf.st_ctime, DefaultStat#statbuf.st_ctime),
        % ensure the mode is relevant for regular files
        st_mode = utils:ensure_defined(Stat#statbuf.st_mode, DefaultStat#statbuf.st_mode) bor 8#100000,
        st_uid = utils:ensure_defined(Stat#statbuf.st_uid, DefaultStat#statbuf.st_uid),
        st_gid = utils:ensure_defined(Stat#statbuf.st_gid, DefaultStat#statbuf.st_gid)
    }.