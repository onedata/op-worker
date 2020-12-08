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

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register/6, create_missing_directory/3]).
-export([init_pool/0, stop_pool/0]).

%% Exported for RPC
-export([register_internal/6]).

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
%%       <<"autoDetectAttributes">> => boolean(),
%%       <<"xattrs">> => json_utils:json_map(),
%%       <<"json">> => json_utils:json_map(),
%%       <<"rdf">> => binary() % base64 encoded RDF
%% }

-export_type([spec/0]).

-define(FILE_REGISTRATION_POOL, file_registration_pool).
-define(FILE_REGISTRATION_POOL_SIZE, application:get_env(?APP_NAME, file_registration_pool_size, 100)).
-define(FILE_REGISTRATION_TIMEOUT, application:get_env(?APP_NAME, file_registration_timeout, 30000)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec register(session:id(), od_space:id(), file_meta:path(), storage:id(), helpers:file_id(), spec()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
register(SessId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec) ->
    Args = [SessId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec],
    Result = worker_pool:call(
        ?FILE_REGISTRATION_POOL,
        {?MODULE, register_internal, Args},
        wpool:default_strategy(),
        ?FILE_REGISTRATION_TIMEOUT
    ),
    case Result of
        {ok, OkResult} -> OkResult;
        {error, ErrorResult} -> throw(ErrorResult)
    end.


-spec create_missing_directory(file_ctx:ctx(), file_meta:name(), od_user:id()) -> {ok, file_ctx:ctx()}.
create_missing_directory(ParentCtx, DirName, UserId) ->
    SpaceId = file_ctx:get_space_id_const(ParentCtx),
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    FileUuid = datastore_key:new(),
    {ParentStorageFileId, _} = file_ctx:get_storage_file_id(ParentCtx),
    ok = dir_location:mark_dir_synced_from_storage(FileUuid, filepath_utils:join([ParentStorageFileId, DirName]), undefined),
    {ok, DirCtx} = storage_import_engine:create_file_meta_and_handle_conflicts(
        FileUuid, DirName, ?DEFAULT_DIR_PERMS, UserId, ParentUuid, SpaceId),
    CurrentTime = global_clock:timestamp_seconds(),
    times:save(FileUuid, SpaceId, CurrentTime, CurrentTime, CurrentTime),
    {ok, DirCtx}.


-spec init_pool() -> ok.
init_pool() ->
    {ok, _} = worker_pool:start_sup_pool(?FILE_REGISTRATION_POOL, [
        {workers, ?FILE_REGISTRATION_POOL_SIZE}
    ]),
    ok.


-spec stop_pool() -> ok.
stop_pool() ->
    ok = wpool:stop_sup_pool(?FILE_REGISTRATION_POOL).

%%%===================================================================
%%% RPC functions
%%%===================================================================

-spec register_internal(session:id(), od_space:id(), file_meta:path(), storage:id(), helpers:file_id(), spec()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
register_internal(SessId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec) ->
    try
        UserCtx = user_ctx:new(SessId),
        FileName = filename:basename(DestinationPath),
        StorageFileId2 = normalize_storage_file_id(StorageId, StorageFileId),
        StorageFileCtx = storage_file_ctx:new(StorageFileId2, FileName, SpaceId, StorageId),
        StorageFileCtx2 = maybe_verify_existence(StorageFileCtx, Spec),
        StorageFileCtx3 = prepare_stat(StorageFileCtx2, Spec),
        CanonicalPath = destination_path_to_canonical_path(SpaceId, DestinationPath),
        FilePartialCtx = file_partial_ctx:new_by_canonical_path(UserCtx, CanonicalPath),
        DirectParentCtx = ensure_all_parents_exist_and_are_dirs(FilePartialCtx, UserCtx),
        % TODO VFS-6508 do not use TraverseInfo as its associated with storage_traverse
        {_, FileCtx, _} = storage_import_engine:sync_file(StorageFileCtx3, #{
            parent_ctx => DirectParentCtx,
            space_storage_file_id => storage_file_id:space_dir_id(SpaceId, StorageId),
            iterator_type => storage_traverse:get_iterator(StorageId),
            is_posix_storage => storage:is_posix_compatible(StorageId),
            sync_acl => false,
            verify_existence => maps:get(<<"autoDetectAttributes">>, Spec, true),
            manual => true
        }),
        case FileCtx =/= undefined of
            true ->
                maybe_set_xattrs(UserCtx, FileCtx, maps:get(<<"xattrs">>, Spec, #{})),
                maybe_set_json_metadata(UserCtx, FileCtx, maps:get(<<"json">>, Spec, #{})),
                maybe_set_rdf_metadata(UserCtx, FileCtx, maps:get(<<"rdf">>, Spec, <<>>)),
                {ok, file_ctx:get_guid_const(FileCtx)};
            false ->
                % TODO VFS-6508 find better error
                % this can happen if sync mechanisms decide not to synchronize file
                ?error("Skipped registration of file ~s located on storage ~s in space ~s under path ~s.",
                    [StorageFileId2, StorageId, SpaceId, DestinationPath]),
                ?ERROR_POSIX(?ENOENT)
        end
    catch
        throw:?ENOTSUP ->
            ?error_stacktrace(
                "Failed registration of file ~s located on storage ~s in space ~s under path ~s.~n"
                "stat (or equivalent) operation is not supported by the storage.",
                [StorageFileId, StorageId, SpaceId, DestinationPath]),
            ?ERROR_STAT_OPERATION_NOT_SUPPORTED(StorageId);
        throw:{error, _} = Error ->
            Error;
        throw:PosixError ->
            % posix errors are thrown as single atoms
            ?ERROR_POSIX(PosixError);
        Error:Reason ->
            ?error_stacktrace(
                "Failed registration of file ~s located on storage ~s in space ~s under path ~s.~n"
                "Operation failed due to ~p:~p",
                [StorageFileId, StorageId, SpaceId, DestinationPath, Error, Reason]),
            {error, Reason}
    end.

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
                throw(?ERROR_POSIX(?ENOTDIR))
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
    try
        {CanonicalPath, _} = file_partial_ctx:get_canonical_path(DirectChildPartialCtx),
        DirectChildName = filename:basename(CanonicalPath),
        {ok, DirectChildCtx} = create_missing_directory_secure(ParentCtx, DirectChildName, UserId),
        create_missing_directories(DirectChildCtx, Rest, UserId)
    catch
        Error:Reason ->
            ?error_stacktrace("Creating missing directories for file being registered has failed with ~p:~p",
                [Error, Reason]),
            throw(?ERROR_POSIX(?ENOENT))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The same as create_missing_directory/3 but run in a critical section
%% to avoid conflicts between processes registering files in parallel
%% for the same user.
%% @end
%%--------------------------------------------------------------------
-spec create_missing_directory_secure(file_ctx:ctx(), file_meta:name(), od_user:id()) -> {ok, file_ctx:ctx()}.
create_missing_directory_secure(ParentCtx, DirName, UserId) ->
    critical_section:run({create_missing_directory, file_ctx:get_uuid_const(ParentCtx), DirName, UserId}, fun() ->
        try
            % ensure whether directory is still missing as it might have been created by other registering process
            {DirCtx, _} = file_ctx:get_child(ParentCtx, DirName, user_ctx:new(?ROOT_SESS_ID)),
            {ok, DirCtx}
        catch
            error:{badmatch,{error,not_found}} ->
                create_missing_directory(ParentCtx, DirName, UserId);
            throw:?ENOENT ->
                create_missing_directory(ParentCtx, DirName, UserId)
        end
    end).


-spec destination_path_to_canonical_path(od_space:id(), file_meta:path()) -> file_meta:path().
destination_path_to_canonical_path(SpaceId, DestinationPath) ->
    filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, SpaceId, DestinationPath]).

-spec maybe_verify_existence(storage_file_ctx:ctx(), spec()) -> storage_file_ctx:ctx().
maybe_verify_existence(StorageFileCtx, Spec) ->
    case maps:get(<<"autoDetectAttributes">>, Spec, true) of
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
    fill_in_missing_stat_fields(StorageFileCtx, Stat, Spec).


-spec maybe_set_xattrs(user_ctx:ctx(), file_ctx:ctx(), json_utils:json_map()) -> ok.
maybe_set_xattrs(UserCtx, FileCtx, Xattrs) ->
    maps:fold(fun(K, V, _) ->
        xattr_req:set_xattr(UserCtx, FileCtx, #xattr{name = K, value = V}, false, false)
    end, undefined, Xattrs),
    ok.


-spec maybe_set_json_metadata(user_ctx:ctx(), file_ctx:ctx(), json_utils:json_map()) -> ok.
maybe_set_json_metadata(_UserCtx, _FileCtx, JSON) when map_size(JSON) =:= 0 ->
    ok;
maybe_set_json_metadata(UserCtx, FileCtx, JSON) ->
    metadata_req:set_metadata(UserCtx, FileCtx, json, JSON, [], false, false),
    ok.


-spec maybe_set_rdf_metadata(user_ctx:ctx(), file_ctx:ctx(), binary()) -> ok.
maybe_set_rdf_metadata(_UserCtx, _FileCtx, <<>>) ->
    ok;
maybe_set_rdf_metadata(UserCtx, FileCtx, EncodedRdf) ->
    RDF = base64:decode(EncodedRdf),
    metadata_req:set_metadata(UserCtx, FileCtx, rdf, RDF, [], false, false),
    ok.


-spec fill_in_missing_stat_fields(storage_file_ctx:ctx(), helpers:stat(), spec()) -> storage_file_ctx:ctx().
fill_in_missing_stat_fields(StorageFileCtx, Stat = #statbuf{
    st_size = Size,
    st_mtime = Mtime,
    st_atime = Atime,
    st_ctime = Ctime,
    st_mode = Mode,
    st_uid = Uid,
    st_gid = Gid
}, _Spec) when Size =/= undefined
    andalso Mtime =/= undefined
    andalso Atime =/= undefined
    andalso Ctime =/= undefined
    andalso Mode =/= undefined
    andalso Uid =/= undefined
    andalso Gid =/= undefined
->
    % all fields were passed by user
    % ensure flag for regular file is set in mode
    Stat2 = Stat#statbuf{st_mode = Mode  bor 8#100000},
    storage_file_ctx:set_stat(StorageFileCtx, Stat2);
fill_in_missing_stat_fields(StorageFileCtx, UserDefinedStat = #statbuf{st_size = Size}, Spec) when Size =/= undefined ->
    % perform stat on storage to fill missing values
    % size is defined, other missing values can be filled with predefined defaults
    % even if stat operation is not supported
    {Stat2, StorageFileCtx2} = get_stat_from_storage_or_return_defaults(StorageFileCtx, Spec),
    FinalStat = ensure_stat_defined(UserDefinedStat, Stat2),
    storage_file_ctx:set_stat(StorageFileCtx2, FinalStat);
fill_in_missing_stat_fields(StorageFileCtx, UserDefinedStat, Spec) ->
    % if stat operation is not supported, below call will throw ?ENOTSUP
    {Stat2, StorageFileCtx2} = get_stat_from_storage_or_throw_missing_size(StorageFileCtx, Spec),
    FinalStat = ensure_stat_defined(UserDefinedStat, Stat2),
    storage_file_ctx:set_stat(StorageFileCtx2, FinalStat).


-spec get_stat_from_storage_or_throw_missing_size(storage_file_ctx:ctx(), spec()) ->
    {helpers:stat(), storage_file_ctx:ctx()}.
get_stat_from_storage_or_throw_missing_size(StorageFileCtx, Spec) ->
    case maps:get(<<"autoDetectAttributes">>, Spec, true) of
        false ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(<<"size">>));
        true ->
            try
                storage_file_ctx:stat(StorageFileCtx)
            catch
                throw:?ENOTSUP ->
                    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"size">>));
                Error:Reason ->
                    erlang:Error(Reason)
            end
    end.


-spec get_stat_from_storage_or_return_defaults(storage_file_ctx:ctx(), spec()) ->
    {helpers:stat(), storage_file_ctx:ctx()}.
get_stat_from_storage_or_return_defaults(StorageFileCtx, Spec) ->
    case maps:get(<<"autoDetectAttributes">>, Spec, true) of
        false ->
            get_default_file_stat(StorageFileCtx);
        true ->
            try
                storage_file_ctx:stat(StorageFileCtx)
            catch
                throw:?ENOTSUP ->
                    get_default_file_stat(StorageFileCtx);
                Error:Reason ->
                    erlang:Error(Reason)
            end
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


-spec get_default_file_stat(storage_file_ctx:ctx()) -> {helpers:stat(), storage_file_ctx:ctx()}.
get_default_file_stat(StorageFileCtx) ->
    {Storage, StorageFileCtx2} = storage_file_ctx:get_storage(StorageFileCtx),
    CurrentTimestamp = global_clock:timestamp_seconds(),
    DefaultStat = #statbuf{
        % st_size is not set intentionally as we cannot assume default size
        st_mtime = CurrentTimestamp,
        st_atime = CurrentTimestamp,
        st_ctime = CurrentTimestamp,
        st_mode = get_default_file_mode(storage:get_helper(Storage)),
        st_uid = ?ROOT_UID,
        st_gid = ?ROOT_GID
    },
    {DefaultStat, storage_file_ctx:set_stat(StorageFileCtx2, DefaultStat)}.


-spec get_default_file_mode(helpers:helper()) -> file_meta:mode().
get_default_file_mode(#helper{name = HelperName, args = Args})
    when HelperName =:= ?HTTP_HELPER_NAME
    orelse HelperName =:= ?S3_HELPER_NAME
    orelse HelperName =:= ?WEBDAV_HELPER_NAME
->
    maps:get(<<"fileMode">>, Args, ?DEFAULT_FILE_MODE);
get_default_file_mode(#helper{name = ?XROOTD_HELPER_NAME, args = Args}) ->
    maps:get(<<"fileModeMask">>, Args, ?DEFAULT_FILE_MODE);
get_default_file_mode(_) ->
    ?DEFAULT_FILE_MODE.