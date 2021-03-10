%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module performs file-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files).

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
%% Functions operating on directories or files
-export([
    unlink/3, rm_recursive/2,
    mv/3, mv/4, cp/3, cp/4,
    get_parent/2, get_file_path/2, get_file_guid/2,
    schedule_file_transfer/5, schedule_view_transfer/7,
    schedule_file_replication/4, schedule_replica_eviction/4,
    schedule_replication_by_view/6, schedule_replica_eviction_by_view/6,
    is_dir/2
]).
%% Functions operating on files
-export([
    create/2, create/3, create/4, make_link/3, make_link/4, make_symlink/3, make_symlink/4,
    open/3, create_and_open/3, create_and_open/4, create_and_open/5,
    get_file_location/2, read_symlink/2, fsync/1, fsync/3, write/3,
    write_without_events/3, read/3, read/4, check_size_and_read/3, read_without_events/3,
    read_without_events/4, silent_read/3, silent_read/4,
    truncate/3, release/1, get_file_distribution/2
]).

-compile({no_auto_import, [unlink/1]}).

-define(DEFAULT_SYNC_PRIORITY,
    application:get_env(?APP_NAME, default_sync_priority, 32)).
-define(SYNC_MAX_RETRIES, application:get_env(?APP_NAME, lfm_sync_max_retries, 5)).
-define(SYNC_MIN_BACKOFF, application:get_env(?APP_NAME, lfm_sync_min_backoff, timer:seconds(1))).
-define(SYNC_BACKOFF_RATE, application:get_env(?APP_NAME, lfm_sync_backoff_rate, 2)).
-define(SYNC_MAX_BACKOFF, application:get_env(?APP_NAME, lfm_sync_max_backoff, timer:minutes(1))).

-type sync_options() :: {priority, non_neg_integer()} | off.
-type check_size_option() :: verify_size | ignore_size.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec unlink(session:id(), fslogic_worker:ext_file(), boolean()) ->
    ok | lfm:error_reply().
unlink(SessId, FileEntry, Silent) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileEntry),
    remote_utils:call_fslogic(SessId, file_request, Guid, #delete_file{silent = Silent},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or directory.
%% NOTE!!!
%% Directory will be moved to trash.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | lfm:error_reply().
rm_recursive(SessId, FileKey) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    case is_dir(SessId, FileKey) of
        true ->
            rm_using_trash(SessId, {guid, Guid});
        false ->
            unlink(SessId, {guid, Guid}, false);
        {error, ?ENOENT} ->
            ok;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mv(SessId, FileKey, TargetPath) ->
    {TargetName, TargetDir} = filepath_utils:basename_and_parent_dir(TargetPath),
    mv(SessId, FileKey, {path, TargetDir}, TargetName).

%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetParentKey :: fslogic_worker:file_guid_or_path(),
    TargetName :: file_meta:name()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mv(SessId, FileKey, TargetParentKey, TargetName) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    {guid, TargetDirGuid} = guid_utils:ensure_guid(SessId, TargetParentKey),
    remote_utils:call_fslogic(SessId, file_request, Guid,
        #rename{target_parent_guid = TargetDirGuid, target_name = TargetName},
        fun(#file_renamed{new_guid = NewGuid}) ->
            {ok, NewGuid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
cp(SessId, FileKey, TargetPath) ->
    {TargetName, TargetParentPath} = filepath_utils:basename_and_parent_dir(TargetPath),
    cp(SessId, FileKey, {path, TargetParentPath}, TargetName).

%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetParentKey :: fslogic_worker:file_guid_or_path(),
    TargetName :: file_meta:name()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
cp(SessId, FileKey, TargetParentKey, TargetName) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    {guid, TargetParentGuid} = guid_utils:ensure_guid(SessId, TargetParentKey),
    case file_copy:copy(SessId, Guid, TargetParentGuid, TargetName) of
        {ok, NewGuid, _} ->
            {ok, NewGuid};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
get_parent(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_parent{},
        fun(#dir{guid = ParentGuid}) ->
            {ok, ParentGuid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(session:id(), FileGuid :: fslogic_worker:file_guid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, FileGuid) ->
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_file_path{},
        fun(#file_path{value = Path}) ->
            {ok, Path}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns guid of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_guid(session:id(), FileKey :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()}.
get_file_guid(SessId, FilePath) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, {path, FilePath}),
    {ok, FileGuid}.


%%--------------------------------------------------------------------
%% @doc
%% Checks if a file is directory.
%% @end
%%--------------------------------------------------------------------
-spec is_dir(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    true | false | lfm:error_reply().
is_dir(SessId, FileKey) ->
    case lfm_attrs:stat(SessId, FileKey) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Schedules file transfer and returns its ID.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_transfer(
    session:id(),
    file_id:file_guid(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_file_transfer(SessId, FileGuid, ReplicatingProviderId, EvictingProviderId, Callback) ->
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #schedule_file_transfer{
            replicating_provider_id = ReplicatingProviderId,
            evicting_provider_id = EvictingProviderId,
            callback = Callback
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules transfer by view and returns its ID.
%% @end
%%--------------------------------------------------------------------
-spec schedule_view_transfer(
    session:id(),
    od_space:id(),
    transfer:view_name(), transfer:query_view_params(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_view_transfer(
    SessId, SpaceId, ViewName, QueryViewParams,
    ReplicatingProviderId, EvictingProviderId, Callback
) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #schedule_view_transfer{
            replicating_provider_id = ReplicatingProviderId,
            evicting_provider_id = EvictingProviderId,
            view_name = ViewName,
            query_view_params = QueryViewParams,
            callback = Callback
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules replication transfer and returns its ID.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(session:id(), fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id(), transfer:callback()) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_file_replication(SessId, FileKey, TargetProviderId, Callback) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #schedule_file_replication{
            target_provider_id = TargetProviderId,
            callback = Callback
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules replication transfer by view and returns its ID.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replication_by_view(session:id(), ProviderId :: oneprovider:id(),
    transfer:callback(), od_space:id(), transfer:view_name(),
    transfer:query_view_params()) -> {ok, transfer:id()} | lfm:error_reply().
schedule_replication_by_view(SessId, TargetProviderId, Callback, SpaceId, ViewName, QueryParams) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #schedule_file_replication{
            target_provider_id = TargetProviderId,
            callback = Callback,
            view_name = ViewName,
            query_view_params = QueryParams
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules replica_eviction transfer on given provider,
%% migrates unique data to provider given as MigrateProviderId.
%% Returns ID of scheduled transfer.
%% TODO VFS-6365 remove deprecated replicas endpoints
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction(session:id(), fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id(), MigrationProviderId :: undefined | oneprovider:id()) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_replica_eviction(SessId, FileKey, ProviderId, MigrationProviderId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #schedule_replica_invalidation{
            source_provider_id = ProviderId,
            target_provider_id = MigrationProviderId
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules replica_eviction transfer by view on given provider,
%% migrates unique data to provider given as MigrateProviderId.
%% Returns ID of scheduled transfer.
%% TODO VFS-6365 remove deprecated replicas endpoints
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction_by_view(session:id(), ProviderId :: oneprovider:id(),
    MigrationProviderId :: undefined | oneprovider:id(), od_space:id(),
    transfer:view_name(), transfer:query_view_params()) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_replica_eviction_by_view(SessId, ProviderId, MigrationProviderId,
    SpaceId, ViewName, QueryViewParams
) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #schedule_replica_invalidation{
            source_provider_id = ProviderId,
            target_provider_id = MigrationProviderId,
            view_name = ViewName,
            query_view_params = QueryViewParams
        },
        fun(#scheduled_transfer{transfer_id = TransferId}) ->
            {ok, TransferId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:uuid()} | lfm:error_reply().
create(SessId, Path) ->
    create(SessId, Path, undefined).

-spec create(session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(SessId, Path, Mode) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create(SessId, ParentGuid, Name, Mode)
        end).

-spec create(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(SessId, ParentGuid, Name, undefined) ->
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    create(SessId, ParentGuid, Name, DefaultMode);
create(SessId, ParentGuid, Name, Mode) ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #make_file{name = Name, mode = Mode},
        fun(#file_attr{guid = Guid}) ->
            {ok, Guid}
        end
    ).


-spec make_link(session:id(), LinkPath :: file_meta:path(), FileGuid :: fslogic_worker:file_guid()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_link(SessId, LinkPath, TargetGuid) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(LinkPath),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = TargetParentGuid}) ->
            lfm_files:make_link(SessId, {guid, TargetGuid}, TargetParentGuid, Name)
        end).

-spec make_link(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetParentGuid :: fslogic_worker:file_guid(), Name :: file_meta:name()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_link(SessId, FileKey, TargetParentGuid, Name) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, Guid,
        #make_link{target_parent_guid = TargetParentGuid, target_name = Name},
        fun(#file_attr{} = FileAttr) ->
            {ok, FileAttr}
        end
    ).


-spec make_symlink(session:id(), LinkPath :: file_meta:path(), LinkTarget :: file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(SessId, LinkPath, LinkTarget) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(LinkPath),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:make_symlink(SessId, {guid, ParentGuid}, Name, LinkTarget)
        end).

-spec make_symlink(session:id(), ParentKey :: fslogic_worker:file_guid_or_path(),
    Name :: file_meta:name(), LinkTarget :: file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(SessId, ParentKey, Name, LinkTarget) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, ParentKey),
    remote_utils:call_fslogic(SessId, file_request, Guid,
        #make_symlink{target_name = Name, link = LinkTarget},
        fun(#file_attr{} = FileAttr) ->
            {ok, FileAttr}
        end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file and opens it
%% @end
%%--------------------------------------------------------------------
-spec create_and_open(session:id(), Path :: file_meta:path(), fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}}
    | lfm:error_reply().
create_and_open(SessId, Path, OpenFlag) ->
    create_and_open(SessId, Path, undefined, OpenFlag).

-spec create_and_open(session:id(), Path :: file_meta:path(),
    Mode :: undefined | file_meta:posix_permissions(), fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}}
    | lfm:error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag)
        end).

-spec create_and_open(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}}
    | lfm:error_reply().
create_and_open(SessId, ParentGuid, Name, undefined, OpenFlag) ->
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    create_and_open(SessId, ParentGuid, Name, DefaultMode, OpenFlag);
create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag) ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #create_file{name = Name, mode = Mode, flag = OpenFlag},
        fun(#file_created{
            file_attr = #file_attr{
                guid = FileGuid
            },
            file_location = #file_location{
                provider_id = ProviderId,
                file_id = FileId,
                storage_id = StorageId
            },
            handle_id = HandleId
        }) ->
            Handle = lfm_context:new(
                HandleId,
                ProviderId,
                SessId,
                FileGuid,
                OpenFlag,
                FileId,
                StorageId
            ),
            {ok, {FileGuid, Handle}}
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Flag :: fslogic_worker:open_flag()) ->
    {ok, lfm:handle()} | lfm:error_reply().
open(SessId, FileKey, Flag) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #open_file_with_extended_info{flag = Flag},
        fun(#file_opened_extended{handle_id = HandleId,
            provider_id = ProviderId, file_id = FileId,
            storage_id = StorageId}) ->
            {ok, lfm_context:new(
                HandleId,
                ProviderId,
                SessId,
                FileGuid,
                Flag,
                FileId,
                StorageId
            )}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened  file.
%% @end
%%--------------------------------------------------------------------
-spec release(lfm:handle()) ->
    ok | lfm:error_reply().
release(Handle) ->
    case lfm_context:get_handle_id(Handle) of
        undefined ->
            ok;
        HandleId ->
            SessionId = lfm_context:get_session_id(Handle),
            FileGuid = lfm_context:get_guid(Handle),
            remote_utils:call_fslogic(SessionId, file_request,
                FileGuid, #release{handle_id = HandleId},
                fun(_) -> ok end)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns location to file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, file_location:record()} | lfm:error_reply().
get_file_location(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_location{}, fun(#file_location{} = FL) -> {ok, FL} end).


-spec read_symlink(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, file_meta_symlinks:symlink()} | lfm:error_reply().
read_symlink(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #read_symlink{}, fun(#symlink{link = Link}) -> {ok, Link} end).


%%--------------------------------------------------------------------
%% @doc
%% Gets necessary data from handle and executes fsync/3
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: lfm:handle()) ->
    ok | lfm:error_reply().
fsync(Handle) ->
    SessionId = lfm_context:get_session_id(Handle),
    FileGuid = lfm_context:get_guid(Handle),
    ProviderId = lfm_context:get_provider_id(Handle),
    fsync(SessionId, {guid, FileGuid}, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(session:id(), fslogic_worker:file_guid_or_path(), oneprovider:id()) ->
    ok | lfm:error_reply().
fsync(SessId, FileKey, ProviderId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    case oneprovider:is_self(ProviderId) of
        true ->
            ok; % flush also flushes events for provider
        _ ->
            lfm_event_controller:flush_event_queue(SessId, ProviderId,
                file_id:guid_to_uuid(FileGuid))
    end,
    remote_utils:call_fslogic(SessId, file_request,
        FileGuid, #fsync{data_only = false}, fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, true)
%%--------------------------------------------------------------------
-spec write(FileHandle :: lfm:handle(),
    Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: lfm:handle(), integer()} |
    lfm:error_reply().
write(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, true).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, false)
%%--------------------------------------------------------------------
-spec write_without_events(FileHandle :: lfm:handle(),
    Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: lfm:handle(), integer()} |
    lfm:error_reply().
write_without_events(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, false).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, true, true,
%% {priority, ?DEFAULT_SYNC_PRIORITY}, ignore_size)
%%--------------------------------------------------------------------
-spec read(FileHandle :: lfm:handle(), Offset :: integer(),
    MaxSize :: integer()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, true, true,
        {priority, ?DEFAULT_SYNC_PRIORITY}, ignore_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, true, true, SyncOptions, ignore_size)
%%--------------------------------------------------------------------
-spec read(FileHandle :: lfm:handle(), Offset :: integer(),
    MaxSize :: integer(), SyncOptions :: sync_options()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
read(FileHandle, Offset, MaxSize, SyncOptions) ->
    read(FileHandle, Offset, MaxSize, true, true, SyncOptions, ignore_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, true, true,
%% {priority, ?DEFAULT_SYNC_PRIORITY}, true)
%%--------------------------------------------------------------------
-spec check_size_and_read(FileHandle :: lfm:handle(), Offset :: integer(),
    MaxSize :: integer()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
check_size_and_read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, true, true,
        {priority, ?DEFAULT_SYNC_PRIORITY}, verify_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, true,
%% {priority, ?DEFAULT_SYNC_PRIORITY), ignore_size}
%%--------------------------------------------------------------------
-spec read_without_events(FileHandle :: lfm:handle(),
    Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
read_without_events(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, true,
        {priority, ?DEFAULT_SYNC_PRIORITY}, ignore_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, true, SyncOptions, ignore_size)
%%--------------------------------------------------------------------
-spec read_without_events(FileHandle :: lfm:handle(),
    Offset :: integer(), MaxSize :: integer(), SyncOptions :: sync_options()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
read_without_events(FileHandle, Offset, MaxSize, SyncOptions) ->
    read(FileHandle, Offset, MaxSize, false, true, SyncOptions, ignore_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, false,
%% {priority, ?DEFAULT_SYNC_PRIORITY), ignore_size}
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: lfm:handle(),
    Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
silent_read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, false,
        {priority, ?DEFAULT_SYNC_PRIORITY}, ignore_size).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, false, SyncOptions, ignore_size)
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: lfm:handle(),
    Offset :: integer(), MaxSize :: integer(), SyncOptions :: sync_options()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
silent_read(FileHandle, Offset, MaxSize, SyncOptions) ->
    read(FileHandle, Offset, MaxSize, false, false, SyncOptions, ignore_size).

%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) ->
    ok | lfm:error_reply().
truncate(SessId, FileKey, Size) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #truncate{size = Size},
        fun(_) ->
            ok = lfm_event_emitter:emit_file_truncated(FileGuid, Size, SessId)
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, Blocks :: [[non_neg_integer()]]} | lfm:error_reply().
get_file_distribution(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_file_distribution{},
        fun(#file_distribution{provider_file_distributions = Distributions}) ->
            {ok, lists:map(fun(#provider_file_distribution{provider_id = ProviderId, blocks = Blocks}) ->
                {BlockList, TotalBlocksSize} = lists:mapfoldl(fun(#file_block{offset = O, size = S}, SizeAcc) ->
                    {[O, S], SizeAcc + S}
                end, 0, Blocks),
                #{
                    <<"providerId">> => ProviderId,
                    <<"blocks">> => BlockList,
                    <<"totalBlocksSize">> => TotalBlocksSize
                }
            end, Distributions)}
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes data to a file. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: lfm:handle(), Offset :: integer(),
    Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, NewHandle :: lfm:handle(), integer()} |
    lfm:error_reply().
write(FileHandle, Offset, Buffer, GenerateEvents) ->
    Size = size(Buffer),
    case write_internal(FileHandle, Offset, Buffer, GenerateEvents) of
        {error, Reason} ->
            {error, Reason};
        {ok, Size} ->
            {ok, FileHandle, Size};
        {ok, 0} ->
            ?warning("File ~p write operation failed (0 bytes written), offset ~p, buffer size ~p",
                [FileHandle, Offset, Size]),
            {error, ?EAGAIN};
        {ok, Written} ->
            case write(FileHandle, Offset + Written,
                binary:part(Buffer, Written, Size - Written), GenerateEvents)
            of
                {ok, NewHandle, Written1} ->
                    {ok, NewHandle, Written + Written1};
                {error, Reason1} ->
                    {error, Reason1}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes one portion of data in write/3
%% @end
%%--------------------------------------------------------------------
-spec write_internal(FileHandle :: lfm:handle(),
    Offset :: non_neg_integer(), Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, non_neg_integer()} |lfm:error_reply().
write_internal(LfmCtx, Offset, Buffer, GenerateEvents) ->
    FileGuid = lfm_context:get_guid(LfmCtx),
    SessId = lfm_context:get_session_id(LfmCtx),
    FileId = lfm_context:get_file_id(LfmCtx),
    StorageId = lfm_context:get_storage_id(LfmCtx),
    HandleId = lfm_context:get_handle_id(LfmCtx),
    ProxyIORequest = #proxyio_request{
        parameters = #{
            ?PROXYIO_PARAMETER_FILE_GUID => FileGuid,
            ?PROXYIO_PARAMETER_HANDLE_ID => HandleId
        },
        file_id = FileId,
        storage_id = StorageId,
        proxyio_request = #remote_write{
            byte_sequence = [#byte_sequence{offset = Offset, data = Buffer}]
        }
    },

    remote_utils:call_fslogic(SessId, proxyio_request, ProxyIORequest,
        fun(#remote_write_result{wrote = Wrote}) ->
            WrittenBlocks = [#file_block{offset = Offset, size = Wrote}],
            ok = lfm_event_emitter:maybe_emit_file_written(FileGuid, WrittenBlocks,
                SessId, GenerateEvents),
            {ok, Wrote}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: lfm:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean(),
    SyncOptions :: sync_options(), VerifySize :: check_size_option()) ->
    {ok, NewHandle :: lfm:handle(), binary()} |
    lfm:error_reply().
read(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData, SyncOptions, VerifySize) ->
    case read_internal(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData, SyncOptions, VerifySize) of
        {error, Reason} ->
            {error, Reason};
        {ok, Bytes} ->
            {ok, FileHandle, Bytes}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads one portion of data in read/3
%% @end
%%--------------------------------------------------------------------
-spec read_internal(FileHandle :: lfm:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean(),
    SyncOptions :: sync_options(), VerifySize :: check_size_option()) ->
    {ok, binary()} | lfm:error_reply().
read_internal(LfmCtx, Offset, MaxSize, GenerateEvents, PrefetchData, SyncOptions, VerifySize) ->
    FileGuid = lfm_context:get_guid(LfmCtx),
    SessId = lfm_context:get_session_id(LfmCtx),

    % TODO VFS-6002 - temporary fix - not needed when helpers do not return data when offset is greater than size
    ReadSize = case VerifySize of
        verify_size ->
            FileCtx = file_ctx:new_by_guid(FileGuid),
            SpaceID = file_ctx:get_space_id_const(FileCtx),
            {ok, Providers} = space_logic:get_provider_ids(?ROOT_SESS_ID, SpaceID),
            case lists:member(oneprovider:get_id(), Providers) of
                true ->
                    {MetadataSize, _} = file_ctx:get_file_size(FileCtx),
                    min(MetadataSize - Offset, MaxSize);
                false ->
                    {ok, #file_attr{size = RemoteSize}} = lfm_attrs:stat(SessId, {guid, FileGuid}),
                    RemoteSize
            end;
        _ ->
            MaxSize
    end,

    case ReadSize > 0 of
        true ->
            case SyncOptions of
                off ->
                    ok;
                {priority, Priority} ->
                    sync_block(SessId, FileGuid, #file_block{offset = Offset, size = ReadSize},
                        PrefetchData, Priority, 0)
            end,

            FileId = lfm_context:get_file_id(LfmCtx),
            StorageId = lfm_context:get_storage_id(LfmCtx),
            HandleId = lfm_context:get_handle_id(LfmCtx),
            ProxyIORequest = #proxyio_request{
                parameters = #{
                    ?PROXYIO_PARAMETER_FILE_GUID => FileGuid,
                    ?PROXYIO_PARAMETER_HANDLE_ID => HandleId
                },
                file_id = FileId,
                storage_id = StorageId,
                proxyio_request = #remote_read{
                    offset = Offset,
                    size = ReadSize
                }
            },

            remote_utils:call_fslogic(SessId, proxyio_request, ProxyIORequest,
                fun(#remote_data{data = Data}) ->
                    ReadBlocks = [#file_block{offset = Offset, size = size(Data)}],
                    ok = lfm_event_emitter:maybe_emit_file_read(
                        FileGuid, ReadBlocks, SessId, GenerateEvents),
                    {ok, Data}
                end
            );
        _ ->
            {ok, <<>>}
    end.

%% @private
-spec sync_block(SessId :: session:id(), FileGuid :: fslogic_worker:file_guid(), Block :: fslogic_blocks:block(),
    PrefetchData :: boolean(), Priority :: non_neg_integer(), RetryNum :: non_neg_integer()) -> ok | no_return().
sync_block(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum) ->
    case remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #synchronize_block{block = Block, prefetch = PrefetchData, priority = Priority},
        fun(_) -> ok end) of
        ok -> ok;
        {error, Error} ->
            ?error("Error during synchronization requested by lfm, error code: ~p", [Error]),
            maybe_retry_sync(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum, Error)
    end.

%% @private
-spec maybe_retry_sync(SessId :: session:id(), FileGuid :: fslogic_worker:file_guid(), Block :: fslogic_blocks:block(),
    PrefetchData :: boolean(), Priority :: non_neg_integer(), RetryNum :: non_neg_integer(), Error :: code()) ->
    ok | no_return().
maybe_retry_sync(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum, Error) ->
    MaxRetires = ?SYNC_MAX_RETRIES,
    case RetryNum >= MaxRetires of
        true ->
            throw(Error);
        false ->
            SleepTime = round(?SYNC_MIN_BACKOFF * math:pow(?SYNC_BACKOFF_RATE, RetryNum)),
            timer:sleep(min(SleepTime, ?SYNC_MAX_BACKOFF)),
            sync_block(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum + 1)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes directory and all its children by moving it to trash.
%% @end
%%--------------------------------------------------------------------
-spec rm_using_trash(session:id(), fslogic_worker:ext_file()) ->
    ok | lfm:error_reply().
rm_using_trash(SessId, FileEntry) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileEntry),
    remote_utils:call_fslogic(SessId, file_request, Guid, #move_to_trash{},
        fun(_) -> ok end
    ).