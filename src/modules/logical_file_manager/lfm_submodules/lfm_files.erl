%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs file-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files).

-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
%% Functions operating on directories or files
-export([
    unlink/3, rm_recursive/2,
    mv/4, cp/4,
    get_parent/2, get_file_path/2, get_file_guid/2, resolve_guid_by_relative_path/3, ensure_dir/4,
    is_dir/2
]).
%% Functions operating on files
-export([
    create/2, create/3, create/4,
    make_link/4,
    make_symlink/4,
    open/3,
    create_and_open/4, create_and_open/5,
    monitored_open/3, monitored_release/1,
    get_file_location/2, read_symlink/2, fsync/1, fsync/3, write/3,
    write_without_events/3, read/3, read/4, check_size_and_read/3, read_without_events/3,
    read_without_events/4, silent_read/3, silent_read/4,
    truncate/3, release/1, get_file_distribution/2
]).

-compile({no_auto_import, [unlink/1]}).

-define(DEFAULT_SYNC_PRIORITY, op_worker:get_env(default_sync_priority, 32)).
-define(SYNC_MAX_RETRIES, op_worker:get_env(lfm_sync_max_retries, 5)).
-define(SYNC_MIN_BACKOFF, op_worker:get_env(lfm_sync_min_backoff, timer:seconds(1))).
-define(SYNC_BACKOFF_RATE, op_worker:get_env(lfm_sync_backoff_rate, 2)).
-define(SYNC_MAX_BACKOFF, op_worker:get_env(lfm_sync_max_backoff, timer:minutes(1))).

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
-spec unlink(session:id(), lfm:file_key(), boolean()) ->
    ok | lfm:error_reply().
unlink(SessId, FileKey, Silent) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(
        SessId, file_request, FileGuid,
        #delete_file{silent = Silent},
        fun(_) -> ok end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or directory.
%% NOTE!!!
%% Directory will be moved to trash.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(session:id(), lfm:file_key()) ->
    ok | lfm:error_reply().
rm_recursive(SessId, FileKey) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    case is_dir(SessId, FileKey) of
        true ->
            rm_using_trash(SessId, ?FILE_REF(Guid));
        false ->
            unlink(SessId, ?FILE_REF(Guid), false);
        {error, ?ENOENT} ->
            ok;
        Error ->
            Error
    end.


-spec mv(session:id(), lfm:file_key(), lfm:file_key(), file_meta:name()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
mv(SessId, FileKey, TargetParentKey, TargetName) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    TargetDirGuid = lfm_file_key:resolve_file_key(SessId, TargetParentKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, Guid,
        #rename{target_parent_guid = TargetDirGuid, target_name = TargetName},
        fun(#file_renamed{new_guid = NewGuid}) ->
            {ok, NewGuid}
        end).


-spec cp(session:id(), lfm:file_key(), lfm:file_key(), file_meta:name()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
cp(SessId, FileKey, TargetParentKey, TargetName) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    TargetParentGuid = lfm_file_key:resolve_file_key(SessId, TargetParentKey, do_not_resolve_symlink),

    case file_copy:copy(SessId, Guid, TargetParentGuid, TargetName) of
        {ok, NewGuid, _} ->
            {ok, NewGuid};
        Error ->
            Error
    end.


-spec get_parent(session:id(), lfm:file_key()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
get_parent(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_parent{},
        fun(#dir{guid = ParentGuid}) -> {ok, ParentGuid} end
    ).


-spec get_file_path(session:id(), file_id:file_guid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, FileGuid) ->
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_file_path{},
        fun(#file_path{value = Path}) -> {ok, Path} end
    ).


-spec get_file_guid(session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()}.
get_file_guid(SessId, FilePath) ->
    remote_utils:call_fslogic(
        SessId, fuse_request,
        #resolve_guid{path = FilePath},
        fun(#guid{guid = Guid}) -> {ok, Guid} end
    ).


-spec resolve_guid_by_relative_path(session:id(), file_id:file_guid(), file_meta:path()) ->
    {ok, file_id:file_guid()}.
resolve_guid_by_relative_path(SessId, RelativeRootGuid, FilePath) ->
    remote_utils:call_fslogic(
        SessId, fuse_request,
        #resolve_guid_by_relative_path{
            root_file = RelativeRootGuid,
            path = FilePath
        },
        fun(#guid{guid = Guid}) -> {ok, Guid} end
    ).


-spec ensure_dir(session:id(), file_id:file_guid(), file_meta:path(), file_meta:mode()) ->
    {ok, file_id:file_guid()}.
ensure_dir(SessId, RelativeRootGuid, FilePath, Mode) ->
    remote_utils:call_fslogic(
        SessId, fuse_request,
        #ensure_dir{
            root_file = RelativeRootGuid,
            path = FilePath,
            mode = Mode
        },
        fun(#guid{guid = Guid}) -> {ok, Guid} end
    ).


-spec is_dir(session:id(), lfm:file_key()) ->
    true | false | lfm:error_reply().
is_dir(SessId, FileKey) ->
    case lfm:stat(SessId, FileKey) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        Error -> Error
    end.


-spec create(session:id(), file_meta:path()) ->
    {ok, file_meta:uuid()} | lfm:error_reply().
create(SessId, Path) ->
    create(SessId, Path, undefined).


-spec create(session:id(), file_meta:path(), undefined | file_meta:posix_permissions()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
create(SessId, Path, Mode) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),

    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create(SessId, ParentGuid, Name, Mode)
        end).


-spec create(
    session:id(),
    file_id:file_guid(),
    file_meta:name(),
    undefined | file_meta:posix_permissions()
) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
create(SessId, ParentGuid, Name, undefined) ->
    DefaultMode = op_worker:get_env(default_file_mode),
    create(SessId, ParentGuid, Name, DefaultMode);
create(SessId, ParentGuid0, Name, Mode) ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),
    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
        #make_file{name = Name, mode = Mode},
        fun(#file_attr{guid = Guid}) ->
            {ok, Guid}
        end
    ).


-spec make_link(session:id(), lfm:file_key(), lfm:file_key(), file_meta:name()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_link(SessId, FileKey, TargetParentKey, Name) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    TargetParentGuid = lfm_file_key:resolve_file_key(SessId, TargetParentKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #make_link{target_parent_guid = TargetParentGuid, target_name = Name},
        fun(#file_attr{} = FileAttr) ->
            {ok, FileAttr}
        end
    ).


-spec make_symlink(session:id(), lfm:file_key(), file_meta:name(), file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(SessId, ParentKey, Name, LinkTarget) ->
    Guid = lfm_file_key:resolve_file_key(SessId, ParentKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, Guid,
        #make_symlink{target_name = Name, link = LinkTarget},
        fun(#file_attr{} = FileAttr) ->
            {ok, FileAttr}
        end
    ).


-spec create_and_open(
    session:id(),
    file_meta:path(),
    undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()
) ->
    {ok, {file_id:file_guid(), lfm:handle()}} | lfm:error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),

    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag)
        end).


-spec create_and_open(
    session:id(),
    file_id:file_guid(),
    file_meta:name(),
    undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()
) ->
    {ok, {file_id:file_guid(), lfm:handle()}} | lfm:error_reply().
create_and_open(SessId, ParentGuid, Name, undefined, OpenFlag) ->
    DefaultMode = op_worker:get_env(default_file_mode),
    create_and_open(SessId, ParentGuid, Name, DefaultMode, OpenFlag);
create_and_open(SessId, ParentGuid0, Name, Mode, OpenFlag) ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),

    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
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


-spec open(session:id(), lfm:file_key(), fslogic_worker:open_flag()) ->
    {ok, lfm:handle()} | lfm:error_reply().
open(SessId, FileKey, Flag) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

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
%% Opens a file in selected mode. The state of process opening file using this function
%% is monitored so that all opened handles can be closed when it unexpectedly dies
%% (e.g. client abruptly closes connection).
%% @end
%%--------------------------------------------------------------------
-spec monitored_open(session:id(), lfm:file_key(), helpers:open_flag()) ->
    {ok, lfm:handle()} | lfm:error_reply().
monitored_open(SessId, FileKey, OpenType) ->
    {ok, FileHandle} = lfm_files:open(SessId, FileKey, OpenType),
    case process_handles:add(FileHandle) of
        ok ->
            {ok, FileHandle};
        {error, _} = Error ->
            ?error("Failed to perform 'monitored_open' due to ~p", [Error]),
            monitored_release(FileHandle),
            {error, ?EAGAIN}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened file. If it is the last handle opened by this process
%% using `monitored_open` then the state of process will no longer be monitored
%% (even if process unexpectedly dies there are no handles to release).
%% @end
%%--------------------------------------------------------------------
-spec monitored_release(lfm:handle()) -> ok | lfm:error_reply().
monitored_release(FileHandle) ->
    Result = release(FileHandle),
    process_handles:remove(FileHandle),
    Result.


%%--------------------------------------------------------------------
%% @doc
%% Returns location to file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(session:id(), lfm:file_key()) ->
    {ok, file_location:record()} | lfm:error_reply().
get_file_location(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_location{},
        fun(#file_location{} = FL) -> {ok, FL} end
    ).


-spec read_symlink(session:id(), lfm:file_key()) ->
    {ok, file_meta_symlinks:symlink()} | lfm:error_reply().
read_symlink(SessId, FileKey) ->
    {ok, FileGuid} = lfm_file_key:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #read_symlink{},
        fun(#symlink{link = Link}) -> {ok, Link} end
    ).


-spec fsync(lfm:handle()) ->
    ok | lfm:error_reply().
fsync(Handle) ->
    SessionId = lfm_context:get_session_id(Handle),
    FileGuid = lfm_context:get_guid(Handle),
    ProviderId = lfm_context:get_provider_id(Handle),
    fsync(SessionId, ?FILE_REF(FileGuid), ProviderId).


-spec fsync(session:id(), lfm:file_key(), oneprovider:id()) ->
    ok | lfm:error_reply().
fsync(SessId, FileKey, ProviderId) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),
    case oneprovider:is_self(ProviderId) of
        true ->
            ok; % flush also flushes events for provider
        _ ->
            lfm_event_controller:flush_event_queue(SessId, ProviderId,
                file_id:guid_to_uuid(FileGuid))
    end,
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #fsync{data_only = false}, fun(_) -> ok end
    ).


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


-spec truncate(session:id(), lfm:file_key(), non_neg_integer()) ->
    ok | lfm:error_reply().
truncate(SessId, FileKey, Size) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

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
-spec get_file_distribution(session:id(), lfm:file_key()) ->
    {ok, Blocks :: [[non_neg_integer()]]} | lfm:error_reply().
get_file_distribution(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

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
                    {ok, #file_attr{
                        size = RemoteSize
                    }} = lfm:stat(SessId, ?FILE_REF(FileGuid)),

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
-spec sync_block(SessId :: session:id(), FileGuid :: file_id:file_guid(), Block :: fslogic_blocks:block(),
    PrefetchData :: boolean(), Priority :: non_neg_integer(), RetryNum :: non_neg_integer()) -> ok | no_return().
sync_block(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum) ->
    case remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #synchronize_block{block = Block, prefetch = PrefetchData, priority = Priority},
        fun(_) -> ok end) of
        ok -> ok;
        {error, Error} ->
            ?error("Error during synchronization requested by lfm, error code: ~p", [Error]),
            maybe_retry_sync(SessId, FileGuid, Block, PrefetchData, Priority, RetryNum, {error, Error})
    end.

%% @private
-spec maybe_retry_sync(SessId :: session:id(), FileGuid :: file_id:file_guid(), Block :: fslogic_blocks:block(),
    PrefetchData :: boolean(), Priority :: non_neg_integer(), RetryNum :: non_neg_integer(), Error :: {error, code()}) ->
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
-spec rm_using_trash(session:id(), lfm:file_key()) ->
    ok | lfm:error_reply().
rm_using_trash(SessId, FileKey) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    remote_utils:call_fslogic(SessId, file_request, Guid, #move_to_trash{},
        fun(_) -> ok end
    ).
