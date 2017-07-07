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
-export([unlink/3, rm/2, mv/3, cp/3, get_parent/2, get_file_path/2, replicate_file/3]).
%% Functions operating on files
-export([create/2, create/3, create/4, open/3, fsync/1, fsync/3, write/3,
    write_without_events/3, read/3, read_without_events/3, silent_read/3,
    truncate/3, release/1, get_file_distribution/2, create_and_open/5,
    create_and_open/4]).

-compile({no_auto_import, [unlink/1]}).

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
    ok | logical_file_manager:error_reply().
unlink(SessId, FileEntry, Silent) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileEntry),
    remote_utils:call_fslogic(SessId, file_request, Guid, #delete_file{silent = Silent},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @equiv remove_utils:rm(SessId, FileKey).
%%--------------------------------------------------------------------
-spec rm(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
rm(SessId, FileKey) ->
    remove_utils:rm(SessId, FileKey).

%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mv(SessId, FileKey, TargetPath) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    {TargetName, TargetDir} = fslogic_path:basename_and_parent(TargetPath),
    {guid, TargetDirGuid} = guid_utils:ensure_guid(SessId, {path, TargetDir}),
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
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
cp(SessId, FileKey, TargetPath) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    {TargetName, TargetParentPath} = fslogic_path:basename_and_parent(TargetPath),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = TargetParentPath},
        fun(#guid{guid = TargetParentGuid}) ->
            case copy_utils:copy(SessId, Guid, TargetParentGuid, TargetName) of
                {ok, NewGuid, _} ->
                    {ok, NewGuid};
                Error ->
                    Error
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
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
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id()) -> ok | logical_file_manager:error_reply().
replicate_file(SessId, FileKey, ProviderId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #replicate_file{provider_id = ProviderId},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
create(SessId, Path) ->
    create(SessId, Path, undefined).

-spec create(session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(SessId, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create(SessId, ParentGuid, Name, Mode)
        end).

-spec create(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(SessId, ParentGuid, Name, undefined) ->
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    create(SessId, ParentGuid, Name, DefaultMode);
create(SessId, ParentGuid, Name, Mode) ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #make_file{name = Name, mode = Mode},
        fun(#file_attr{guid = Guid}) ->
            {ok, Guid}  %todo consider returning file_attr
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file and opens it
%% @end
%%--------------------------------------------------------------------
-spec create_and_open(session:id(), Path :: file_meta:path(),
    Mode :: undefined | file_meta:posix_permissions(), fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), logical_file_manager:handle()}}
    | logical_file_manager:error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            lfm_files:create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag)
        end).

-spec create_and_open(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), logical_file_manager:handle()}}
    | logical_file_manager:error_reply().
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
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(SessId, FileKey, Flag) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_location{},
        fun(#file_location{provider_id = ProviderId, file_id = FileId,
            storage_id = StorageId}) ->
            remote_utils:call_fslogic(SessId, file_request, FileGuid,
                #open_file{flag = Flag},
                fun(#file_opened{handle_id = HandleId}) ->
                    {ok, lfm_context:new(
                        HandleId,
                        ProviderId,
                        SessId,
                        FileGuid,
                        Flag,
                        FileId,
                        StorageId
                    )}
                end)
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened  file.
%% @end
%%--------------------------------------------------------------------
-spec release(logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
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
%% Gets necessary data from handle and executes fsync/3
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
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
    ok | logical_file_manager:error_reply().
fsync(SessId, FileKey, ProviderId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),

    lfm_event_utils:flush_event_queue(SessId, ProviderId,
        fslogic_uuid:guid_to_uuid(FileGuid)),
    remote_utils:call_fslogic(SessId, file_request,
        FileGuid, #fsync{data_only = false}, fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, true)
%%--------------------------------------------------------------------
-spec write(FileHandle :: logical_file_manager:handle(),
    Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} |
    logical_file_manager:error_reply().
write(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, true).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, false)
%%--------------------------------------------------------------------
-spec write_without_events(FileHandle :: logical_file_manager:handle(),
    Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} |
    logical_file_manager:error_reply().
write_without_events(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, false).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, true, true)
%%--------------------------------------------------------------------
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} |
    logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, true, true).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, true)
%%--------------------------------------------------------------------
-spec read_without_events(FileHandle :: logical_file_manager:handle(),
    Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} |
    logical_file_manager:error_reply().
read_without_events(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, true).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, false)
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: logical_file_manager:handle(),
    Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} |
    logical_file_manager:error_reply().
silent_read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, false).

%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(SessId, FileKey, Size) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #truncate{size = Size},
        fun(_) ->
            ok = lfm_event_utils:emit_file_truncated(FileGuid, Size, SessId)
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, list()} | logical_file_manager:error_reply().
get_file_distribution(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_file_distribution{},
        fun(#file_distribution{provider_file_distributions = Distributions}) ->
            Distribution =
                lists:map(fun(#provider_file_distribution{
                    provider_id = ProviderId,
                    blocks = Blocks
                }) ->
                    #{
                        <<"providerId">> => ProviderId,
                        <<"blocks">> => lists:map(fun(#file_block{offset = O, size = S}) ->
                            [O, S] end, Blocks)
                    }
                end, Distributions),
            {ok, Distribution}
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
-spec write(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} |
    logical_file_manager:error_reply().
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
-spec write_internal(FileHandle :: logical_file_manager:handle(),
    Offset :: non_neg_integer(), Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, non_neg_integer()} |logical_file_manager:error_reply().
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
            ok = lfm_event_utils:maybe_emit_file_written(FileGuid, WrittenBlocks,
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
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} |
    logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData) ->
    case read_internal(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData) of
        {error, Reason} ->
            {error, Reason};
        {ok, Bytes} ->
            case size(Bytes) of
                MaxSize ->
                    {ok, FileHandle, Bytes};
                0 ->
                    {ok, FileHandle, Bytes};
                Size ->
                    case read(FileHandle, Offset + Size, MaxSize - Size,
                        GenerateEvents, PrefetchData)
                    of
                        {ok, NewHandle1, Bytes1} ->
                            {ok, NewHandle1, <<Bytes/binary, Bytes1/binary>>};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads one portion of data in read/3
%% @end
%%--------------------------------------------------------------------
-spec read_internal(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean()) ->
    {ok, binary()} | logical_file_manager:error_reply().
read_internal(LfmCtx, Offset, MaxSize, GenerateEvents, PrefetchData) ->
    FileGuid = lfm_context:get_guid(LfmCtx),
    SessId = lfm_context:get_session_id(LfmCtx),
    ok = remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #synchronize_block{block = #file_block{offset = Offset, size = MaxSize},
            prefetch = PrefetchData},
        fun(_) -> ok end),

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
            size = MaxSize
        }
    },

    remote_utils:call_fslogic(SessId, proxyio_request, ProxyIORequest,
        fun(#remote_data{data = Data}) ->
            ReadBlocks = [#file_block{offset = Offset, size = size(Data)}],
            ok = lfm_event_utils:maybe_emit_file_read(FileGuid, ReadBlocks, SessId, GenerateEvents),
            {ok, Data}
        end
    ).
