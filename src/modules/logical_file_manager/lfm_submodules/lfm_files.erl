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
-include_lib("ctool/include/logging.hrl").

%% API
%% Functions operating on directories or files
-export([unlink/3, rm/2, mv/3, cp/3, get_parent/2, get_file_path/2, replicate_file/3]).
%% Functions operating on files
-export([create/2, create/3, create/4, open/3, fsync/1, write/3, write_without_events/3,
    read/3, read_without_events/3, silent_read/3, truncate/3, release/1,
    get_file_distribution/2]).

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
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileEntry),
    remote_utils:call_fslogic(SessId, file_request, Guid, #delete_file{silent = Silent},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @equiv remove_utils:rm(SessId, FileKey).
%%--------------------------------------------------------------------
-spec rm(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
rm(SessId, FileKey) ->
    remove_utils:rm(SessId, FileKey).

%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mv(SessId, FileKey, TargetPath) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    {TargetName, TargetDir} = fslogic_path:basename_and_parent(TargetPath),
    {guid, TargetDirGuid} = fslogic_uuid:ensure_guid(SessId, {path, TargetDir}),
    remote_utils:call_fslogic(SessId, file_request, Guid,
        #rename{target_parent_uuid = TargetDirGuid, target_name = TargetName},
        fun(#file_renamed{new_uuid = NewGuid}) ->
            {ok, NewGuid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(), TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
cp(SessId, FileKey, TargetPath) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    copy_utils:copy(SessId, {guid, Guid}, TargetPath).

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
get_parent(SessId, FileKey) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_parent{},
        fun(#dir{uuid = ParentGuid}) ->
            {ok, ParentGuid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(SessId :: session:id(), FileGuid :: fslogic_worker:file_guid()) ->
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
-spec replicate_file(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(), ProviderId :: oneprovider:id()) ->
    ok | logical_file_manager:error_reply().
replicate_file(SessId, FileKey, ProviderId) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #replicate_file{provider_id = ProviderId},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
create(SessId, Path) ->
    create(SessId, Path, undefined).

-spec create(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(SessId, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    remote_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#uuid{uuid = ParentGuid}) ->
            lfm_files:create(SessId, ParentGuid, Name, Mode)
        end).

-spec create(SessId :: session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(SessId, ParentGuid, Name, undefined) ->
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    create(SessId, ParentGuid, Name, DefaultMode);
create(SessId, ParentGuid, Name, Mode) ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #make_file{name = Name, mode = Mode},
        fun(#file_attr{uuid = Guid}) ->
            {ok, Guid}  %todo consider returning file_attr
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Flag :: fslogic_worker:open_flag()) ->
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(SessId, FileKey, Flag) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    ShareId = fslogic_uuid:guid_to_share_id(FileGuid),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_location{},
        fun(#file_location{provider_id = ProviderId, file_id = FileId,
            storage_id = StorageId} = Location) ->
            remote_utils:call_fslogic(SessId, file_request, FileGuid,
                #open_file{flag = Flag},
                fun(#file_opened{handle_id = HandleId}) ->
                    {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGuid),
                    SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                    SFMHandle0 = storage_file_manager:new_handle(SessId, SpaceUUID,
                        FileUUID, StorageId, FileId, ShareId, ProviderId),

                    case storage_file_manager:open(SFMHandle0, Flag) of
                        {ok, Handle} ->
                            {ok, lfm_context:new(
                                HandleId,
                                normalize_file_location(Location),
                                ProviderId,
                                #{default => {{StorageId, FileId}, Handle}},
                                SessId,
                                FileGuid,
                                Flag
                            )};
                        {error, Reason} ->
                            {error, Reason}
                    end
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
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
fsync(Handle) ->
    SFMHandles = lfm_context:get_sfm_handles(Handle),
    SessionId = lfm_context:get_session_id(Handle),
    FileGuid = lfm_context:get_guid(Handle),
    ProviderId = lfm_context:get_provider_id(Handle),

    lists:foreach(fun({_, SFMHandle}) ->
        ok = storage_file_manager:fsync(SFMHandle)
    end, maps:values(SFMHandles)),
    fslogic_event:flush_event_queue(SessionId, ProviderId, fslogic_uuid:guid_to_uuid(FileGuid)).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, true)
%%--------------------------------------------------------------------
-spec write(FileHandle :: logical_file_manager:handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} | logical_file_manager:error_reply().
write(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, true).

%%--------------------------------------------------------------------
%% @equiv write(FileHandle, Offset, Buffer, false)
%%--------------------------------------------------------------------
-spec write_without_events(FileHandle :: logical_file_manager:handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} | logical_file_manager:error_reply().
write_without_events(FileHandle, Offset, Buffer) ->
    write(FileHandle, Offset, Buffer, false).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, true, true)
%%--------------------------------------------------------------------
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, true, true).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, true)
%%--------------------------------------------------------------------
-spec read_without_events(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read_without_events(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, true).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false, false)
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
silent_read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false, false).

%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(SessId, FileKey, Size) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #truncate{size = Size},
        fun(_) ->
            ok = fslogic_event:emit_file_truncated(FileGuid, Size, SessId)
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, list()} | logical_file_manager:error_reply().
get_file_distribution(SessId, FileKey) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_file_distribution{},
        fun(#file_distribution{provider_file_distributions = Distributions}) ->
            Distribution =
                lists:map(fun(#provider_file_distribution{provider_id = ProviderId, blocks = Blocks}) ->
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
%% For given file and byte range, returns storage's ID and file's ID
%% (on storage) that shall be used to store this byte range. Also returns
%% maximum byte count that can be used for this location. Atom 'default' is
%% returned when location of specific block cannnot be found and default
%% locations shall be used instead.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_key(OpType :: write | read, lfm_context:handle(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key(OpType, Handle, Offset, Size) ->
    Guid = lfm_context:get_guid(Handle),
    #file_location{blocks = InitBlocks} = lfm_context:get_file_location(Handle),

    Blocks = try %todo cache location in handle
        #document{value = LocalLocation} = fslogic_utils:get_local_file_location({guid, Guid}), %todo VFS-2813 support multi location
        #file_location{blocks = Blocks0} = LocalLocation,
        Blocks0
    catch
        _:_ ->
            InitBlocks
    end,
    case get_sfm_handle_key_internal(Guid, Offset, Size, Blocks) of
        {default, _} = SFMKey when OpType =:= read -> %% For read operation there has to be a explict block in file_location
            SFMKey;
        SFMKey ->
            SFMKey
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal impl. of get_sfm_handle_key/3
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_key_internal(file_meta:uuid(), Offset :: non_neg_integer(), Size :: non_neg_integer(), fslogic_blocks:blocks() | fslogic_blocks:block()) ->
    {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key_internal(UUID, Offset, Size, [#file_block{offset = O, size = S} | T]) when O + S =< Offset ->
    get_sfm_handle_key_internal(UUID, Offset, Size, T);
get_sfm_handle_key_internal(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
    when Offset >= O, Offset + Size =< O + S ->
    {{SID, FID}, Size};
get_sfm_handle_key_internal(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
    when Offset >= O, Offset + Size > O + S ->
    {{SID, FID}, S - (Offset - O)};
get_sfm_handle_key_internal(_UUID, Offset, Size, [#file_block{offset = O, size = _S} | _]) when Offset + Size =< O ->
    {default, Size};
get_sfm_handle_key_internal(_UUID, Offset, _Size, [#file_block{offset = O, size = _S} | _]) ->
    {default, O - Offset};
get_sfm_handle_key_internal(_UUID, _Offset, Size, []) ->
    {default, Size}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Caches read/write handles. Returns given handle or creates new and updates
%% master handle.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_n_update_handle(Handle :: logical_file_manager:handle(), Key :: term(),
    OpenType :: helpers:open_flag()) ->
    {{StorageId :: storage:id(), FileId :: file_meta:uuid()},
        SFMHandle :: storage_file_manager:handle(),
        NewHandle :: logical_file_manager:handle()} |  no_return().
get_sfm_handle_n_update_handle(Handle, Key, OpenType) ->
    ProviderId = lfm_context:get_provider_id(Handle),
    FileGuid = lfm_context:get_guid(Handle),
    SessId = lfm_context:get_session_id(Handle),
    ShareId = lfm_context:get_share_id(Handle),
    SFMHandles = lfm_context:get_sfm_handles(Handle),

    {{StorageId, FileId}, SFMHandle} =
        case maps:get(Key, SFMHandles, undefined) of
            undefined ->
                {SID, FID} = Key,
                {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGuid),
                SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                SFMHandle0 = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, SID, FID, ShareId, ProviderId),

                case storage_file_manager:open(SFMHandle0, OpenType) of
                    {ok, NewSFMHandle} ->
                        {{SID, FID}, NewSFMHandle};
                    {error, Reason1} ->
                        {error, Reason1}
                end;
            {{SID, FID}, CachedHandle} ->
                {{SID, FID}, CachedHandle}
        end,
    NewHandle = lfm_context:set_sfm_handles(Handle, maps:put(Key, {{StorageId, FileId}, SFMHandle}, SFMHandles)),
    {{StorageId, FileId}, SFMHandle, NewHandle}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes data to a file. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, NewHandle :: logical_file_manager:handle(), integer()} | logical_file_manager:error_reply().
write(FileHandle, Offset, Buffer, GenerateEvents) ->
    Size = size(Buffer),
    case write_internal(FileHandle, Offset, Buffer, GenerateEvents) of
        {error, Reason} ->
            {error, Reason};
        {ok, _, Size} = Ret1 ->
            Ret1;
        {ok, _, 0} ->
            ?warning("File ~p write operation failed (0 bytes written), offset ~p, buffer size ~p",
                [FileHandle, Offset, Size]),
            {error, ?EAGAIN};
        {ok, NewHandle, Written} ->
            case write(NewHandle, Offset + Written, binary:part(Buffer, Written, Size - Written), GenerateEvents) of
                {ok, NewHandle1, Written1} ->
                    {ok, NewHandle1, Written + Written1};
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
-spec write_internal(FileHandle :: logical_file_manager:handle(), Offset :: non_neg_integer(),
    Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, logical_file_manager:handle(), non_neg_integer()} | logical_file_manager:error_reply().
write_internal(Handle, Offset, Buffer, GenerateEvents) ->
    Guid = lfm_context:get_guid(Handle),
    Flag = lfm_context:get_open_flag(Handle),
    SessId = lfm_context:get_session_id(Handle),

    {Key, NewSize} = get_sfm_handle_key(write, Handle, Offset, byte_size(Buffer)),

    {{StorageId, FileId}, SFMHandle, NewHandle}
        = get_sfm_handle_n_update_handle(Handle, Key, Flag),
    Location = #file_location{blocks = CBlocks} = lfm_context:get_file_location(NewHandle),

    case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
        {ok, Written} ->

            WrittenBlocks = [#file_block{
                file_id = FileId, storage_id = StorageId, offset = Offset, size = Written
            }],
            NewBlocks = fslogic_blocks:merge(WrittenBlocks, CBlocks),
            ok = fslogic_event:maybe_emit_file_written(Guid, WrittenBlocks, SessId, GenerateEvents),
            NewLocationHandle = lfm_context:set_file_location(NewHandle, Location#file_location{blocks = NewBlocks}),
            {ok, NewLocationHandle, Written};
        {error, Reason2} ->
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData) ->
    case read_internal(FileHandle, Offset, MaxSize, GenerateEvents, PrefetchData) of
        {error, Reason} ->
            {error, Reason};
        {ok, NewHandle, Bytes} = Ret1 ->
            case size(Bytes) of
                MaxSize ->
                    Ret1;
                0 ->
                    Ret1;
                Size ->
                    case read(NewHandle, Offset + Size, MaxSize - Size, GenerateEvents, PrefetchData) of
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
    {ok, logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read_internal(Handle, Offset, MaxSize, GenerateEvents, PrefetchData) ->
    Guid = lfm_context:get_guid(Handle),
    Flag = lfm_context:get_open_flag(Handle),
    SessId = lfm_context:get_session_id(Handle),

    ok = remote_utils:call_fslogic(SessId, file_request, Guid,
        #synchronize_block{block = #file_block{offset = Offset, size = MaxSize},
            prefetch = PrefetchData},
        fun(_) -> ok end),

    {Key, NewSize} = get_sfm_handle_key(read, Handle, Offset, MaxSize),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, Flag),

    case storage_file_manager:read(SFMHandle, Offset, NewSize) of
        {ok, Data} ->
            ReadBlocks = [#file_block{
                file_id = FileId, storage_id = StorageId, offset = Offset,
                size = size(Data)
            }],
            ok = fslogic_event:maybe_emit_file_read(Guid, ReadBlocks, SessId, GenerateEvents),

            {ok, NewHandle, Data};
        {error, Reason2} ->
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns given file_location with updated blocks filled with storage_id and/or
%% file_id using default values if needed.
%% @end
%%--------------------------------------------------------------------
-spec normalize_file_location(#file_location{}) -> #file_location{}.
normalize_file_location(Loc = #file_location{storage_id = SID, file_id = FID, blocks = Blocks}) ->
    NewBlocks = [normalize_file_block(SID, FID, Block) || Block <- Blocks],
    Loc#file_location{blocks = NewBlocks}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns given block with filled storage_id and/or file_id using provided
%% default values if needed.
%% @end
%%--------------------------------------------------------------------
-spec normalize_file_block(storage:id(), helpers:file(), #file_block{}) -> #file_block{}.
normalize_file_block(SID, FID, #file_block{storage_id = undefined, file_id = undefined} = Block) ->
    Block#file_block{storage_id = SID, file_id = FID};
normalize_file_block(SID, _FID, #file_block{storage_id = undefined} = Block) ->
    Block#file_block{storage_id = SID};
normalize_file_block(_SID, FID, #file_block{file_id = undefined} = Block) ->
    Block#file_block{file_id = FID};
normalize_file_block(_SID, _FID, #file_block{} = Block) ->
    Block.
