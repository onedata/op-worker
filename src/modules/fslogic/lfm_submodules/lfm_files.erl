%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs file-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files).

-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
%% Functions operating on directories or files
-export([exists/1, mv/3, cp/3, get_parent/2, get_file_path/2]).
%% Functions operating on files
-export([create/2, create/3, open/3, fsync/1, write/3, write_without_events/3,
    read/3, read_without_events/3, silent_read/3,
    truncate/2, truncate/3, unlink/2, unlink/3, release/1,
    get_file_distribution/2, replicate_file/3]).

-compile({no_auto_import, [unlink/1]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: logical_file_manager:file_key()) ->
    {ok, boolean()} | logical_file_manager:error_reply().
exists(_FileKey) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mv(SessId, FileKey, TargetPath) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, file_request, GUID,
        #rename{target_path = TargetPath},
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
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, GUID,
        #copy{target_path = TargetPath},
        fun(#file_copied{new_uuid = NewGuid}) ->
            {ok, NewGuid}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
get_parent(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, FileGUID,
        #get_parent{},
        fun(#dir{uuid = ParentGUID}) ->
            {ok, ParentGUID}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(SessId :: session:id(), FileGUID :: fslogic_worker:file_guid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, FileGUID) ->
    lfm_utils:call_fslogic(SessId, provider_request, FileGUID,
        #get_file_path{},
        fun(#file_path{value = Path}) ->
            {ok, Path}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% If parameter Silent is true, file_removal_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec unlink(logical_file_manager:handle(), boolean()) ->
    ok | logical_file_manager:error_reply().
unlink(#lfm_handle{fslogic_ctx = #fslogic_ctx{session_id = SessId}, file_guid = GUID}, Silent) ->
    unlink(SessId, {guid, GUID}, Silent).

-spec unlink(session:id(), fslogic_worker:ext_file(), boolean()) ->
    ok | logical_file_manager:error_reply().
unlink(SessId, FileEntry, Silent) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileEntry),
    lfm_utils:call_fslogic(SessId, file_request, GUID, #delete_file{silent = Silent},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file with default mode.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
create(SessId, Path) ->
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    create(SessId, Path, DefaultMode).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(SessId, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    lfm_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = ParentPath},
        fun(#file_attr{uuid = ParentGUID}) ->
            lfm_utils:call_fslogic(SessId, file_request, ParentGUID,
                #get_new_file_location{
                    name = Name, mode = Mode, create_handle = false
                },
                fun(#file_location{uuid = GUID}) -> {ok, GUID} end
            )
        end).


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    OpenType :: helpers:open_mode()) ->
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(SessId, FileKey, OpenType) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    CTX2 = #fslogic_ctx{share_id = ShareId} = fslogic_context:set_space_and_share_id(CTX, {guid, FileGUID}),
    lfm_utils:call_fslogic(SessId, file_request, FileGUID,
        #get_file_location{flags = OpenType},
        fun(#file_location{provider_id = ProviderId, uuid = FileGUID,
            file_id = FileId, storage_id = StorageId} = Location) ->
            {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
            SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
            SFMHandle0 = storage_file_manager:new_handle(SessId, SpaceUUID,
                FileUUID, StorageId, FileId, ShareId, ProviderId),

            case storage_file_manager:open(SFMHandle0, OpenType) of
                {ok, NewSFMHandle} ->
                    {ok, #lfm_handle{file_location = normalize_file_location(Location),
                        provider_id = ProviderId,
                        sfm_handles = maps:from_list([{default,
                            {{StorageId, FileId}, NewSFMHandle}}]),
                        fslogic_ctx = CTX2, file_guid = FileGUID,
                        open_mode = OpenType}};
                {error, Reason} ->
                    {error, Reason}
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened  file.
%% @end
%%--------------------------------------------------------------------
-spec release(logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
release(#lfm_handle{file_location = #file_location{handle_id = undefined}}) ->
    ok;
release(#lfm_handle{file_guid = FileGUID, fslogic_ctx = CTX,
    file_location = #file_location{handle_id = FSLogicHandle}}) ->
    lfm_utils:call_fslogic(fslogic_context:get_session_id(CTX), file_request,
        FileGUID, #release{handle_id = FSLogicHandle},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
fsync(#lfm_handle{sfm_handles = SFMHandles, fslogic_ctx = #fslogic_ctx{session_id = SpecialSession}})
    when SpecialSession =:= ?ROOT_SESS_ID orelse SpecialSession =:= ?GUEST_SESS_ID ->
    lists:foreach(fun({_, SFMHandle}) ->
        ok = storage_file_manager:fsync(SFMHandle)
    end, maps:values(SFMHandles));
fsync(#lfm_handle{provider_id = ProviderId, file_guid = FileGUID, sfm_handles = SFMHandles, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    lists:foreach(fun({_, SFMHandle}) ->
            ok = storage_file_manager:fsync(SFMHandle)
        end, maps:values(SFMHandles)),
    RecvRef = event:flush(ProviderId, fslogic_uuid:guid_to_uuid(FileGUID), ?FSLOGIC_SUB_ID, self(), SessId),
    receive
        {RecvRef, Response} ->
            Response
    after ?DEFAULT_REQUEST_TIMEOUT ->
        {error, timeout}
    end.

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
-spec truncate(FileHandle :: logical_file_manager:handle(), Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(#lfm_handle{file_guid = FileGUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, Size) ->
    truncate(SessId, {guid, FileGUID}, Size).

-spec truncate(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(SessId, FileKey, Size) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, file_request, FileGUID,
        #truncate{size = Size},
        fun(_) ->
            event:emit(#write_event{file_uuid = FileGUID, blocks = [],
                file_size = Size}, SessId)
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, list()} | logical_file_manager:error_reply().
get_file_distribution(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, FileGUID,
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

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(), ProviderId :: oneprovider:id()) ->
    ok | logical_file_manager:error_reply().
replicate_file(SessId, FileKey, ProviderId) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, FileGUID,
        #replicate_file{provider_id = ProviderId},
        fun(_) -> ok end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For given file and byte range, returns storage's ID and file's ID (on storage)
%% that shall be used to store this byte range. Also returns maximum byte count that can be
%% used for this location. Atom 'default' is returned when location of specific block cannnot be found
%% and default locations shall be used instead.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_key(OpType :: write | read, #lfm_handle{}, Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key(OpType, #lfm_handle{file_guid = GUID, file_location = #file_location{blocks = InitBlocks}}, Offset, Size) ->
    Blocks = try
        [#document{value = LocalLocation}] = fslogic_utils:get_local_file_locations({guid, GUID}),
        #file_location{blocks = Blocks0} = LocalLocation,
        Blocks0
    catch
        _:_ ->
            InitBlocks
    end,
    case get_sfm_handle_key_internal(GUID, Offset, Size, Blocks) of
        {default, _} = SFMKey when OpType =:= read -> %% For read operation there has to be a explict block in file_location
            SFMKey;
        SFMKey ->
            SFMKey
    end.


%%--------------------------------------------------------------------
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
%% @doc
%% Helper function for read/write handles caching. Returns given handle or creates new and updates master handle.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_n_update_handle(Handle :: logical_file_manager:handle(), Key :: term(), SFMHandles :: sfm_handles_map(),
    OpenType :: helpers:open_mode()) ->
    {{StorageId :: storage:id(), FileId :: file_meta:uuid()},
        SFMHandle :: storage_file_manager:handle(),
        NewHandle :: logical_file_manager:handle()} |  no_return().
get_sfm_handle_n_update_handle(#lfm_handle{provider_id = ProviderId, file_guid = FileGUID,
    fslogic_ctx = #fslogic_ctx{session_id = SessId, share_id = ShareId}} = Handle,
    Key, SFMHandles, OpenType) ->
    {{StorageId, FileId}, SFMHandle} =
        case maps:get(Key, SFMHandles, undefined) of
            undefined ->
                {SID, FID} = Key,
                {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
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
    NewHandle = Handle#lfm_handle{sfm_handles = maps:put(Key, {{StorageId, FileId}, SFMHandle}, Handle#lfm_handle.sfm_handles)},
    {{StorageId, FileId}, SFMHandle, NewHandle}.


%%--------------------------------------------------------------------
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
%% @doc
%% Internal function for writing one portion of data in write/3
%% @end
%%--------------------------------------------------------------------
-spec write_internal(FileHandle :: logical_file_manager:handle(), Offset :: non_neg_integer(),
    Buffer :: binary(), GenerateEvents :: boolean()) ->
    {ok, logical_file_manager:handle(), non_neg_integer()} | logical_file_manager:error_reply().
write_internal(#lfm_handle{sfm_handles = SFMHandles, file_guid = UUID, open_mode = OpenType,
    fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, Buffer, GenerateEvents) ->
    {Key, NewSize} = get_sfm_handle_key(write, Handle, Offset, byte_size(Buffer)),

    {{StorageId, FileId}, SFMHandle, NewHandle = #lfm_handle{file_location = #file_location{blocks = CBlocks} = Location}}
        = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
        {ok, Written} ->

            WrittenBlocks = [#file_block{
                file_id = FileId, storage_id = StorageId, offset = Offset, size = Written
            }],
            NewBlocks = fslogic_blocks:merge(WrittenBlocks, CBlocks),
            case GenerateEvents of
                true ->
                    ok = event:emit(#write_event{
                        file_uuid = UUID, blocks = WrittenBlocks
                    }, SessId);
                false ->
                    ok
            end,
            {ok, NewHandle#lfm_handle{file_location = Location#file_location{blocks = NewBlocks}}, Written};
        {error, Reason2} ->
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
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
%% @doc
%% Internal function for reading one portion of data in read/3
%% @end
%%--------------------------------------------------------------------
-spec read_internal(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean(), PrefetchData :: boolean()) ->
    {ok, logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read_internal(#lfm_handle{sfm_handles = SFMHandles, file_guid = GUID, open_mode = OpenType,
    fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, MaxSize,
    GenerateEvents, PrefetchData) ->

    ok = lfm_utils:call_fslogic(SessId, file_request, GUID,
        #synchronize_block{block = #file_block{offset = Offset, size = MaxSize},
            prefetch = PrefetchData},
        fun(_) -> ok end),

    {Key, NewSize} = get_sfm_handle_key(read, Handle, Offset, MaxSize),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    case storage_file_manager:read(SFMHandle, Offset, NewSize) of
        {ok, Data} ->
            case GenerateEvents of
                true ->
                    ok = event:emit(#read_event{
                        file_uuid = GUID, blocks = [#file_block{
                            file_id = FileId, storage_id = StorageId, offset = Offset,
                            size = size(Data)
                        }]
                    }, SessId);
                false ->
                    ok
            end,
            {ok, NewHandle, Data};
        {error, Reason2} ->
            {error, Reason2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns given file_location with updated blocks filled with storage_id and/or file_id using default values if needed.
%% @end
%%--------------------------------------------------------------------
-spec normalize_file_location(#file_location{}) -> #file_location{}.
normalize_file_location(Loc = #file_location{storage_id = SID, file_id = FID, blocks = Blocks}) ->
    NewBlocks = [normalize_file_block(SID, FID, Block) || Block <- Blocks],
    Loc#file_location{blocks = NewBlocks}.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns given block with filled storage_id and/or file_id using provided default values if needed.
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