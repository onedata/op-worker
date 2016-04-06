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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2, get_parent/2, get_file_path/2]).
%% Functions operating on files
-export([create/3, open/3, fsync/1, write/3, write_without_events/3, read/3,
    read_without_events/3, truncate/2, truncate/3, get_block_map/1,
    get_block_map/2, unlink/1, unlink/2]).

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
-spec mv(FileKeyFrom :: logical_file_manager:file_key(), PathTo :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
mv(_FileKeyFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: logical_file_manager:file_key(), PathTo :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
cp(_PathFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessId :: session:id(), FileKey :: file_meta:uuid_or_path()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
get_parent(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, UUID} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_parent{uuid = UUID},
        fun(#dir{uuid = ParentUUID}) ->
            {ok, ParentUUID}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(SessId :: session:id(), Uuid :: file_meta:uuid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, Uuid) ->
    CTX = fslogic_context:new(SessId),
    {ok, fslogic_uuid:uuid_to_path(CTX, Uuid)}.


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
unlink(#lfm_handle{fslogic_ctx = #fslogic_ctx{session_id = SessId}, file_uuid = UUID}) ->
    unlink(SessId, {uuid, UUID}).

-spec unlink(session:id(), fslogic_worker:file()) ->
    ok | logical_file_manager:error_reply().
unlink(SessId, FileEntry) ->
    CTX = fslogic_context:new(SessId),
    {uuid, UUID} = fslogic_uuid:ensure_uuid(CTX, FileEntry),
    lfm_utils:call_fslogic(SessId, #delete_file{uuid = UUID},
        fun(_) ->
            ok
        end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
create(SessId, Path, Mode) ->
    CTX = fslogic_context:new(SessId),
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    Entry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, CanonicalPath} = fslogic_path:gen_path(Entry, SessId),
    {Name, ParentPath} = fslogic_path:basename_and_parent(CanonicalPath),
    case file_meta:resolve_path(ParentPath) of
        {ok, {#document{key = ParentUUID}, _}} ->
            lfm_utils:call_fslogic(SessId,
                #get_new_file_location{
                    name = Name, parent_uuid = ParentUUID, mode = Mode
                },
                fun(#file_location{uuid = UUID}) -> {ok, UUID} end
            );
        {error, Error} -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(SessId :: session:id(), FileKey :: file_meta:uuid_or_path(),
    OpenType :: helpers:open_mode()) ->
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(SessId, FileKey, OpenType) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUUID} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_file_location{uuid = FileUUID, flags = OpenType},
        fun(#file_location{uuid = _UUID, file_id = FileId, storage_id = StorageId}) ->
            {ok, #document{value = Storage}} = storage:get(StorageId),
            {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, _UUID}, fslogic_context:get_user_id(CTX)),
            SFMHandle0 = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FileId),

            case storage_file_manager:open(SFMHandle0, OpenType) of
                {ok, NewSFMHandle} ->
                    {ok, #lfm_handle{sfm_handles = maps:from_list([{default, {{StorageId, FileId}, NewSFMHandle}}]),
                        fslogic_ctx = CTX, file_uuid = _UUID, open_mode = OpenType}};
                {error, Reason} ->
                    {error, Reason}
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
fsync(#lfm_handle{fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    event:flush(?FSLOGIC_SUB_ID, self(), SessId),
    receive
        {handler_executed, Results} ->
            Errors = lists:filter(fun
                ({error, _}) -> true;
                (_) -> false
            end, Results),
            case Errors of
                [] -> ok;
                _ -> {error, {handler_error, Errors}}
            end
    after
        ?FSYNC_TIMEOUT ->
            {error, handler_timeout}
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
%% @equiv read(FileHandle, Offset, MaxSize, true)
%%--------------------------------------------------------------------
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, true).

%%--------------------------------------------------------------------
%% @equiv read(FileHandle, Offset, MaxSize, false)
%%--------------------------------------------------------------------
-spec read_without_events(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read_without_events(FileHandle, Offset, MaxSize) ->
    read(FileHandle, Offset, MaxSize, false).

%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileHandle :: logical_file_manager:handle(), Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(#lfm_handle{file_uuid = FileUUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, Size) ->
    truncate(SessId, {uuid, FileUUID}, Size).

-spec truncate(SessId :: session:id(), FileKey :: file_meta:uuid_or_path(),
    Size :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
truncate(SessId, FileKey, Size) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUUID} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #truncate{uuid = FileUUID, size = Size},
        fun(_) ->
            event:emit(#write_event{file_uuid = FileUUID, blocks = [], file_size = Size}, SessId)
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(FileHandle :: logical_file_manager:handle()) ->
    {ok, fslogic_blocks:blocks()} | logical_file_manager:error_reply().
get_block_map(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    get_block_map(SessId, {uuid, UUID}).

-spec get_block_map(SessId :: session:id(), FileKey :: file_meta:uuid_or_path()) ->
    {ok, fslogic_blocks:blocks()} | logical_file_manager:error_reply().
get_block_map(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, UUID} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    #document{value = LocalLocation} = fslogic_utils:get_local_file_location({uuid, UUID}),
    #file_location{blocks = Blocks} = LocalLocation,
    {ok, Blocks}.

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
-spec get_sfm_handle_key(file_meta:uuid(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key(UUID, Offset, Size) ->
    #document{value = LocalLocation} = fslogic_utils:get_local_file_location({uuid, UUID}),
    #file_location{blocks = Blocks} = LocalLocation,
    get_sfm_handle_key(UUID, Offset, Size, Blocks).


%%--------------------------------------------------------------------
%% @doc
%% Internal impl. of get_sfm_handle_key/3
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_key(file_meta:uuid(), Offset :: non_neg_integer(), Size :: non_neg_integer(), fslogic_blocks:blocks() | fslogic_blocks:block()) ->
    {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key(UUID, Offset, Size, [#file_block{offset = O, size = S} | T]) when O + S =< Offset ->
    get_sfm_handle_key(UUID, Offset, Size, T);
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
    when Offset >= O, Offset + Size =< O + S ->
    {{SID, FID}, Size};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
    when Offset >= O, Offset + Size > O + S ->
    {{SID, FID}, S - (Offset - O)};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = _S} | _]) when Offset + Size =< O ->
    {default, Size};
get_sfm_handle_key(_UUID, _Offset, Size, []) ->
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
get_sfm_handle_n_update_handle(#lfm_handle{file_uuid = FileUUID, fslogic_ctx = #fslogic_ctx{session_id = SessId} = CTX} = Handle,
    Key, SFMHandles, OpenType) ->
    {{StorageId, FileId}, SFMHandle} =
        case maps:get(Key, SFMHandles, undefined) of
            undefined ->
                {SID, FID} = Key,
                {ok, #document{value = Storage}} = storage:get(SID),
                {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, FileUUID}, fslogic_context:get_user_id(CTX)),
                SFMHandle0 = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),

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
        {ok, _, 0} = Ret2 ->
            Ret2;
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
write_internal(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType,
    fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, Buffer, GenerateEvents) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, byte_size(Buffer)),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
        {ok, Written} ->
            case GenerateEvents of
                true ->
                    ok = event:emit(#write_event{
                        file_uuid = UUID, blocks = [#file_block{
                            file_id = FileId, storage_id = StorageId, offset = Offset, size = Written
                        }]
                    }, SessId);
                false ->
                    ok
            end,
            {ok, NewHandle, Written};
        {error, Reason2} ->
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: logical_file_manager:handle(), Offset :: integer(),
    MaxSize :: integer(), GenerateEvents :: boolean()) ->
    {ok, NewHandle :: logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read(FileHandle, Offset, MaxSize, GenerateEvents) ->
    case read_internal(FileHandle, Offset, MaxSize, GenerateEvents) of
        {error, Reason} ->
            {error, Reason};
        {ok, NewHandle, Bytes} = Ret1 ->
            case size(Bytes) of
                MaxSize ->
                    Ret1;
                0 ->
                    Ret1;
                Size ->
                    case read(NewHandle, Offset + Size, MaxSize - Size, GenerateEvents) of
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
-spec read_internal(FileHandle :: logical_file_manager:handle(), Offset :: integer(), MaxSize :: integer(), GenerateEvents :: boolean()) ->
    {ok, logical_file_manager:handle(), binary()} | logical_file_manager:error_reply().
read_internal(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType,
    fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, MaxSize, GenerateEvents) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, MaxSize),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    lfm_utils:call_fslogic(SessId, #synchronize_block{uuid = UUID, block = #file_block{offset = Offset, size = MaxSize}},
        fun(_) -> ok end),
    case storage_file_manager:read(SFMHandle, Offset, NewSize) of
        {ok, Data} ->
            case GenerateEvents of
                true ->
                    ok = event:emit(#read_event{
                        file_uuid = UUID, blocks = [#file_block{
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