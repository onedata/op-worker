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

-include("types.hrl").
-include("errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2]).
%% Functions operating on files
-export([create/3, open/3, write/3, read/3, truncate/3, get_block_map/2, unlink/2]).

-compile({no_auto_import, [unlink/1]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(_FileKey) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(_FileKeyFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(_PathFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(fslogic_worker:ctx(), {uuid, file_uuid()}) -> ok | error_reply().
unlink(#fslogic_ctx{session_id = SessId}, {uuid, UUID}) ->
    lfm_utils:call_fslogic(SessId, #unlink{uuid = UUID},
        fun(_) ->
            ok
        end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(fslogic_worker:ctx(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
                    {ok, file_uuid()} | error_reply().
create(#fslogic_ctx{session_id = SessId} = _CTX, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    {ok, {#document{key = ParentUUID}, _}} = file_meta:resolve_path(ParentPath),
    lfm_utils:call_fslogic(SessId, #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode},
        fun(#file_location{uuid = UUID}) ->
            {ok, UUID}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(fslogic_worker:ctx(), {uuid, file_uuid()}, OpenType :: open_mode()) -> {ok, file_handle()} | error_reply().
open(#fslogic_ctx{session_id = SessId} = CTX, {uuid, UUID}, OpenType) ->
    lfm_utils:call_fslogic(SessId, #get_file_location{uuid = UUID},
        fun(#file_location{uuid = UUID, file_id = FileId, storage_id = StorageId}) ->
            {ok, #document{value = Storage}} = storage:get(StorageId),
            case storage_file_manager:open(Storage, FileId, OpenType) of
                {ok, SFMHandle} ->
                    {ok, #lfm_handle{sfm_handles = maps:from_list([{default, {{StorageId, FileId}, SFMHandle}}]),
                        fslogic_ctx = CTX, file_uuid = UUID, open_mode = OpenType}};
                {error, Reason} ->
                    {error, Reason}
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: non_neg_integer(), Buffer :: binary()) ->
                   {ok, file_handle(), non_neg_integer()} | error_reply().
write(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType,
                  fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, Buffer) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, byte_size(Buffer)),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
        {ok, Written} ->
            ok = event_manager:emit(
                   #write_event{
                      file_uuid = UUID,
                      blocks = [
                                #file_block{file_id = FileId, storage_id = StorageId, offset = Offset, size = Written}
                               ]}, SessId),
            {ok, NewHandle, Written};
        {error, Reason2} ->
            {error, Reason2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) ->
                  {ok, file_handle(), binary()} | error_reply().
read(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_mode = OpenType,
                 fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, MaxSize) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, MaxSize),
    {{StorageId, FileId}, SFMHandle, NewHandle} = get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType),

    case storage_file_manager:read(SFMHandle, Offset, NewSize) of
        {ok, Data} ->
            ok = event_manager:emit(
                   #read_event{
                      file_uuid = UUID,
                      blocks = [
                                #file_block{
                                   file_id = FileId, storage_id = StorageId,
                                   offset = Offset, size = size(Data)}
                               ]}, SessId),
            {ok, NewHandle, Data};
        {error, Reason2} ->
            {error, Reason2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(fslogic_worker:ctx(), FileUUID :: file_uuid(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(#fslogic_ctx{session_id = SessId}, FileUUID, Size) ->
    lfm_utils:call_fslogic(SessId, #truncate{uuid = FileUUID, size = Size},
    fun(_) ->
        event_manager:emit(#write_event{file_uuid = FileUUID, blocks = [], file_size = Size}, SessId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(fslogic_worker:ctx(), FileKey :: file_id_or_path()) -> {ok, [block_range()]} | error_reply().
get_block_map(_CTX, File) ->
    #document{value = LocalLocation} = fslogic_utils:get_local_file_location(File),
    #file_location{blocks = Blocks} = LocalLocation,
    {ok, Blocks}.


%%--------------------------------------------------------------------
%% @doc
%% For given file and byte range, returns storage's ID and file's ID (on storage)
%% that shall be used to store this byte range. Also returns maximum byte count that can be
%% used for this location. Atom 'default' is returned when location of specific block cannnot be found
%% and default locations shall be used instead.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_key(file_uuid(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
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
-spec get_sfm_handle_key(file_uuid(), Offset :: non_neg_integer(), Size :: non_neg_integer(), fslogic_blocks:blocks() | fslogic_blocks:block()) ->
                                {default | {storage:id(), helpers:file()}, non_neg_integer()}.
get_sfm_handle_key(UUID, Offset, Size, [#file_block{offset = O, size = S} | T]) when O + S =< Offset ->
    get_sfm_handle_key(UUID, Offset, Size, T);
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
  when Offset >= O, Offset + Size =< O + S ->
    {{SID, FID}, Size};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _])
  when Offset >= O, Offset + Size > O + S ->
    {{SID, FID}, S - (Offset - O)};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S} | _]) when Offset + Size =< O ->
    {default, Size};
get_sfm_handle_key(_UUID, _Offset, Size, []) ->
    {default, Size}.



%%--------------------------------------------------------------------
%% @doc
%% Helper function for read/write handles caching. Returns given handle or creates new and updates master handle.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle_n_update_handle(Handle :: file_handle(), Key :: term(), SFMHandles :: sfm_handles_map(),
                                     OpenType :: helpers:open_mode()) ->
                                            {{StorageId :: storage:id(), FileId :: file_uuid()},
                                             SFMHandle :: storage_file_manager:handle(),
                                             NewHandle :: file_handle()} |  no_return().
get_sfm_handle_n_update_handle(Handle, Key, SFMHandles, OpenType) ->
    {{StorageId, FileId}, SFMHandle} =
        case maps:get(Key, SFMHandles, undefined) of
            undefined ->
                {SID, FID} = Key,
                {ok, #document{value = Storage}} = storage:get(SID),
                case storage_file_manager:open(Storage, FID, OpenType) of
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