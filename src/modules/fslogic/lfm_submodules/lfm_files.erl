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
-include("modules/datastore/datastore.hrl").
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
%%
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(_FileKey) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(_FileKeyFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%%
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(_PathFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec unlink(fslogic_worker:ctx(), FileKey :: file_key()) -> ok | error_reply().
unlink(#fslogic_ctx{session_id = SessId}, {uuid, UUID}) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #unlink{uuid = UUID}}) of
        #fuse_response{status = #status{code = ?OK}} ->
            ok;
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(fslogic_worker:ctx(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_id()} | error_reply().
create(#fslogic_ctx{session_id = SessId} = _CTX, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    ?error("lfm_files:create ~p ~p ~p", [Path, Name, ParentPath]),
    {ok, {#document{key = ParentUUID}, _}} = file_meta:resolve_path(ParentPath),
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode}}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{uuid = UUID, file_id = FileId, storage_id = StorageId}} ->
            %% @todo: handle different cluster_ids (via cluster proxy)
            {ok, #document{value = Storage}} = storage:get(StorageId),
            case storage_file_manager:create(Storage, FileId, Mode, true) of
                ok ->
                    %% @todo: remove file watchers
                    {ok, UUID};
                {error, Reason} ->
                    file_meta:delete({uuid, UUID}),
                    %% @todo: cleanup
                    {error, Reason}
            end;
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%%
%% @end
%%--------------------------------------------------------------------
-spec open(fslogic_worker:ctx(), FileKey :: file_id_or_path(), OpenType :: open_type()) -> {ok, file_handle()} | error_reply().
open(#fslogic_ctx{session_id = SessId} = CTX, {uuid, UUID}, OpenType) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #get_file_location{uuid = UUID}}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{uuid = UUID, file_id = FileId, storage_id = StorageId}} ->
            %% @todo: handle different cluster_ids (via cluster proxy)
            {ok, #document{value = Storage}} = storage:get(StorageId),
            case storage_file_manager:open(Storage, FileId, OpenType) of
                {ok, SFMHandle} ->
                    {ok, #lfm_handle{sfm_handles = maps:from_list([{default, {{StorageId, FileId}, SFMHandle}}]), fslogic_ctx = CTX,
                                     file_uuid = UUID, open_type = OpenType}};
                {error, Reason} ->
                    %% @todo: cleanup
                    {error, Reason}
            end;
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, file_handle(), integer()} | error_reply().
write(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_type = OpenType,
                  fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, Buffer) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, byte_size(Buffer)),
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

    case storage_file_manager:write(SFMHandle, Offset, binary:part(Buffer, 0, NewSize)) of
        {ok, Written} ->
            ok = event_manager:emit(#write_event{file_uuid = UUID, blocks = [
                #file_block{file_id = FileId, storage_id = StorageId, offset = Offset, size = Written}
            ]}, SessId),
            {ok, NewHandle, Written};
        {error, Reason2} ->
            {error, Reason2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, file_handle(), binary()} | error_reply().
read(#lfm_handle{sfm_handles = SFMHandles, file_uuid = UUID, open_type = OpenType,
                 fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle, Offset, MaxSize) ->
    {Key, NewSize} = get_sfm_handle_key(UUID, Offset, MaxSize),
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
    %FileSize = fslogic_blocks:get_file_size({uuid, UUID}),
    case storage_file_manager:read(SFMHandle, Offset, NewSize) of
        {ok, Data} ->
            ok = event_manager:emit(#read_event{file_uuid = UUID, blocks = [
                #file_block{file_id = FileId, storage_id = StorageId, offset = Offset, size = size(Data)}
            ]}, SessId),
            {ok, NewHandle, Data};
        {error, Reason2} ->
            {error, Reason2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(fslogic_worker:ctx(), FileKey :: file_id_or_path(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(#fslogic_ctx{session_id = SessId}, FileKey, Size) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #get_file_attr{entry = FileKey}}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{uuid = FileUUID}} ->
            #document{value = LocalLocation} = fslogic_utils:get_local_file_location({uuid, FileUUID}),
            #file_location{blocks = Blocks, file_id = DFID, storage_id = DSID} = LocalLocation,
            ?info("==============> OMG2 ~p", [LocalLocation]),
            Locations = lists:usort([{DSID, DFID} | [{SID, FID} || #file_block{file_id = FID, storage_id = SID} <- Blocks]]),
            Results = lists:map(
                fun({SID, FID}) ->
                    ?info("==============> OMG1 ~p", [{SID, FID}]),
                    {ok, #document{value = Storage}} = storage:get(SID),
                    case storage_file_manager:truncate(Storage, FID, Size) of
                        ok -> ok;
                        {error, Reason} ->
                            ?error("Cannot truncate file ~p on storage ~p due to: ~p", [FID, SID, Reason]),
                            {error, Reason}
                    end
                end, Locations),

            Failures = Results -- [ok],
            OKs = Results -- Failures,

            EmitTruncate =
                fun() ->
                    event_manager:emit(#write_event{file_uuid = FileUUID, blocks = [], file_size = Size}, SessId)
                end,

            case {OKs, Failures} of
                {_, []} ->      %% Full success
                    EmitTruncate();
                {[_ | _], _} -> %% Partial success
                    EmitTruncate();
                {[], Errors} ->
                    Reasons = [Reason0 || {error, Reason0} <- Errors],
                    {error, Reasons}
            end;
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(fslogic_worker:ctx(), FileKey :: file_id_or_path()) -> {ok, [block_range()]} | error_reply().
get_block_map(_CTX, File) ->
    #document{value = LocalLocation} = fslogic_utils:get_local_file_location(File),
    #file_location{blocks = Blocks} = LocalLocation,
    {ok, Blocks}.


get_sfm_handle_key(UUID, Offset, Size) ->
    #document{value = LocalLocation} = fslogic_utils:get_local_file_location({uuid, UUID}),
    #file_location{blocks = Blocks} = LocalLocation,
    get_sfm_handle_key(UUID, Offset, Size, Blocks).


get_sfm_handle_key(UUID, Offset, Size, [#file_block{offset = O, size = S} | T]) when O + S =< Offset ->
    get_sfm_handle_key(UUID, Offset, Size, T);
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _]) when Offset >= O, Offset + Size =< O + S ->
    {{SID, FID}, Size};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S, storage_id = SID, file_id = FID} | _]) when Offset >= O, Offset + Size > O + S ->
    {{SID, FID}, S - (Offset - O)};
get_sfm_handle_key(_UUID, Offset, Size, [#file_block{offset = O, size = S} | _]) when Offset + Size =< O ->
    {default, Size};
get_sfm_handle_key(_UUID, Offset, Size, []) ->
    {default, Size}.

