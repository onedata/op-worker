%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling bulk download main process. 
%%% This process is responsible for reading files data and passing it on 
%%% to the connection process (abbreviated as Conn). All given files are 
%%% processed sequentially and if directory is encountered new `bulk_download_traverse` 
%%% is started for it. This traverse is responsible for informing main process about 
%%% files that are to be added to the tarball. 
%%% If connection process dies during download main process is being suspended up to 
%%% ?BULK_DOWNLOAD_RESUME_TIMEOUT milliseconds, to allow resuming of such download.
%%% Because some of data sent just before failure might have been lost main process buffers 
%%% ?MAX_BUFFER_SIZE of last sent bytes. Thanks to this it is possible to resend unreceived data.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download_main_process).
-author("Michal Stanisz").


-include("modules/bulk_download/bulk_download.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/4, resume/2, abort/1]).
-export([report_next_file/2, report_data_sent/2, report_traverse_done/1]).
-export([is_offset_allowed/2]).

-record(state, {
    id :: bulk_download:id(),
    sent_bytes = 0 :: integer(),
    buffer = <<>> :: binary(),
    connection_pid :: pid(),
    tar_stream :: tar_utils:stream(),
    send_retry_delay = 100 :: time:millis()
}).

-type state() :: #state{}.


-define(TARBALL_DOWNLOAD_TRAVERSE_POOL_NAME, bulk_download_traverse:get_pool_name()).
-define(BULK_DOWNLOAD_RESUME_TIMEOUT, timer:seconds(op_worker:get_env(
    download_code_expiration_interval_seconds, 1800))).
% buffer cannot be smaller than tar stream internal buffer (32768 bytes)
-define(MAX_BUFFER_SIZE, (op_worker:get_env(max_download_buffer_size, 104857600) + 32768)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(bulk_download:id(), [lfm_attrs:file_attributes()], session:id(), pid()) -> 
    {ok, pid()} | {error, term()}.
start(BulkDownloadId, FileAttrsList, SessionId, InitialConn) ->
    case tree_traverse_session:setup_for_task(user_ctx:new(SessionId), BulkDownloadId) of
        ok -> {ok, spawn(fun() -> main(BulkDownloadId, FileAttrsList, SessionId, InitialConn) end)};
        {error, _} = Error -> Error
    end.


-spec resume(pid(), non_neg_integer()) -> ok.
resume(MainPid, ResumeOffset) ->
    MainPid ! ?MSG_RESUMED(self(), ResumeOffset),
    ok.


-spec abort(pid()) -> ok.
abort(MainPid) ->
    MainPid ! ?MSG_ABORT,
    ok.


-spec report_data_sent(pid(), time:millis()) -> ok.
report_data_sent(MainPid, NewDelay) -> 
    MainPid ! ?MSG_DATA_SENT(NewDelay),
    ok.


-spec report_next_file(pid(), lfm_attrs:file_attributes()) -> ok.
report_next_file(MainPid, FileAttrs) -> 
    MainPid ! ?MSG_NEXT_FILE(FileAttrs, self()),
    ok.


-spec report_traverse_done(pid()) -> ok.
report_traverse_done(MainPid) -> 
    MainPid ! ?MSG_DONE,
    ok.


-spec is_offset_allowed(pid(), non_neg_integer()) -> boolean().
is_offset_allowed(MainPid, Offset) ->
    MainPid ! ?MSG_CHECK_OFFSET(self(), Offset),
    receive
        Res -> Res
    after ?LOOP_TIMEOUT -> 
        false
    end.

%%%===================================================================
%%% Internal functions responsible for streaming file data
%%%===================================================================

%% @private
-spec main(bulk_download:id(), [lfm_attrs:file_attributes()], session:id(), pid()) -> no_return().
main(BulkDownloadId, FileAttrsList, SessionId, InitialConn) ->
    bulk_download_task:save_main_pid(BulkDownloadId, self()),
    TarStream = tar_utils:open_archive_stream(#{gzip => false}),
    State = #state{id = BulkDownloadId, connection_pid = InitialConn, tar_stream = TarStream},
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?TARBALL_DOWNLOAD_TRAVERSE_POOL_NAME, BulkDownloadId),
    #state{tar_stream = FinalTarStream} = UpdatedState = 
        handle_multiple_files(FileAttrsList, BulkDownloadId, UserCtx, State),
    #state{connection_pid = Conn} = FinalState =
        send_data(tar_utils:close_archive_stream(FinalTarStream), UpdatedState),
    tree_traverse_session:close_for_task(BulkDownloadId),
    Conn ! ?MSG_DONE,
    wait_for_conn_upon_finish(FinalState). % do not die yet, last chunk might have failed


%% @private
-spec handle_multiple_files([lfm_attrs:file_attributes()], bulk_download:id(), 
    user_ctx:ctx(), state()) -> state().
handle_multiple_files([], _BulkDownloadId, _UserCtx, State) -> 
    State;
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?DIRECTORY_TYPE} = FileAttrs | Tail],
    BulkDownloadId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    % add starting dir to the tarball here as traverse does not execute slave job on it
    {Bytes, UpdatedState} = new_tar_file_entry(State, FileAttrs, PathPrefix),
    UpdatedState1 = send_data(Bytes, UpdatedState),
    bulk_download_traverse:start(BulkDownloadId, UserCtx, Guid),
    FinalState = wait_for_traverse(UpdatedState1, user_ctx:get_session_id(UserCtx), PathPrefix),
    handle_multiple_files(Tail, BulkDownloadId, UserCtx, FinalState);
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?REGULAR_FILE_TYPE} = FileAttrs | Tail], 
    BulkDownloadId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    UpdatedState = stream_file(State, user_ctx:get_session_id(UserCtx), FileAttrs, PathPrefix),
    handle_multiple_files(Tail, BulkDownloadId, UserCtx, UpdatedState);
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?SYMLINK_TYPE} = FileAttrs | Tail],
    BulkDownloadId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    UpdatedState = stream_symlink(State, user_ctx:get_session_id(UserCtx), FileAttrs, PathPrefix),
    handle_multiple_files(Tail, BulkDownloadId, UserCtx, UpdatedState).


%% @private
-spec stream_file(state(), session:id(), lfm_attrs:file_attributes(),
    file_meta:path()) -> state().
stream_file(State, SessionId, FileAttrs, StartingDirPath) ->
    #file_attr{size = FileSize, guid = Guid} = FileAttrs,
    case check_read_result(lfm:monitored_open(SessionId, ?FILE_REF(Guid), read)) of
        {ok, FileHandle} ->
            {Bytes, UpdatedState} = new_tar_file_entry(State, FileAttrs, StartingDirPath),
            UpdatedState1 = send_data(Bytes, UpdatedState),
            Range = {0, FileSize - 1},
            StreamingCtx = http_streamer:build_ctx(FileHandle, FileSize),
            StreamingCtx2 = http_streamer:set_range_policy(StreamingCtx, strict),
            StreamingCtx3 = http_streamer:set_send_fun(StreamingCtx2, fun(Data, InFunState, _MaxReadBlocksCount, SendRetryDelay) -> 
                DataSize = byte_size(Data),
                #state{tar_stream = TarStream} = InFunState,
                TarStream2 = tar_utils:append_to_file_content(TarStream, Data, DataSize),
                {BytesToSend, FinalTarStream} = tar_utils:flush_buffer(TarStream2),
                #state{send_retry_delay = NewDelay} = UpdatedInFunState =
                    send_data(BytesToSend, InFunState#state{tar_stream = FinalTarStream, send_retry_delay = SendRetryDelay}),
                {NewDelay, UpdatedInFunState}
            end),
            FinalState = http_streamer:stream_bytes_range(StreamingCtx3, Range, UpdatedState1),
            lfm:monitored_release(FileHandle),
            FinalState;
        ignored ->
            State;
        {error, _} = Error ->
            ?warning("Unexpected error during bulk download: ~p. File ~p will be ignored", [Error, Guid]),
            State
    end.


%% @private
-spec stream_symlink(state(), session:id(), lfm_attrs:file_attributes(), file_meta:path()) -> state().
stream_symlink(State, SessionId, FileAttrs, StartingDirPath) ->
    #file_attr{guid = Guid} = FileAttrs,
    case check_read_result(lfm:read_symlink(SessionId, ?FILE_REF(Guid))) of
        {ok, LinkPath} ->
            {Bytes, UpdatedState} = new_tar_file_entry(State, FileAttrs, StartingDirPath, LinkPath),
            send_data(Bytes, UpdatedState);
        ignored ->
            State;
        {error, _} = Error ->
            ?warning("Unexpected error during bulk download: ~p. File ~p will be ignored", [Error, Guid]),
            State
    end.


%% @private
-spec wait_for_traverse(state(), session:id(), file_meta:path()) -> state().
wait_for_traverse(State, SessionId, RootDirPath) ->
    receive
        ?MSG_NEXT_FILE(#file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs, TraversePid) ->
            State2 = stream_file(State, SessionId, FileAttrs, RootDirPath),
            TraversePid ! ?MSG_DONE,
            wait_for_traverse(State2, SessionId, RootDirPath);
        ?MSG_NEXT_FILE(#file_attr{type = ?SYMLINK_TYPE} = FileAttrs, TraversePid) ->
            State2 = stream_symlink(State, SessionId, FileAttrs, RootDirPath),
            TraversePid ! ?MSG_DONE,
            wait_for_traverse(State2, SessionId, RootDirPath);
        ?MSG_NEXT_FILE(#file_attr{type = ?DIRECTORY_TYPE} = FileAttrs, TraversePid) ->
            {Bytes, State2} = new_tar_file_entry(State, FileAttrs, RootDirPath),
            State3 = send_data(Bytes, State2),
            TraversePid ! ?MSG_DONE,
            wait_for_traverse(State3, SessionId, RootDirPath);
        ?MSG_DONE -> 
            State
    end.


%% @private
-spec send_data(binary(), state()) -> state().
send_data(<<>>, State) -> State;
send_data(Data, #state{send_retry_delay = SendRetryDelay, connection_pid = Conn} = State) ->
    UpdatedState = update_sent_bytes_buffer(Data, State),
    Conn ! ?MSG_DATA_CHUNK(Data, SendRetryDelay),
    wait_for_conn(UpdatedState).


%% @private
-spec update_sent_bytes_buffer(binary(), state()) -> state().
update_sent_bytes_buffer(SentChunk, State) ->
    #state{sent_bytes = SentBytes, buffer = Buffer} = State,
    ChunkSize = byte_size(SentChunk),
    BufferSize = byte_size(Buffer),
    NewBuffer = case {ChunkSize > ?MAX_BUFFER_SIZE, ChunkSize + BufferSize > ?MAX_BUFFER_SIZE} of
        {true, _} -> binary:part(SentChunk, ChunkSize, -?MAX_BUFFER_SIZE);
        {false, true} -> binary:part(<<Buffer/binary, SentChunk/binary>>, ChunkSize + BufferSize, -?MAX_BUFFER_SIZE);
        {false, false} -> <<Buffer/binary, SentChunk/binary>>
    end,
    State#state{sent_bytes = SentBytes + ChunkSize, buffer = NewBuffer}.


%% @private
-spec new_tar_file_entry(state(), lfm_attrs:file_attributes(), file_meta:path()) ->
    {binary(), state()}.
new_tar_file_entry(TarStream, FileAttrs, StartingDirPath) ->
    new_tar_file_entry(TarStream, FileAttrs, StartingDirPath, undefined).


%% @private
-spec new_tar_file_entry(state(), lfm_attrs:file_attributes(), file_meta:path(), 
    file_meta_symlinks:symlink() | undefined) -> {binary(), state()}.
new_tar_file_entry(#state{tar_stream = TarStream} = State, FileAttrs, StartingDirPath, SymlinkPath) ->
    #file_attr{mode = Mode, mtime = MTime, type = Type, size = FileSize, guid = Guid} = FileAttrs,
    FinalMode = case {file_id:is_share_guid(Guid), Type} of
        {true, ?REGULAR_FILE_TYPE} -> ?DEFAULT_FILE_PERMS;
        {true, ?DIRECTORY_TYPE} -> ?DEFAULT_DIR_PERMS;
        {_, _} -> Mode
    end,
    {ok, Path} = get_file_path(Guid),
    FileRelPath = string:prefix(Path, StartingDirPath),
    TypeSpec = case Type of
        ?DIRECTORY_TYPE -> ?DIRECTORY_TYPE;
        ?REGULAR_FILE_TYPE -> ?REGULAR_FILE_TYPE;
        ?SYMLINK_TYPE -> {?SYMLINK_TYPE, SymlinkPath}
    end,
    UpdatedTarStream = tar_utils:new_file_entry(TarStream, FileRelPath, FileSize, FinalMode, MTime, TypeSpec),
    {Bytes, FinalTarStream} = tar_utils:flush_buffer(UpdatedTarStream),
    {Bytes, State#state{tar_stream = FinalTarStream}}.


%%%===================================================================
%%% Communication with connection process
%%%===================================================================

%% @private
-spec wait_for_conn(state()) -> state().
wait_for_conn(#state{id = Id} = State) ->
    receive
        ?MSG_ABORT ->
            finalize(State);
        ?MSG_DATA_SENT(NewDelay) ->
            State#state{send_retry_delay = NewDelay};
        ?MSG_CHECK_OFFSET(NewConn, Offset) ->
            NewConn ! is_offset_within_buffer_bounds(State, Offset),
            wait_for_conn(State);
        ?MSG_RESUMED(NewConn, ResumeOffset) ->
            UpdatedState = handle_resume(State, NewConn, ResumeOffset),
            wait_for_conn(UpdatedState)
    after ?BULK_DOWNLOAD_RESUME_TIMEOUT ->
        file_download_code:remove(Id),
        bulk_download_task:delete(Id),
        finalize(State)
    end.


%% @private
-spec wait_for_conn_upon_finish(state()) -> no_return().
wait_for_conn_upon_finish(State) ->
    #state{connection_pid = NewConn} = UpdatedState = wait_for_conn(State),
    NewConn ! ?MSG_DONE,
    wait_for_conn_upon_finish(UpdatedState).


%% @private
-spec handle_resume(state(), pid(), non_neg_integer()) -> state().
handle_resume(State, NewConn, ResumeOffset) ->
    UpdatedState = State#state{connection_pid = NewConn},
    case resend_unreceived_data(UpdatedState, ResumeOffset) of
        true -> ok;
        false -> NewConn ! ?MSG_ERROR
    end,
    UpdatedState.


%% @private
-spec resend_unreceived_data(state(), non_neg_integer()) -> boolean().
resend_unreceived_data(State, ResumeOffset) ->
    #state{
        connection_pid = Conn, 
        buffer = Buffer, 
        sent_bytes = SentBytes, 
        send_retry_delay = SendRetryDelay
    } = State,
    BufferSize = byte_size(Buffer),
    case is_offset_within_buffer_bounds(State, ResumeOffset) of
        true -> 
            BytesToResend = binary:part(Buffer, BufferSize, ResumeOffset - SentBytes),
            Conn ! ?MSG_DATA_CHUNK(BytesToResend, SendRetryDelay),
            true;
        false -> 
            false
    end.


%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec is_offset_within_buffer_bounds(state(), non_neg_integer()) -> boolean().
is_offset_within_buffer_bounds(#state{buffer = Buffer, sent_bytes = SentBytes}, Offset) ->
    BufferSize = byte_size(Buffer),
    Offset =< SentBytes andalso Offset > (SentBytes - BufferSize).


%% @private
-spec finalize(state()) -> no_return().
finalize(#state{id = Id, tar_stream = TarStream}) ->
    traverse:cancel(bulk_download_traverse:get_pool_name(), Id),
    % tar stream could have already been closed, so crash here is expected
    catch tar_utils:close_archive_stream(TarStream),
    exit(kill).


%% TODO VFS-6057 resolve share path up to share not user root dir
%% @private
-spec get_file_path(fslogic_worker:file_guid()) -> {ok, file_meta:path()} | {error, term()}.
get_file_path(ShareGuid) ->
    {Uuid, SpaceId, _} = file_id:unpack_share_guid(ShareGuid),
    Guid = file_id:pack_guid(Uuid, SpaceId),
    lfm:get_file_path(?ROOT_SESS_ID, Guid).


%% @private
-spec check_read_result({ok, term()} | {error, term()}) -> {ok, term()} | {error, term()} | ignored.
check_read_result({ok, _} = Result) -> Result;
check_read_result({error, ?ENOENT}) -> ignored;
check_read_result({error, ?EPERM}) -> ignored;
check_read_result({error, ?EACCES}) -> ignored;
check_read_result({error, _} = Error) -> Error.
