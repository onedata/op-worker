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
%%% files that are to be added to archive. 
%%% If connection process dies during download main process is being suspended up to 
%%% ?BULK_DOWNLOAD_RESUME_TIMEOUT milliseconds, to allow for resuming of such download.
%%% Because some of data sent just before failure might have been lost main process buffers 
%%% ?MAX_BUFFER_SIZE of last sent bytes. Thanks to this it is possible to catch up lost data.
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
-export([start/4, resume/2, finish/1]).
-export([report_next_file/2, report_data_sent/2, report_traverse_done/1]).

-record(state, {
    id :: bulk_download:id(),
    sent_bytes = 0 :: integer(),
    buffer = <<>> :: binary(),
    connection_pid :: pid(),
    tar_stream :: tar_utils:stream(),
    send_retry_delay = 100 :: time:millis()
}).

-type state() :: #state{}.
-type traverse_status() :: in_progress | finished.


-define(TARBALL_DOWNLOAD_TRAVERSE_POOL_NAME, bulk_download_traverse:get_pool_name()).
-define(BULK_DOWNLOAD_RESUME_TIMEOUT, timer:seconds(op_worker:get_env(download_code_expiration_interval_seconds, 1800))).
-define(MAX_BUFFER_SIZE, op_worker:get_env(max_download_buffer_size, 20971520) + 32768). % buffer cannot be smaller than tar stream internal buffer (32768 bytes)


%%%===================================================================
%%% API
%%%===================================================================

-spec start(bulk_download:id(), [lfm_attrs:file_attributes()], session:id(), pid()) -> 
    {ok, pid()} | {error, term()}.
start(TaskId, FileAttrsList, SessionId, InitialConn) ->
    case tree_traverse_session:setup_for_task(user_ctx:new(SessionId), TaskId) of
        ok -> {ok, spawn(fun() -> main(TaskId, FileAttrsList, SessionId, InitialConn) end)};
        {error, _} = Error -> Error
    end.


-spec resume(pid(), non_neg_integer()) -> ok.
resume(MainPid, ResumeOffset) ->
    MainPid ! ?MSG_RESUMED(self(), ResumeOffset),
    ok.


-spec finish(pid()) -> ok.
finish(MainPid) ->
    MainPid ! ?MSG_FINISH,
    ok.


-spec report_data_sent(pid(), time:millis()) -> ok.
report_data_sent(MainPid, NewDelay) -> 
    MainPid ! ?MSG_CONTINUE(NewDelay),
    ok.


-spec report_next_file(pid(), lfm_attrs:file_attributes()) -> ok.
report_next_file(MainPid, FileAttrs) -> 
    MainPid ! ?MSG_NEXT_FILE(FileAttrs, self()),
    ok.


-spec report_traverse_done(pid()) -> ok.
report_traverse_done(MainPid) -> 
    MainPid ! ?MSG_DONE,
    ok.

%%%===================================================================
%%% Internal functions responsible for streaming file data
%%%===================================================================

%% @private
-spec main(bulk_download:id(), [lfm_attrs:file_attributes()], session:id(), pid()) -> state().
main(TaskId, FileAttrsList, SessionId, InitialConn) ->
    bulk_download_persistence:save_main_pid(TaskId, self()),
    TarStream = tar_utils:open_archive_stream(),
    State = #state{id = TaskId, connection_pid = InitialConn, tar_stream = TarStream},
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?TARBALL_DOWNLOAD_TRAVERSE_POOL_NAME, TaskId),
    #state{tar_stream = FinalTarStream} = UpdatedState = 
        handle_multiple_files(FileAttrsList, TaskId, UserCtx, State),
    #state{connection_pid = Conn} = FinalState =
        send_data(tar_utils:close_archive_stream(FinalTarStream), UpdatedState),
    tree_traverse_session:close_for_task(TaskId),
    Conn ! ?MSG_DONE,
    loop_with_conn(FinalState, finished). % do not die yet, last chunk might have failed


%% @private
-spec handle_multiple_files([lfm_attrs:file_attributes()], bulk_download:id(), 
    user_ctx:ctx(), state()) -> state().
handle_multiple_files([], _TaskId, _UserCtx, State) -> 
    State;
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?DIRECTORY_TYPE} = FileAttrs | Tail],
    TaskId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    % add starting dir to archive here as traverse do not execute slave job on it
    {Bytes, UpdatedState} = new_tar_file_entry(State, FileAttrs, PathPrefix),
    UpdatedState1 = send_data(Bytes, UpdatedState),
    bulk_download_traverse:start(TaskId, UserCtx, Guid),
    FinalState = loop_with_traverse(UpdatedState1, user_ctx:get_session_id(UserCtx), PathPrefix),
    handle_multiple_files(Tail, TaskId, UserCtx, FinalState);
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?REGULAR_FILE_TYPE} = FileAttrs | Tail], 
    TaskId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    UpdatedState = stream_file(State, user_ctx:get_session_id(UserCtx), FileAttrs, PathPrefix),
    handle_multiple_files(Tail, TaskId, UserCtx, UpdatedState);
handle_multiple_files(
    [#file_attr{guid = Guid, type = ?SYMLINK_TYPE} = FileAttrs | Tail],
    TaskId, UserCtx, State
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    UpdatedState = stream_symlink(State, user_ctx:get_session_id(UserCtx), FileAttrs, PathPrefix),
    handle_multiple_files(Tail, TaskId, UserCtx, UpdatedState).


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
            StreamingCtx1 = http_streamer:set_range_policy(StreamingCtx, strict),
            StreamingCtx2 = http_streamer:set_send_fun(StreamingCtx1, fun(Data, InFunState, _MaxReadBlocksCount, SendRetryDelay) -> 
                DataSize = byte_size(Data),
                #state{tar_stream = TarStream} = InFunState,
                TarStream2 = tar_utils:append_to_file_content(TarStream, Data, DataSize),
                {BytesToSend, FinalTarStream} = tar_utils:flush_buffer(TarStream2),
                #state{send_retry_delay = NewDelay} = UpdatedInFunState =
                    send_data(BytesToSend, InFunState#state{tar_stream = FinalTarStream, send_retry_delay = SendRetryDelay}),
                {NewDelay, UpdatedInFunState}
            end),
            FinalState = http_streamer:stream_bytes_range(StreamingCtx2, Range, UpdatedState1),
            lfm:monitored_release(FileHandle),
            FinalState;
        ignored ->
            State;
        {error, _} = Error ->
            ?warning("Unexpected error during tarball download: ~p. File ~p will be ignored", [Error, Guid]),
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
            ?warning("Unexpected error during tarball download: ~p. File ~p will be ignored", [Error, Guid]),
            State
    end.


%% @private
-spec loop_with_traverse(state(), session:id(), file_meta:path()) -> state().
loop_with_traverse(State, SessionId, RootDirPath) ->
    receive
        ?MSG_NEXT_FILE(#file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs, TraversePid) ->
            State2 = stream_file(State, SessionId, FileAttrs, RootDirPath),
            TraversePid ! ?MSG_DONE,
            loop_with_traverse(State2, SessionId, RootDirPath);
        ?MSG_NEXT_FILE(#file_attr{type = ?SYMLINK_TYPE} = FileAttrs, TraversePid) ->
            State2 = stream_symlink(State, SessionId, FileAttrs, RootDirPath),
            TraversePid ! ?MSG_DONE,
            loop_with_traverse(State2, SessionId, RootDirPath);
        ?MSG_NEXT_FILE(#file_attr{type = ?DIRECTORY_TYPE} = FileAttrs, TraversePid) ->
            {Bytes, State2} = new_tar_file_entry(State, FileAttrs, RootDirPath),
            State3 = send_data(Bytes, State2),
            TraversePid ! ?MSG_DONE,
            loop_with_traverse(State3, SessionId, RootDirPath);
        ?MSG_DONE -> 
            State
    end.


%% @private
-spec send_data(binary(), state()) -> state().
send_data(<<>>, State) -> State;
send_data(Data, #state{send_retry_delay = SendRetryDelay, connection_pid = Conn} = State) ->
    UpdatedState = update_stats(Data, State),
    Conn ! ?MSG_DATA_CHUNK(Data, SendRetryDelay),
    loop_with_conn(UpdatedState).


%% @private
-spec update_stats(binary(), state()) -> state().
update_stats(SentChunk, State) ->
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
-spec loop_with_conn(state()) -> state().
loop_with_conn(State) ->
    loop_with_conn(State, in_progress).


%% @private
-spec loop_with_conn(state(), traverse_status()) -> state().
loop_with_conn(#state{id = Id} = State, TraverseStatus) ->
    receive
        ?MSG_FINISH ->
            finalize(State);
        ?MSG_CONTINUE(NewDelay) -> 
            case TraverseStatus of
                finished -> loop_with_conn(State#state{send_retry_delay = NewDelay}, TraverseStatus);
                in_progress -> State#state{send_retry_delay = NewDelay}
            end;
        ?MSG_RESUMED(NewConn, ResumeOffset) ->
            UpdatedState = handle_resume(State, NewConn, ResumeOffset, TraverseStatus),
            loop_with_conn(UpdatedState, TraverseStatus)
    after ?BULK_DOWNLOAD_RESUME_TIMEOUT ->
        file_download_code:remove(Id),
        bulk_download_persistence:delete(Id),
        finalize(State)
    end.


%% @private
-spec finalize(state()) -> no_return().
finalize(#state{id = Id, tar_stream = TarStream}) ->
    traverse:cancel(bulk_download_traverse:get_pool_name(), Id),
    try
        tar_utils:close_archive_stream(TarStream)
    catch _:_ ->
        ok % tar stream could have already been closed, so crash here is expected
    end,
    exit(kill).


%% @private
-spec handle_resume(state(), pid(), non_neg_integer(), traverse_status()) -> state().
handle_resume(State, NewConn, ResumeOffset, TraverseStatus) ->
    #state{connection_pid = PrevConn, send_retry_delay = SendRetryDelay} = State,
    case check_pid(PrevConn) of
        {ok, _} -> 
            NewConn ! ?MSG_ERROR,
            State;
        _ ->
            UpdatedState = State#state{connection_pid = NewConn},
            case catch_up_data(UpdatedState, ResumeOffset) of
                {ok, DataToCatchUp} ->
                    NewConn ! ?MSG_DATA_CHUNK(DataToCatchUp, SendRetryDelay),
                    TraverseStatus == finished andalso (NewConn ! ?MSG_DONE);
                error ->
                    NewConn ! ?MSG_ERROR
            end,
            UpdatedState
    end.


%% @private
-spec catch_up_data(state(), non_neg_integer()) -> {ok, binary()} | error.
catch_up_data(#state{buffer = Buffer, sent_bytes = SentBytes}, ResumeOffset) ->
    BufferSize = byte_size(Buffer),
    case ResumeOffset =< SentBytes andalso ResumeOffset > (SentBytes - BufferSize) of
        true -> {ok, binary:part(Buffer, BufferSize, ResumeOffset - SentBytes)};
        false -> error
    end.


%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec check_pid(pid()) -> {ok, pid()} | {error, term()}.
check_pid(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> {error, noproc}
    end.


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
