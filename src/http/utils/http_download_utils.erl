%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file download using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(http_download_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/cdmi.hrl").
-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_read_block_size/1,
    stream_file/3, stream_file/4, stream_bytes_range/6,
    stream_archive/4
]).

-type read_block_size() :: non_neg_integer().

-define(DEFAULT_READ_BLOCK_SIZE, application:get_env(
    ?APP_NAME, default_download_read_block_size, 10485760) % 10 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, application:get_env(
    ?APP_NAME, max_download_buffer_size, 20971520) % 20 MB
).

% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
% Due to lack of backpressure mechanism in cowboy when streaming files it must
% be additionally implemented. This module implementation checks cowboy process
% msg queue len to see if next data chunk can be queued. To account for
% differences in speed between network and storage a simple backoff is
% implemented with below boundaries.
-define(MIN_SEND_RETRY_DELAY, 100).
-define(MAX_SEND_RETRY_DELAY, 1000).

-record(download_ctx, {
    file_size :: file_meta:size(),

    file_handle :: lfm:handle(),
    read_block_size :: read_block_size(),
    max_read_blocks_count :: non_neg_integer(),
    encoding_fun :: fun((Data :: binary()) -> EncodedData :: binary()),
    tar_archive = undefined :: undefined | tar_utils:state(),
    min_bytes_to_read = 0 :: non_neg_integer(),
    
    on_success_callback :: fun(() -> ok)
}).
-type download_ctx() :: #download_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec get_read_block_size(lfm_context:ctx()) -> non_neg_integer().
get_read_block_size(FileHandle) ->
    case storage:get_block_size(lfm_context:get_storage_id(FileHandle)) of
        undefined ->
            ?DEFAULT_READ_BLOCK_SIZE;
        0 ->
            ?DEFAULT_READ_BLOCK_SIZE;
        Int ->
            Int
    end.


-spec stream_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
stream_file(SessionId, FileAttrs, Req) ->
    stream_file(SessionId, FileAttrs, fun() -> ok end, Req).


-spec stream_file(
    session:id(),
    lfm_attrs:file_attributes(),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_file(SessionId, #file_attr{
    guid = FileGuid,
    name = FileName,
    size = FileSize
}, OnSuccessCallback, Req0) ->
    case http_parser:parse_range_header(Req0, FileSize) of
        invalid ->
            cowboy_req:reply(
                ?HTTP_416_RANGE_NOT_SATISFIABLE,
                #{?HDR_CONTENT_RANGE => str_utils:format_bin("bytes */~B", [FileSize])},
                Req0
            );
        Ranges ->
            Req1 = ensure_content_type_header_set(FileName, Req0),

            case lfm:monitored_open(SessionId, {guid, FileGuid}, read) of
                {ok, FileHandle} ->
                    try
                        ReadBlockSize = get_read_block_size(FileHandle),

                        stream_file_insecure(Ranges, #download_ctx{
                            file_size = FileSize,
                            file_handle = FileHandle,
                            read_block_size = ReadBlockSize,
                            max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
                            encoding_fun = fun(Data) -> Data end,
                            on_success_callback = OnSuccessCallback
                        }, Req1)
                    catch Type:Reason ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while processing file (~p) download "
                                          "for user ~p - ~p:~p", [
                            FileGuid, UserId, Type, Reason
                        ]),
                        http_req:send_error(Reason, Req1)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req1)
            end
    end.


-spec stream_archive(
    session:id(),
    [lfm_attrs:file_attributes()],
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_archive(SessionId, FileAttrsList, OnSuccessCallback, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges">>},
        Req0
    ),
    TarArchive = tar_utils:open_archive_stream(),
    FinalTarArchive = lists:foldl(fun(#file_attr{guid = FileGuid, type = Type}, InnerTarArchive) ->
        FileCtx = file_ctx:new_by_guid(FileGuid),
        {Path, FileCtx1} = file_ctx:get_canonical_path(FileCtx),
        PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
        {UpdatedTarArchive, FileCtx2} = case Type of
            ?DIRECTORY_TYPE ->
                {Bytes, A, FCtx} = new_tar_file_entry(InnerTarArchive, FileCtx1, PathPrefix),
                send_data_chunk(Bytes, Req1),
                {A, FCtx};
            _ -> 
                {TarArchive, FileCtx1}
        end,
        dir_streaming_traverse:start(FileCtx2, SessionId, self()),
        stream_archive_loop(UpdatedTarArchive, Req1, SessionId, PathPrefix)
    end, TarArchive, FileAttrsList),
    send_data_chunk(tar_utils:close_archive_stream(FinalTarArchive), Req1),
    execute_on_success_callback(<<>>, OnSuccessCallback),
    Req1.


-spec stream_bytes_range(
    lfm:handle(),
    file_meta:size(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size()
) ->
    ok | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize) ->
    stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, undefined, 0).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec calculate_max_read_blocks_count(read_block_size()) -> non_neg_integer().
calculate_max_read_blocks_count(ReadBlockSize) ->
    max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize).


%% @private
-spec ensure_content_type_header_set(file_meta:name(), cowboy_req:req()) -> cowboy_req:req().
ensure_content_type_header_set(FileName, Req) ->
    case cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req, undefined) of
        undefined ->
            {Type, Subtype, _} = cow_mimetypes:all(FileName),
            cowboy_req:set_resp_header(?HDR_CONTENT_TYPE, [Type, "/", Subtype], Req);
        _ ->
            Req
    end.


%% @private
-spec build_content_range_header_value(http_parser:bytes_range(), file_meta:size()) -> binary().
build_content_range_header_value({RangeStart, RangeEnd}, FileSize) ->
    str_utils:format_bin("bytes ~B-~B/~B", [RangeStart, RangeEnd, FileSize]).


%% @private
-spec stream_file_insecure(undefined | [http_parser:bytes_range()],download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_file_insecure(undefined, DownloadCtx, Req) ->
    stream_whole_file(DownloadCtx, Req);
stream_file_insecure([OneRange], DownloadCtx, Req) ->
    stream_one_ranged_body(OneRange, DownloadCtx, Req);
stream_file_insecure(Ranges, DownloadCtx, Req) ->
    stream_multipart_ranged_body(Ranges, DownloadCtx, Req).


%% @private
-spec stream_whole_file(download_ctx(), cowboy_req:req()) -> cowboy_req:req().
stream_whole_file(#download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),

    stream_bytes_range({0, FileSize - 1}, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_one_ranged_body(http_parser:bytes_range(), download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_one_ranged_body({RangeStart, RangeEnd} = Range, #download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        Req0
    ),

    stream_bytes_range(Range, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_multipart_ranged_body([http_parser:bytes_range()], download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_multipart_ranged_body(Ranges, #download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),

    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Req0
    ),

    lists:foreach(fun(Range) ->
        NextPartHead = cow_multipart:first_part(Boundary, [
            {?HDR_CONTENT_TYPE, ContentType},
            {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
        ]),
        cowboy_req:stream_body(NextPartHead, nofin, Req1),

        stream_bytes_range(Range, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY)
    end, Ranges),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(cow_multipart:close(Boundary), fin, Req1),
    Req1.


%% @private
-spec stream_archive_loop(tar_utils:state(), cowboy_req:req(), session:id(), file_meta:path()) -> 
    tar_utils:state().
stream_archive_loop(TarArchive, Req, SessionId, RootDirPath) ->
    receive
        done -> TarArchive;
        {file_ctx, FileCtx, Pid} ->
            TarArchive2 = case file_ctx:is_dir(FileCtx) of
                {true, FileCtx1} ->
                    {Bytes, TarArchive1, _FileCtx2} = new_tar_file_entry(TarArchive, FileCtx1, RootDirPath),
                    send_data_chunk(Bytes, Req),
                    TarArchive1;
                {false, FileCtx2} ->
                    stream_archive_file(TarArchive, Req, SessionId, FileCtx2, RootDirPath)
            end,
            Pid ! done,
            stream_archive_loop(TarArchive2, Req, SessionId, RootDirPath)
    end.


%% @private
-spec stream_archive_file(tar_utils:state(), cowboy_req:req(), session:id(), file_ctx:ctx(), 
    file_meta:path()) -> tar_utils:state().
stream_archive_file(TarArchive, Req, SessionId, FileCtx, RootDirPath) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx),
    case lfm:monitored_open(SessionId, {guid, file_ctx:get_guid_const(FileCtx1)}, read) of
        {ok, FileHandle} ->
            {Bytes, TarArchive1, _FileCtx2} = new_tar_file_entry(TarArchive, FileCtx1, RootDirPath),
            send_data_chunk(Bytes, Req),
            Range = {0, FileSize - 1},
            ReadBlockSize = get_read_block_size(FileHandle),
            TarArchive2 = stream_bytes_range(
                FileHandle, FileSize, Range, Req, fun(D) -> D end, ReadBlockSize, TarArchive1, FileSize),
            lfm:monitored_release(FileHandle),
            TarArchive2;
        {error, ?ENOENT} ->
            TarArchive;
        {error, ?EACCES} ->
            % ignore files with no access
            TarArchive
    end.


%% @private
-spec stream_bytes_range(
    lfm:handle(),
    file_meta:size(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size(),
    tar_utils:state(),
    MinReadBytes :: non_neg_integer()
) ->
    tar_utils:state() | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, TarArchive, MinReadBytes) ->
    stream_bytes_range(Range, #download_ctx{
        file_size = FileSize,
        file_handle = FileHandle,
        read_block_size = ReadBlockSize,
        max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
        encoding_fun = EncodingFun,
        tar_archive = TarArchive,
        min_bytes_to_read = MinReadBytes,
        on_success_callback = fun() -> ok end
    }, Req, ?MIN_SEND_RETRY_DELAY).


%% @private
-spec stream_bytes_range(
    http_parser:bytes_range(),
    download_ctx(),
    cowboy_req:req(),
    SendRetryDelay :: time:millis()
) ->
    tar_utils:state() | no_return().
stream_bytes_range(Range, DownloadCtx, Req, SendRetryDelay) ->
    stream_bytes_range(Range, DownloadCtx, Req, SendRetryDelay, 0).


%% @private
-spec stream_bytes_range(
    http_parser:bytes_range(),
    download_ctx(),
    cowboy_req:req(),
    SendRetryDelay :: time:millis(),
    ReadBytes :: non_neg_integer()
) ->
    ok | no_return().
stream_bytes_range({From, To}, #download_ctx{tar_archive = TarArchive}, _, _, _) when From > To ->
    TarArchive;
stream_bytes_range({From, To}, #download_ctx{
    file_handle = FileHandle,
    read_block_size = ReadBlockSize,
    max_read_blocks_count = MaxReadBlocksCount,
    encoding_fun = EncodingFun,
    tar_archive = TarArchive,
    min_bytes_to_read = MinBytesToRead
} = DownloadCtx, Req, SendRetryDelay, ReadBytes) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, MinBytesToRead - ReadBytes),

    case byte_size(Data) of
        0 -> ok;
        DataSize ->
            {BytesToSend, FinalTarArchive} = case TarArchive of
                undefined -> {Data, undefined};
                _ ->
                    TarArchive2 = tar_utils:append_to_file_content(TarArchive, EncodingFun(Data), DataSize),
                    tar_utils:flush(TarArchive2)
            end,
            NextSendRetryDelay = send_data_chunk(BytesToSend, Req, MaxReadBlocksCount, SendRetryDelay),
            stream_bytes_range(
                {From + DataSize, To}, 
                DownloadCtx#download_ctx{file_handle = NewFileHandle, tar_archive = FinalTarArchive}, 
                Req, NextSendRetryDelay, ReadBytes + DataSize
            )
    end.


%% @private
-spec read_file_data(lfm:handle(), From :: non_neg_integer(), ToRead :: non_neg_integer(), 
    MinBytes :: non_neg_integer()) -> {lfm:handle(), binary()}.
read_file_data(FileHandle, From, ToRead, MinBytes) ->
    case lfm:read(FileHandle, From, ToRead) of
        {error, ?ENOSPC} ->
            throw(?ERROR_QUOTA_EXCEEDED);
        Res ->
            {ok, NewFileHandle, Data} = ?check(Res),
            FinalData = case byte_size(Data) < MinBytes of
                true -> str_utils:pad_right(Data, min(MinBytes, ToRead), <<0>>);
                false -> Data
            end,
            {NewFileHandle, FinalData}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
%% Cowboy uses separate process to manage socket and all messages, including
%% data, to stream are sent to that process. However because it doesn't enforce
%% any backpressure mechanism it is easy, on slow networks and fast storages,
%% to read to memory entire file while sending process doesn't keep up with
%% sending those data. To avoid this it is necessary to check message_queue_len
%% of sending process and ensure it is not larger than max allowed blocks to
%% read into memory.
%% @end
%%--------------------------------------------------------------------
-spec send_data_chunk(Data :: binary(), cowboy_req:req()) -> NextRetryDelay :: time:millis().
send_data_chunk(Data, Req) -> 
    send_data_chunk(Data, Req, ?MAX_DOWNLOAD_BUFFER_SIZE div ?DEFAULT_READ_BLOCK_SIZE, ?MIN_SEND_RETRY_DELAY).


%% @private
-spec send_data_chunk(
    Data :: binary(),
    cowboy_req:req(),
    MaxReadBlocksCount :: non_neg_integer(),
    RetryDelay :: time:millis()
) ->
    NextRetryDelay :: time:millis().
send_data_chunk(<<>>, _Req, _MaxReadBlocksCount, RetryDelay) -> 
    RetryDelay;
send_data_chunk(Data, #{pid := ConnPid} = Req, MaxReadBlocksCount, RetryDelay) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),

    case MsgQueueLen < MaxReadBlocksCount of
        true ->
            cowboy_req:stream_body(Data, nofin, Req),
            max(RetryDelay div 2, ?MIN_SEND_RETRY_DELAY);
        false ->
            timer:sleep(RetryDelay),
            send_data_chunk(
                Data, Req, MaxReadBlocksCount,
                min(2 * RetryDelay, ?MAX_SEND_RETRY_DELAY)
            )
    end.


%% @private
-spec execute_on_success_callback(fslogic_worker:file_guid(), OnSuccessCallback :: fun(() -> ok)) -> ok.
execute_on_success_callback(Guid, OnSuccessCallback) ->
    try
        ok = OnSuccessCallback()
    catch Type:Reason ->
        ?warning("Failed to execute file download successfully finished callback for file (~p) "
                 "due to ~p:~p", [Guid, Type, Reason])
    end.


%% @private
-spec new_tar_file_entry(tar_utils:state(), file_ctx:ctx(), file_meta:path()) -> 
    {binary(), tar_utils:state(), file_ctx:ctx()}.
new_tar_file_entry(TarArchive, FileCtx, RootDirPath) ->
    {Mode, FileCtx1} = file_ctx:get_mode(FileCtx),
    {{_ATime, _CTime, MTime}, FileCtx2} = file_ctx:get_times(FileCtx1),
    {IsDir, FileCtx3} = file_ctx:is_dir(FileCtx2),
    {Size, FileCtx4} = file_ctx:get_file_size(FileCtx3),
    {Path, FileCtx5} = file_ctx:get_canonical_path(FileCtx4),
    Filename = string:prefix(Path, RootDirPath),
    FileType = case IsDir of
        true -> directory;
        false -> regular
    end,
    TarArchive1 = tar_utils:new_file_entry(TarArchive, Filename, Size, Mode, MTime, FileType),
    {Bytes, TarArchive2} = tar_utils:flush(TarArchive1),
    {Bytes, TarArchive2, FileCtx5}.
