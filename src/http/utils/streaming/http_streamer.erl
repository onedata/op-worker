%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @author Michal Stanisz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions for streaming data using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(http_streamer).
-author("Bartosz Walkowicz").

-include("http/gui_download.hrl").
-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    get_read_block_size/1, calculate_max_read_blocks_count/1,
    stream_file/6,
    stream_bytes_range/3, stream_bytes_range/6, stream_bytes_range/7,
    send_data_chunk/2
]).

-type read_block_size() :: non_neg_integer().
-type download_ctx() :: #download_ctx{}.

-export_type([read_block_size/0, download_ctx/0]).

% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
% Due to lack of backpressure mechanism in cowboy when streaming files it must
% be additionally implemented. This module implementation checks cowboy process
% msg queue len to see if next data chunk can be queued. To account for
% differences in speed between network and storage a simple backoff is
% implemented with below boundaries.
-define(MIN_SEND_RETRY_DELAY, 100).
-define(MAX_SEND_RETRY_DELAY, 1000).

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


-spec calculate_max_read_blocks_count(read_block_size()) -> non_neg_integer().
calculate_max_read_blocks_count(ReadBlockSize) ->
    max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize).


-spec stream_file(
    undefined | [http_parser:bytes_range()],
    lfm:handle(),
    file_meta:size(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) -> 
    cowboy_req:req().
stream_file(Ranges, FileHandle, FileSize, EncodingFun, OnSuccessCallback, Req) ->
    ReadBlockSize = get_read_block_size(FileHandle),
    DownloadCtx = #download_ctx{
        file_size = FileSize,
        file_handle = FileHandle,
        read_block_size = ReadBlockSize,
        max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
        encoding_fun = EncodingFun,
        on_success_callback = OnSuccessCallback
    },
    stream_file_insecure(Ranges, DownloadCtx, Req).


-spec stream_bytes_range(
    lfm:handle(),
    file_meta:size(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size()
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize) ->
    stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, undefined).


-spec stream_bytes_range(
    lfm:handle(),
    file_meta:size(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size(),
    tar_utils:stream() | undefined
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, TarStream) ->
    stream_bytes_range(Range, #download_ctx{
        file_size = FileSize,
        file_handle = FileHandle,
        read_block_size = ReadBlockSize,
        max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
        encoding_fun = EncodingFun,
        tar_stream = TarStream,
        on_success_callback = fun() -> ok end
    }, Req).


-spec stream_bytes_range(http_parser:bytes_range(), download_ctx(), cowboy_req:req()) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(Range, DownloadCtx, Req) ->
    stream_bytes_range(Range, DownloadCtx, Req, ?MIN_SEND_RETRY_DELAY, 0).


-spec send_data_chunk(Data :: binary(), cowboy_req:req()) -> NextRetryDelay :: time:millis().
send_data_chunk(Data, Req) ->
    send_data_chunk(Data, Req, ?MAX_DOWNLOAD_BUFFER_SIZE div ?DEFAULT_READ_BLOCK_SIZE, ?MIN_SEND_RETRY_DELAY).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_content_range_header_value(http_parser:bytes_range(), file_meta:size()) -> binary().
build_content_range_header_value({RangeStart, RangeEnd}, FileSize) ->
    str_utils:format_bin("bytes ~B-~B/~B", [RangeStart, RangeEnd, FileSize]).


%% @private
-spec stream_file_insecure(undefined | [http_parser:bytes_range()], http_streamer:download_ctx(),
    cowboy_req:req()) -> cowboy_req:req().
stream_file_insecure(undefined, DownloadCtx, Req) ->
    stream_whole_file(DownloadCtx, Req);
stream_file_insecure([OneRange], DownloadCtx, Req) ->
    stream_one_ranged_body(OneRange, DownloadCtx, Req);
stream_file_insecure(Ranges, DownloadCtx, Req) ->
    stream_multipart_ranged_body(Ranges, DownloadCtx, Req).


%% @private
-spec stream_whole_file(http_streamer:download_ctx(), cowboy_req:req()) -> cowboy_req:req().
stream_whole_file(#download_ctx{
    file_size = FileSize,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),
    
    stream_bytes_range({0, FileSize - 1}, DownloadCtx, Req1),
    OnSuccessCallback(),
    
    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_one_ranged_body(http_parser:bytes_range(), http_streamer:download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_one_ranged_body({RangeStart, RangeEnd} = Range, #download_ctx{
    file_size = FileSize,
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
    
    stream_bytes_range(Range, DownloadCtx, Req1),
    OnSuccessCallback(),
    
    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_multipart_ranged_body([http_parser:bytes_range()], http_streamer:download_ctx(),
    cowboy_req:req()) -> cowboy_req:req().
stream_multipart_ranged_body(Ranges, #download_ctx{
    file_size = FileSize,
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
        
        stream_bytes_range(Range, DownloadCtx, Req1)
    end, Ranges),
    OnSuccessCallback(),
    
    cowboy_req:stream_body(cow_multipart:close(Boundary), fin, Req1),
    Req1.


%% @private
-spec stream_bytes_range(
    http_parser:bytes_range(),
    download_ctx(),
    cowboy_req:req(),
    SendRetryDelay :: time:millis(),
    ReadBytes :: non_neg_integer()
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range({From, To}, #download_ctx{tar_stream = TarStream}, _, _, _) when From > To ->
    TarStream;
stream_bytes_range({From, To}, #download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    read_block_size = ReadBlockSize,
    max_read_blocks_count = MaxReadBlocksCount,
    encoding_fun = EncodingFun,
    tar_stream = TarStream
} = DownloadCtx, Req, SendRetryDelay, ReadBytes) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    MinBytesToRead = case TarStream of
        undefined -> 
            0;
        _ -> 
            % in tar header file size was provided and such number of bytes 
            % must be streamed, otherwise incorrect tar archive will be created
            FileSize 
    end,
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, MinBytesToRead - ReadBytes),

    case byte_size(Data) of
        0 -> 
            TarStream;
        DataSize ->
            EncodedData = EncodingFun(Data),
            {BytesToSend, FinalTarStream} = case TarStream of
                undefined -> 
                    {EncodedData, undefined};
                _ ->
                    TarStream2 = tar_utils:append_to_file_content(TarStream, EncodedData, DataSize),
                    tar_utils:flush(TarStream2)
            end,
            NextSendRetryDelay = send_data_chunk(BytesToSend, Req, MaxReadBlocksCount, SendRetryDelay),
            stream_bytes_range(
                {From + DataSize, To}, 
                DownloadCtx#download_ctx{file_handle = NewFileHandle, tar_stream = FinalTarStream}, 
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
