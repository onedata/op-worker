%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file download using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(http_streaming_utils).
-author("Bartosz Walkowicz").

-include("http/gui_download.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    get_read_block_size/1,
    stream_bytes_range/4, stream_bytes_range/6, stream_bytes_range/7,
    send_data_chunk/2
]).

-type read_block_size() :: non_neg_integer().
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
    }, Req, ?MIN_SEND_RETRY_DELAY).


-spec stream_bytes_range(
    http_parser:bytes_range(),
    download_ctx(),
    cowboy_req:req(),
    SendRetryDelay :: time:millis()
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(Range, DownloadCtx, Req, SendRetryDelay) ->
    stream_bytes_range(Range, DownloadCtx, Req, SendRetryDelay, 0).


%%--------------------------------------------------------------------
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec calculate_max_read_blocks_count(read_block_size()) -> non_neg_integer().
calculate_max_read_blocks_count(ReadBlockSize) ->
    max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize).


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
        undefined -> 0;
        _ -> FileSize
    end,
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, MinBytesToRead - ReadBytes),

    case byte_size(Data) of
        0 -> TarStream;
        DataSize ->
            {BytesToSend, FinalTarStream} = case TarStream of
                undefined -> {EncodingFun(Data), undefined};
                _ ->
                    TarStream2 = tar_utils:append_to_file_content(TarStream, EncodingFun(Data), DataSize),
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
