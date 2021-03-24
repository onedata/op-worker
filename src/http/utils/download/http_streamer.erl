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

-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    init_stream/2, init_stream/3, close_stream/2,
    stream_bytes_range/4, stream_bytes_range/6, stream_bytes_range/7,
    send_data_chunk/2,
    get_read_block_size/1
]).

-define(DEFAULT_READ_BLOCK_SIZE, op_worker:get_env(
    default_download_read_block_size, 10485760) % 10 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, op_worker:get_env(
    max_download_buffer_size, 20971520) % 20 MB
).

-type read_block_size() :: non_neg_integer().
-type encoding_fun() :: fun((Data :: binary()) -> EncodedData :: binary()).

-record(streaming_ctx, {
    file_size :: file_meta:size(),
    file_handle :: lfm:handle(),
    read_block_size :: read_block_size(),
    max_read_blocks_count :: non_neg_integer(),
    encoding_fun :: encoding_fun(),
    tar_stream = undefined :: undefined | tar_utils:stream()
}).

-type streaming_ctx() :: #streaming_ctx{}.

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

-spec init_stream(cowboy:http_status(), cowboy_req:req()) ->
    cowboy_req:req().
init_stream(Status, Req) ->
    init_stream(Status, #{}, Req).


-spec init_stream(cowboy:http_status(), cowboy:http_headers(), cowboy_req:req()) ->
    cowboy_req:req().
init_stream(Status, Headers, Req) ->
    cowboy_req:stream_reply(Status, Headers, Req).


-spec close_stream(undefined | binary(), cowboy_req:req()) -> ok.
close_stream(undefined, Req) ->
    cowboy_req:stream_body(<<"">>, fin, Req);
close_stream(Boundary, Req) ->
    cowboy_req:stream_body(cow_multipart:close(Boundary), fin, Req).


-spec stream_bytes_range(lfm:handle(), file_meta:size(), http_parser:bytes_range(), cowboy_req:req()) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req) ->
    stream_bytes_range(FileHandle, FileSize, Range, Req, 
        fun(Data) -> Data end, get_read_block_size(FileHandle)).


-spec stream_bytes_range( lfm:handle(), file_meta:size(), http_parser:bytes_range(), cowboy_req:req(),
    encoding_fun(), read_block_size() ) -> tar_utils:stream() | undefined | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize) ->
    stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, undefined).


-spec stream_bytes_range(
    lfm:handle(), 
    file_meta:size(), 
    http_parser:bytes_range(), 
    cowboy_req:req(),
    encoding_fun(), 
    read_block_size(), 
    tar_utils:stream() | undefined
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(FileHandle, FileSize, Range, Req, EncodingFun, ReadBlockSize, TarStream) ->
    stream_bytes_range_internal(Range, #streaming_ctx{
        file_size = FileSize,
        file_handle = FileHandle,
        read_block_size = ReadBlockSize,
        max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
        encoding_fun = EncodingFun,
        tar_stream = TarStream
    }, Req, ?MIN_SEND_RETRY_DELAY, 0).


-spec send_data_chunk(Data :: iodata(), cowboy_req:req()) -> NextRetryDelay :: time:millis().
send_data_chunk(Data, Req) ->
    send_data_chunk(Data, Req, ?MAX_DOWNLOAD_BUFFER_SIZE div ?DEFAULT_READ_BLOCK_SIZE, ?MIN_SEND_RETRY_DELAY).


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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec calculate_max_read_blocks_count(read_block_size()) -> non_neg_integer().
calculate_max_read_blocks_count(ReadBlockSize) ->
    max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize).


%% @private
-spec stream_bytes_range_internal(
    http_parser:bytes_range(),
    streaming_ctx(),
    cowboy_req:req(),
    SendRetryDelay :: time:millis(),
    ReadBytes :: non_neg_integer()
) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range_internal({From, To}, #streaming_ctx{tar_stream = TarStreamOrUndefined}, _, _, _) when From > To ->
    TarStreamOrUndefined;
stream_bytes_range_internal({From, To}, #streaming_ctx{
    file_handle = FileHandle,
    read_block_size = ReadBlockSize,
    max_read_blocks_count = MaxReadBlocksCount,
    encoding_fun = EncodingFun,
    tar_stream = undefined
} = DownloadCtx, Req, SendRetryDelay, ReadBytes) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, 0),

    case byte_size(Data) of
        0 -> 
            undefined;
        DataSize ->
            EncodedData = EncodingFun(Data),
            NextSendRetryDelay = send_data_chunk(EncodedData, Req, MaxReadBlocksCount, SendRetryDelay),
            stream_bytes_range_internal(
                {From + DataSize, To}, 
                DownloadCtx#streaming_ctx{file_handle = NewFileHandle}, 
                Req, NextSendRetryDelay, ReadBytes + DataSize
            )
    end;
stream_bytes_range_internal({From, To}, #streaming_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    read_block_size = ReadBlockSize,
    max_read_blocks_count = MaxReadBlocksCount,
    encoding_fun = EncodingFun,
    tar_stream = TarStream
} = DownloadCtx, Req, SendRetryDelay, ReadBytes) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    % in tar header file size was provided and such number of bytes 
    % must be streamed, otherwise incorrect tar archive will be created
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, FileSize - ReadBytes),
    
    case byte_size(Data) of
        0 ->
            TarStream;
        DataSize ->
            EncodedData = EncodingFun(Data),
            TarStream2 = tar_utils:append_to_file_content(TarStream, EncodedData, DataSize),
            {BytesToSend, FinalTarStream} = tar_utils:flush_buffer(TarStream2),
            NextSendRetryDelay = send_data_chunk(BytesToSend, Req, MaxReadBlocksCount, SendRetryDelay),
            stream_bytes_range_internal(
                {From + DataSize, To},
                DownloadCtx#streaming_ctx{file_handle = NewFileHandle, tar_stream = FinalTarStream},
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
    Data :: iodata(),
    cowboy_req:req(),
    MaxReadBlocksCount :: non_neg_integer(),
    RetryDelay :: time:millis()
) ->
    NextRetryDelay :: time:millis().
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
