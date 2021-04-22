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
    new_file_stream/2, 
    set_read_block_size/2, set_encoding_fun/2, set_send_fun/2, set_range_policy/2, 
    stream_bytes_range/3,
    send_data_chunk/2, send_data_chunk/3,
    get_read_block_size/1
]).

-define(DEFAULT_READ_BLOCK_SIZE, op_worker:get_env(
    default_download_read_block_size, 10485760) % 10 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, op_worker:get_env(
    max_download_buffer_size, 20971520) % 20 MB
).

-type read_block_size() :: non_neg_integer().
-type send_state() :: term().
-type encoding_fun() :: fun((Data :: binary()) -> EncodedData :: binary()).
-type send_fun() :: fun(
    (Data :: binary, SendState :: send_state(), MaxReadBlocksCount :: non_neg_integer(), RetryDelay :: non_neg_integer()) -> 
        {NewRetryDelay :: non_neg_integer(), UpdatedSendState :: send_state()}
).

% Strict range policy means that exactly requested number of bytes will be sent despite it being smaller or
% if file was truncated in the mean time, whereas with soft policy maximum of file size bytes will be sent.
-type range_policy() :: strict | soft.

-record(streaming_ctx, {
    file_size :: file_meta:size(),
    file_handle :: lfm:handle(),
    read_block_size :: read_block_size(),
    max_read_blocks_count :: non_neg_integer(),
    encoding_fun :: encoding_fun(),
    send_fun :: send_fun(),
    range_policy = soft :: range_policy()
}).

-opaque streaming_ctx() :: #streaming_ctx{}.

-export_type([streaming_ctx/0]).

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


-spec new_file_stream(lfm:handle(), file_meta:size()) -> streaming_ctx().
new_file_stream(FileHandle, FileSize) ->
    #streaming_ctx{
        file_handle = FileHandle,
        file_size = FileSize
    }.


-spec set_read_block_size(streaming_ctx(), read_block_size()) -> streaming_ctx().
set_read_block_size(StreamingCtx, ReadBlockSize) ->
    StreamingCtx#streaming_ctx{
        read_block_size = ReadBlockSize,
        max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize)
    }.


-spec set_encoding_fun(streaming_ctx(), encoding_fun()) -> streaming_ctx().
set_encoding_fun(StreamingCtx, EncodingFun) ->
    StreamingCtx#streaming_ctx{
        encoding_fun = EncodingFun
    }.


-spec set_send_fun(streaming_ctx(), send_fun()) -> streaming_ctx().
set_send_fun(StreamingCtx, SendFun) ->
    StreamingCtx#streaming_ctx{
        send_fun = SendFun
    }.


-spec set_range_policy(streaming_ctx(), range_policy()) -> streaming_ctx().
set_range_policy(StreamingCtx, NewPolicy) ->
    StreamingCtx#streaming_ctx{
        range_policy = NewPolicy
    }.


-spec stream_bytes_range(streaming_ctx(), http_parser:bytes_range(), send_state()) ->
    tar_utils:stream() | undefined | no_return().
stream_bytes_range(StreamingCtx, Range, SendState) ->
    stream_bytes_range_internal(Range, 
        set_streaming_ctx_defaults(StreamingCtx), SendState, ?MIN_SEND_RETRY_DELAY, 0).


-spec send_data_chunk(Data :: iodata(), cowboy_req:req()) -> 
    {NextRetryDelay :: time:millis(), cowboy_req:req()}.
send_data_chunk(Data, Req) ->
    send_data_chunk(Data, Req, ?MIN_SEND_RETRY_DELAY).


-spec send_data_chunk(Data :: iodata(), cowboy_req:req(), time:millis()) ->
    {NextRetryDelay :: time:millis(), cowboy_req:req()}.
send_data_chunk(Data, Req, SendRetryDelay) ->
    send_data_chunk(Data, Req, ?MAX_DOWNLOAD_BUFFER_SIZE div ?DEFAULT_READ_BLOCK_SIZE, SendRetryDelay).


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
    State :: send_state(),
    SendRetryDelay :: time:millis(),
    ReadBytes :: non_neg_integer()
) ->
    term() | no_return().
stream_bytes_range_internal({From, To}, _, SendState, _, _) when From > To ->
    SendState;
stream_bytes_range_internal({From, To}, #streaming_ctx{
    file_handle = FileHandle,
    file_size = FileSize,
    read_block_size = ReadBlockSize,
    max_read_blocks_count = MaxReadBlocksCount,
    encoding_fun = EncodingFun,
    send_fun = SendFun,
    range_policy = RangePolicy
} = StreamingCtx, SendState, SendRetryDelay, ReadBytes) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    MinBytes = case RangePolicy of
        soft -> 0;
        strict -> FileSize - ReadBytes
    end,
    {NewFileHandle, Data} = read_file_data(FileHandle, From, ToRead, MinBytes),

    case byte_size(Data) of
        0 ->
            SendState;
        DataSize ->
            EncodedData = EncodingFun(Data),
            {NextSendRetryDelay, UpdatedState} = SendFun(EncodedData, SendState, MaxReadBlocksCount, SendRetryDelay),
            stream_bytes_range_internal(
                {From + DataSize, To}, 
                StreamingCtx#streaming_ctx{file_handle = NewFileHandle},
                UpdatedState, NextSendRetryDelay, ReadBytes + DataSize
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
    {NextRetryDelay :: time:millis(), cowboy_req:req()}.
send_data_chunk(Data, #{pid := ConnPid} = Req, MaxReadBlocksCount, RetryDelay) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),
    
    case MsgQueueLen < MaxReadBlocksCount of
        true ->
            cowboy_req:stream_body(Data, nofin, Req),
            {max(RetryDelay div 2, ?MIN_SEND_RETRY_DELAY), Req};
        false ->
            timer:sleep(RetryDelay),
            send_data_chunk(
                Data, Req, MaxReadBlocksCount,
                min(2 * RetryDelay, ?MAX_SEND_RETRY_DELAY)
            )
    end.


%% @private
-spec set_streaming_ctx_defaults(streaming_ctx()) -> streaming_ctx().
set_streaming_ctx_defaults(#streaming_ctx{read_block_size = undefined, file_handle = FileHandle} = StreamingCtx) ->
    set_streaming_ctx_defaults(set_read_block_size(StreamingCtx, get_read_block_size(FileHandle)));
set_streaming_ctx_defaults(#streaming_ctx{encoding_fun = undefined} = StreamingCtx) ->
    set_streaming_ctx_defaults(set_encoding_fun(StreamingCtx, fun(Data) -> Data end));
set_streaming_ctx_defaults(#streaming_ctx{send_fun = undefined} = StreamingCtx) ->
    set_streaming_ctx_defaults(set_send_fun(StreamingCtx,
        fun(Data, Req, MaxReadBlocksCount, SendRetryDelay) ->
            send_data_chunk(Data, Req, MaxReadBlocksCount, SendRetryDelay)
        end));
set_streaming_ctx_defaults(StreamingCtx) ->
    StreamingCtx.
