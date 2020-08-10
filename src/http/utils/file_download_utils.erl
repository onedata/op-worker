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
-module(file_download_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_read_block_size/1, stream_range/5]).

-type range() :: {From :: non_neg_integer(), To :: non_neg_integer()}.

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


-spec stream_range(
    lfm:handle(),
    range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    ReadBlockSize :: non_neg_integer()
) ->
    ok | no_return().
stream_range(FileHandle, Range, Req, EncodingFun, ReadBlockSize) ->
    MaxReadBlocks = max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize),
    stream_range(
        FileHandle, Range, Req, EncodingFun,
        ReadBlockSize, MaxReadBlocks, ?MIN_SEND_RETRY_DELAY
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec stream_range(
    lfm:handle(),
    range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    ReadBlockSize :: non_neg_integer(),
    MaxReadBlocks :: non_neg_integer(),
    SendRetryDelay :: non_neg_integer()
) ->
    ok | no_return().
stream_range(_, {From, To}, _, _, _, _, _) when From > To ->
    ok;
stream_range(
    FileHandle, {From, To}, Req, EncodingFun,
    ReadBlockSize, MaxReadBlocks, SendRetryDelay
) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {ok, NewFileHandle, Data} = ?check(lfm:read(FileHandle, From, ToRead)),

    case byte_size(Data) of
        0 ->
            ok;
        DataSize ->
            EncodedData = EncodingFun(Data),
            NextSendRetryDelay = send_data(EncodedData, Req, MaxReadBlocks, SendRetryDelay),

            stream_range(
                NewFileHandle, {From + DataSize, To}, Req, EncodingFun,
                ReadBlockSize, MaxReadBlocks, NextSendRetryDelay
            )
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
%% Cowboy uses separate process to manage socket and all messages, including
%% data to stream are sent to that process. However because it doesn't enforce
%% any backpressure mechanism it is easy, on slow networks and fast storages,
%% to read to memory entire file while sending process doesn't keep up with
%% sending those data. To avoid this it is necessary to check message_queue_len
%% of sending process and ensure it is not larger than max allowed blocks to
%% read into memory.
%% @end
%%--------------------------------------------------------------------
-spec send_data(
    Data :: binary(),
    cowboy_req:req(),
    MaxReadBlocks :: non_neg_integer(),
    RetryDelay :: non_neg_integer()
) ->
    NextRetryDelay :: non_neg_integer().
send_data(Data, #{pid := ConnPid} = Req, MaxReadBlocks, RetryDelay) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),

    case MsgQueueLen < MaxReadBlocks of
        true ->
            cowboy_req:stream_body(Data, nofin, Req),
            max(RetryDelay div 2, ?MIN_SEND_RETRY_DELAY);
        false ->
            timer:sleep(RetryDelay),
            send_data(Data, Req, MaxReadBlocks, min(2 * RetryDelay, ?MAX_SEND_RETRY_DELAY))
    end.
