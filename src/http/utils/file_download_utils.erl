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
    ?APP_NAME, default_download_read_block_size, 1048576) % 1 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, application:get_env(
    ?APP_NAME, max_download_buffer_size, 20971520) % 20 MB
).
-define(SEND_RETRY_DELAY, 100).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_read_block_size(lfm_context:ctx()) -> non_neg_integer().
get_read_block_size(FileHandle) ->
    utils:ensure_defined(
        storage:get_block_size(lfm_context:get_storage_id(FileHandle)),
        ?DEFAULT_READ_BLOCK_SIZE
    ).


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
    stream_range(FileHandle, Range, Req, EncodingFun, ReadBlockSize, MaxReadBlocks).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec stream_range(
    lfm:handle(),
    range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    ReadBlockSize :: non_neg_integer(),
    MaxReadBlocks :: non_neg_integer()
) ->
    ok | no_return().
stream_range(_, {From, To}, _, _, _, _) when From > To ->
    ok;
stream_range(FileHandle, {From, To}, Req, EncodingFun, ReadBlockSize, MaxReadBlocks) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {ok, NewFileHandle, Data} = ?check(lfm:read(FileHandle, From, ToRead)),

    case byte_size(Data) of
        0 ->
            ok;
        DataSize ->
            send_data(EncodingFun(Data), Req, MaxReadBlocks),
            stream_range(
                NewFileHandle, {From + DataSize, To}, Req,
                EncodingFun, ReadBlockSize, MaxReadBlocks
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
send_data(Data, #{pid := ConnPid} = Req, MaxReadBlocks) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),

    case MsgQueueLen < MaxReadBlocks of
        true ->
            cowboy_req:stream_body(Data, nofin, Req);
        false ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_data(Data, Req, MaxReadBlocks)
    end.
