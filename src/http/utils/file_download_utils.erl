%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file download.
%%% @end
%%%--------------------------------------------------------------------
-module(file_download_utils).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_read_block_size/1, stream_range/5]).

-type encoding() :: binary().  % <<"base64">> | <<"utf-8">>

-export_type([encoding/0]).

-define(DEFAULT_READ_BLOCK_SIZE, 1048576). % 1 MB

-define(MAX_READ_BLOCKS_IN_MEMORY, 5).
-define(SEND_RETRY_DELAY, 10).


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
    Range :: {From :: non_neg_integer(), To :: non_neg_integer()},
    cowboy_req:req(),
    encoding(),
    ReadBlockSize :: non_neg_integer()
) ->
    ok | no_return().
stream_range(FileHandle, {From, To}, Req, Encoding, ReadBlockSize) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {ok, NewFileHandle, Data} = ?check(lfm:read(FileHandle, From, ToRead)),

    case byte_size(Data) of
        0 ->
            ok;
        DataSize ->
            EncodedData = cdmi_encoder:encode(Data, Encoding),
            stream_data(EncodedData, Req),

            stream_range(
                NewFileHandle, {From + DataSize, To}, Req,
                Encoding, ReadBlockSize
            )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
%% Cowboy uses separate process to manage socket and all messages, including
%% data to stream are sent to that process. However because it doesn't enforce
%% any backpressure mechanism it is easy, on slow networks and fast storages,
%% to read to memory entire file while sending process doesn't keep up with
%% sending those data. To avoid this it is necessary to check message_queue_len
%% of sending process.
%% @end
%%--------------------------------------------------------------------
stream_data(Data, #{pid := ConnPid} = Req) ->
    {message_queue_len, Len} = process_info(ConnPid, message_queue_len),
    case Len < ?MAX_READ_BLOCKS_IN_MEMORY of
        true ->
            cowboy_req:stream_body(Data, nofin, Req);
        false ->
            timer:sleep(?SEND_RETRY_DELAY),
            stream_data(Data, Req)
    end.
