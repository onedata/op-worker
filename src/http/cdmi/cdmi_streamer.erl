%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions responsible for streaming content of files.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_streamer).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("http/cdmi.hrl").
-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([stream_cdmi/6]).


%%%===================================================================
%%% API
%%%===================================================================


-spec stream_cdmi(
    cowboy_req:req(),
    cdmi_handler:cdmi_req(),
    Range :: default | http_parser:bytes_range(),
    ValueTransferEncoding :: binary(),
    JsonBodyPrefix :: binary(),
    JsonBodySuffix :: binary()
) ->
    cowboy_req:req() | no_return().
stream_cdmi(Req, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid, size = Size}
}, Range0, Encoding, JsonBodyPrefix, JsonBodySuffix) ->
    Range1 = case Range0 of
        default -> {0, Size - 1};
        _ -> Range0
    end,
    StreamSize = cdmi_stream_size(
        Range1, Size, Encoding, JsonBodyPrefix, JsonBodySuffix
    ),

    {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, {guid, Guid}, read)),
    try
        ReadBlockSize0 = http_download_utils:get_read_block_size(FileHandle),
        ReadBlockSize = case Encoding of
            <<"base64">> ->
                % Base64 translates every 3 bytes of original data into 4 base64
                % characters (6 bits for each character, which gives 2^6 = 64
                % characters and hence the name - base64).
                % That is why in order to allow on the fly conversion the buffer
                % size must be shortened so it is divisible by 3.
                ReadBlockSize0 - (ReadBlockSize0 rem 3);
            _ ->
                ReadBlockSize0
        end,

        Req2 = cowboy_req:stream_reply(?HTTP_200_OK, #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(StreamSize)
        }, Req),
        cowboy_req:stream_body(JsonBodyPrefix, nofin, Req2),
        http_download_utils:stream_bytes_range(
            FileHandle, Size, Range1, Req2,
            fun(Data) -> cdmi_encoder:encode(Data, Encoding) end, ReadBlockSize
        ),
        cowboy_req:stream_body(JsonBodySuffix, fin, Req2),

        Req2
    after
        lfm:monitored_release(FileHandle)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets size of a cdmi stream, which is the size of streamed json representing
%% cdmi_object.
%% @end
%%--------------------------------------------------------------------
-spec cdmi_stream_size(http_parser:bytes_range(), FileSize :: non_neg_integer(),
    Encoding :: binary(), DataPrefix :: binary(), DataSuffix :: binary()) ->
    non_neg_integer().
cdmi_stream_size({0, -1}, _FileSize, _Encoding, _DataPrefix, _DataSuffix) ->
    % Empty file
    0;
cdmi_stream_size({From, To}, FileSize, Encoding, DataPrefix, DataSuffix) when To >= From ->
    DataSize = min(FileSize - 1, To) - From + 1,
    EncodedDataSize = case Encoding of
        <<"base64">> -> trunc(4 * utils:ceil(DataSize / 3.0));
        <<"utf-8">> -> DataSize
    end,
    byte_size(DataPrefix) + EncodedDataSize + byte_size(DataSuffix).
