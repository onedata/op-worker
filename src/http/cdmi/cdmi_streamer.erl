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

-include("middleware/middleware.hrl").
-include("http/cdmi.hrl").
-include("http/rest.hrl").
-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([stream_binary/4, stream_cdmi/6]).

-type range() :: {From :: non_neg_integer(), To :: non_neg_integer()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec stream_binary(
    HttpStatus :: non_neg_integer(),
    cowboy_req:req(),
    cdmi_handler:cdmi_req(),
    Ranges :: undefined | [range()]
) ->
    cowboy_req:req() | no_return().
stream_binary(HttpStatus, Req, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid, size = FileSize}
}, Ranges0) ->
    Ranges = case Ranges0 of
        undefined -> [{0, FileSize - 1}];
        _ -> Ranges0
    end,
    StreamSize = binary_stream_size(Ranges, FileSize),

    {ok, FileHandle} = ?check(lfm:open(SessionId, {guid, Guid}, read)),
    try
        ReadBlockSize = file_download_utils:get_read_block_size(FileHandle),

        Req2 = cowboy_req:stream_reply(HttpStatus, #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(StreamSize)
        }, Req),
        lists:foreach(fun(Range) ->
            file_download_utils:stream_range(
                FileHandle, Range, Req2, fun(Data) -> Data end, ReadBlockSize
            )
        end, Ranges),
        cowboy_req:stream_body(<<"">>, fin, Req2),

        Req2
    after
        lfm:release(FileHandle)
    end.


-spec stream_cdmi(
    cowboy_req:req(),
    cdmi_handler:cdmi_req(),
    Range :: default | range(),
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

    {ok, FileHandle} = ?check(lfm:open(SessionId, {guid, Guid}, read)),
    try
        ReadBlockSize0 = file_download_utils:get_read_block_size(FileHandle),
        ReadBlockSize = case Encoding of
            <<"base64">> ->
                % buffer size is shortened (so it's divisible by 3)
                % to allow base64 on the fly conversion
                ReadBlockSize0 - (ReadBlockSize0 rem 3);
            _ ->
                ReadBlockSize0
        end,

        Req2 = cowboy_req:stream_reply(?HTTP_200_OK, #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(StreamSize)
        }, Req),
        cowboy_req:stream_body(JsonBodyPrefix, nofin, Req2),
        file_download_utils:stream_range(
            FileHandle, Range1, Req2,
            fun(Data) -> cdmi_encoder:encode(Data, Encoding) end, ReadBlockSize
        ),
        cowboy_req:stream_body(JsonBodySuffix, fin, Req2),

        Req2
    after
        lfm:release(FileHandle)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets size of a stream, which is the size of streamed binary data of file.
%% @end
%%--------------------------------------------------------------------
-spec binary_stream_size([range()], FileSize :: non_neg_integer()) ->
    non_neg_integer().
binary_stream_size(Ranges, FileSize) ->
    lists:foldl(fun
        ({From, To}, Acc) when To >= From ->
            max(0, Acc + min(FileSize - 1, To) - From + 1);
        ({_, _}, Acc)  ->
            Acc
    end, 0, Ranges).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets size of a cdmi stream, which is the size of streamed json representing
%% cdmi_object.
%% @end
%%--------------------------------------------------------------------
-spec cdmi_stream_size(range(), FileSize :: non_neg_integer(),
    Encoding :: binary(), DataPrefix :: binary(), DataSuffix :: binary()) ->
    non_neg_integer().
cdmi_stream_size({From, To}, FileSize, Encoding, DataPrefix, DataSuffix) when To >= From ->
    DataSize = min(FileSize - 1, To) - From + 1,
    EncodedDataSize = case Encoding of
        <<"base64">> -> trunc(4 * utils:ceil(DataSize / 3.0));
        <<"utf-8">> -> DataSize
    end,
    byte_size(DataPrefix) + EncodedDataSize + byte_size(DataSuffix).
