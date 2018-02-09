%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions responsible for streaming files.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_streamer).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([binary_stream_size/2, cdmi_stream_size/5, stream_binary/5,
    stream_cdmi/7, stream_range/6, write_body_to_file/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets size of a stream, which is the size of streamed binary data of file
%%--------------------------------------------------------------------
-spec binary_stream_size(Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined,
  FileSize :: non_neg_integer()) -> non_neg_integer().
binary_stream_size(undefined, FileSize) -> FileSize;
binary_stream_size(Ranges, FileSize) ->
    lists:foldl(fun
        ({From, To}, Acc) when To >= From -> max(0, Acc + min(FileSize - 1, To) - From + 1);
        ({_, _}, Acc)  -> Acc
    end, 0, Ranges).

%%--------------------------------------------------------------------
%% @doc
%% Gets size of a cdmi stream, which is the size of streamed json representing
%% cdmi_object
%% @end
%%--------------------------------------------------------------------
-spec cdmi_stream_size(Range :: {non_neg_integer(), non_neg_integer()},
  FileSize :: non_neg_integer(), ValueTransferEncoding :: binary(),
  JsonBodyPrefix :: binary(), JsonBodySuffix :: binary()) -> non_neg_integer().
cdmi_stream_size(Range, FileSize, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix) ->
    DataSize =
        case Range of
            {From, To} when To >= From -> min(FileSize - 1, To) - From + 1;
            default -> FileSize
        end,
    case ValueTransferEncoding of
        <<"base64">> ->
            byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix)
                + cdmi_encoder:base64_encoded_data_size(DataSize);
        <<"utf-8">> ->
            byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + DataSize
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream_binary(HttpStatus :: non_neg_integer(), Req :: req(),
    State ::maps:map(), Size :: non_neg_integer(),
    Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined) -> req().
stream_binary(
    HttpStatus, Req, #{attributes := #file_attr{size = Size}} = State, Size, undefined
) ->
    stream_binary(HttpStatus, Req, State, Size, [{0, Size -1}]);
stream_binary(HttpStatus, Req, #{path := Path, auth := Auth} = State, Size, Ranges) ->
    StreamSize = binary_stream_size(Ranges, Size),
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),

    Req2 = cowboy_req:stream_reply(HttpStatus, #{
        <<"content-length">> => integer_to_binary(StreamSize)
    }, Req),
    lists:foreach(fun(Range) ->
        stream_range(Req2, State, Range, <<"utf-8">>, BufferSize, FileHandle)
    end, Ranges),
    cowboy_req:stream_body(<<"">>, fin, Req2),
    Req2.

%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket as
%% json representing cdmi object.
%% @end
%%--------------------------------------------------------------------
-spec stream_cdmi(Req :: req(), State ::maps:map(), Size :: non_neg_integer(),
    Range :: {non_neg_integer(), non_neg_integer()},
    ValueTransferEncoding :: binary(), JsonBodyPrefix :: binary(),
    JsonBodySuffix :: binary()) -> req().
stream_cdmi(Req, #{path := Path, auth := Auth} = State, Size, Range,
    ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix
) ->
    StreamSize = cdmi_streamer:cdmi_stream_size(
        Range, Size, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix
    ),
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
    Req2 = cowboy_req:stream_reply(?HTTP_OK, #{
        <<"content-length">> => integer_to_binary(StreamSize)
    }, Req),
    cowboy_req:stream_body(JsonBodyPrefix, nofin, Req2),
    stream_range(Req2, State, Range, ValueTransferEncoding, BufferSize, FileHandle),
    cowboy_req:stream_body(JsonBodySuffix, fin, Req2),
    Req2.

%%--------------------------------------------------------------------
%% @doc
%% Reads given range of bytes (defaults to whole file) from file (obtained
%% from state path), result is encoded according to 'Encoding' argument
%% and streamed to given Request object.
%% @end
%%--------------------------------------------------------------------
-spec stream_range(Req :: req(), State :: maps:map(), Range, Encoding :: binary(),
  BufferSize :: integer(), FileHandle :: onedata_file_api:file_handle()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
stream_range(Req, State, Range, Encoding, BufferSize, FileHandle)
    when (BufferSize rem 3) =/= 0 ->
    %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
    stream_range(Req, State, Range, Encoding, BufferSize - (BufferSize rem 3), FileHandle);
stream_range(Req, #{attributes := #file_attr{size = Size}} = State,
  default, Encoding, BufferSize, FileHandle) ->
    %default range should remain consistent with parse_object_ans/2 value range clause
    stream_range(Req, State, {0, Size - 1}, Encoding, BufferSize, FileHandle);
stream_range(Req, State, {From, To}, Encoding, BufferSize, FileHandle) ->
    ToRead = To - From + 1,
    ReadBufSize = min(ToRead, BufferSize),
    {ok, NewFileHandle, Data} = onedata_file_api:read(FileHandle, From, ReadBufSize),
    DataSize = size(Data),
    case DataSize of
        0 ->
           ok;
        _ ->
            cowboy_req:stream_body(cdmi_encoder:encode(Data, Encoding), nofin, Req),
            stream_range(Req, State, {From + DataSize, To},
                Encoding, BufferSize, NewFileHandle)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Reads request's body and writes it to file given by handler.
%% Returns updated request.
%% @end
%%--------------------------------------------------------------------
-spec write_body_to_file(req(), integer(), onedata_file_api:file_handle()) ->
    {ok, req()}.
write_body_to_file(Req0, Offset, FileHandle) ->
    WriteFun = fun Write(Req, WriteOffset, Handle) ->
        {Status, Chunk, Req1} = cowboy_req:read_body(Req),
        {ok, _NewHandle, Bytes} = onedata_file_api:write(Handle, WriteOffset, Chunk),
        case Status of
            more -> Write(Req1, WriteOffset + Bytes, Handle);
            ok -> {ok, Req1}
        end
    end,

    WriteFun(Req0, Offset, FileHandle).
