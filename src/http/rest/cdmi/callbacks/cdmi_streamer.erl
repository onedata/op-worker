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
-export([binary_stream_size/2, cdmi_stream_size/5, stream_binary/2,
    stream_cdmi/5, stream_range/7, write_body_to_file/3]).

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
-spec stream_binary(Stata ::maps:map(),
  Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined) -> function().
stream_binary(#{attributes := #file_attr{size = Size}} = State, undefined) ->
    stream_binary(State, [{0, Size -1}]);
stream_binary(#{path := Path, auth := Auth} = State, Ranges) ->
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    fun(Socket, Transport) ->
            {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
            lists:foreach(fun(Rng) ->
                stream_range(Socket, Transport, State, Rng, <<"utf-8">>, BufferSize, FileHandle)
            end, Ranges)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket as
%% json representing cdmi object.
%% @end
%%--------------------------------------------------------------------
-spec stream_cdmi(Stata ::maps:map(), Range :: {non_neg_integer(), non_neg_integer()},
  ValueTransferEncoding :: binary(), JsonBodyPrefix :: binary(),
  JsonBodySuffix :: binary()) -> function().
stream_cdmi(#{path := Path, auth := Auth} = State, Range, ValueTransferEncoding,
  JsonBodyPrefix, JsonBodySuffix) ->
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    fun(Socket, Transport) ->
            Transport:send(Socket, JsonBodyPrefix),
            {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
            stream_range(Socket, Transport, State, Range,
                ValueTransferEncoding, BufferSize, FileHandle),
            Transport:send(Socket,JsonBodySuffix)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads given range of bytes (defaults to whole file) from file (obtained
%% from state path), result is encoded according to 'Encoding' argument
%% and streamed to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream_range(Socket :: term(), Transport :: atom(), State :: maps:map(), Range, Encoding :: binary(),
  BufferSize :: integer(), FileHandle :: onedata_file_api:file_handle()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
stream_range(Socket, Transport, State, Range, Encoding, BufferSize, FileHandle)
    when (BufferSize rem 3) =/= 0 ->
    %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
    stream_range(Socket, Transport, State, Range, Encoding, BufferSize - (BufferSize rem 3), FileHandle);
stream_range(Socket, Transport, #{attributes := #file_attr{size = Size}} = State,
  default, Encoding, BufferSize, FileHandle) ->
    %default range should remain consistent with parse_object_ans/2 valuerange clause
    stream_range(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize, FileHandle);
stream_range(Socket, Transport, State, {From, To}, Encoding, BufferSize, FileHandle) ->
    ToRead = To - From + 1,
    ReadBufSize = min(ToRead, BufferSize),
    {ok, NewFileHandle, Data} = onedata_file_api:read(FileHandle, From, ReadBufSize),
    Transport:send(Socket, cdmi_encoder:encode(Data, Encoding)),
    DataSize = size(Data),
    case DataSize of
        0 ->
           ok;
        _ ->
            stream_range(Socket, Transport, State, {From + DataSize, To},
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
write_body_to_file(Req, Offset, FileHandle) ->
    WriteFun = fun Write(Req, Offset, FileHandle) ->
        {Status, Chunk, Req1} = cowboy_req:body(Req),
        {ok, _NewHandle, Bytes} = onedata_file_api:write(FileHandle, Offset, Chunk),
        case Status of
            more -> Write(Req1, Offset + Bytes, FileHandle);
            ok -> {ok, Req1}
        end
    end,

    WriteFun(Req, Offset, FileHandle).
