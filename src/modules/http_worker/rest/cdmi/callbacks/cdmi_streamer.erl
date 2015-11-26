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
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([binary_stream_size/2, cdmi_stream_size/5,
    stream_binary/2, stream_cdmi/5, stream_range/7]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets size of a stream
%%--------------------------------------------------------------------
-spec binary_stream_size(Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined,
  FileSize :: non_neg_integer()) -> non_neg_integer().
binary_stream_size(undefined, FileSize) -> FileSize;
binary_stream_size(Ranges, _FileSize) ->
    lists:foldl(fun
        ({From, To}, Acc) when To >= From -> max(0, Acc + To - From + 1);
        ({_, _}, Acc)  -> Acc
    end, 0, Ranges).

%%--------------------------------------------------------------------
%% @doc Gets size of a cdmi stream
%%--------------------------------------------------------------------
-spec cdmi_stream_size(Range :: {non_neg_integer(), non_neg_integer()},
  FileSize :: non_neg_integer(), ValueTransferEncoding :: binary(),
  JsonBodyPrefix :: binary(), JsonBodySuffix :: binary()) -> non_neg_integer().
cdmi_stream_size(Range, FileSize, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix) ->
    DataSize =
        case Range of
            {From, To} when To >= From -> To - From + 1;
            default -> FileSize
        end,
    case ValueTransferEncoding of
        <<"base64">> ->
            byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + trunc(4 * utils:ceil(DataSize / 3.0));
        <<"utf-8">> ->
            byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + DataSize
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream_binary(Stata ::#{},
  Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined) -> function().
stream_binary(#{attributes := #file_attr{size = Size}} = State, undefined) ->
    stream_binary(State, [{0, Size -1}]);
stream_binary(#{path := Path, auth := Auth} = State, Ranges) ->
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    fun(Socket, Transport) ->
        try
            {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
            lists:foreach(fun(Rng) ->
                stream_range(Socket, Transport, State, Rng, <<"utf-8">>, BufferSize, FileHandle) end, Ranges)
        catch Type:Message ->
            % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
            ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Path, Type, Message])
        end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket as
%% json representing cdmi object.
%% @end
%%--------------------------------------------------------------------
-spec stream_cdmi(Stata ::#{}, Range :: {non_neg_integer(), non_neg_integer()},
  ValueTransferEncoding :: binary(), JsonBodyPrefix :: binary(),
  JsonBodySuffix :: binary()) -> function().
stream_cdmi(#{path := Path, auth := Auth} = State, Range, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix) ->
    {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path} ,read),
    fun(Socket, Transport) ->
        try
            Transport:send(Socket, JsonBodyPrefix),
            {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
            cdmi_streamer:stream_range(Socket, Transport, State, Range, ValueTransferEncoding, BufferSize, FileHandle),
            Transport:send(Socket,JsonBodySuffix)
        catch Type:Message ->
            % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
            ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Path, Type, Message])
        end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads given range of bytes (defaults to whole file) from file (obtained
%% from state path), result is encoded according to 'Encoding' argument
%% and streamed to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream_range(Socket :: term(), Transport :: atom(), State :: #{}, Range, Encoding :: binary(),
  BufferSize :: integer(), FileHandle :: onedata_file_api:file_handle()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
stream_range(Socket, Transport, State, Range, Encoding, BufferSize, FileHandle) when (BufferSize rem 3) =/= 0 ->
    %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
    stream_range(Socket, Transport, State, Range, Encoding, BufferSize - (BufferSize rem 3), FileHandle);
stream_range(Socket, Transport, #{attributes := #file_attr{size = Size}} = State, default, Encoding, BufferSize, FileHandle) ->
    %default range should remain consistent with parse_object_ans/2 valuerange clause
    stream_range(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize, FileHandle);
stream_range(Socket, Transport, State, {From, To}, Encoding, BufferSize, FileHandle) ->
    ToRead = To - From + 1,
    case ToRead > BufferSize of
        true ->
            {ok, NewFileHandle, Data} = onedata_file_api:read(FileHandle, From, BufferSize),
            Transport:send(Socket, encode(Data, Encoding)),
            stream_range(Socket, Transport, State, {From + BufferSize, To}, Encoding, BufferSize, NewFileHandle);
        false ->
            {ok, _NewFileHandle, Data} = onedata_file_api:read(FileHandle, From, ToRead),
            Transport:send(Socket, encode(Data, Encoding))
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Encodes data according to given ecoding
%%--------------------------------------------------------------------
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
encode(Data, Encoding) when Encoding =:= <<"base64">> ->
    base64:encode(Data);
encode(Data, _) ->
    Data.