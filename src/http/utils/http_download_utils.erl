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
-module(http_download_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/cdmi.hrl").
-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_read_block_size/1,
    stream_file/3, stream_bytes_range/5
]).

-type read_block_size() :: non_neg_integer().

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


-spec stream_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
stream_file(SessionId, #file_attr{guid = FileGuid, name = FileName, size = FileSize}, Req0) ->
    case http_parser:parse_range_header(Req0, FileSize) of
        invalid ->
            cowboy_req:reply(
                ?HTTP_416_RANGE_NOT_SATISFIABLE,
                #{?HDR_CONTENT_RANGE => str_utils:format_bin("bytes */~B", [FileSize])},
                Req0
            );
        Ranges ->
            Req1 = ensure_content_type_header_set(FileName, Req0),

            case lfm:monitored_open(SessionId, {guid, FileGuid}, read) of
                {ok, FileHandle} ->
                    try
                        ReadBlockSize = get_read_block_size(FileHandle),
                        stream_file_insecure(FileHandle, Ranges, FileSize, ReadBlockSize, Req1)
                    catch Type:Reason ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while processing file (~p) download "
                                          "for user ~p - ~p:~p", [
                            FileGuid, UserId, Type, Reason
                        ]),
                        http_req_utils:send_error_response(Reason, Req1)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req_utils:send_error_response(?ERROR_POSIX(Errno), Req1)
            end
    end.


-spec stream_bytes_range(
    lfm:handle(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size()
) ->
    ok | no_return().
stream_bytes_range(FileHandle, Range, Req, EncodingFun, ReadBlockSize) ->
    MaxReadBlocks = max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize),
    stream_bytes_range(
        FileHandle, Range, Req, EncodingFun,
        ReadBlockSize, MaxReadBlocks, ?MIN_SEND_RETRY_DELAY
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_content_type_header_set(file_meta:name(), cowboy_req:req()) -> cowboy_req:req().
ensure_content_type_header_set(FileName, Req) ->
    case cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req, undefined) of
        undefined ->
            {Type, Subtype, _} = cow_mimetypes:all(FileName),
            cowboy_req:set_resp_header(?HDR_CONTENT_TYPE, [Type, "/", Subtype], Req);
        _ ->
            Req
    end.


%% @private
-spec build_content_range_header_value(http_parser:bytes_range(), file_meta:size()) -> binary().
build_content_range_header_value({RangeStart, RangeEnd}, FileSize) ->
    str_utils:format_bin("bytes ~B-~B/~B", [RangeStart, RangeEnd, FileSize]).


%% @private
-spec stream_file_insecure(
    lfm:handle(),
    undefined | [http_parser:bytes_range()],
    file_meta:size(),
    read_block_size(),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_file_insecure(FileHandle, undefined, FileSize, ReadBlockSize, Req) ->
    stream_whole_file(FileHandle, FileSize, ReadBlockSize, Req);
stream_file_insecure(FileHandle, [OneRange], FileSize, ReadBlockSize, Req) ->
    stream_one_ranged_body(FileHandle, OneRange, FileSize, ReadBlockSize, Req);
stream_file_insecure(FileHandle, Ranges, FileSize, ReadBlockSize, Req) ->
    stream_multipart_ranged_body(FileHandle, Ranges, FileSize, ReadBlockSize, Req).


%% @private
-spec stream_whole_file(lfm:handle(), file_meta:size(), read_block_size(), cowboy_req:req()) ->
    cowboy_req:req().
stream_whole_file(FileHandle, FileSize, ReadBlockSize, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),
    stream_bytes_range(FileHandle, {0, FileSize - 1}, Req1, fun(Data) -> Data end, ReadBlockSize),
    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_one_ranged_body(
    lfm:handle(),
    http_parser:bytes_range(),
    file_meta:size(),
    read_block_size(),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_one_ranged_body(FileHandle, {RangeStart, RangeEnd} = Range, FileSize, ReadBlockSize, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        Req0
    ),
    stream_bytes_range(FileHandle, Range, Req1, fun(Data) -> Data end, ReadBlockSize),
    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_multipart_ranged_body(
    lfm:handle(),
    [http_parser:bytes_range()],
    file_meta:size(),
    read_block_size(),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_multipart_ranged_body(FileHandle, Ranges, FileSize, ReadBlockSize, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),

    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Req0
    ),
    stream_multipart_byteranges(
        FileHandle, Ranges, FileSize, ReadBlockSize, Boundary, ContentType, Req1
    ),
    cowboy_req:stream_body(cow_multipart:close(Boundary), fin, Req1),
    Req1.


%% @private
-spec stream_multipart_byteranges(
    lfm:handle(),
    [http_parser:bytes_range()],
    file_meta:size(),
    read_block_size(),
    MultipartBoundary :: binary(),
    ContentType :: binary(),
    cowboy_req:req()
) ->
    ok.
stream_multipart_byteranges(_Handle, [], _Size, _ReadBlockSize, _Boundary, _ContentType, _Req) ->
    ok;
stream_multipart_byteranges(
    FileHandle, [Range | RestRanges], FileSize, ReadBlockSize,
    Boundary, ContentType, Req
) ->
    NextPartHead = cow_multipart:first_part(Boundary, [
        {?HDR_CONTENT_TYPE, ContentType},
        {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
    ]),
    cowboy_req:stream_body(NextPartHead, nofin, Req),

    stream_bytes_range(FileHandle, Range, Req, fun(Data) -> Data end, ReadBlockSize),

    stream_multipart_byteranges(
        FileHandle, RestRanges, FileSize, ReadBlockSize, Boundary, ContentType, Req
    ).


%% @private
-spec stream_bytes_range(
    lfm:handle(),
    http_parser:bytes_range(),
    cowboy_req:req(),
    EncodingFun :: fun((Data :: binary()) -> EncodedData :: binary()),
    read_block_size(),
    MaxReadBlocks :: non_neg_integer(),
    SendRetryDelay :: non_neg_integer()
) ->
    ok | no_return().
stream_bytes_range(_, {From, To}, _, _, _, _, _) when From > To ->
    ok;
stream_bytes_range(
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
            NextSendRetryDelay = send_data_chunk(EncodedData, Req, MaxReadBlocks, SendRetryDelay),

            stream_bytes_range(
                NewFileHandle, {From + DataSize, To}, Req, EncodingFun,
                ReadBlockSize, MaxReadBlocks, NextSendRetryDelay
            )
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
    Data :: binary(),
    cowboy_req:req(),
    MaxReadBlocks :: non_neg_integer(),
    RetryDelay :: non_neg_integer()
) ->
    NextRetryDelay :: non_neg_integer().
send_data_chunk(Data, #{pid := ConnPid} = Req, MaxReadBlocks, RetryDelay) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),

    case MsgQueueLen < MaxReadBlocks of
        true ->
            cowboy_req:stream_body(Data, nofin, Req),
            max(RetryDelay div 2, ?MIN_SEND_RETRY_DELAY);
        false ->
            timer:sleep(RetryDelay),
            send_data_chunk(Data, Req, MaxReadBlocks, min(2 * RetryDelay, ?MAX_SEND_RETRY_DELAY))
    end.
