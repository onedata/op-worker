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

-include("http/cdmi.hrl").
-include("http/gui_download.hrl").
-include("http/rest.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_read_block_size/1,
    stream_file/3, stream_file/4,
    stream_tarball/4
]).

-type read_block_size() :: non_neg_integer().
-type download_ctx() :: #download_ctx{}.


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
stream_file(SessionId, FileAttrs, Req) ->
    stream_file(SessionId, FileAttrs, fun() -> ok end, Req).


-spec stream_file(
    session:id(),
    lfm_attrs:file_attributes(),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_file(SessionId, #file_attr{
    guid = FileGuid,
    name = FileName,
    size = FileSize
}, OnSuccessCallback, Req0) ->
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

                        stream_file_insecure(Ranges, #download_ctx{
                            file_size = FileSize,
                            file_handle = FileHandle,
                            read_block_size = ReadBlockSize,
                            max_read_blocks_count = calculate_max_read_blocks_count(ReadBlockSize),
                            encoding_fun = fun(Data) -> Data end,
                            on_success_callback = OnSuccessCallback
                        }, Req1)
                    catch Type:Reason ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while processing file (~p) download "
                                          "for user ~p - ~p:~p", [
                            FileGuid, UserId, Type, Reason
                        ]),
                        http_req:send_error(Reason, Req1)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req1)
            end
    end.


-spec stream_tarball(
    session:id(),
    [lfm_attrs:file_attributes()],
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_tarball(SessionId, FileAttrsList, OnSuccessCallback, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges">>},
        Req0
    ),
    dir_streaming_traverse:run(FileAttrsList, SessionId, Req1),
    execute_on_success_callback(<<>>, OnSuccessCallback),
    Req1.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec calculate_max_read_blocks_count(read_block_size()) -> non_neg_integer().
calculate_max_read_blocks_count(ReadBlockSize) ->
    max(1, ?MAX_DOWNLOAD_BUFFER_SIZE div ReadBlockSize).


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
-spec stream_file_insecure(undefined | [http_parser:bytes_range()],download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_file_insecure(undefined, DownloadCtx, Req) ->
    stream_whole_file(DownloadCtx, Req);
stream_file_insecure([OneRange], DownloadCtx, Req) ->
    stream_one_ranged_body(OneRange, DownloadCtx, Req);
stream_file_insecure(Ranges, DownloadCtx, Req) ->
    stream_multipart_ranged_body(Ranges, DownloadCtx, Req).


%% @private
-spec stream_whole_file(download_ctx(), cowboy_req:req()) -> cowboy_req:req().
stream_whole_file(#download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),

    http_streaming_utils:stream_bytes_range({0, FileSize - 1}, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_one_ranged_body(http_parser:bytes_range(), download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_one_ranged_body({RangeStart, RangeEnd} = Range, #download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        Req0
    ),

    http_streaming_utils:stream_bytes_range(Range, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(<<"">>, fin, Req1),
    Req1.


%% @private
-spec stream_multipart_ranged_body([http_parser:bytes_range()], download_ctx(), cowboy_req:req()) ->
    cowboy_req:req().
stream_multipart_ranged_body(Ranges, #download_ctx{
    file_size = FileSize,
    file_handle = FileHandle,
    on_success_callback = OnSuccessCallback
} = DownloadCtx, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),

    Req1 = cowboy_req:stream_reply(
        ?HTTP_206_PARTIAL_CONTENT,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Req0
    ),

    lists:foreach(fun(Range) ->
        NextPartHead = cow_multipart:first_part(Boundary, [
            {?HDR_CONTENT_TYPE, ContentType},
            {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
        ]),
        cowboy_req:stream_body(NextPartHead, nofin, Req1),

        http_streaming_utils:stream_bytes_range(Range, DownloadCtx, Req1, ?MIN_SEND_RETRY_DELAY)
    end, Ranges),
    execute_on_success_callback(lfm_context:get_guid(FileHandle), OnSuccessCallback),

    cowboy_req:stream_body(cow_multipart:close(Boundary), fin, Req1),
    Req1.


%% @private
-spec execute_on_success_callback(fslogic_worker:file_guid(), OnSuccessCallback :: fun(() -> ok)) -> ok.
execute_on_success_callback(Guid, OnSuccessCallback) ->
    try
        ok = OnSuccessCallback()
    catch Type:Reason ->
        ?warning("Failed to execute file download successfully finished callback for file (~p) "
                 "due to ~p:~p", [Guid, Type, Reason])
    end.
