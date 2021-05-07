%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions for handling file download using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(file_download_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    download_single_file/3, download_single_file/4,
    download_tarball/4
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec download_single_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
download_single_file(SessionId, FileAttrs, Req) ->
    download_single_file(SessionId, FileAttrs, fun() -> ok end, Req).


-spec download_single_file(
    session:id(),
    lfm_attrs:file_attributes(),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_file(SessionId, #file_attr{
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

            case lfm:monitored_open(SessionId, ?FILE_REF(FileGuid), read) of
                {ok, FileHandle} ->
                    try
                        {Boundary, Req2} = stream_file_internal(Ranges, FileHandle, FileSize, Req1),
                        execute_on_success_callback(FileGuid, OnSuccessCallback),
                        http_streamer:close_stream(Boundary, Req2),
                        Req2
                    catch Type:Reason ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while processing file (~p) download "
                                          "for user ~p - ~p:~p", [FileGuid, UserId, Type, Reason]),
                        http_req:send_error(Reason, Req1)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req1)
            end
    end.


-spec download_tarball(
    bulk_download:id(),
    session:id(),
    [lfm_attrs:file_attributes()],
    cowboy_req:req()
) ->
    cowboy_req:req().
download_tarball(BulkDownloadId, SessionId, FileAttrsList, Req0) ->
    case http_parser:parse_range_header(Req0, unknown) of
        undefined ->
            stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, Req0);
        [{0, unknown}] -> 
            stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, Req0);
        Range ->
            stream_partial_tarball(BulkDownloadId, Req0, Range)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_content_range_header_value(http_parser:bytes_range(), file_meta:size()) -> binary().
build_content_range_header_value({RangeStart, RangeEnd}, FileSize) ->
    str_utils:format_bin("bytes ~B-~B/~B", [RangeStart, RangeEnd, FileSize]).


%% @private
-spec stream_file_internal(undefined | [http_parser:bytes_range()], lfm:handle(), file_meta:size(), 
    cowboy_req:req()) -> {binary(), cowboy_req:req()}.
stream_file_internal(undefined, FileHandle, FileSize, Req) ->
    stream_whole_file(FileHandle, FileSize, Req);
stream_file_internal([OneRange], FileHandle, FileSize, Req) ->
    stream_one_ranged_body(OneRange, FileHandle, FileSize, Req);
stream_file_internal(Ranges, FileHandle, FileSize, Req) ->
    stream_multipart_ranged_body(Ranges, FileHandle, FileSize, Req).


%% @private
-spec stream_whole_file(lfm:handle(), file_meta:size(), cowboy_req:req()) -> 
    {undefined, cowboy_req:req()}.
stream_whole_file(FileHandle, FileSize, Req0) -> 
    Req1 = http_streamer:init_stream(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),
    StreamingCtx = http_streamer:build_ctx(FileHandle, FileSize),
    http_streamer:stream_bytes_range(StreamingCtx, {0, FileSize - 1}, Req1),
    {undefined, Req1}.


%% @private
-spec stream_one_ranged_body(http_parser:bytes_range(), lfm:handle(), file_meta:size(), cowboy_req:req()) ->
    {undefined, cowboy_req:req()}.
stream_one_ranged_body({RangeStart, RangeEnd} = Range, FileHandle, FileSize, Req0) ->
    Req1 = http_streamer:init_stream(
        ?HTTP_206_PARTIAL_CONTENT,
        #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        Req0
    ),
    StreamingCtx = http_streamer:build_ctx(FileHandle, FileSize),
    http_streamer:stream_bytes_range(StreamingCtx, Range, Req1),
    {undefined, Req1}.


%% @private
-spec stream_multipart_ranged_body([http_parser:bytes_range()], lfm:handle(), file_meta:size(),
    cowboy_req:req()) -> {binary(), cowboy_req:req()}.
stream_multipart_ranged_body(Ranges, FileHandle, FileSize, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),
    
    Req1 = http_streamer:init_stream(
        ?HTTP_206_PARTIAL_CONTENT,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Req0
    ),
    
    StreamingCtx = http_streamer:build_ctx(FileHandle, FileSize),
    lists:foreach(fun(Range) ->
        NextPartHead = cow_multipart:first_part(Boundary, [
            {?HDR_CONTENT_TYPE, ContentType},
            {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
        ]),
        http_streamer:send_data_chunk(NextPartHead, Req1),
        http_streamer:stream_bytes_range(StreamingCtx, Range, Req1)
    end, Ranges),
    
    {Boundary, Req1}.


%% @private
-spec stream_whole_tarball(bulk_download:id(), session:id(), [lfm_attrs:file_attributes()], 
    cowboy_req:req()) -> cowboy_req:req().
stream_whole_tarball(_BulkDownloadId, _SessionId, [], Req0) ->
    % can happen when requested download from the beginning and download 
    % code has expired but bulk download still allowed for resume
    http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req0);
stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, Req0) ->
    Req1 = http_streamer:init_stream(?HTTP_200_OK, Req0),
    ok = bulk_download:run(BulkDownloadId, FileAttrsList, SessionId, Req1),
    http_streamer:close_stream(undefined, Req1),
    Req1.


%% @private
-spec stream_partial_tarball(bulk_download:id(), cowboy_req:req(), [http_parser:bytes_range()] | invalid) -> 
    cowboy_req:req().
stream_partial_tarball(BulkDownloadId, Req0, [{RangeBegin, unknown}]) ->
    case bulk_download:is_offset_allowed(BulkDownloadId, RangeBegin) of
        true ->
            Req1 = http_streamer:init_stream(?HTTP_206_PARTIAL_CONTENT, Req0),
            ok = bulk_download:continue(BulkDownloadId, RangeBegin, Req1),
            http_streamer:close_stream(undefined, Req1),
            Req1;
        false ->
            cowboy_req:stream_reply(?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE => <<"bytes */*">>}, Req0)
    end;
stream_partial_tarball(_BulkDownloadId, Req0, _InvalidRange) ->
    cowboy_req:stream_reply(?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE => <<"bytes */*">>}, Req0).


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
-spec execute_on_success_callback(fslogic_worker:file_guid(), OnSuccessCallback :: fun(() -> ok)) -> ok.
execute_on_success_callback(Guid, OnSuccessCallback) ->
    try
        ok = OnSuccessCallback()
    catch Type:Reason ->
        ?warning("Failed to execute file download successfully finished callback for file (~p) "
                 "due to ~p:~p", [Guid, Type, Reason])
    end.
