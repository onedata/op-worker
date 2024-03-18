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
-module(file_content_download_utils).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    download_single_file/3, download_single_file/4, download_single_file/5,
    download_tarball/6
]).

-type on_success_callback() :: fun(() -> ok).

-export_type([on_success_callback/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec download_single_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
download_single_file(SessionId, FileAttrs, Req) ->
    download_single_file(SessionId, FileAttrs, fun() -> ok end, Req).


-spec download_single_file(session:id(), lfm_attrs:file_attributes(), on_success_callback(), cowboy_req:req()) ->
    cowboy_req:req().
download_single_file(SessionId, FileAttrs, OnSuccessCallback, Req) ->
    download_single_file(SessionId, FileAttrs, FileAttrs#file_attr.name, OnSuccessCallback, Req).


-spec download_single_file(
    session:id(),
    lfm_attrs:file_attributes(),
    file_meta:name(),
    on_success_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_file(SessionId, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttr, FileName, OnSuccessCallback, Req0) ->
    download_single_regular_file(SessionId, FileAttr, FileName, OnSuccessCallback, Req0);
download_single_file(SessionId, #file_attr{type = ?SYMLINK_TYPE} = FileAttr, FileName, OnSuccessCallback, Req0) ->
    download_single_symlink(SessionId, FileAttr, FileName, OnSuccessCallback, Req0).


-spec download_tarball(
    bulk_download:id(),
    session:id(),
    [lfm_attrs:file_attributes()],
    file_meta:name(),
    boolean(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_tarball(BulkDownloadId, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req0) ->
    case http_parser:parse_range_header(Req0, unknown) of
        undefined ->
            stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req0);
        [{0, unknown}] ->
            stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req0);
        Range ->
            stream_partial_tarball(BulkDownloadId, TarballName, Range, Req0)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec download_single_regular_file(
    session:id(),
    lfm_attrs:file_attributes(),
    file_meta:name(),
    on_success_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_regular_file(SessionId, #file_attr{
    guid = FileGuid,
    size = FileSize
}, FileName, OnSuccessCallback, Req0) ->
    case http_parser:parse_range_header(Req0, FileSize) of
        invalid ->
            cowboy_req:reply(
                ?HTTP_416_RANGE_NOT_SATISFIABLE,
                #{?HDR_CONTENT_RANGE => str_utils:format_bin("bytes */~B", [FileSize])},
                Req0
            );
        Ranges ->
            case lfm:monitored_open(SessionId, ?FILE_REF(FileGuid), read) of
                {ok, FileHandle} ->
                    try
                        Req1 = ensure_content_type_header_set(FileName, Req0),
                        Req2 = http_download_utils:set_content_disposition_header(Req1, FileName),
                        {Boundary, Req3} = stream_file_internal(Ranges, FileHandle, FileSize, Req2),
                        execute_on_success_callback(FileGuid, OnSuccessCallback),
                        file_content_streamer:close_stream(Boundary, Req3),
                        Req3
                    catch Class:Reason:Stacktrace ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace(
                            "Error while processing file (~s) download for user ~s~nError was: ~w:~p",
                            [FileGuid, UserId, Class, Reason],
                            Stacktrace
                        ),
                        http_req:send_error(Reason, Req0)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req0)
            end
    end.


%% @private
-spec download_single_symlink(
    session:id(),
    lfm_attrs:file_attributes(),
    file_meta:name(),
    on_success_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_symlink(SessionId, #file_attr{guid = Guid}, FileName, OnSuccessCallback, Req0) ->
    case lfm:read_symlink(SessionId, ?FILE_REF(Guid, false)) of
        {ok, LinkPath} ->
            Req1 = http_download_utils:set_content_disposition_header(Req0, FileName),
            Req2 = file_content_streamer:init_stream(
                ?HTTP_200_OK,
                #{?HDR_CONTENT_LENGTH => integer_to_binary(byte_size(LinkPath))},
                Req1
            ),
            file_content_streamer:send_data_chunk(LinkPath, Req2),
            execute_on_success_callback(Guid, OnSuccessCallback),
            file_content_streamer:close_stream(undefined, Req2),
            Req2;
        {error, Errno} ->
            http_req:send_error(?ERROR_POSIX(Errno), Req0)
    end.


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
    Req1 = file_content_streamer:init_stream(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        Req0
    ),
    StreamingCtx = file_content_streamer:build_ctx(FileHandle, FileSize),
    file_content_streamer:stream_bytes_range(StreamingCtx, {0, FileSize - 1}, Req1),
    {undefined, Req1}.


%% @private
-spec stream_one_ranged_body(http_parser:bytes_range(), lfm:handle(), file_meta:size(), cowboy_req:req()) ->
    {undefined, cowboy_req:req()}.
stream_one_ranged_body({RangeStart, RangeEnd} = Range, FileHandle, FileSize, Req0) ->
    Req1 = file_content_streamer:init_stream(
        ?HTTP_206_PARTIAL_CONTENT,
        #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        Req0
    ),
    StreamingCtx = file_content_streamer:build_ctx(FileHandle, FileSize),
    file_content_streamer:stream_bytes_range(StreamingCtx, Range, Req1),
    {undefined, Req1}.


%% @private
-spec stream_multipart_ranged_body([http_parser:bytes_range()], lfm:handle(), file_meta:size(),
    cowboy_req:req()) -> {binary(), cowboy_req:req()}.
stream_multipart_ranged_body(Ranges, FileHandle, FileSize, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),

    Req1 = file_content_streamer:init_stream(
        ?HTTP_206_PARTIAL_CONTENT,
        #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Req0
    ),

    StreamingCtx = file_content_streamer:build_ctx(FileHandle, FileSize),
    lists:foreach(fun(Range) ->
        NextPartHead = cow_multipart:first_part(Boundary, [
            {?HDR_CONTENT_TYPE, ContentType},
            {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
        ]),
        file_content_streamer:send_data_chunk(NextPartHead, Req1),
        file_content_streamer:stream_bytes_range(StreamingCtx, Range, Req1)
    end, Ranges),

    {Boundary, Req1}.


%% @private
-spec stream_whole_tarball(
    bulk_download:id(),
    session:id(),
    [lfm_attrs:file_attributes()],
    file_meta:name(),
    boolean(),
    cowboy_req:req()
) -> cowboy_req:req().
stream_whole_tarball(_BulkDownloadId, _SessionId, [], _TarballName, _FollowSymlinks, Req0) ->
    % can happen when requested download from the beginning and download 
    % code has expired but bulk download still allowed for resume
    http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req0);

stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req0) ->
    Req1 = http_download_utils:set_content_disposition_header(Req0, TarballName),
    Req2 = file_content_streamer:init_stream(?HTTP_200_OK, Req1),
    ok = bulk_download:run(BulkDownloadId, FileAttrsList, SessionId, FollowSymlinks, Req2),
    file_content_streamer:close_stream(undefined, Req2),
    Req2.


%% @private
-spec stream_partial_tarball(
    bulk_download:id(),
    file_meta:name(),
    [http_parser:bytes_range()] | invalid, cowboy_req:req()
) ->
    cowboy_req:req().
stream_partial_tarball(BulkDownloadId, TarballName, [{RangeBegin, unknown}], Req0) ->
    case bulk_download:is_offset_allowed(BulkDownloadId, RangeBegin) of
        true ->
            Req1 = http_download_utils:set_content_disposition_header(Req0, TarballName),
            Req2 = file_content_streamer:init_stream(?HTTP_206_PARTIAL_CONTENT, Req1),
            ok = bulk_download:continue(BulkDownloadId, RangeBegin, Req2),
            file_content_streamer:close_stream(undefined, Req2),
            Req2;
        false ->
            cowboy_req:stream_reply(?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE => <<"bytes */*">>}, Req0)
    end;
stream_partial_tarball(_BulkDownloadId, _TarballName, _InvalidRange, Req0) ->
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
-spec execute_on_success_callback(fslogic_worker:file_guid(), on_success_callback()) -> ok.
execute_on_success_callback(Guid, OnSuccessCallback) ->
    try
        ok = OnSuccessCallback()
    catch Type:Reason ->
        ?warning("Failed to execute file download successfully finished callback for file (~p) "
                 "due to ~p:~p", [Guid, Type, Reason])
    end.
