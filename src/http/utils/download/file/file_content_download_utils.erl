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
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    download_single_file/3, download_single_file/5, download_single_file/6,
    provide_single_file_download_headers/3,
    download_tarball/6
]).

-type on_started_callback() :: fun(() -> ok).
-type on_finished_callback() :: fun((ok | {error, errors:error()}) -> ok).

-export_type([on_started_callback/0, on_finished_callback/0]).

%% When streaming a file download, if a helper process fails during reading, 
%% we cannot simply send an error response, as HTTP does not allow it once 
%% the stream has begun. Therefore, we attempt to read at least the first 
%% chunk of the file before starting the HTTP stream. If the read is successful, 
%% we initiate the stream and send this first chunk.
%% This record stores all information needed to initiate the stream. 
%% See also: build_streaming_send_fun.
-record(initial_stream_state, {
    status :: cowboy:http_status(),
    headers :: cowboy:http_headers(),
    initial_chunk = undefined :: undefined | iodata(),
    req :: cowboy_req:req()
}).
-type initial_stream_state() :: #initial_stream_state{}.


%%%===================================================================
%%% API
%%%===================================================================


%% REST/CDMI entry point -- no streaming lifecycle tracking (callers see the HTTP
%% status directly).
-spec download_single_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
download_single_file(SessionId, FileAttrs, Req) ->
    NoopStarted = fun() -> ok end,
    NoopFinished = fun(_) -> ok end,
    download_single_file(SessionId, FileAttrs, FileAttrs#file_attr.name, NoopStarted, NoopFinished, Req).


%% GUI entry point that reuses the file's own name.
-spec download_single_file(
    session:id(),
    lfm_attrs:file_attributes(),
    on_started_callback(),
    on_finished_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_file(SessionId, FileAttrs, OnStartedCallback, OnFinishedCallback, Req) ->
    download_single_file(SessionId, FileAttrs, FileAttrs#file_attr.name, OnStartedCallback, OnFinishedCallback, Req).


-spec download_single_file(
    session:id(),
    lfm_attrs:file_attributes(),
    file_meta:name(),
    on_started_callback(),
    on_finished_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_file(SessionId, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttr, FileName, OnStartedCallback, OnFinishedCallback, Req0) ->
    download_single_regular_file(SessionId, FileAttr, FileName, OnStartedCallback, OnFinishedCallback, Req0);
download_single_file(SessionId, #file_attr{type = ?SYMLINK_TYPE} = FileAttr, FileName, OnStartedCallback, OnFinishedCallback, Req0) ->
    download_single_symlink(SessionId, FileAttr, FileName, OnStartedCallback, OnFinishedCallback, Req0).


-spec provide_single_file_download_headers(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
provide_single_file_download_headers(_SessionId, #file_attr{
    type = ?REGULAR_FILE_TYPE,
    name = FileName,
    size = FileSize
}, Req0) ->
    reply_with_download_headers(FileName, FileSize, Req0);
provide_single_file_download_headers(SessionId, #file_attr{
    type = ?SYMLINK_TYPE,
    guid = Guid,
    name = FileName
}, Req0) ->
    case lfm:read_symlink(SessionId, ?FILE_REF(Guid, false)) of
        {ok, LinkPath} ->
            reply_with_download_headers(FileName, byte_size(LinkPath), Req0);
        {error, Errno} ->
            http_req:send_error(?ERR_POSIX(?err_ctx(), Errno), Req0)
    end.


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
    on_started_callback(),
    on_finished_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_regular_file(SessionId, #file_attr{
    guid = FileGuid,
    size = FileSize
}, FileName, OnStartedCallback, OnFinishedCallback, Req0) ->
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
                        Req1 = http_download_utils:set_file_download_headers(Req0, FileName),
                        {Boundary, Req2} = stream_file_internal(Ranges, FileHandle, FileSize, OnStartedCallback, Req1),
                        ?catch_exceptions(OnFinishedCallback(ok)),
                        file_content_streamer:close_stream(Boundary, Req2),
                        Req2
                    catch Class:Reason:Stacktrace ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        Error = ?examine_exception(
                            "Error while processing file (~ts) download for user ~ts",
                            [FileGuid, UserId],
                            Class, Reason, Stacktrace
                        ),
                        ?catch_exceptions(OnFinishedCallback(Error)),
                        http_req:send_error(Error, Req0)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    Error = ?ERR_POSIX(?err_ctx(), Errno),
                    ?catch_exceptions(OnFinishedCallback(Error)),
                    http_req:send_error(Error, Req0)
            end
    end.


%% @private
-spec download_single_symlink(
    session:id(),
    lfm_attrs:file_attributes(),
    file_meta:name(),
    on_started_callback(),
    on_finished_callback(),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_single_symlink(SessionId, #file_attr{guid = Guid}, FileName, OnStartedCallback, OnFinishedCallback, Req0) ->
    case lfm:read_symlink(SessionId, ?FILE_REF(Guid, false)) of
        {ok, LinkPath} ->
            Req1 = http_download_utils:set_file_download_headers(Req0, FileName),
            Req2 = file_content_streamer:init_stream(
                ?HTTP_200_OK,
                #{?HDR_CONTENT_LENGTH => integer_to_binary(byte_size(LinkPath))},
                Req1
            ),
            file_content_streamer:send_data_chunk(LinkPath, Req2),
            %% The single chunk has been sent at this point -- treat it as `started`.
            ?catch_exceptions(OnStartedCallback()),
            ?catch_exceptions(OnFinishedCallback(ok)),
            file_content_streamer:close_stream(undefined, Req2),
            Req2;
        {error, Errno} ->
            Error = ?ERR_POSIX(?err_ctx(), Errno),
            ?catch_exceptions(OnFinishedCallback(Error)),
            http_req:send_error(Error, Req0)
    end.


%% @private
%% NOTE: only ever called while handling an HTTP HEAD request. The content-length
%% header cannot simply be set explicitly and replied with an empty body, because
%% cowboy_req:reply/4 unconditionally overwrites content-length with the size of the
%% given body (0 for an empty body). Passing a {sendfile, 0, FileSize, _} body instead
%% makes cowboy derive content-length from FileSize; for a HEAD request cowboy then
%% strips the body before ever touching the file, so the (empty) path is never opened
%% and no data is read (see cowboy_req:reply/4 and cowboy_req:do_reply/4).
-spec reply_with_download_headers(file_meta:name(), file_meta:size(), cowboy_req:req()) ->
    cowboy_req:req().
reply_with_download_headers(FileName, FileSize, Req0) ->
    Req1 = http_download_utils:set_file_download_headers(Req0, FileName),
    cowboy_req:reply(
        ?HTTP_200_OK,
        #{?HDR_ACCEPT_RANGES => <<"bytes">>},
        {sendfile, 0, FileSize, <<>>},
        Req1
    ).


%% @private
-spec build_content_range_header_value(http_parser:bytes_range(), file_meta:size()) -> binary().
build_content_range_header_value({RangeStart, RangeEnd}, FileSize) ->
    str_utils:format_bin("bytes ~B-~B/~B", [RangeStart, RangeEnd, FileSize]).


%% @private
-spec stream_file_internal(undefined | [http_parser:bytes_range()], lfm:handle(), file_meta:size(),
    on_started_callback(), cowboy_req:req()) -> {binary(), cowboy_req:req()}.
stream_file_internal(undefined, FileHandle, FileSize, OnStartedCallback, Req) ->
    stream_whole_file(FileHandle, FileSize, OnStartedCallback, Req);
stream_file_internal([OneRange], FileHandle, FileSize, OnStartedCallback, Req) ->
    stream_one_ranged_body(OneRange, FileHandle, FileSize, OnStartedCallback, Req);
stream_file_internal(Ranges, FileHandle, FileSize, OnStartedCallback, Req) ->
    stream_multipart_ranged_body(Ranges, FileHandle, FileSize, OnStartedCallback, Req).


%% @private
-spec stream_whole_file(lfm:handle(), file_meta:size(), on_started_callback(), cowboy_req:req()) ->
    {undefined, cowboy_req:req()}.
stream_whole_file(FileHandle, FileSize, OnStartedCallback, Req0) ->
    SendState0 = #initial_stream_state{
        status = ?HTTP_200_OK,
        headers = #{?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)},
        req = Req0
    },
    StreamingCtx0 = file_content_streamer:build_ctx(FileHandle, FileSize),
    StreamingCtx1 = file_content_streamer:set_send_fun(StreamingCtx0, build_streaming_send_fun(OnStartedCallback)),
    SendState1 = file_content_streamer:stream_bytes_range(StreamingCtx1, {0, FileSize - 1}, SendState0),
    {undefined, ensure_stream_initiated(SendState1, OnStartedCallback)}.


%% @private
-spec stream_one_ranged_body(
    http_parser:bytes_range(), lfm:handle(), file_meta:size(), on_started_callback(), cowboy_req:req()
) ->
    {undefined, cowboy_req:req()}.
stream_one_ranged_body({RangeStart, RangeEnd} = Range, FileHandle, FileSize, OnStartedCallback, Req0) ->
    SendState0 = #initial_stream_state{
        status = ?HTTP_206_PARTIAL_CONTENT,
        headers = #{
            ?HDR_CONTENT_LENGTH => integer_to_binary(RangeEnd - RangeStart + 1),
            ?HDR_CONTENT_RANGE => build_content_range_header_value(Range, FileSize)
        },
        req = Req0
    },
    StreamingCtx0 = file_content_streamer:build_ctx(FileHandle, FileSize),
    StreamingCtx1 = file_content_streamer:set_send_fun(StreamingCtx0, build_streaming_send_fun(OnStartedCallback)),
    SendState1 = file_content_streamer:stream_bytes_range(StreamingCtx1, Range, SendState0),
    {undefined, ensure_stream_initiated(SendState1, OnStartedCallback)}.


%% @private
-spec stream_multipart_ranged_body(
    [http_parser:bytes_range()], lfm:handle(), file_meta:size(), on_started_callback(), cowboy_req:req()
) ->
    {binary(), cowboy_req:req()}.
stream_multipart_ranged_body(Ranges, FileHandle, FileSize, OnStartedCallback, Req0) ->
    Boundary = cow_multipart:boundary(),
    ContentType = cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req0),
    BuildNextPartHead = fun(Range) ->
        cow_multipart:first_part(Boundary, [
            {?HDR_CONTENT_TYPE, ContentType},
            {?HDR_CONTENT_RANGE, build_content_range_header_value(Range, FileSize)}
        ])
    end,

    StreamingCtx0 = file_content_streamer:build_ctx(FileHandle, FileSize),
    StreamingCtx1 = file_content_streamer:set_send_fun(StreamingCtx0, build_streaming_send_fun(OnStartedCallback)),

    FinalSendState = lists:foldl(fun
        (FirstRange, undefined) ->
            file_content_streamer:stream_bytes_range(StreamingCtx1, FirstRange, #initial_stream_state{
                status = ?HTTP_206_PARTIAL_CONTENT,
                headers = #{?HDR_CONTENT_TYPE => <<"multipart/byteranges; boundary=", Boundary/binary>>},
                initial_chunk = BuildNextPartHead(FirstRange),
                req = Req0
            });
        (NextRange, ReqAcc0) ->
            {_, ReqAcc1} = file_content_streamer:send_data_chunk(BuildNextPartHead(NextRange), ReqAcc0),
            file_content_streamer:stream_bytes_range(StreamingCtx1, NextRange, ReqAcc1)
    end, undefined, Ranges),

    {Boundary, ensure_stream_initiated(FinalSendState, OnStartedCallback)}.


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
    http_req:send_error(?ERR_BAD_VALUE_ID_NOT_FOUND(?err_ctx(), <<"code">>), Req0);

stream_whole_tarball(BulkDownloadId, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req0) ->
    Req1 = http_download_utils:set_file_download_headers(Req0, TarballName),
    Req2 = file_content_streamer:init_stream(?HTTP_200_OK, Req1),
    file_download_code:mark_started(BulkDownloadId),
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
            Req1 = http_download_utils:set_file_download_headers(Req0, TarballName),
            Req2 = file_content_streamer:init_stream(?HTTP_206_PARTIAL_CONTENT, Req1),
            file_download_code:mark_started(BulkDownloadId),
            ok = bulk_download:continue(BulkDownloadId, RangeBegin, Req2),
            file_content_streamer:close_stream(undefined, Req2),
            Req2;
        false ->
            cowboy_req:stream_reply(?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE => <<"bytes */*">>}, Req0)
    end;
stream_partial_tarball(_BulkDownloadId, _TarballName, _InvalidRange, Req0) ->
    cowboy_req:stream_reply(?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE => <<"bytes */*">>}, Req0).


%% @private
%% The `OnStartedCallback` is invoked exactly once -- after the first chunk has
%% been handed to cowboy. This is the moment HTTP response headers are committed
%% to the wire (`init_stream` is called by the first branch).
-spec build_streaming_send_fun(on_started_callback()) -> file_content_streamer:send_fun().
build_streaming_send_fun(OnStartedCallback) ->
    fun
        (DataChunk, SendState = #initial_stream_state{}, MaxReadBlocksCount, SendRetryDelay) ->
            Req = init_stream(SendState),
            Data = case SendState#initial_stream_state.initial_chunk of
                undefined -> DataChunk;
                InitialData -> [InitialData, DataChunk]
            end,
            Result = http_download_utils:send_data_chunk(
                Data, Req, MaxReadBlocksCount, SendRetryDelay
            ),
            ?catch_exceptions(OnStartedCallback()),
            Result;
        (DataChunk, Req, MaxReadBlocksCount, SendRetryDelay) ->
            http_download_utils:send_data_chunk(
                DataChunk, Req, MaxReadBlocksCount, SendRetryDelay
            )
    end.


%% NOTE: handle edge case when stream have been not initiated by send_fun (see 'build_streaming_send_fun')
%% (e.g. streaming empty file)
%% @private
-spec ensure_stream_initiated(initial_stream_state() | cowboy_req:req(), on_started_callback()) ->
    cowboy_req:req().
ensure_stream_initiated(SendState = #initial_stream_state{}, OnStartedCallback) ->
    ?catch_exceptions(OnStartedCallback()),
    init_stream(SendState);
ensure_stream_initiated(Req, _OnStartedCallback) ->
    Req.


%% @private
-spec init_stream(initial_stream_state()) -> cowboy_req:req().
init_stream(#initial_stream_state{status = Status, headers = Headers, req = Req}) ->
    file_content_streamer:init_stream(Status, Headers, Req).
