%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when file download page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_file_download).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("global_definitions.hrl").
-include("http/gui_paths.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

% Default buffer size used to send file to a client. It is used if env variable
% gui_download_buffer cannot be found.
-define(DOWNLOAD_BUFFER_SIZE, application:get_env(?APP_NAME, gui_download_buffer, 4194304)). % 4MB

-export([get_file_download_url/2, handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the URL under which given file can be downloaded. The URL contains
%% a one-time download code. Performs a permissions test first and denies
%% requests for inaccessible files.
%% @end
%%--------------------------------------------------------------------
-spec get_file_download_url(session:id(), fslogic_worker:file_guid()) ->
    {ok, binary()} | {error, term()}.
get_file_download_url(SessionId, FileId) ->
    case logical_file_manager:check_perms(SessionId, {guid, FileId}, read) of
        {ok, true} ->
            Hostname = op_gui_session:get_requested_host(),
            {ok, Code} = file_download_code:create(SessionId, FileId),
            URL = str_utils:format_bin("https://~s~s/~s", [
                Hostname, ?FILE_DOWNLOAD_PATH, Code
            ]),
            {ok, URL};
        {ok, false} ->
            ?ERROR_FORBIDDEN;
        _ ->
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    FileDownloadCode = cowboy_req:binding(code, Req),
    case file_download_code:consume(FileDownloadCode) of
        false ->
            cowboy_req:reply(401, #{<<"connection">> => <<"close">>}, Req);
        {true, SessionId, FileId} ->
            Req2 = gui_cors:allow_origin(oneprovider:get_oz_url(), Req),
            Req3 = gui_cors:allow_frame_origin(oneprovider:get_oz_url(), Req2),
            handle_http_download(Req3, SessionId, FileId)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts the validity of multipart POST request and proceeds with
%% parsing or returns an error. Returns list of parsed filed values and
%% file body.
%% @end
%%--------------------------------------------------------------------
-spec handle_http_download(cowboy_req:req(), session:id(), fslogic_worker:file_guid()) ->
    cowboy_req:req().
handle_http_download(Req, SessionId, FileId) ->
    try
        {ok, FileHandle} = logical_file_manager:open(
            SessionId, {guid, FileId}, read),
        try
            {ok, #file_attr{size = Size, name = FileName}} =
                logical_file_manager:stat(SessionId, {guid, FileId}),
            Headers = attachment_headers(FileName),
            % Reply with attachment headers and a streaming function
            Req2 = cowboy_req:stream_reply(200, Headers#{
                <<"content-length">> => integer_to_binary(Size)
            }, Req),
            stream_file(Req2, FileHandle, Size)
        catch
            Type2:Reason2 ->
                {ok, UserId2} = session:get_user_id(SessionId),
                ?error_stacktrace("Error while processing file download "
                "for user ~p - ~p:~p", [UserId2, Type2, Reason2]),
                logical_file_manager:release(FileHandle), % release if possible
                cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req)
        end
    catch
        Type:Reason ->
            {ok, UserId} = session:get_user_id(SessionId),
            ?error_stacktrace("Error while processing file download "
            "for user ~p - ~p:~p", [UserId, Type, Reason]),
            cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams given file chunk by chunk.
%% @end
%%--------------------------------------------------------------------
-spec stream_file(cowboy_req:req(), FileHandle :: lfm_context:ctx(), Size :: integer()) ->
    cowboy_req:req().
stream_file(Req, FileHandle, Size) ->
    try
        stream_file(Req, FileHandle, Size, ?DOWNLOAD_BUFFER_SIZE)
    catch Type:Message ->
        % Any exceptions that occur during file streaming must be caught
        % here for cowboy to close the connection cleanly.
        ?error_stacktrace("Error while streaming file '~p' - ~p:~p",
            [lfm_context:get_guid(FileHandle), Type, Message]),
        ok
    end,
    Req.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams given file chunk by chunk using provided buffer size.
%% @end
%%--------------------------------------------------------------------
-spec stream_file(Req :: cowboy_req:req(), FileHandle :: lfm_context:ctx(),
    Size :: integer(), BufSize :: integer()) -> ok.
stream_file(Req, FileHandle, Size, BufSize) ->
    stream_file(Req, FileHandle, Size, 0, BufSize).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams given file chunk by chunk recursively.
%% @end
%%--------------------------------------------------------------------
-spec stream_file(Req :: cowboy_req:req(), FileHandle :: lfm_context:ctx(),
    Size :: integer(), Sent :: integer(), BufSize :: integer()) -> ok.
stream_file(Req, FileHandle, Size, BytesSent, _) when BytesSent >= Size ->
    cowboy_req:stream_body(<<"">>, fin, Req),
    ok = logical_file_manager:release(FileHandle);
stream_file(Req, FileHandle, Size, BytesSent, BufSize) ->
    {ok, NewHandle, BytesRead} = logical_file_manager:read(
        FileHandle, BytesSent, min(Size - BytesSent, BufSize)),
    NewSent = BytesSent + size(BytesRead),
    case size(BytesRead) of
        0 ->
            cowboy_req:stream_body(<<"">>, fin, Req),
            ok = logical_file_manager:release(FileHandle);
        _ ->
            cowboy_req:stream_body(BytesRead, nofin, Req),
            stream_file(Req, NewHandle, Size, NewSent, BufSize)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc Returns attachment headers that will cause web browser to
%% interpret received data as attachment (and save it to disk).
%% Proper filename is set, both in utf8 encoding and legacy for older browsers,
%% based on given filepath or filename.
%% @end
%%--------------------------------------------------------------------
-spec attachment_headers(FileName :: file_meta:name()) -> http_client:headers().
attachment_headers(FileName) ->
    %% @todo VFS-2073 - check if needed
    %% FileNameUrlEncoded = http_utils:url_encode(FileName),
    {Type, Subtype, _} = cow_mimetypes:all(FileName),
    MimeType = <<Type/binary, "/", Subtype/binary>>,
    #{
        <<"content-type">> => MimeType,
        <<"content-disposition">> =>
        <<"attachment; filename=\"", FileName/binary, "\"">>
        %% @todo VFS-2073 - check if needed
        %% "filename*=UTF-8''", FileNameUrlEncoded/binary>>
    }.
