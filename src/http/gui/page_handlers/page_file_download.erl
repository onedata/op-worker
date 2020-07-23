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
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

-define(CONN_CLOSE_HEADERS, #{<<"connection">> => <<"close">>}).

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
get_file_download_url(SessionId, FileGuid) ->
    case lfm:check_perms(SessionId, {guid, FileGuid}, read) of
        {ok, true} ->
            Hostname = oneprovider:get_domain(),
            {ok, Code} = file_download_code:create(SessionId, FileGuid),
            URL = str_utils:format_bin("https://~s~s/~s", [
                Hostname, ?FILE_DOWNLOAD_PATH, Code
            ]),
            {ok, URL};
        {ok, false} ->
            ?ERROR_FORBIDDEN;
        Error ->
            Error
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
            cowboy_req:reply(?HTTP_401_UNAUTHORIZED, ?CONN_CLOSE_HEADERS, Req);
        {true, SessionId, FileGuid} ->
            OzUrl = oneprovider:get_oz_url(),
            Req2 = gui_cors:allow_origin(OzUrl, Req),
            Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),
            handle_http_download(Req3, SessionId, FileGuid)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
handle_http_download(Req, SessionId, FileGuid) ->
    case lfm:open(SessionId, {guid, FileGuid}, read) of
        {ok, FileHandle} ->
            try
                stream_file(FileGuid, FileHandle, SessionId, Req)
            catch Type:Reason ->
                {ok, UserId} = session:get_user_id(SessionId),
                ?error_stacktrace("Error while processing file (~p) download "
                                  "for user ~p - ~p:~p", [
                    FileGuid, UserId, Type, Reason
                ]),
                cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, ?CONN_CLOSE_HEADERS, Req)
            after
                lfm:release(FileHandle)
            end;
        {error, _} ->
            cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, ?CONN_CLOSE_HEADERS, Req)
    end.


%% @private
-spec stream_file(
    file_id:file_guid(),
    FileHandle :: lfm_context:ctx(),
    session:id(),
    cowboy_req:req()
) ->
    cowboy_req:req().
stream_file(FileGuid, FileHandle, SessionId, Req) ->
    {ok, #file_attr{size = FileSize, name = FileName}} = lfm:stat(
        SessionId, {guid, FileGuid}
    ),
    ReadBlockSize = file_download_utils:get_read_block_size(FileHandle),

    % Reply with attachment headers and a streaming function
    AttachmentHeaders = attachment_headers(FileName),

    Req2 = cowboy_req:stream_reply(?HTTP_200_OK, AttachmentHeaders#{
        <<"content-length">> => integer_to_binary(FileSize)
    }, Req),
    file_download_utils:stream_range(
        FileHandle, {0, FileSize-1}, Req2, fun(Data) -> Data end, ReadBlockSize
    ),
    cowboy_req:stream_body(<<"">>, fin, Req2),

    Req2.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns attachment headers that will cause web browser to
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
