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

-include("http/gui_paths.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/logging.hrl").

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
            handle_http_download(SessionId, FileGuid, Req3)
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
-spec handle_http_download(session:id(), fslogic_worker:file_guid(), cowboy_req:req()) ->
    cowboy_req:req().
handle_http_download(SessionId, FileGuid, Req0) ->
    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{name = FileName} = FileAttrs} ->
            %% @todo VFS-2073 - check if needed
            %% FileNameUrlEncoded = http_utils:url_encode(FileName),
            Req1 = cowboy_req:set_resp_header(
                <<"content-disposition">>,
                <<"attachment; filename=\"", FileName/binary, "\"">>,
                %% @todo VFS-2073 - check if needed
                %% "filename*=UTF-8''", FileNameUrlEncoded/binary>>
                Req0
            ),
            http_download_utils:stream_file(SessionId, FileAttrs, Req1);
        {error, Errno} ->
            http_req_utils:send_error_response(?ERROR_POSIX(Errno), Req0)
    end.
