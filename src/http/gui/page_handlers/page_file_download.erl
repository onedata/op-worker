%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018-2020 ACK CYFRONET AGH
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
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").


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
    {ok, binary()} | errors:error().
get_file_download_url(SessionId, FileGuid) ->
    try
        maybe_sync_first_file_block(SessionId, FileGuid),
        Hostname = oneprovider:get_domain(),
        {ok, Code} = file_download_code:create(SessionId, FileGuid),
        URL = str_utils:format_bin("https://~s~s/~s", [
            Hostname, ?FILE_DOWNLOAD_PATH, Code
        ]),
        {ok, URL}
    catch
        throw:?ERROR_POSIX(Errno) when Errno == ?EACCES; Errno == ?EPERM ->
            ?ERROR_FORBIDDEN;
        throw:Error ->
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
    case file_download_code:verify(FileDownloadCode) of
        {true, SessionId, FileGuid} ->
            OzUrl = oneprovider:get_oz_url(),
            Req2 = gui_cors:allow_origin(OzUrl, Req),
            Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),
            handle_http_download(
                SessionId, FileGuid,
                fun() -> file_download_code:remove(FileDownloadCode) end,
                Req3
            );
        false ->
            http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec maybe_sync_first_file_block(session:id(), file_id:file_guid()) -> ok.
maybe_sync_first_file_block(SessionId, FileGuid) ->
    {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, {guid, FileGuid}, read)),
    ReadBlockSize = http_download_utils:get_read_block_size(FileHandle),
    case lfm:read(FileHandle, 0, ReadBlockSize) of
        {error, ?ENOSPC} -> 
            % Translate POSIX error to something better understandable by user.
            throw(?ERROR_QUOTA_EXCEEDED);
        Res ->
            ?check(Res)
    end,
    lfm:monitored_release(FileHandle),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts the validity of multipart POST request and proceeds with
%% parsing or returns an error. Returns list of parsed filed values and
%% file body.
%% @end
%%--------------------------------------------------------------------
-spec handle_http_download(
    session:id(),
    fslogic_worker:file_guid(),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
handle_http_download(SessionId, FileGuid, OnSuccessCallback, Req0) ->
    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, FileAttrs} ->
            http_download_utils:stream_file(SessionId, FileAttrs, OnSuccessCallback, Req0);
        {error, Errno} ->
            http_req:send_error(?ERROR_POSIX(Errno), Req0)
    end.
