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
%% Returns the URL under which given files can be downloaded. The URL contains
%% a one-time download code. When downloading single file performs a permissions 
%% test first and denies requests if inaccessible. In case of multi_file/directory 
%% download no such test is performed - inaccessible files are ignored during streaming.
%% @end
%%--------------------------------------------------------------------
-spec get_file_download_url(session:id(), [fslogic_worker:file_guid()]) ->
    {ok, binary()} | errors:error().
get_file_download_url(SessionId, FileGuids) ->
    try
        maybe_sync_first_file_block(SessionId, FileGuids),
        Hostname = oneprovider:get_domain(),
        {ok, Code} = file_download_code:create(SessionId, FileGuids),
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
        {true, SessionId, FileGuidList} ->
            OzUrl = oneprovider:get_oz_url(),
            Req2 = gui_cors:allow_origin(OzUrl, Req),
            Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),
            handle_http_download(
                SessionId, FileGuidList,
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
%% @doc
%% Checks file permissions and syncs first file block when downloading single 
%% regular file. In case of multi file download access test is not performed, 
%% as inaccessible files will be ignored. Also first block sync is not needed, 
%% because first bytes (gzip header) are sent instantly after streaming started.
%% @end
%%--------------------------------------------------------------------
-spec maybe_sync_first_file_block(session:id(), [fslogic_worker:file_guid()]) -> ok.
maybe_sync_first_file_block(SessionId, [FileGuid]) ->
    case ?check(lfm:stat(SessionId, {guid, FileGuid})) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} ->
            {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, {guid, FileGuid}, read)),
            ReadBlockSize = http_download_utils:get_read_block_size(FileHandle),
            case lfm:read(FileHandle, 0, ReadBlockSize) of
                {error, ?ENOSPC} ->
                    throw(?ERROR_QUOTA_EXCEEDED);
                Res ->
                    ?check(Res)
            end,
            lfm:monitored_release(FileHandle),
            ok;
        _ -> 
            ok
    end;
maybe_sync_first_file_block(_SessionId, _FileGuidList) ->
    ok.


-spec handle_http_download(
    session:id(),
    [fslogic_worker:file_guid()],
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
handle_http_download(SessionId, FileGuidList, OnSuccessCallback, Req0) ->
    FileAttrsList = lists_utils:foldl_while(
        fun (FileGuid, Acc) ->
                case lfm:stat(SessionId, {guid, FileGuid}) of
                    {ok, #file_attr{} = FileAttr} -> {cont, [FileAttr | Acc]};
                    {error, ?EACCES} -> {cont, Acc};
                    {error, _Errno} = Error -> {halt, Error}
                end
    end, [], FileGuidList),
    case FileAttrsList of
        {error, Errno} ->
            http_req:send_error(?ERROR_POSIX(Errno), Req0);
        [#file_attr{name = FileName, type = ?REGULAR_FILE_TYPE} = Attr] ->
            Req1 = set_content_disposition_header(normalize_filename(FileName), Req0),
            http_download_utils:stream_file(
                SessionId, Attr, OnSuccessCallback, Req1
            );
        [#file_attr{name = FileName, type = ?DIRECTORY_TYPE}] ->
            Req1 = set_content_disposition_header(<<(normalize_filename(FileName))/binary, ".tar.gz">>, Req0),
            http_download_utils:stream_tarball(
                SessionId, FileAttrsList, OnSuccessCallback, Req1
            );
        _ ->
            Timestamp = integer_to_binary(global_clock:timestamp_seconds()),
            Req1 = set_content_disposition_header(<<"onedata-download-", Timestamp/binary, ".tar.gz">>, Req0),
            http_download_utils:stream_tarball(
                SessionId, FileAttrsList, OnSuccessCallback, Req1
            )
    end.


%% @private
-spec set_content_disposition_header(file_meta:name(), cowboy_req:req()) -> cowboy_req:req().
set_content_disposition_header(Filename, Req) ->
    %% @todo VFS-2073 - check if needed
    %% FileNameUrlEncoded = http_utils:url_encode(FileName),
    cowboy_req:set_resp_header(
        <<"content-disposition">>,
        <<"attachment; filename=\"", Filename/binary, "\"">>,
        %% @todo VFS-2073 - check if needed
        %% "filename*=UTF-8''", FileNameUrlEncoded/binary>>
        Req
    ).


%% @private
-spec normalize_filename(file_meta:name()) -> file_meta:name().
normalize_filename(Filename) ->
    case re:run(Filename, <<"^ *$">>, [{capture, none}]) of
        match -> <<"_">>;
        nomatch -> Filename
    end.