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


-export([gen_file_download_url/2, handle/2]).


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
-spec gen_file_download_url(session:id(), [fslogic_worker:file_guid()]) ->
    {ok, binary()} | errors:error().
gen_file_download_url(SessionId, FileGuids) ->
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
        {true, SessionId, FileGuids} ->
            handle_http_download(FileDownloadCode, SessionId, FileGuids, Req);
        false ->
            case bulk_download:can_continue(FileDownloadCode) of
                true -> 
                    handle_http_download(FileDownloadCode, <<>>, [], Req);
                false -> 
                    http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req)
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks file permissions and syncs first file block when downloading single 
%% regular file. In case of multi file/directory download access test is not 
%% performed, as inaccessible files will be ignored. Also first block sync is 
%% not needed, because first bytes (first file TAR header) are sent instantly 
%% after streaming started.
%% @end
%%--------------------------------------------------------------------
-spec maybe_sync_first_file_block(session:id(), [file_id:file_guid()]) -> ok.
maybe_sync_first_file_block(SessionId, [FileGuid]) ->
    FileRef = ?FILE_REF(FileGuid),

    case ?check(lfm:stat(SessionId, FileRef)) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} ->
            {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, FileRef, read)),
            ReadBlockSize = http_streamer:get_read_block_size(FileHandle),
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
maybe_sync_first_file_block(_SessionId, _FileGuids) ->
    ok.


-spec handle_http_download(
    file_download_code:code(),
    session:id(),
    [fslogic_worker:file_guid()],
    cowboy_req:req()
) ->
    cowboy_req:req().
 handle_http_download(FileDownloadCode, SessionId, FileGuids, Req) ->
    OzUrl = oneprovider:get_oz_url(),
    Req2 = gui_cors:allow_origin(OzUrl, Req),
    Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),
    FileAttrsList = lists_utils:foldl_while(fun (FileGuid, Acc) ->
        case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of % todo pass option here to follow links?
            {ok, #file_attr{} = FileAttr} -> {cont, [FileAttr | Acc]};
            {error, ?EACCES} -> {cont, Acc};
            {error, ?EPERM} -> {cont, Acc};
            {error, _Errno} = Error -> {halt, Error}
        end
    end, [], FileGuids),

    case FileAttrsList of
        {error, Errno} ->
            http_req:send_error(?ERROR_POSIX(Errno), Req3);
        [#file_attr{name = FileName, type = ?DIRECTORY_TYPE}] ->
            Req4 = set_content_disposition_header(<<(normalize_filename(FileName))/binary, ".tar">>, Req3),
            file_download_utils:download_tarball(
                FileDownloadCode, SessionId, FileAttrsList, Req4
            );
        [#file_attr{name = FileName, type = ?REGULAR_FILE_TYPE} = Attr] ->
            Req4 = set_content_disposition_header(normalize_filename(FileName), Req3),
            file_download_utils:download_single_file(
                SessionId, Attr, fun() -> file_download_code:remove(FileDownloadCode) end, Req4
            );
        _ ->
            Timestamp = integer_to_binary(global_clock:timestamp_seconds()),
            Req4 = set_content_disposition_header(<<"onedata-download-", Timestamp/binary, ".tar">>, Req3),
            file_download_utils:download_tarball(
                FileDownloadCode, SessionId, FileAttrsList, Req4
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