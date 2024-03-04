%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when file download page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_file_content_download).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("http/gui_paths.hrl").
-include("http/http_download.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-export([gen_file_download_url/3, handle/2]).

% TODO VFS-11735 Determine highest sync priority
-define(FIRST_FILE_BLOCK_SYNC_PRIORITY, op_worker:get_env(download_first_file_block_sync_priority, 32)).


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
-spec gen_file_download_url(session:id(), [fslogic_worker:file_guid()], boolean()) ->
    {ok, binary()} | errors:error().
gen_file_download_url(SessionId, FileGuids, FollowSymlinks) ->
    try
        maybe_sync_first_file_block(SessionId, FileGuids),

        Hostname = oneprovider:get_domain(),
        {ok, Code} = file_download_code:create(#file_content_download_args{
            session_id = SessionId,
            file_guids = FileGuids,
            follow_symlinks = FollowSymlinks
        }),
        URL = str_utils:format_bin("https://~s~s/~s", [
            Hostname, ?GUI_FILE_CONTENT_DOWNLOAD_PATH, Code
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
        {true, #file_content_download_args{
            session_id = SessionId,
            file_guids = FileGuids,
            follow_symlinks = FollowSymlinks
        }} ->
            handle_http_download(FileDownloadCode, SessionId, FileGuids, FollowSymlinks, Req);

        {true, _} ->
            http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req);

        false ->
            case bulk_download:find_started_for_code(FileDownloadCode) of
                {ok, SessionId} ->
                    % the list of guids and the follow links parameter is not important, 
                    % as it will be overwritten by an existing bulk download instance
                    handle_http_download(FileDownloadCode, SessionId, [], true, Req);
                error ->
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

    case ?lfm_check(lfm:stat(SessionId, FileRef)) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}} ->
            ?lfm_check(lfm:check_perms(SessionId, FileRef, read)),

            SpaceId = file_id:guid_to_space_id(FileGuid),
            SyncBlock = #file_block{
                offset = 0,
                size = min(FileSize, file_content_streamer:get_read_block_size(SpaceId))
            },
            case lfm:sync_block(SessionId, FileRef, SyncBlock, ?FIRST_FILE_BLOCK_SYNC_PRIORITY) of
                {error, ?ENOSPC} ->
                    throw(?ERROR_QUOTA_EXCEEDED);
                Res ->
                    ?lfm_check(Res)
            end;
        _ -> 
            ok
    end;
maybe_sync_first_file_block(_SessionId, _FileGuids) ->
    ok.


%% @private
-spec handle_http_download(
    file_download_code:code(),
    session:id(),
    [fslogic_worker:file_guid()],
    boolean(),
    cowboy_req:req()
) ->
    cowboy_req:req().
handle_http_download(FileDownloadCode, SessionId, FileGuids, FollowSymlinks, InitialReq) ->
    Req = add_headers_regulating_frame_ancestors(SessionId, InitialReq),
    FileAttrsList = lists_utils:foldl_while(fun (FileGuid, Acc) ->
        case lfm:stat(SessionId, ?FILE_REF(FileGuid, false)) of
            {ok, #file_attr{} = FileAttr} -> {cont, [FileAttr | Acc]};
            {error, ?EACCES} -> {cont, Acc};
            {error, ?EPERM} -> {cont, Acc};
            {error, _Errno} = Error -> {halt, Error}
        end
    end, [], FileGuids),
    case {FileAttrsList, FollowSymlinks} of
        {{error, Errno}, _} ->
            http_req:send_error(?ERROR_POSIX(Errno), Req);
        {[#file_attr{type = ?DIRECTORY_TYPE, guid = Guid, name = FileName}], _} ->
            TargetName = case archivisation_tree:uuid_to_archive_id(file_id:guid_to_uuid(Guid)) of
                undefined ->
                    FileName;
                ArchiveId ->
                    archivisation_tree:get_filename_for_download(ArchiveId)
            end,
            file_content_download_utils:download_tarball(
                FileDownloadCode, SessionId, FileAttrsList, <<TargetName/binary, ".tar">>, FollowSymlinks, Req
            );
        {[#file_attr{type = ?REGULAR_FILE_TYPE} = Attr], _} ->
            file_content_download_utils:download_single_file(
                SessionId, Attr, fun() -> file_download_code:remove(FileDownloadCode) end, Req
            );
        {[#file_attr{type = ?SYMLINK_TYPE, guid = Guid, name = SymlinkName}], true} ->
            case lfm:stat(SessionId, ?FILE_REF(Guid, true)) of
                {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
                    file_content_download_utils:download_tarball(
                        FileDownloadCode, SessionId, FileAttrsList, <<SymlinkName/binary, ".tar">>, FollowSymlinks, Req
                    );
                {ok, #file_attr{} = ResolvedAttr} ->
                    file_content_download_utils:download_single_file(
                        SessionId, ResolvedAttr, SymlinkName,
                        fun() -> file_download_code:remove(FileDownloadCode) end,
                        Req
                    );
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req)
            end;
        {[#file_attr{type = ?SYMLINK_TYPE} = Attr], false} ->
            file_content_download_utils:download_single_file(
                SessionId, Attr, fun() -> file_download_code:remove(FileDownloadCode) end, Req
            );
        _ ->
            Timestamp = integer_to_binary(global_clock:timestamp_seconds()),
            TarballName = <<"onedata-download-", Timestamp/binary, ".tar">>,
            file_content_download_utils:download_tarball(
                FileDownloadCode, SessionId, FileAttrsList, TarballName, FollowSymlinks, Req
            )
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add CSP headers depending if the download is in share mode or not.
%% For simplicity, to determine if the download is in share mode, we check
%% the SessionId which comes from a file download code / resumed bulk download task.
%% There is no need to check the Guids to be downloaded (it's not possible for them
%% to be non-share guids, but if that somehow happens, the download will simply fail
%% as the files will not be readable with the guest session).
%% @end
%%--------------------------------------------------------------------
-spec add_headers_regulating_frame_ancestors(session:id(), cowboy_req:req()) -> cowboy_req:req().
add_headers_regulating_frame_ancestors(?GUEST_SESS_ID, Req) ->
    % for public downloads, the page will be embeddable everywhere (no CSP headers)
    Req;
add_headers_regulating_frame_ancestors(_, Req) ->
    % authorized downloads will work only from the Onedata GUI (served from Onezone origin)
    http_download_utils:allow_onezone_as_frame_ancestor(Req).
