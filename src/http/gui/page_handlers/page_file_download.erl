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
-include("http/rest.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DEFAULT_READ_BLOCK_SIZE, 10485760). % 10 MB

-define(CONN_CLOSE_HEADERS, #{?HDR_CONNECTION => <<"close">>}).

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
        ok ->
            Hostname = oneprovider:get_domain(),
            {ok, Code} = file_download_code:create(SessionId, FileGuid),
            URL = str_utils:format_bin("https://~s~s/~s", [
                Hostname, ?FILE_DOWNLOAD_PATH, Code
            ]),
            {ok, URL};
        {error, ?EACCES} ->
            ?ERROR_FORBIDDEN;
        {error, ?EPERM} ->
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
        {true, SessionId, FileGuid} ->
            OzUrl = oneprovider:get_oz_url(),
            Req2 = gui_cors:allow_origin(OzUrl, Req),
            Req3 = gui_cors:allow_frame_origin(OzUrl, Req2),
            handle_http_download(Req3, SessionId, FileGuid);
        false ->
            send_error_response(?ERROR_BAD_DATA(<<"code">>), Req)
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
                send_error_response(Reason, Req)
            after
                lfm:release(FileHandle)
            end;
        {error, Errno} ->
            send_error_response(?ERROR_POSIX(Errno), Req)
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
    {ok, #file_attr{size = FileSize, name = FileName}} = ?check(lfm:stat(
        SessionId, {guid, FileGuid}
    )),
    ReadBlockSize = get_read_block_size(FileHandle),

    % Reply with attachment headers and a streaming function
    AttachmentHeaders = attachment_headers(FileName),
    Req2 = cowboy_req:stream_reply(?HTTP_200_OK, AttachmentHeaders#{
        ?HDR_CONTENT_LENGTH => integer_to_binary(FileSize)
    }, Req),

    stream_file_content(FileHandle, FileSize, 0, Req2, ReadBlockSize),
    Req2.


%% @private
-spec get_read_block_size(lfm_context:ctx()) -> non_neg_integer().
get_read_block_size(FileHandle) ->
    StorageId = lfm_context:get_storage_id(FileHandle),
    Helper = storage:get_helper(StorageId),
    utils:ensure_defined(helper:get_block_size(Helper), ?DEFAULT_READ_BLOCK_SIZE).


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
        ?HDR_CONTENT_TYPE => MimeType,
        ?HDR_CONTENT_DISPOSITION =>
        <<"attachment; filename=\"", FileName/binary, "\"">>
        %% @todo VFS-2073 - check if needed
        %% "filename*=UTF-8''", FileNameUrlEncoded/binary>>
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Streams given file chunk by chunk recursively.
%% @end
%%--------------------------------------------------------------------
-spec stream_file_content(
    FileHandle :: lfm_context:ctx(),
    FileSize :: file_meta:size(),
    Offset :: non_neg_integer(),
    cowboy_req:req(),
    ReadBlockSize :: non_neg_integer()
) ->
    ok.
stream_file_content(_FileHandle, FileSize, FileSize, Req, _ReadBlockSize) ->
    cowboy_req:stream_body(<<"">>, fin, Req);
stream_file_content(FileHandle, FileSize, Offset, Req, ReadBlockSize) ->
    ToRead = min(FileSize - Offset, ReadBlockSize - Offset rem ReadBlockSize),
    {ok, NewHandle, Data} = lfm:read(FileHandle, Offset, ToRead),

    case byte_size(Data) of
        0 ->
            cowboy_req:stream_body(<<"">>, fin, Req);
        DataSize ->
            cowboy_req:stream_body(Data, nofin, Req),
            stream_file_content(
                NewHandle, FileSize, Offset + DataSize, Req, ReadBlockSize
            )
    end.


%% @private
-spec send_error_response(errors:error(), cowboy_req:req()) -> cowboy_req:req().
send_error_response(Error, Req) ->
    ErrorResp = rest_translator:error_response(Error),

    cowboy_req:reply(
        ErrorResp#rest_resp.code,
        maps:merge(ErrorResp#rest_resp.headers, ?CONN_CLOSE_HEADERS),
        json_utils:encode(ErrorResp#rest_resp.body),
        Req
    ).
