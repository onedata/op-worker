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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

% Default buffer size used to send file to a client. It is used if env variable
% gui_download_buffer cannot be found.

-define(DOWNLOAD_BUFFER_SIZE, application:get_env(?APP_NAME, gui_download_buffer, 4194304)). % 4MB

-export([handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(new_gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    FileId = cowboy_req:binding(id, Req),
    handle_http_download(Req, FileId).

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
-spec handle_http_download(Req :: cowboy_req:req(),
    FileId :: file_meta:uuid()) -> cowboy_req:req().
handle_http_download(Req, FileId) ->
    case op_gui_session:get(Req) of
        {error, not_found} ->
            cowboy_req:reply(401, #{<<"connection">> => <<"close">>}, Req);
        {ok, Session} ->
            try
                SessionId = case fslogic_uuid:is_share_guid(FileId) of
                    true -> ?GUEST_SESS_ID;
                    false -> Session#document.key
                end,
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
                        {ok, UserId2} = session:get_user_id(Session),
                        ?error_stacktrace("Error while processing file download "
                        "for user ~p - ~p:~p", [UserId2, Type2, Reason2]),
                        logical_file_manager:release(FileHandle), % release if possible
                        cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req)
                end
            catch
                Type:Reason ->
                    {ok, UserId} = session:get_user_id(Session),
                    ?error_stacktrace("Error while processing file download "
                    "for user ~p - ~p:~p", [UserId, Type, Reason]),
                    cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req)
            end
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
