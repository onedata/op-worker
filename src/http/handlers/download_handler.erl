%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module processes file download requests originating from GUI.
%%% @end
%%%-------------------------------------------------------------------
-module(download_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_handler).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

% Default buffer size used to send file to a client. It is used if env variable
% gui_download_buffer cannot be found.
-define(DEFAULT_DOWNLOAD_BUFFER_SIZE, 4194304). % 4MB

%% Cowboy API
-export([init/2]).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning current version of Onezone.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    FileId = cowboy_req:binding(id, Req),
    NewReq = handle_http_download(Req, FileId),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.


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
    % Try to retrieve user's session (no session is also a valid session)
    InitSession =
        try
            gui_ctx:init(Req, false)
        catch _:_ ->
            % Logging is done inside gui_ctx:init
            error
        end,

    case InitSession of
        error ->
            gui_ctx:reply(500, #{}, <<"">>);
        ok ->
            try
                SessionId = case fslogic_uuid:is_share_guid(FileId) of
                    true -> ?GUEST_SESS_ID;
                    false -> gui_session:get_session_id()
                end,
                {ok, FileHandle} = logical_file_manager:open(
                    SessionId, {guid, FileId}, read),
                try
                    {ok, #file_attr{size = Size, name = FileName}} =
                        logical_file_manager:stat(SessionId, {guid, FileId}),
                    StreamFun = cowboy_file_stream_fun(FileHandle, Size),
                    Headers = attachment_headers(FileName),
                    % Reply with attachment headers and a streaming function
                    gui_ctx:reply(200, Headers, {Size, StreamFun})
                catch
                    T2:M2 ->
                        ?error_stacktrace("Error while processing file download "
                        "for user ~p - ~p:~p", [gui_session:get_user_id(), T2, M2]),
                        logical_file_manager:release(FileHandle), % release if possible
                        gui_ctx:reply(500, #{}, <<"">>)
                end
            catch
                T:M ->
                    ?error_stacktrace("Error while processing file download "
                    "for user ~p - ~p:~p", [gui_session:get_user_id(), T, M]),
                    gui_ctx:reply(500, #{}, <<"">>)
            end
    end,
    gui_ctx:finish().


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a cowboy-compliant streaming function, that will be evaluated
%% by cowboy to send data (file content) to receiving socket.
%% @end
%%--------------------------------------------------------------------
-spec cowboy_file_stream_fun(FileHandle :: lfm_context:ctx(), Size :: integer()) ->
    fun((cowboy_req:req()) -> ok).
cowboy_file_stream_fun(FileHandle, Size) ->
    fun(Req) ->
        try
            BufSize = get_download_buffer_size(),
            stream_file(Req, FileHandle, Size, BufSize)
        catch T:M ->
            % Any exceptions that occur during file streaming must be caught
            % here for cowboy to close the connection cleanly.
            ?error_stacktrace("Error while streaming file '~p' - ~p:~p",
                [lfm_context:get_guid(FileHandle), T, M]),
            ok
        end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function that will be evaluated by cowboy to stream a file to client.
%% NOTE! Filename must be a unicode string (not utf8)
%% @end
%%--------------------------------------------------------------------
-spec stream_file(Req :: cowboy_req:req(), FileHandle :: lfm_context:ctx(),
    Size :: integer(), BufSize :: integer()) -> ok.
stream_file(Req, FileHandle, Size, BufSize) ->
    stream_file(Req, FileHandle, Size, 0, BufSize).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function that will be evaluated by cowboy to stream a file to client.
%% NOTE! Filename must be a unicode string (not utf8)
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
%% @doc
%% Returns buffer size for file downloads, as specified in config,
%% or a default value if not found in config.
%% @end
%%--------------------------------------------------------------------
-spec get_download_buffer_size() -> integer().
get_download_buffer_size() ->
    application:get_env(
        ?APP_NAME, gui_download_buffer, ?DEFAULT_DOWNLOAD_BUFFER_SIZE).


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
