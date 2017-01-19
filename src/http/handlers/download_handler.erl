%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C): 2016 ACK CYFRONET AGH
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
-behaviour(cowboy_http_handler).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

% Default buffer size used to send file to a client. It is used if env variable
% gui_download_buffer cannot be found.
-define(DEFAULT_DOWNLOAD_BUFFER_SIZE, 4194304). % 4MB

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback.
%% @end
%%--------------------------------------------------------------------
-spec init({TransportName :: atom(), ProtocolName :: http},
    Req :: cowboy_req:req(), Opts :: any()) ->
    {ok, cowboy_req:req(), []}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles an upload request.
%% @end
%%--------------------------------------------------------------------
-spec handle(Req :: cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
handle(Req, State) ->
    {FileId, _} = cowboy_req:binding(id, Req),
    NewReq = handle_http_download(Req, FileId),
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), Req :: cowboy_req:req(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.


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
-spec cowboy_file_stream_fun(FileHandle :: lfm_context:handle(), Size :: integer()) ->
    fun((any(), module()) -> ok).
cowboy_file_stream_fun(FileHandle, Size) ->
    fun(Socket, Transport) ->
        try
            BufSize = get_download_buffer_size(),
            stream_file(Socket, Transport, FileHandle, Size, BufSize)
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
-spec stream_file(Socket :: term(), Transport :: atom(),
    FileHandle :: lfm_context:handle(), Size :: integer(), BufSize :: integer()) -> ok.
stream_file(Socket, Transport, FileHandle, Size, BufSize) ->
    stream_file(Socket, Transport, FileHandle, Size, 0, BufSize).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Function that will be evaluated by cowboy to stream a file to client.
%% NOTE! Filename must be a unicode string (not utf8)
%% @end
%%--------------------------------------------------------------------
-spec stream_file(Socket :: term(), Transport :: atom(),
    FileHandle :: lfm_context:handle(), Size :: integer(),
    Sent :: integer(), BufSize :: integer()) -> ok.
stream_file(_, _, FileHandle, Size, BytesSent, _) when BytesSent >= Size ->
    ok = logical_file_manager:release(FileHandle);
stream_file(Socket, Transport, FileHandle, Size, BytesSent, BufSize) ->
    {ok, NewHandle, BytesRead} = logical_file_manager:read(
        FileHandle, BytesSent, min(Size - BytesSent, BufSize)),
    ok = Transport:send(Socket, BytesRead),
    NewSent = BytesSent + size(BytesRead),
    case size(BytesRead) of
        0 ->
            ok = logical_file_manager:release(FileHandle);
        _ ->
            stream_file(Socket, Transport, NewHandle, Size, NewSent, BufSize)
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
%%    FileNameUrlEncoded = http_utils:url_encode(FileName),
    {Type, Subtype, _} = cow_mimetypes:all(FileName),
    MimeType = <<Type/binary, "/", Subtype/binary>>,
    #{
        <<"content-type">> => MimeType,
        <<"content-disposition">> =>
            <<"attachment; filename=\"", FileName/binary, "\"">>
            %% @todo VFS-2073 - check if needed
%%            "filename*=UTF-8''", FileNameUrlEncoded/binary>>
    }.
