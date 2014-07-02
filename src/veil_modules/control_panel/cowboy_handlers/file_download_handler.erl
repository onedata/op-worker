%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles file download requests, both for shared files and
%% user content.
%% @end
%% ===================================================================

-module(file_download_handler).
-behaviour(cowboy_http_handler).

-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("err.hrl").

% Buffer size used to send file to a client. Override with control_panel_download_buffer.
-define(DOWNLOAD_BUFFER_SIZE, 1048576). % 1MB

%% Cowboy callbacks
-export([init/3, handle/2, terminate/3]).
%% Functions used in external modules (e.g. rest_handlers)
-export([cowboy_file_stream_fun/2, content_disposition_attachment_headers/2]).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy handler callback.
-spec init(any(), term(), any()) -> {ok, term(), atom()}.
%% ====================================================================
init(_Type, Req, Opts) ->
    RequestType = proplists:get_value(type, Opts),
    {ok, Req, RequestType}.


%% handle/2
%% ====================================================================
%% @doc Handles a request. Supports user content and shared files downloads.
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State = ?user_content_request_type) ->
    {Path, _} = cowboy_req:binding(path, Req),
    {ok, NewReq} = handle_user_content_request(Req, Path),
    {ok, NewReq, State};


handle(Req, State = ?shared_files_request_type) ->
    {Path, _} = cowboy_req:binding(path, Req),
    {ok, NewReq} = handle_shared_files_request(Req, Path),
    {ok, NewReq, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% API functions
%% ====================================================================

%% cowboy_file_stream_fun/2
%% ====================================================================
%% @doc Returns a cowboy-compliant streaming function, that will be evealuated
%% by cowboy to send data (file content) to receiveing socket.
%% @end
-spec cowboy_file_stream_fun(string() | {uuid, string()}, integer()) -> function().
%% ====================================================================
cowboy_file_stream_fun(FilePathOrUUID, Size) ->
    fun(Socket, Transport) ->
        stream_file(Socket, Transport, FilePathOrUUID, Size, get_download_buffer_size())
    end.


%% content_disposition_attachment_headers/2
%% ====================================================================
%% @doc Sets response headers on cowboy #Req record, so that the data
%% is interpreted as attachment (browsers will save it to disk).
%% Proper filename is set, both in utf8 encoding and legacy for older browsers,
%% based on given filepath or filename.
%% @end
-spec content_disposition_attachment_headers(req(), string()) -> req().
%% ====================================================================
content_disposition_attachment_headers(Req, FileName) ->
    {Type, Subtype, _} = cow_mimetypes:all(gui_str:to_binary(FileName)),
    Mimetype = <<Type/binary, "/", Subtype/binary>>,
    Headers = [
        {<<"content-type">>, Mimetype},
        {<<"content-disposition">>, <<"attachment;",
        % Offer safely-encoded UTF-8 filename and filename*=UTF-8 for browsers supporting it
        " filename=", (gui_str:url_encode(FileName))/binary,
        "; filename*=UTF-8''", (gui_str:url_encode(FileName))/binary>>
        }
    ],
    lists:foldl(fun({Header, Value}, R) -> gui_utils:cowboy_ensure_header(Header, Value, R) end, Req, Headers).


%% ====================================================================
%% Internal functions
%% ====================================================================

%% handle_user_content_request/2
%% ====================================================================
%% @doc Handles user content download requests. Tries to retrieve user's credentials
%% from session cookie. Redirects to error page if there is no valid session or
%% if sending has failed, otherwise streams the file via ssl socket.
%% @end
-spec handle_user_content_request(req(), string()) -> {ok, req()}.
%% ====================================================================
handle_user_content_request(Req, Path) ->
    % Try to initialize session handler and retrieve user's session
    InitSession =
        try
            Context1 = wf_context:init_context(Req),
            SessHandler = proplists:get_value(session, Context1#context.handlers),
            {ok, St, Context2} = SessHandler:init([], Context1),
            wf_context:context(Context2),
            {ok, UserDoc} = user_logic:get_user({login, gui_ctx:get_user_id()}),
            Login = user_logic:get_login(UserDoc),
            fslogic_context:set_user_dn(lists:nth(1, user_logic:get_dn_list(UserDoc))),
            {St, Context2, SessHandler, Login}
        catch T1:M1 ->
            ?warning("Cannot establish session context for user content request - ~p:~p", [T1, M1]),
            error
        end,

    case InitSession of
        error ->
            {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_user_content_not_logged_in);
        {State, NewContext, SessionHandler, UserLogin} ->
            % Try to get file by given path
            FileInfo =
                try
                    TryFilepath = binary_to_list(Path),
                    {ok, Fileattr} = logical_files_manager:getfileattr(TryFilepath),
                    "REG" = Fileattr#fileattributes.type,
                    TrySize = Fileattr#fileattributes.size,
                    {TryFilepath, TrySize}
                catch T2:M2 ->
                    ?warning("Cannot resolve fileattributes for user content request - ~p:~p", [T2, M2]),
                    error
                end,

            case FileInfo of
                error ->
                    {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_user_content_file_not_found);
                {Filepath, Size} ->
                    % Send the file
                    try
                        % Finalize session handler, set new cookie
                        {ok, [], FinalCtx} = SessionHandler:finish(State, NewContext),
                        Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>,
                            FinalCtx#context.req),
                        {ok, _NewReq} = send_file(Req2, Filepath, filename:basename(Filepath), Size)
                    catch Type:Message ->
                        ?error_stacktrace("Error while sending file ~p to user ~p - ~p:~p",
                            [Filepath, UserLogin, Type, Message]),
                        {ok, _FinReq} = cowboy_req:reply(500, Req#http_req{connection = close})
                    end
            end
    end.


%% handle_shared_files_request/4
%% ====================================================================
%% @doc Handles shared file download requests. Tries to resolve target
%% file UUID by share UUID given in request path. Redirects to error page
%% if the uuid doesn't point to any file or if sending has failed, otherwise
%% it streams file via ssl socket.
%% @end
-spec handle_shared_files_request(string(), req()) -> {ok, req()}.
%% ====================================================================
handle_shared_files_request(Req, ShareID) ->
% Try to get file by share uuid
    FileInfo =
        try
            fslogic_context:set_user_dn(undefined),
            true = (ShareID /= <<"">>),

            {ok, #veil_document{record = #share_desc{file = TFileID}}} =
                logical_files_manager:get_share({uuid, binary_to_list(ShareID)}),
            {ok, TFileName} = logical_files_manager:get_file_name_by_uuid(TFileID),
            {ok, Fileattr} = logical_files_manager:getfileattr({uuid, TFileID}),
            "REG" = Fileattr#fileattributes.type,
            TSize = Fileattr#fileattributes.size,
            {TFileID, TFileName, TSize}
        catch T1:M1 ->
            ?warning("Cannot resolve fileattributes for shared file request - ~p:~p", [T1, M1]),
            error
        end,

    % Redirect to error page if the link was incorrect, or send the file
    case FileInfo of
        error ->
            {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_shared_file_not_found);
        {FileID, FileName, Size} ->
            try
                {ok, _NewReq} = send_file(Req, {uuid, FileID}, FileName, Size)
            catch Type:Message ->
                ?error_stacktrace("Error while sending shared file ~p - ~p:~p",
                    [FileID, Type, Message]),
                {ok, _FinReq} = cowboy_req:reply(500, Req#http_req{connection = close})
            end
    end.


%% send_file/4
%% ====================================================================
%% @doc Sends file as a http response, file is given by uuid or filepath (FilePathOrUUID).
%% Note that to send a file by filepath, user_id must be stored in process dictionary:
%% fslogic_context:set_user_dn(UsersDnString)
%% @end
-spec send_file(req(), string() | {uuid, string()}, string(), integer()) -> {ok, req()}.
%% ====================================================================
send_file(Req, FilePathOrUUID, FileName, Size) ->
    StreamFun = cowboy_file_stream_fun(FilePathOrUUID, Size),
    Req2 = content_disposition_attachment_headers(Req, FileName),
    Req3 = cowboy_req:set_resp_body_fun(Size, StreamFun, Req2),
    {ok, _FinReq} = cowboy_req:reply(200, Req3).


%% stream_file/5
%% ====================================================================
%% @doc Function that will be evaluated by cowboy to stream a file to client.
%% @end
-spec stream_file(term(), atom(), string() | {uuid, string()}, integer(), integer()) -> ok.
%% ====================================================================
stream_file(Socket, Transport, File, Size, BufferSize) ->
    stream_file(Socket, Transport, File, Size, 0, BufferSize).

stream_file(Socket, Transport, File, Size, Sent, BufferSize) ->
    {ok, BytesRead} = logical_files_manager:read(File, Sent, BufferSize),
    ok = Transport:send(Socket, BytesRead),
    NewSent = Sent + size(BytesRead),
    if
        NewSent =:= Size -> ok;
        true -> stream_file(Socket, Transport, File, Size, NewSent, BufferSize)
    end.


%% stream_file/0
%% ====================================================================
%% @doc Returns buffer size for file downloads, as specified in config.yml.
%% @end
-spec get_download_buffer_size() -> integer().
%% ====================================================================
get_download_buffer_size() ->
    _Size = case application:get_env(veil_cluster_node, control_panel_download_buffer) of
                {ok, Value} ->
                    Value;
                _ ->
                    ?error("Could not read 'control_panel_download_buffer' from config. Make sure it is present in config.yml and .app.src."),
                    ?DOWNLOAD_BUFFER_SIZE
            end.