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

-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_share.hrl").
-include("oneprovider_modules/control_panel/common.hrl").
-include("err.hrl").

% Buffer size used to send file to a client. Override with control_panel_download_buffer.
-define(DOWNLOAD_BUFFER_SIZE, 1048576). % 1MB

%% Cowboy callbacks
-export([init/3, handle/2, terminate/3]).
%% Functions used in external modules (e.g. rest_handlers)
-export([cowboy_file_stream_fun/3, content_disposition_attachment_headers/2]).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy handler callback.
-spec init(Type :: any(), Req :: req(), Opts :: any()) -> {ok, term(), atom()}.
%% ====================================================================
init(_Type, Req, Opts) ->
    RequestType = proplists:get_value(type, Opts),
    {ok, Req, RequestType}.


%% handle/2
%% ====================================================================
%% @doc Handles a request. Supports user content and shared files downloads.
%% @end
-spec handle(Req :: req(), State :: term()) -> {ok, term(), term()}.
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
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% API functions
%% ====================================================================

%% cowboy_file_stream_fun/3
%% ====================================================================
%% @doc Returns a cowboy-compliant streaming function, that will be evealuated
%% by cowboy to send data (file content) to receiving socket.
%% NOTE! FilePathOrUUID must be a unicode string (not utf8)
%% @end
-spec cowboy_file_stream_fun(Context, FilePathOrUUID :: string() | {uuid, string()}, Size :: integer()) -> function() when
    Context :: {Dn :: binary(), {GRUID :: binary(), Token :: binary()}}.
%% ====================================================================
cowboy_file_stream_fun(Context, FilePathOrUUID, Size) ->
    fun(Socket, Transport) ->
        try
            fslogic_context:set_user_context(Context),
            stream_file(Socket, Transport, FilePathOrUUID, Size, get_download_buffer_size())
        catch Type:Message ->
            % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
            ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [FilePathOrUUID, Type, Message])
        end
    end.


%% content_disposition_attachment_headers/2
%% ====================================================================
%% @doc Sets response headers on cowboy #Req record, so that the data
%% is interpreted as attachment (browsers will save it to disk).
%% Proper filename is set, both in utf8 encoding and legacy for older browsers,
%% based on given filepath or filename.
%% NOTE! Filename must be a unicode string (not utf8)
%% @end
-spec content_disposition_attachment_headers(Req :: req(), FileName :: string()) -> req().
%% ====================================================================
content_disposition_attachment_headers(Req, FileName) ->
    FileNameUtf8 = gui_str:unicode_list_to_binary(FileName),
    {Type, Subtype, _} = cow_mimetypes:all(FileNameUtf8),
    Mimetype = <<Type/binary, "/", Subtype/binary>>,
    Headers = [
        {<<"content-type">>, Mimetype},
        {<<"content-disposition">>, <<"attachment;",
        % Offer safely-encoded UTF-8 filename and filename*=UTF-8 for browsers supporting it
        " filename=", (gui_str:url_encode(FileNameUtf8))/binary,
        "; filename*=UTF-8''", (gui_str:url_encode(FileNameUtf8))/binary>>
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
-spec handle_user_content_request(Req :: req(), Path :: string()) -> {ok, req()}.
%% ====================================================================
handle_user_content_request(Req, Path) ->
    % Try to initialize session handler and retrieve user's session
    InitSession =
        try
            Context1 = wf_context:init_context(Req),
            SessHandler = proplists:get_value(session, Context1#context.handlers),
            {ok, St, Context2} = SessHandler:init([], Context1),
            wf_context:context(Context2),
            {ok, UserDocument} = user_logic:get_user({uuid, gui_ctx:get_user_id()}),
            fslogic_context:set_user_context(UserDocument),
            {St, Context2, SessHandler, UserDocument}
        catch T1:M1 ->
            ?warning("Cannot establish session context for user content request - ~p:~p", [T1, M1]),
            error
        end,

    case InitSession of
        error ->
            {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_user_content_not_logged_in);
        {State, NewContext, SessionHandler, UserDoc} ->
            UserLogin = user_logic:get_login(UserDoc),
            % Try to get file by given path
            FileInfo =
                try
                    UserFilePath = gui_str:binary_to_unicode_list(Path),
                    case logical_files_manager:check_file_perm(UserFilePath, read) of
                        false ->
                            error_perms;
                        true ->
                            {ok, #fileattributes{size = TrySize}} = logical_files_manager:getfileattr(UserFilePath),
                            {UserFilePath, TrySize}
                    end
                catch T2:M2 ->
                    ?warning_stacktrace("Cannot resolve fileattributes for user content request - ~p:~p", [T2, M2]),
                    error
                end,

            case FileInfo of
                error ->
                    {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_user_content_file_not_found);
                error_perms ->
                    {ok, _RedirectReq} = page_error:generate_redirect_request(Req, ?error_user_permission_denied);
                {Filepath, Size} ->
                    % Send the file
                    try
                        % Finalize session handler
                        {ok, [], FinalCtx} = SessionHandler:finish(State, NewContext),
                        Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>,
                            FinalCtx#context.req),
                        {ok, _NewReq} = send_file(Req2, Filepath, filename:basename(Filepath), Size)
                    catch Type:Message ->
                        ?error_stacktrace("Error while preparing to send file ~p to user ~p - ~p:~p",
                            [Filepath, UserLogin, Type, Message]),
                        {ok, _FinReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [500, cowboy_req:set([{connection, close}], Req)])
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
-spec handle_shared_files_request(Req :: req(), ShareID :: string()) -> {ok, req()}.
%% ====================================================================
handle_shared_files_request(Req, ShareID) ->
% Try to get file by share uuid
    FileInfo =
        try
            fslogic_context:set_user_dn(undefined),
            true = (ShareID /= <<"">>),

            {ok, #db_document{record = #share_desc{file = TFileID}}} =
                logical_files_manager:get_share({uuid, gui_str:binary_to_unicode_list(ShareID)}),
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
                ?error_stacktrace("Error while preparing to send shared file ~p - ~p:~p",
                    [FileID, Type, Message]),
                {ok, _FinReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [500, cowboy_req:set([{connection, close}], Req)])
            end
    end.


%% send_file/4
%% ====================================================================
%% @doc Sends file as a http response, file is given by uuid or filepath (FilePathOrUUID).
%% Note that to send a file by filepath, user_id must be stored in process dictionary:
%% fslogic_context:set_user_dn(UsersDnString)
%% NOTE! FilePathOrUUID and FileName must be a unicode string (not utf8)
%% @end
-spec send_file(Req :: req(), FilePathOrUUID :: string() | {uuid, string()}, FileName :: string(), Size :: integer()) -> {ok, req()}.
%% ====================================================================
send_file(Req, FilePathOrUUID, FileName, Size) ->
    StreamFun = cowboy_file_stream_fun(fslogic_context:get_user_context(), FilePathOrUUID, Size),
    Req2 = content_disposition_attachment_headers(Req, FileName),
    Req3 = cowboy_req:set_resp_body_fun(Size, StreamFun, Req2),
    {ok, _FinReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [200, cowboy_req:set([{connection, close}], Req3)]).


%% stream_file/5
%% ====================================================================
%% @doc Function that will be evaluated by cowboy to stream a file to client.
%% NOTE! Filename must be a unicode string (not utf8)
%% @end
-spec stream_file(Socket :: term(), Transport :: atom(), Filename :: string() | {uuid, string()}, Size :: integer(), BufferSize :: integer()) -> ok.
%% ====================================================================
stream_file(Socket, Transport, Filename, Size, BufferSize) ->
    stream_file(Socket, Transport, Filename, Size, 0, BufferSize).


%% stream_file/6
%% ====================================================================
%% @doc Function that will be evaluated by cowboy to stream a file to client.
%% NOTE! Filename must be a unicode string (not utf8)
%% @end
-spec stream_file(Socket :: term(), Transport :: atom(), Filename :: string() | {uuid, string()}, Size :: integer(), Sent :: integer(), BufferSize :: integer()) -> ok.
%% ====================================================================
stream_file(Socket, Transport, Filename, Size, BytesSent, BufferSize) ->
    {ok, BytesRead} = logical_files_manager:read(Filename, BytesSent, BufferSize),
    ok = Transport:send(Socket, BytesRead),
    NewSent = BytesSent + size(BytesRead),
    if
        NewSent =:= Size -> ok;
        true -> stream_file(Socket, Transport, Filename, Size, NewSent, BufferSize)
    end.


%% stream_file/0
%% ====================================================================
%% @doc Returns buffer size for file downloads, as specified in config.yml.
%% @end
-spec get_download_buffer_size() -> integer().
%% ====================================================================
get_download_buffer_size() ->
    _Size = case application:get_env(?APP_Name, control_panel_download_buffer) of
                {ok, Value} ->
                    Value;
                _ ->
                    ?error("Could not read 'control_panel_download_buffer' from config. Make sure it is present in config.yml and .app.src."),
                    ?DOWNLOAD_BUFFER_SIZE
            end.