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
    [Mimetype] = mimetypes:path_to_mimes(FileName),
    Headers = [
        {<<"content-type">>, Mimetype},
        {<<"content-disposition">>, <<"attachment;",
        % Replace spaces with underscores
        " filename=", (re:replace(FileName, " ", "_", [global, {return, binary}]))/binary,
        % Offer safely-encoded UTF-8 filename for browsers supporting it
        "; filename*=UTF-8''", (list_to_binary(http_uri:encode(FileName)))/binary>>
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
    try
        try_or_throw(
            fun() ->
                Context = wf_context:init_context(Req),
                SessionHandler = proplists:get_value(session, Context#context.handlers),
                SessionHandler:init([], Context),
                UserID = wf:session(user_doc),
                true = (UserID /= undefined),
                put(user_id, lists:nth(1, user_logic:get_dn_list(UserID)))
            end, not_logged_in),

        {Filepath, Size} = try_or_throw(
            fun() ->
                TryFilepath = binary_to_list(Path),
                {ok, Fileattr} = logical_files_manager:getfileattr(TryFilepath),
                "REG" = Fileattr#fileattributes.type,
                TrySize = Fileattr#fileattributes.size,
                {TryFilepath, TrySize}
            end, file_not_found),

        {ok, _NewReq} = try_or_throw(
            fun() ->
                send_file(Req, Filepath, filename:basename(Filepath), Size)
            end, {sending_failed, Filepath})

    catch Type:Message ->
        case Message of
            {sending_failed, Path} ->
                ?error_stacktrace("Error while sending file ~p to user ~p - ~p:~p",
                    [Path, user_logic:get_login(get(user_id)), Type, Message]);
            _ ->
                skip
        end,
        {ok, _RedirectReq} = page_error:user_content_request_error(Message, Req)
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
    try
        {FileID, FileName, Size} = try_or_throw(
            fun() ->
                put(user_id, undefined),
                true = (ShareID /= <<"">>),

                {ok, #veil_document{record = #share_desc{file = FileID}}} =
                    logical_files_manager:get_share({uuid, binary_to_list(ShareID)}),
                {ok, FileName} = logical_files_manager:get_file_name_by_uuid(FileID),
                {ok, Fileattr} = logical_files_manager:getfileattr({uuid, FileID}),
                "REG" = Fileattr#fileattributes.type,
                TrySize = Fileattr#fileattributes.size,
                {FileID, FileName, TrySize}
            end, file_not_found),

        {ok, _NewReq} = try_or_throw(
            fun() ->
                send_file(Req, {uuid, FileID}, FileName, Size)
            end, {sending_failed, FileName})

    catch Type:Message ->
        case Message of
            {sending_failed, Path} ->
                ?error_stacktrace("Error while sending shared file ~p - ~p:~p", [Path, Type, Message]);
            _ ->
                skip
        end,
        {ok, _RedirectReq} = page_error:shared_file_request_error(Message, Req)
    end.


%% send_file/4
%% ====================================================================
%% @doc Sends file as a http response, file is given by uuid or filepath (FilePathOrUUID).
%% Note that to send a file by filepath, user_id must be stored in process dictionary:
%% put(user_id, UsersDnString)
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


%% try_or_throw/2
%% ====================================================================
%% @doc Convienience function to envelope a piece of code in try throw statement.
%% Used for clear code where multiple try catches are nested.
%% @end
-spec try_or_throw(function(), term()) -> term() | no_return().
%% ====================================================================
try_or_throw(Fun, ThrowWhat) ->
    try
        Fun()
    catch _:_ ->
        throw(ThrowWhat)
    end.