%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module processes file upload requests, both originating from
%% REST and web GUI.
%% @end
%% ===================================================================

-module(file_upload_handler).
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_share.hrl").
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/control_panel/rest_messages.hrl").
-include("err.hrl").

% Buffer size used to stream file from a client. Override with control_panel_upload_buffer.
-define(UPLOAD_BUFFER_SIZE, 1048576). % 1MB

% Size of data read from the socket at a time.
-define(UPLOAD_PART_SIZE, 1024). % 1KB

% Timeout for fetching single part of data from socket
-define(UPLOAD_PART_TIMEOUT, 30000). % 30 seconds

% How many tries of creating a unique filename before failure.
-define(MAX_UNIEQUE_FILENAME_COUNTER, 20).

%% Cowboy callbacks
-export([init/3, handle/2, terminate/3]).
%% Functions used in external modules (e.g. rest_handlers)
-export([handle_http_upload/1, handle_rest_upload/3]).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy handler callback.
-spec init(any(), term(), any()) -> {ok, term(), atom()}.
%% ====================================================================
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request. Supports user content and shared files downloads.
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->
    {ok, NewReq} = handle_http_upload(Req),
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

%% handle_http_upload/1
%% ====================================================================
%% @doc Asserts the validity of multipart POST request and proceeds with
%% parsing or returns an error. Returns list of parsed filed values and
%% file body.
%% @end
-spec handle_http_upload(req()) -> {ok, req()}.
%% ====================================================================
handle_http_upload(Req) ->
    % Try to initialize session handler and retrieve user's session
    InitSession =
        try
            Context1 = wf_context:init_context(Req),
            SessHandler = proplists:get_value(session, Context1#context.handlers),
            {ok, St, Context2} = SessHandler:init([], Context1),
            wf_context:context(Context2),
            {ok, UserDoc} = user_logic:get_user({uuid, gui_ctx:get_user_id()}),
            fslogic_context:set_user_context(UserDoc),
            {St, Context2, SessHandler}
        catch T1:M1 ->
            ?warning("Cannot establish session context for user content request - ~p:~p", [T1, M1]),
            error
        end,

    case InitSession of
        error ->
            {ok, _ErrorReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [500, cowboy_req:set([{connection, close}], Req)]);

        {State, NewContext, SessionHandler} ->
            try
                % Params and _FilePath are not currently used but there are cases when they could be useful
                {ok, _Params, [{OriginalFileName, _FilePath}]} = parse_http_upload(Req, [], []),

                % Return a struct conforming to upload plugin requirements
                RespBody = rest_utils:encode_to_json(
                    {struct, [
                        {files, [
                            {struct, [
                                {name, OriginalFileName}
                            ]}
                        ]}
                    ]}),

                % Finalize session handler, set new cookie
                {ok, [], FinalCtx} = SessionHandler:finish(State, NewContext),
                Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>,
                    FinalCtx#context.req),
                Req3 = cowboy_req:set_resp_body(RespBody, Req2),
                % Force connection to close, so that every upload is in
                {ok, _FinReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [200, cowboy_req:set([{connection, close}], Req3)])

            catch Type:Message ->
                ?error_stacktrace("Error while processing file upload from user ~p - ~p:~p",
                    [fslogic_context:get_user_id(), Type, Message]),
                {ok, _ErrorReq} = opn_cowboy_bridge:apply(cowboy_req, reply, [500, cowboy_req:set([{connection, close}], Req)])
            end
    end.


%% handle_rest_upload/3
%% ====================================================================
%% @doc Asserts the validity of mutlipart POST request and proceeds with
%% parsing and writing its data to a file at specified path. Returns
%% values conforming to rest_module_behaviour requirements.
%% @end
-spec handle_rest_upload(Req :: req(), Path :: string(), Overwrite :: boolean()) -> {ok, req()}.
%% ====================================================================
handle_rest_upload(Req, Path, Overwrite) ->
    try
        case cowboy_req:parse_header(<<"content-length">>, Req) of
            {ok, Length, NewReq} when is_integer(Length) ->
                case try_to_create_file(Path, Overwrite) of
                    ok ->
                        case parse_rest_upload(NewReq, Path) of
                            {true, NewReq2} ->
                                {{body, rest_utils:success_reply(?success_file_uploaded)}, NewReq2};
                            {false, NewReq2} ->
                                ErrorRec = ?report_error(?error_upload_unprocessable),
                                {{error, rest_utils:error_reply(ErrorRec)}, NewReq2}
                        end;
                    {error, _Error} ->
                        ?error("Cannot upload file due to: ~p", [_Error]),
                        ErrorRec = ?report_error(?error_upload_cannot_create),
                        {{error, rest_utils:error_reply(ErrorRec)}, NewReq}
                end;
            _ ->
                ErrorRec = ?report_error(?error_upload_unprocessable),
                {{error, rest_utils:error_reply(ErrorRec)}, Req}
        end
    catch Type:Message ->
        ?error_stacktrace("Cannot upload file - ~p:~p", [Type, Message]),
        ErrorRec2 = ?report_error(?error_upload_unprocessable),
        {{error, rest_utils:error_reply(ErrorRec2)}, Req}
    end.


%% ====================================================================
%% INTERNAL FUNCTIONS
%% ====================================================================

%% parse_http_upload/3
%% ====================================================================
%% @doc Parses a multipart data POST request and returns set of field values and
%% filenames of files that were successfully uploaded.
%% @end
-spec parse_http_upload(Req :: req(), Params :: [tuple()], Files :: [tuple()]) -> {ok, Params :: [tuple()], Files :: [tuple()]}.
%% ====================================================================
parse_http_upload(Req, Params, Files) ->
    case opn_cowboy_bridge:apply(cowboy_req, part, [Req]) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    % Form field
                    {ok, FieldBody, Req3} = opn_cowboy_bridge:apply(cowboy_req, part_body, [Req2]),
                    parse_http_upload(Req3, [{FieldName, FieldBody} | Params], Files);
                {file, _FieldName, Filename, _CType, _CTransferEncoding} ->
                    % File
                    TargetDir = case proplists:get_value(<<"targetDir">>, Params) of
                                    undefined -> throw("Error in parse_file - no upload target specified");
                                    Path -> Path
                                end,
                    RequestedFullPath = filename:absname(Filename, TargetDir),
                    FullPath = gui_str:binary_to_unicode_list(ensure_unique_filename(RequestedFullPath, 0)),
                    try
                        ok = logical_files_manager:create(FullPath),
                        stream_file_to_fslogic(Req2, FullPath, get_upload_buffer_size())
                    catch Type:Message ->
                        ?error_stacktrace("Error in parse_file: ~p:~p",[Type,Message]),
                        catch logical_files_manager:delete(FullPath),
                        throw({"Error in parse_file", Type, Message})
                    end,
                    {ok, Params, [{Filename, FullPath} | Files]}
            end;
        {done, _Req2} ->
            {ok, Params, Files}
    end.


%% parse_rest_upload/3
%% ====================================================================
%% @doc Parses a multipart data POST request and writes its data to a file
%% under specified path.
%% @end
-spec parse_rest_upload(Req :: req(), Path :: string()) -> {boolean(), req()}.
%% ====================================================================
parse_rest_upload(Req, Path) ->
    case opn_cowboy_bridge:apply(cowboy_req, part, [Req]) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, _FieldName} ->
                    % Form field
                    {ok, _FieldBody, Req3} = opn_cowboy_bridge:apply(cowboy_req, part_body, [Req2]),
                    parse_rest_upload(Req3, Path);
                {file, _FieldName, _Filename, _CType, _CTransferEncoding} ->
                    % File
                    try
                        Req3 = stream_file_to_fslogic(Req2, Path, get_upload_buffer_size()),
                        {true, Req3}
                    catch T:M ->
                        ?error_stacktrace("Cannot process REST upload request - ~p:~p", [T, M]),
                        logical_files_manager:delete(Path),
                        {false, Req2}
                    end
            end;
        {done, Req2} ->
            {false, Req2}
    end.


%% stream_file_to_fslogic/3
%% ====================================================================
%% @doc Streams a data to file from socket (incoming multipart data)
%% @end
-spec stream_file_to_fslogic(Req :: req(), FullPath :: string(), BufferSize :: integer()) -> req().
%% ====================================================================
stream_file_to_fslogic(Req, FullPath, BufferSize) ->
    case opn_cowboy_bridge:apply(cowboy_req, part_body, [Req, [{length, BufferSize}, {read_timeout, ?UPLOAD_PART_TIMEOUT}]]) of
        {ok, Binary, Req2} ->
            write_to_file(Binary, FullPath),
            Req2;
        {more, Binary, Req2} ->
            write_to_file(Binary, FullPath),
            stream_file_to_fslogic(Req2, FullPath, BufferSize)
    end.


%% write_to_file/2
%% ====================================================================
%% @doc Writes a chunk of data to a file via logical_files_manager
%% @end
-spec write_to_file(Binary :: binary(), FullPath :: string()) -> done | no_return().
%% ====================================================================
write_to_file(Binary, FullPath) ->
    Size = size(Binary),
    BytesWritten = logical_files_manager:write_file_chunk(FullPath, Binary),
    case BytesWritten of
        I when is_integer(I) ->
            case BytesWritten of
                Size -> done;
                Offset ->
                    write_to_file(binary:part(Binary, Offset, Size - Offset), FullPath)
            end;
        Error ->
            throw({"Error in write_to_file", Error})
    end.


%% get_upload_buffer_size/0
%% ====================================================================
%% @doc Returns buffer size used to send file to a client (from config), or default.
%% @end
-spec get_upload_buffer_size() -> integer().
%% ====================================================================
get_upload_buffer_size() ->
    _Size = case application:get_env(?APP_Name, control_panel_upload_buffer) of
                {ok, Value} ->
                    Value;
                _ ->
                    ?error("Could not read 'control_panel_upload_buffer' from config. Make sure it is present in config.yml and .app.src."),
                    ?UPLOAD_BUFFER_SIZE
            end.


%% ensure_unique_filename/2
%% ====================================================================
%% @doc Tries to find a unique filename for a file (changing its name every time by adding a counter).
%% @end
-spec ensure_unique_filename(RequestedPath :: binary(), Counter :: integer()) -> binary() | no_return().
%% ====================================================================
ensure_unique_filename(RequestedPath, 0) ->
    case logical_files_manager:exists(gui_str:binary_to_unicode_list(RequestedPath)) of
        false -> RequestedPath;
        _ -> ensure_unique_filename(RequestedPath, 1)
    end;

ensure_unique_filename(_, ?MAX_UNIEQUE_FILENAME_COUNTER) ->
    throw({"Error in ensure_unique_filename", counter_limit});

ensure_unique_filename(RequestedPath, Counter) ->
    Ext = filename:extension(RequestedPath),
    Rootname = filename:rootname(RequestedPath),
    NewName = <<Rootname/binary, "(", (integer_to_binary(Counter))/binary, ")", Ext/binary>>,
    case logical_files_manager:exists(gui_str:binary_to_unicode_list(NewName)) of
        false -> NewName;
        _ -> ensure_unique_filename(RequestedPath, Counter + 1)
    end.


%% try_to_create_file/2
%% ====================================================================
%% @doc Tries to create empty file at specified path if it doesn't exist
%% or truncate its size to 0 if it exists and "overwrite" is set.
%% @end
-spec try_to_create_file(Path :: string(), Overwrite :: boolean()) -> ok | {error, term()}.
%% ====================================================================
try_to_create_file(Path, Overwrite) ->
    try_to_create_file("/", string:tokens(Path, "/"), Overwrite).

%% Check if file can be created at specified path
%% namely path is a valid filesystem path composed of directories
%% except last regular file.
try_to_create_file(Path, [Subdir | Subdirs], Overwrite) ->
    case logical_files_manager:exists(Path) of
        true -> case logical_files_manager:getfileattr(Path) of
                    {ok, Attr} ->
                        case Attr#fileattributes.type of
                            "DIR" -> try_to_create_file(Path ++ Subdir ++ "/", Subdirs, Overwrite);
                            _ -> {error, illegal_path}
                        end;
                    {_, Error} -> {error, Error}
                end;
        false -> create_file_and_required_parent_dirs(Path, [Subdir | Subdirs]);
        {_, Error} -> {error, Error}
    end;

%% Check if file exists at specified path and truncate its size
%% to 0 if "overwrite" is set.
try_to_create_file(Path, [], Overwrite) ->
    case logical_files_manager:exists(Path) of
        true -> case logical_files_manager:getfileattr(Path) of
                    {ok, Attr} ->
                        case Attr#fileattributes.type of
                            "REG" ->
                                case Overwrite of
                                    true ->
                                        case logical_files_manager:truncate(Path, 0) of
                                            ok -> ok;
                                            {_, Error} -> {error, Error}
                                        end;
                                    false -> {error, file_exists}
                                end;
                            _ -> {error, illegal_path}
                        end;
                    {_, Error} -> {error, Error}
                end;
        false -> create_file_and_required_parent_dirs(Path, []);
        {_, Error} -> {error, Error}
    end.


%% create_file_and_required_parent_dirs/2
%% ====================================================================
%% @doc Creates all required parent directories to create a file
%% specified as the last element on the list of subdirectories.
%% @end
-spec create_file_and_required_parent_dirs(Path :: string(), [string()]) -> ok | {error, term()}.
%% ====================================================================
create_file_and_required_parent_dirs(Path, [Subdir | Subdirs]) ->
    case logical_files_manager:mkdir(Path) of
        ok -> create_file_and_required_parent_dirs(Path ++ Subdir ++ "/", Subdirs);
        {_, Error} -> {error, Error}
    end;

create_file_and_required_parent_dirs(Path, []) ->
    case logical_files_manager:create(Path) of
        ok -> ok;
        {_, Error} -> {error, Error}
    end.
