%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module processes file download and upload requests. After validating 
%% them it conducts streaming to or from a socket or makes nitrogen display a proper error page.
%% @end
%% ===================================================================

-module(file_transfer_handler).
-include("logging.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("veil_modules/control_panel/common.hrl").

% Buffer size used to send file to a client. Override with control_panel_download_buffer.
-define(DOWNLOAD_BUFFER_SIZE, 1048576). % 1MB

% Buffer size used to stream file from a client. Override with control_panel_upload_buffer.
-define(UPLOAD_BUFFER_SIZE, 1048576). % 1MB

% Size of data read from the socket at a time.
-define(UPLOAD_PART_SIZE, 1024). % 1KB

% Timeout for fetching single part of data from socket
-define(UPLOAD_PART_TIMEOUT, 30000). % 30 seconds

%% ====================================================================
%% API functions
%% ====================================================================
-export([maybe_handle_request/1, handle_upload_request/2, handle_rest_upload/3, get_download_buffer_size/0]).


%% maybe_handle_request/1
%% ====================================================================
%% @doc Intercepts the request if its a user content or shared file download request or
%% doeas nothinf otherwise. Checks its validity and serves the file or redirects to error page. 
%% Should be called on a request coming directly to nitrogen_handler.
%% @end
-spec maybe_handle_request(Req :: req()) -> {boolean(), NewReq :: req()}.
%% ====================================================================
maybe_handle_request(Req) ->
    {Path, _} = cowboy_req:path(Req),
    {Qs, _} = cowboy_req:qs(Req),
    case {Path, Qs} of
        {<<?user_content_download_path>>, <<"f=", _/binary>>} -> handle_user_content_request(Req);
        {<<?shared_files_download_path, _/binary>>, _} -> handle_shared_file_request(Req);
        _ -> {false, Req}
    end.


%% handle_upload_request/2
%% ====================================================================
%% @doc Asserts the validity of mutlipart POST request and proceeds with
%% parsing or returns an error. Returns list of parsed filed values and
%% file body.
%% @end
-spec handle_upload_request(Req :: req(), record()) -> 
    {ok, Params :: [tuple()], Files :: [#uploaded_file{}]} |
    {error, incorrect_session}.
%% ====================================================================
handle_upload_request(Req, UserDoc) ->
    case UserDoc of
        undefined -> 
            {error, incorrect_session};
        Doc -> 
            case user_logic:get_dn_list(Doc) of
                [] -> 
                    {error, incorrect_session};
                undefined -> 
                    {error, incorrect_session};
                List when is_list(List) ->
                    % This will cause logical_files_manager to create the file for proper user
                    put(user_id, lists:nth(1, List)),
                    {ok, _Params, _Files} = parse_multipart(Req, [], [])
            end
    end.

%% handle_rest_upload/3
%% ====================================================================
%% @doc Asserts the validity of mutlipart POST request and proceeds with
%% parsing and writing its data to a file at specified path. Returns
%% true for successful upload anf false otherwise.
%% @end
handle_rest_upload(Req, Path, Overwrite) ->
    case cowboy_req:parse_header(<<"content-length">>, Req) of
        {ok, Length, NewReq} when is_integer(Length) ->
            case try_to_create_file(binary_to_list(Path), Overwrite) of
                ok -> parse_rest_upload(NewReq, binary_to_list(Path));
                {error, _Error} -> {false, NewReq}
            end;
        _ -> {false, Req}
    end.

%% ====================================================================
%% INTERNAL FUNCTIONS
%% ====================================================================

%% ====================================================================
%% File download (download requests)
%% ====================================================================

% Checks if user content download request is valid and serves a file
handle_user_content_request(Req) ->
    try
        try_or_throw(
            fun() ->
                RequestBridge = simple_bridge:make_request(cowboy_request_bridge, {Req, ""}),
                wf_context:init_context(RequestBridge, undefined),
                wf_handler:call(session_handler, init),
                UserID = wf:session(user_doc),
                true = (UserID /= undefined),
                put(user_id, lists:nth(1, user_logic:get_dn_list(UserID)))
            end, not_logged_in),

        {Filepath, Size} = try_or_throw(
            fun() ->
                {FilepathBin, _} = cowboy_req:qs_val(<<"f">>, Req),
                TryFilepath = binary_to_list(FilepathBin), 
                {ok, Fileattr} = logical_files_manager:getfileattr(TryFilepath),
                "REG" = Fileattr#fileattributes.type,
                TrySize = Fileattr#fileattributes.size,
                {TryFilepath, TrySize}
            end, file_not_found),

        {ok, NewReq} = try_or_throw(
            fun() ->
                send_file_by_path(Req, Filepath, Size)
            end, {sending_failed, Filepath}),
        {true, NewReq}

    catch Type:Message ->
        case Message of
            {sending_failed, Path} ->
                lager:error("Error while sending file ~p to user ~p - ~p:~p~n~p", 
                    [Path, user_logic:get_login(get(user_id)), 
                        Type, Message, erlang:get_stacktrace()]);
            _ -> skip
        end,
        {false, _RedirectReq} = page_error:user_content_request_error(Message, Req)
    end.


% Checks if shared file download request is valid and serves a file
handle_shared_file_request(Req) ->
    try
        {FileID, FileName, Size} = try_or_throw(
            fun() ->
                put(user_id, undefined),        
                {Path, _} = cowboy_req:path(Req),
                <<"/share/", ShareID/binary>> = Path,
                true = (ShareID /= <<"">>),

                {ok, #veil_document { record=#share_desc { file=FileID } } } = 
                    logical_files_manager:get_share({uuid, binary_to_list(ShareID)}),
                {ok, FileName} = logical_files_manager:get_file_name_by_uuid(FileID),
                {ok, Fileattr} = logical_files_manager:getfileattr({uuid, FileID}),
                "REG" = Fileattr#fileattributes.type,
                TrySize = Fileattr#fileattributes.size,
                {FileID, FileName, TrySize}
            end, file_not_found),

        {ok, NewReq} = try_or_throw(
            fun() ->
                send_file_by_uuid(Req, FileID, FileName, Size)
            end, {sending_failed, FileName}),
        {true, NewReq}

    catch Type:Message ->
        case Message of
            {sending_failed, Path} ->
                lager:error("Error while sending shared file ~p - ~p:~p~n~p", 
                    [Path, Type, Message, erlang:get_stacktrace()]);
            _ -> skip
        end,
        {false, _RedirectReq} = page_error:shared_file_request_error(Message, Req)
    end.

% Sends file as a http response, file is given by uuid
send_file_by_uuid(Req, FileID, FileName, Size) ->
    [Mimetype] = mimetypes:path_to_mimes(FileName),
    Headers = simple_bridge_util:ensure_header([], {"Content-Type", Mimetype}),
	Headers2 = simple_bridge_util:ensure_header(Headers, 
        {"Content-Disposition", "attachment;" ++
            % Replace spaces with underscores
            " filename=" ++ re:replace(FileName, " ", "_", [global, {return, list}]) ++
            % Offer safely-encoded UTF-8 filename for browsers supporting it
            "; filename*=UTF-8''" ++ http_uri:encode(FileName)
            }),

	StreamFun = fun(Socket, Transport) ->
		stream_file(Socket, Transport, {uuid, FileID}, Size, get_download_buffer_size())
	end,

    Req2 = prepare_headers(Req, Headers2),
    Req3 = cowboy_req:set_resp_body_fun(Size, StreamFun, Req2),
    {ok, _FinReq} = cowboy_req:reply(200, Req3).

% Sends file as a http response, file is given by logical path
send_file_by_path(Req, Filepath, Size) ->
    [Mimetype] = mimetypes:path_to_mimes(Filepath),
    Headers = simple_bridge_util:ensure_header([], {"Content-Type", Mimetype}),
    Filename = filename:basename(Filepath),
    Headers2 = simple_bridge_util:ensure_header(Headers,
        {"Content-Disposition", "attachment;" ++
            % Replace spaces with underscores
            " filename=" ++ re:replace(Filename, " ", "_", [global, {return, list}]) ++
            % Offer safely-encoded UTF-8 filename for browsers supporting it
            "; filename*=UTF-8''" ++ http_uri:encode(Filename)
        }),

    StreamFun = fun(Socket, Transport) ->
        stream_file(Socket, Transport, Filepath, Size, get_download_buffer_size())
    end,

    Req2 = prepare_headers(Req, Headers2),
    Req3 = cowboy_req:set_resp_body_fun(Size, StreamFun, Req2),
    {ok, _FinReq} = cowboy_req:reply(200, Req3).

% Streams file from cluster using logical_files_manager to http client (via socket)
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


% Sets headers to a cowboy req record
prepare_headers(Req, Headers) ->
    lists:foldl(fun({Header, Value}, R) -> cowboy_req:set_resp_header(Header, Value, R) end, Req, Headers).




%% ====================================================================
%% File upload (upload requests)
%% ====================================================================

% parses a multipart data POST request and write its data to a file
% at specified path
parse_rest_upload(Req, Path) ->
  Part = try multipart_data(Req) catch _:_ -> {false, Req} end,
  case Part of
    {eof, NewReq} ->
      {true, NewReq};
    {headers, Headers, NewReq} ->
      case (length(Headers) == 1) of
        true -> {true, NewReq};
        false -> NewReq2 = try
          stream_file_to_fslogic(NewReq, Path, get_upload_buffer_size())
                           catch _:_ ->
                             logical_files_manager:delete(Path),
                             {false, NewReq}
                           end,
          {true, NewReq2}
      end;
    _ -> {false, Req}
  end.

% try to create empty file at specified path if it doesn't exist
% or truncate its size to 0 if it exists and "overwrite" is set
try_to_create_file(Path, Overwrite) ->
  try_to_create_file("/", string:tokens(Path, "/"), Overwrite).

% checkig if file can be created at specified path
% namely path is a valid filesystem path compose of directories
% except last regular file
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

% check if file exists at specified path and truncate its
% to 0 if "overwrite" is set
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

% create all required parent directories to create a file
% specified as the last element on the list of subdirectories
create_file_and_required_parent_dirs(Path, [Subdir | Subdirs]) ->
  case logical_files_manager:mkdir(Path) of
    ok -> create_file_and_required_parent_dirs(Path ++ Subdir ++ "/", Subdirs);
    {_, Error} -> {error, Error}
  end;

% create empty file at specified path
create_file_and_required_parent_dirs(Path, []) ->
  case logical_files_manager:create(Path) of
    ok -> ok;
    {_, Error} -> {error, Error}
  end.

% Parses a multipart data POST request and returns set of field values and file body
parse_multipart(Req, Params, Files) ->
    case parse_part(Req, Params) of
        {NewReq, {param, Param}} ->
            parse_multipart(NewReq, [Param|Params], Files);
        {NewReq, {file, File}} ->
            parse_multipart(NewReq, Params, [File|Files]);
        {_NewReq, done} ->
            {ok, Params, Files}
    end.

% Params are needed when it stumbles upon a file, so it can retrieve its
% target location from hidden field
parse_part(Req, Params) ->
    case multipart_data(Req) of
        {eof, NewReq} -> 
            {NewReq, done};
        {headers, Headers, NewReq} -> 
            case (length(Headers) == 1) of
                % Form hidden value, only content-disposition header so lenght==1
                true -> parse_param(NewReq, Headers);
                % If not hidden value, it's a file
                false -> parse_file(NewReq, Headers, Params)
            end;
        Error -> 
            throw({"Error in parse_part", Error})
    end.


% Parses out a field value from multipart data
parse_param(Req, Headers) ->
    Name = get_field_name(Headers),
    {Value, NewReq} = accumulate_body(Req, <<"">>),
    {NewReq, {param, {Name, Value}}}.


% Parses out field name from headers
get_field_name(Headers) ->
    try
        ContentDispValue = proplists:get_value(<<"content-disposition">>, Headers), 
        {"form-data", [], Params} = parse_header(ContentDispValue),
        proplists:get_value("name", Params, "")
    catch _:_ ->
        erlang:error({cannot_parse_field_name, Headers})
    end.


% Accumulates body of multipart data field
accumulate_body(Req, Acc) ->
    case cowboy_req:multipart_data(Req) of
        {end_of_part, NewReq} -> 
            {binary_to_list(Acc), NewReq};
        {body, Binary, NewReq} -> 
            accumulate_body(NewReq, <<Acc/binary, Binary/binary>>)
    end. 
    

% Parses a portion of multipart data that holds file body
parse_file(Req, Headers, Params) ->
    TargetDir = case proplists:get_value("targetDir", Params) of
        undefined -> throw({"Error in parse_file", no_upload_target_specified});
        Path -> Path
    end,
    Name = get_file_name(Headers),
    RequestedFullPath = filename:absname(Name, TargetDir),
    FullPath = ensure_unique_filename(RequestedFullPath, 0),
    NewReq = try
        stream_file_to_fslogic(Req, FullPath, get_upload_buffer_size())
    catch Type:Message ->
        logical_files_manager:delete(FullPath),
        throw({"Error in parse_file", Type, Message})
    end,
    File = #uploaded_file {
        name=Name,
        path=FullPath,
        field_name=get_field_name(Headers)
    },
    {NewReq, {file, File}}.


% Tries to create a file as long as one gets created (changing its name eveery time)
ensure_unique_filename(RequestedPath, 0) ->
    Ans = logical_files_manager:create(RequestedPath),
    case Ans of
        ok -> RequestedPath;
        _ -> ensure_unique_filename(RequestedPath, 1)
    end;

ensure_unique_filename(_, 20) ->
    throw({"Error in ensure_unique_filename", counter_hit_20});

ensure_unique_filename(RequestedPath, Counter) ->
    Ext = filename:extension(RequestedPath),
    Rootname = filename:rootname(RequestedPath), 
    NewName = lists:flatten(io_lib:format("~s(~B)~s", [Rootname, Counter, Ext])),
    case logical_files_manager:create(NewName) of
        ok -> NewName;
        {_, ?VEEXIST} -> ensure_unique_filename(RequestedPath, Counter + 1);
        Error -> throw({"Error in ensure_unique_filename", Error})
    end.


% Streams a chunk of data to file from socket (incoming multipart data)
stream_file_to_fslogic(Req, FullPath, BufferSize) ->
    case accumulate_multipart_data(Req, BufferSize) of
        {done, Binary, NewReq} ->
            write_to_file(Binary, FullPath),
            NewReq;
        {more, Binary, NewReq} ->
            write_to_file(Binary, FullPath),
            stream_file_to_fslogic(NewReq, FullPath, BufferSize)
    end. 


% Accumulates a whole buffer of multipart data with use of cowboy's multipart_data function
accumulate_multipart_data(Req, BufferSize) ->
    accumulate_multipart_data(Req, <<"">>, BufferSize).

accumulate_multipart_data(Req, Acc, BufferSize) when size(Acc) + ?UPLOAD_PART_SIZE > BufferSize ->
    {more, Acc, Req};

accumulate_multipart_data(Req, Acc, BufferSize) ->
    case multipart_data(Req) of  % This will return ?UPLOAD_PART_SIZE of data or end_of_part
        {end_of_part, NewReq} -> 
            {done, Acc, NewReq};
        {body, Binary, NewReq} -> 
            accumulate_multipart_data(NewReq, <<Acc/binary, Binary/binary>>, BufferSize);
        Error -> 
            throw({"Error in accumulate_multipart_data", Error})
    end.           
    

% Writes a chunk of data to a file via logical_files_manager
write_to_file(Binary, FullPath) ->
    Size = size(Binary),
    BytesWritten = logical_files_manager:write(FullPath, Binary),
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


% Parses out filename from headers
get_file_name(Headers) ->
    try
        ContentDispValue = proplists:get_value(<<"content-disposition">>, Headers), 
        {"form-data", [], Params} = parse_header(ContentDispValue),
        Filename = proplists:get_value("filename", Params, ""),
        Filename
    catch _:_ ->
        erlang:error({cannot_parse_file_name, Headers})
    end.


% Convienience function to try and catch a block of code
try_or_throw(Fun, ThrowWhat) ->
    try
        Fun()
    catch _:_ ->
        throw(ThrowWhat)
    end.


% Returns buffer size used to send file to a client (from config), or default.
get_download_buffer_size() -> 
    _Size = case application:get_env(veil_cluster_node, control_panel_download_buffer) of
        {ok, Value} -> Value;
        _ ->
            ?error("Could not read 'control_panel_download_buffer' from config. Make sure it is present in config.yml and .app.src."),
            ?DOWNLOAD_BUFFER_SIZE
    end.


% Returns buffer size used to send file to a client (from config), or default.
get_upload_buffer_size() -> 
    _Size = case application:get_env(veil_cluster_node, control_panel_upload_buffer) of
        {ok, Value} -> Value;
        _ ->
            ?error("Could not read 'control_panel_upload_buffer' from config. Make sure it is present in config.yml and .app.src."),
            ?UPLOAD_BUFFER_SIZE
    end.


%% ====================================================================
%% Code from simple_brdige_multipart
%% ====================================================================

parse_header(B) when is_binary(B) -> parse_header(binary_to_list(B));
parse_header(String) ->
    [First|Rest] = [string:strip(S) || S <- string:tokens(String, ";")],
    {Name, Value} = parse_keyvalue($:, First),
    Params = [parse_keyvalue($=,X) || X <- Rest],
    Params1 = [{K,V} || {K,V} <- Params, K /= "", V /= ""],
    {Name, Value, Params1}.

parse_keyvalue(Char, S) ->
    % If Char not found, then use an empty value...
    {Key, Value} = case string:chr(S, Char) of
        0   -> {S, ""};
        Pos -> {string:substr(S, 1, Pos - 1), string:substr(S, Pos + 1)}
    end,
    {string:to_lower(string:strip(Key)), 
        unquote_header(string:strip(Value))}.

unquote_header("\"" ++ Rest) -> unquote_header(Rest, []);
unquote_header(S) -> S.
unquote_header("", Acc) -> lists:reverse(Acc);
unquote_header("\"", Acc) -> lists:reverse(Acc);
unquote_header([$\\, C | Rest], Acc) -> unquote_header(Rest, [C | Acc]);
unquote_header([C | Rest], Acc) -> unquote_header(Rest, [C | Acc]).



%% ====================================================================
%% Code from cowboy_req, slightly modified
%% ====================================================================

%% Multipart Request API.

%% @doc Return data from the multipart parser.
%%
%% Use this function for multipart streaming. For each part in the request,
%% this function returns <em>{headers, Headers}</em> followed by a sequence of
%% <em>{body, Data}</em> tuples and finally <em>end_of_part</em>. When there
%% is no part to parse anymore, <em>eof</em> is returned.
-spec multipart_data(Req)
    -> {headers, cowboy:http_headers(), Req} | {body, binary(), Req}
        | {end_of_part | eof, Req} when Req::req().
multipart_data(Req=#http_req{body_state=waiting}) ->
    {ok, {<<"multipart">>, _SubType, Params}, Req2} =
        cowboy_req:parse_header(<<"content-type">>, Req),
    {_, Boundary} = lists:keyfind(<<"boundary">>, 1, Params),
    {ok, Length, Req3} = cowboy_req:parse_header(<<"content-length">>, Req2),
    multipart_data(Req3, Length, {more, cowboy_multipart:parser(Boundary)});
multipart_data(Req=#http_req{multipart={Length, Cont}}) ->
    multipart_data(Req, Length, Cont());
multipart_data(Req=#http_req{body_state=done}) ->
    {eof, Req}.

multipart_data(Req, Length, {headers, Headers, Cont}) ->
    {headers, Headers, Req#http_req{multipart={Length, Cont}}};
multipart_data(Req, Length, {body, Data, Cont}) ->
    {body, Data, Req#http_req{multipart={Length, Cont}}};
multipart_data(Req, Length, {end_of_part, Cont}) ->
    {end_of_part, Req#http_req{multipart={Length, Cont}}};
multipart_data(Req, 0, eof) ->
    {eof, Req#http_req{body_state=done, multipart=undefined}};
multipart_data(Req=#http_req{socket=Socket, transport=Transport},
        Length, eof) ->
    %% We just want to skip so no need to stream data here.
    {ok, _Data} = Transport:recv(Socket, Length, 5000),
    {eof, Req#http_req{body_state=done, multipart=undefined}};
multipart_data(Req, Length, {more, Parser}) when Length > 0 ->
    case stream_body(?UPLOAD_PART_SIZE, Req) of
        {ok, << Data:Length/binary, Buffer/binary >>, Req2} ->
            multipart_data(Req2#http_req{buffer=Buffer}, 0, Parser(Data));
        {ok, Data, Req2} ->
            multipart_data(Req2, Length - byte_size(Data), Parser(Data));
        Error ->
            throw({"Error in stream_body", Error})
    end.

%% @doc Stream the request's body.
%%
%% This is the most low level function to read the request body.
%%
%% In most cases, if they weren't defined before using init_stream/4,
%% this function will guess which transfer and content encodings were
%% used for building the request body, and configure the decoding
%% functions that will be used when streaming.
%%
%% It then starts streaming the body, returning {ok, Data, Req}
%% for each streamed part, and {done, Req} when it's finished streaming.
%%
%% You can limit the size of the chunks being returned by using the
%% first argument which is the size in bytes. It defaults to 1000000 bytes.
-spec stream_body(non_neg_integer(), Req) -> {ok, binary(), Req}
    | {done, Req} | {error, atom()} when Req::req().
stream_body(MaxLength, Req=#http_req{body_state=waiting, version=Version,
        transport=Transport, socket=Socket}) ->
    {ok, ExpectHeader, Req1} = cowboy_req:parse_header(<<"expect">>, Req),
    case ExpectHeader of
        [<<"100-continue">>] ->
            HTTPVer = atom_to_binary(Version, latin1),
            Transport:send(Socket,
                << HTTPVer/binary, " ", (<<"100 Continue">>)/binary, "\r\n\r\n" >>);
        undefined ->
            ok
    end,
    case cowboy_req:parse_header(<<"transfer-encoding">>, Req1) of
        {ok, [<<"chunked">>], Req2} ->
            stream_body(MaxLength, Req2#http_req{body_state=
                {stream, 0,
                    fun cowboy_http:te_chunked/2, {0, 0},
                    fun cowboy_http:ce_identity/1}});
        {ok, [<<"identity">>], Req2} ->
            {Length, Req3} = cowboy_req:body_length(Req2),
            case Length of
                0 ->
                    {done, Req3#http_req{body_state=done}};
                Length ->
                    stream_body(MaxLength, Req3#http_req{body_state=
                        {stream, Length,
                            fun cowboy_http:te_identity/2, {0, Length},
                            fun cowboy_http:ce_identity/1}})
            end
    end;
stream_body(_, Req=#http_req{body_state=done}) ->
    {done, Req};
stream_body(_, Req=#http_req{buffer=Buffer})
        when Buffer =/= <<>> ->
    transfer_decode(Buffer, Req#http_req{buffer= <<>>});
stream_body(MaxLength, Req) ->
    stream_body_recv(MaxLength, Req).

-spec stream_body_recv(non_neg_integer(), Req)
    -> {ok, binary(), Req} | {error, atom()} when Req::req().
stream_body_recv(MaxLength, Req=#http_req{
        transport=Transport, socket=Socket, buffer=Buffer,
        body_state={stream, Length, _, _, _}}) ->
    %% @todo Allow configuring the timeout.
    case Transport:recv(Socket, min(Length, MaxLength), ?UPLOAD_PART_TIMEOUT) of
        {ok, Data} -> transfer_decode(<< Buffer/binary, Data/binary >>,
            Req#http_req{buffer= <<>>});
        {error, Reason} -> 
            lager:error("Cannot recv upload part data with len: ~p due to: ~p", [min(Length, MaxLength), Reason]),
            {error, Reason}
    end.

-spec transfer_decode(binary(), Req)
    -> {ok, binary(), Req} | {error, atom()} when Req::req().
transfer_decode(Data, Req=#http_req{body_state={stream, _,
        TransferDecode, TransferState, ContentDecode}}) ->
    case TransferDecode(Data, TransferState) of
        {ok, Data2, Rest, TransferState2} ->
            content_decode(ContentDecode, Data2,
                Req#http_req{buffer=Rest, body_state={stream, 0,
                TransferDecode, TransferState2, ContentDecode}});
        %% @todo {header(s) for chunked
        more ->
            stream_body_recv(0, Req#http_req{buffer=Data, body_state={stream,
                0, TransferDecode, TransferState, ContentDecode}});
        {more, Length, Data2, TransferState2} ->
            content_decode(ContentDecode, Data2,
                Req#http_req{body_state={stream, Length,
                TransferDecode, TransferState2, ContentDecode}});
        {done, Length, Rest} ->
            Req2 = transfer_decode_done(Length, Rest, Req),
            {done, Req2};
        {done, Data2, Length, Rest} ->
            Req2 = transfer_decode_done(Length, Rest, Req),
            content_decode(ContentDecode, Data2, Req2);
        {error, Reason} ->
            {error, Reason}
    end.

-spec transfer_decode_done(non_neg_integer(), binary(), Req)
    -> Req when Req::req().
transfer_decode_done(Length, Rest, Req=#http_req{
        headers=Headers, p_headers=PHeaders}) ->
    Headers2 = lists:keystore(<<"content-length">>, 1, Headers,
        {<<"content-length">>, list_to_binary(integer_to_list(Length))}),
    %% At this point we just assume TEs were all decoded.
    Headers3 = lists:keydelete(<<"transfer-encoding">>, 1, Headers2),
    PHeaders2 = lists:keystore(<<"content-length">>, 1, PHeaders,
        {<<"content-length">>, Length}),
    PHeaders3 = lists:keydelete(<<"transfer-encoding">>, 1, PHeaders2),
    Req#http_req{buffer=Rest, body_state=done,
        headers=Headers3, p_headers=PHeaders3}.

%% @todo Probably needs a Rest.
-spec content_decode(content_decode_fun(), binary(), Req)
    -> {ok, binary(), Req} | {error, atom()} when Req::req().
content_decode(ContentDecode, Data, Req) ->
    case ContentDecode(Data) of
        {ok, Data2} -> {ok, Data2, Req};
        {error, Reason} -> {error, Reason}
    end.