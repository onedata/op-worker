%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing basic operations on
%% cdmi objects
%% @end
%% ===================================================================
-module(cdmi_object).

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_capabilities.hrl").

-define(default_get_file_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>, <<"valuerange">>, <<"value">>]).
-define(default_post_file_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]).

%% API
-export([allowed_methods/2, malformed_request/2, resource_exists/2, content_types_provided/2, content_types_accepted/2,delete_resource/2]).
-export([get_binary/2, get_cdmi_object/2, put_binary/2, put_cdmi_object/2]).
-export([stream_file/6]).

%% allowed_methods/2
%% ====================================================================
%% @doc Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%% malformed_request/2
%% ====================================================================
%% @doc
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}} | no_return().
%% ====================================================================
malformed_request(Req, #state{method = <<"PUT">>, cdmi_version = Version } = State) when is_binary(Version) -> % put cdmi
    {<<"application/cdmi-object">>, _} = cowboy_req:header(<<"content-type">>, Req),
    {false,Req,State};
malformed_request(Req, State) ->
    {false, Req, State}.


%% resource_exists/2
%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, State = #state{filepath = Filepath}) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, #fileattributes{type = "REG"} = Attr} -> {true, Req, State#state{attributes = Attr}};
        _ -> {false, Req, State}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_provided(req(), #state{}) -> {[{ContentType, Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().
%% ====================================================================
content_types_provided(Req, #state{cdmi_version = undefined} = State) ->
    {[
        {<<"application/binary">>, get_binary}
    ], Req, State};
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-object">>, get_cdmi_object}
    ], Req, State}.

%% content_types_accepted/2
%% ====================================================================
%% @doc
%% Returns content-types that are accepted and what
%% functions should be used to process the requests.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_accepted(req(), #state{}) -> {[{ContentType, Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().
%% ====================================================================
content_types_accepted(Req, #state{cdmi_version = undefined} = State) ->
    {[
        {'*', put_binary}
    ], Req, State};
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi_object}
    ], Req, State}.

%% delete_resource/3
%% ====================================================================
%% @doc Deletes the resource. Returns whether the deletion was successful.
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:delete(Filepath) of
        ok -> {true, Req, State};
        Error -> cdmi_error:error_reply(Req, State, ?error_bad_request_code, "Deleting cdmi object end up with error: ~p",[Error])
    end.

%% ====================================================================
%% User callbacks registered in content_types_provided/content_types_accepted and present
%% in main cdmi_handler. They can handle get/put requests depending on content type.
%% ====================================================================

%% get_binary/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file, returning file content as response body.
%% @end
-spec get_binary(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_binary(Req, #state{filepath = Filepath, attributes = #fileattributes{size = Size}} = State) ->
    StreamFun = file_download_handler:cowboy_file_stream_fun(Filepath, Size),
    NewReq = file_download_handler:content_disposition_attachment_headers(Req, filename:basename(Filepath)),
    {Type, Subtype, _} = cow_mimetypes:all(list_to_binary(Filepath)),
    ContentType = <<Type/binary, "/", Subtype/binary>>,
    Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, NewReq),
    {{stream, Size, StreamFun}, Req2, State}.

%% get_cdmi_object/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file, returning cdmi-object content type.
%% @end
-spec get_cdmi_object(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_cdmi_object(Req, #state{opts = Opts, attributes = #fileattributes{size = Size}, filepath = Filepath} = State) ->
    DirCdmi = prepare_object_ans(case Opts of [] -> ?default_get_file_opts; _ -> Opts end, State),
    case proplists:get_value(<<"value">>, DirCdmi) of
        {range, Range} ->
            BodyWithoutValue = proplists:delete(<<"value">>, DirCdmi),
            JsonBodyWithoutValue = rest_utils:encode_to_json({struct, BodyWithoutValue}),

            JsonBodyPrefix = case BodyWithoutValue of
                                 [] -> <<"{\"value\":\"">>;
                                 _ -> <<(erlang:binary_part(JsonBodyWithoutValue,0,byte_size(JsonBodyWithoutValue)-1))/binary,",\"value\":\"">>
                             end,
            JsonBodySuffix = <<"\"}">>,
            DataSize = case Range of
                           {From,To} when To >= From -> To - From +1;
                           default -> Size
                       end,
            Base64EncodedSize = byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + trunc(4*ceil(DataSize / 3.0)),

            StreamFun = fun (Socket,Transport) ->
                try
                    Transport:send(Socket,JsonBodyPrefix),
                    {ok,BufferSize} = application:get_env(veil_cluster_node, control_panel_download_buffer),
                    stream_file(Socket, Transport, State, Range, <<"base64">>, BufferSize), %todo send also utf-8 when possible)
                    Transport:send(Socket,JsonBodySuffix)
                catch Type:Message ->
                    % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
                    ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Filepath, Type, Message])
                end
            end,

            {{stream, Base64EncodedSize, StreamFun}, Req, State};
        undefined ->
            Response = rest_utils:encode_to_json({struct, DirCdmi}),
            {Response, Req, State}
    end.

%% put_binary/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation with non-cdmi
%% body content-type. In that case we treat whole body as file content.
%% @end
-spec put_binary(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_binary(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:create(Filepath) of
        ok ->
            write_body_to_file(Req, State, 0);
        {error, file_exists} -> %todo update
            {ok, Req2} = cowboy_req:reply(?error_conflict_code, Req),
            {halt, Req2, State};
        Error -> cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Creating cdmi object end up with error: ~p",[Error])
    end.

%% put_cdmi_object/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation with cdmi body
%% content type. It parses body as JSON string and gets cdmi data to create file.
%% @end
-spec put_cdmi_object(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_cdmi_object(Req, #state{filepath = Filepath,opts = Opts} = State) -> %todo read body in chunks
    {ok, RawBody, _} = cowboy_req:body(Req),
    Body = rest_utils:parse_body(RawBody),
    ValueTransferEncoding = proplists:get_value(<<"valuetransferencoding">>, Body, <<"utf-8">>),  %todo check given body opts, store given mimetype
    Value = proplists:get_value(<<"value">>, Body, <<>>),
    Range = case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From_, To_} -> {From_,To_};
        false -> undefined
    end,
    RawValue = case ValueTransferEncoding of
                   <<"base64">> -> base64:decode(Value);
                   <<"utf-8">> -> Value
               end,
    case logical_files_manager:create(Filepath) of
        ok ->
            case logical_files_manager:write(Filepath, RawValue) of
                Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                    case logical_files_manager:getfileattr(Filepath) of
                        {ok,Attrs} ->
                            Response = rest_utils:encode_to_json({struct, prepare_object_ans(?default_post_file_opts, State#state{attributes = Attrs})}),
                            Req2 = cowboy_req:set_resp_body(Response, Req),
                            {true, Req2, State};
                        Error -> 
                            logical_files_manager:delete(Filepath),
                            cdmi_error:error_reply(Req,State,?error_forbidden_code,"Getting attributes end up with error: ~p",Error)
                    end;
                Error ->
                    logical_files_manager:delete(Filepath),
                    cdmi_error:error_reply(Req,State,?error_forbidden_code,"Writing to cdmi object end up with error: ~p",Error)
            end;
        {error, file_exists} ->
            case Range of
                {From, To} when is_binary(Value) andalso To-From+1 == byte_size(RawValue) ->
                    case logical_files_manager:write(Filepath, From, RawValue) of
                        Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                            {true, Req, State};
                        Error -> cdmi_error:error_reply(Req,State,?error_forbidden_code,"Writing to cdmi object end up with error: ~p",Error)
                    end;
                undefined when is_binary(Value) ->
                    logical_files_manager:truncate(Filepath,0),
                    case logical_files_manager:write(Filepath, RawValue) of
                        Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                            {true, Req, State};
                        Error -> cdmi_error:error_reply(Req,State,?error_forbidden_code,"Writing to cdmi object end up with error: ~p", Error)
                    end;
                undefined -> {true, Req, State};
                _ -> cdmi_error:error_reply(Req, State, ?error_bad_request_code, "Updating cdmi object end up with error",[])
            end;
        Error -> cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Creating cdmi object end up with error: ~p",[Error])
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% prepare_object_ans/2
%% ====================================================================
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%% @end
-spec prepare_object_ans([FieldName :: binary()], #state{}) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_object_ans([], _State) ->
    [];
prepare_object_ans([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-object">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"objectID">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectID">>, cdmi_id:uuid_to_objectid(logical_files_manager:get_uuid_by_filepath(Filepath))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, list_to_binary(filename:basename(Filepath))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentURI">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    ParentURI = case fslogic_path:strip_path_leaf(Filepath) of
                    "/" -> <<"/">>;
                    Other -> <<(list_to_binary(Other))/binary,"/">>
                end,
    [{<<"parentURI">>, ParentURI} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentID">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentID">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentID">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"parentID">>, cdmi_id:uuid_to_objectid(logical_files_manager:get_uuid_by_filepath(fslogic_path:strip_path_leaf(Filepath)))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, list_to_binary(?dataobject_capability_path)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"mimetype">> | Tail], #state{filepath = Filepath} = State) ->
    {Type, Subtype, _} = cow_mimetypes:all(gui_str:to_binary(Filepath)),
    [{<<"mimetype">>, <<Type/binary, "/", Subtype/binary>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"metadata">> | Tail], #state{attributes = Attrs} = State) ->
    [{<<"metadata">>, rest_utils:prepare_metadata(Attrs)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"valuetransferencoding">> | Tail], State) ->
    [{<<"valuetransferencoding">>, <<"base64">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"value">> | Tail], State) ->
    [{<<"value">>, {range, default}} | prepare_object_ans(Tail, State)];
prepare_object_ans([{<<"value">>, From, To} | Tail], State) ->
    [{<<"value">>, {range, {From, To}}} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"valuerange">> | Tail], #state{opts = Opts, attributes = Attrs} = State) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From, To} ->
            [{<<"valuerange">>, iolist_to_binary([integer_to_binary(From), <<"-">>, integer_to_binary(To)])} | prepare_object_ans(Tail, State)];
        _ ->
            [{<<"valuerange">>, iolist_to_binary([<<"0-">>, integer_to_binary(Attrs#fileattributes.size - 1)])} | prepare_object_ans(Tail, State)]
    end;
prepare_object_ans([_Other | Tail], State) ->
    prepare_object_ans(Tail, State).

%% write_body_to_file/3
%% ====================================================================
%% @doc Reads request's body and writes it to file obtained from state.
%% This callback return value is compatibile with put requests.
%% @end
-spec write_body_to_file(req(), #state{}, integer()) -> {boolean(), req(), #state{}}.
%% ====================================================================
write_body_to_file(Req, #state{filepath = Filepath} = State, Offset) ->
    {Status, Chunk, Req2} = cowboy_req:body(Req),
    case logical_files_manager:write(Filepath, Offset, Chunk) of
        Bytes when is_integer(Bytes) ->
            case Status of
                more -> write_body_to_file(Req2, State, Offset + Bytes);
                ok -> {true, Req2, State}
            end;
        Error ->
            logical_files_manager:delete(Filepath),
            cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Writing to cdmi object end up with error: ~p",[Error])
    end.

%% stream_file/6
%% ====================================================================
%% @doc Reads given range of bytes (defaults to whole file) from file (obtained from state filepath), result is
%% encoded according to 'Encoding' argument and streamed to given Socket.
%% @end
-spec stream_file(Socket :: term(), Transport :: atom(), State :: #state{}, Range, Encoding :: binary(), BufferSize :: integer()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
%% ====================================================================
stream_file(Socket, Transport, State, Range, Encoding, BufferSize) when (BufferSize rem 3) =/= 0 ->
    stream_file(Socket, Transport, State, Range, Encoding, BufferSize - (BufferSize rem 3)); %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
stream_file(Socket, Transport, #state{attributes = #fileattributes{ size = Size}} = State, default, Encoding, BufferSize) ->
    stream_file(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize); %default range should remain consistent with parse_object_ans/2 valuerange clause
stream_file(Socket, Transport, #state{filepath = Path} = State, {From, To}, Encoding, BufferSize) ->
    ToRead = To - From + 1,
    case ToRead > BufferSize of
        true ->
            {ok,Data} = logical_files_manager:read(Path, From, BufferSize),
            Transport:send(Socket,encode(Data,Encoding)),
            stream_file(Socket,Transport,State, {From+BufferSize,To},Encoding,BufferSize);
        false ->
            {ok,Data} = logical_files_manager:read(Path, From, ToRead),
            Transport:send(Socket,encode(Data,Encoding))
    end.

%% encode/2
%% ====================================================================
%% @doc Encodes data according to given ecoding
%% @end
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
%% ====================================================================
encode(Data,Encoding) when Encoding =:= <<"base64">> ->
    base64:encode(Data);
encode(Data,_) ->
    Data.

%% ceil/1
%% ====================================================================
%% @doc math ceil function (works on positive values)
-spec ceil(N :: number()) -> integer().
%% ====================================================================
ceil(N) when trunc(N) == N -> N;
ceil(N) -> trunc(N+1).
