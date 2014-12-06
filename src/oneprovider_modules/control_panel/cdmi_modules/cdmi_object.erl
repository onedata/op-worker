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

-include("registered_names.hrl").
-include("oneprovider_modules/control_panel/cdmi.hrl").
-include("oneprovider_modules/control_panel/cdmi_capabilities.hrl").
-include("oneprovider_modules/control_panel/cdmi_object.hrl").
-include("oneprovider_modules/control_panel/cdmi_error.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").

%% API
-export([allowed_methods/2, malformed_request/2, resource_exists/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).
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
malformed_request(Req, #state{method = <<"PUT">>, cdmi_version = Version, filepath = Filepath } = State) when is_binary(Version) -> % put cdmi
     case cowboy_req:header(<<"content-type">>, Req) of
         {<<"application/cdmi-object">>, Req1} -> {false,Req1,State#state{filepath = fslogic_path:get_short_file_name(Filepath)}};
         _ -> cdmi_error:error_reply(Req, State, ?invalid_content_type)
     end;
malformed_request(Req, #state{filepath = Filepath} = State) ->
    {false, Req, State#state{filepath = fslogic_path:get_short_file_name(Filepath)}}.


%% resource_exists/2
%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, State = #state{filepath = Filepath}) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, #fileattributes{type = ?REG_TYPE_PROT} = Attr} -> {true, Req, State#state{attributes = Attr}};
        {ok, _} ->
            Req1 = cowboy_req:set_resp_header(<<"Location">>, utils:ensure_unicode_binary(Filepath++"/"), Req),
            cdmi_error:error_reply(Req1, State, {?moved_permanently, Filepath});
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
        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES -> cdmi_error:error_reply(Req, State, ?forbidden);
        Error -> cdmi_error:error_reply(Req, State, {?file_delete_unknown_error,Error})
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
    % check read permission
    case logical_files_manager:check_file_perm(Filepath, read) of
        true -> ok;
        false -> throw(?forbidden)
    end,

    % get optional 'Range' header
    {RawRange, Req1} = cowboy_req:header(<<"range">>, Req),
    Ranges = case RawRange of
                 undefined -> [{0,Size-1}];
                 _ -> parse_byte_range(State,RawRange)
             end,

    % return bad request if Range is invalid
    case Ranges of
        invalid -> cdmi_error:error_reply(Req1, State, {?invalid_range, RawRange});
        _ ->
            % prepare data size and stream function
            StreamSize = lists:foldl(fun
                ({From, To}, Acc) when To >= From -> max(0, Acc + To - From + 1);
                ({_, _}, Acc)  -> Acc
            end, 0, Ranges),
            Context = fslogic_context:get_user_context(),
            StreamFun = fun(Socket, Transport) ->
                try
                    fslogic_context:set_user_context(Context),
                    {ok, BufferSize} = application:get_env(?APP_Name, control_panel_download_buffer),
                    lists:foreach(fun(Rng) ->
                        stream_file(Socket, Transport, State, Rng, <<"utf-8">>, BufferSize) end, Ranges)
                catch Type:Message ->
                    % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
                    ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Filepath, Type, Message])
                end
            end,

            % set mimetype
            Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, get_mimetype(Filepath), Req1),

            % reply with stream and adequate status
            {ok, Req3} = case RawRange of
                             undefined ->
                                 opn_cowboy_bridge:apply(cowboy_req, reply, [?ok, [], {StreamSize, StreamFun}, Req2]);
                             _ ->
                                 opn_cowboy_bridge:apply(cowboy_req, reply, [?ok_partial_content, [], {StreamSize, StreamFun}, Req2])
                         end,
            {halt, Req3, State}
    end.

%% get_cdmi_object/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file, returning cdmi-object content type.
%% @end
-spec get_cdmi_object(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_cdmi_object(Req, #state{opts = Opts, attributes = #fileattributes{size = Size}, filepath = Filepath} = State) ->
    case logical_files_manager:check_file_perm(Filepath, read) of
        true -> ok;
        false -> throw(?forbidden)
    end,
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
            {DataSize, Encoding} = case Range of
                                       {From,To} when To >= From -> {To - From +1, <<"base64">>};
                                       default -> {Size, get_encoding(Filepath)}
                                   end,

            EncodedDataSize = case Encoding of
                                  <<"base64">> -> byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + trunc(4 * utils:ceil(DataSize / 3.0));
                                  _ -> byte_size(JsonBodyPrefix) + byte_size(JsonBodySuffix) + DataSize
                              end,

            Context = fslogic_context:get_user_context(),
            StreamFun = fun(Socket, Transport) ->
                try
                    fslogic_context:set_user_context(Context),
                    Transport:send(Socket,JsonBodyPrefix),
                    {ok,BufferSize} = application:get_env(?APP_Name, control_panel_download_buffer),
                    stream_file(Socket, Transport, State, Range, Encoding, BufferSize),
                    Transport:send(Socket,JsonBodySuffix)
                catch Type:Message ->
                    Transport:send(Socket,JsonBodySuffix),
                    % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
                    ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Filepath, Type, Message])
                end
            end,

            {{stream, EncodedDataSize, StreamFun}, Req, State};
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
put_binary(ReqArg, #state{filepath = Filepath} = State) ->
    % prepare request data
    {Content, Req0} = cowboy_req:header(<<"content-type">>, ReqArg, ?mimetype_default_value),
    {CdmiPartialFlag, Req} = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    {Mimetype, Encoding} = parse_content(Content),

    case logical_files_manager:create(Filepath) of
        ok ->
            update_completion_status(Filepath, <<"Processing">>),
            update_mimetype(Filepath, Mimetype),
            update_encoding(Filepath, Encoding),
            Ans = write_body_to_file(Req, State, 0),
            set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
            Ans;
        {error, file_exists} ->
            case logical_files_manager:check_file_perm(Filepath, write) of
                true -> ok;
                false -> throw(?forbidden)
            end,
            update_completion_status(Filepath, <<"Processing">>),
            update_mimetype(Filepath, Mimetype),
            update_encoding(Filepath, Encoding),
            {RawRange, Req1} = cowboy_req:header(<<"content-range">>, Req),
            case RawRange of
                undefined ->
                    case logical_files_manager:truncate(Filepath, 0) of
                        ok -> ok;
                        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES ->
                            set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                            throw(?forbidden);
                        Error ->
                            set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                            throw({?put_object_unknown_error, Error})
                    end,
                    Ans = write_body_to_file(Req1, State, 0, false),
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    Ans;
                _ ->
                    {Length, Req2} = cowboy_req:body_length(Req1),
                    case parse_byte_range(State, RawRange) of
                        [{From, To}] when Length =:= undefined orelse Length =:= To - From + 1 ->
                            Ans = write_body_to_file(Req2, State, From, false),
                            set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                            Ans;
                        _ ->
                            set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                            cdmi_error:error_reply(Req2, State, {?invalid_range, RawRange})
                    end
            end;
        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES ->
            cdmi_error:error_reply(Req, State, ?forbidden);
        Error ->
            cdmi_error:error_reply(Req, State, {?put_object_unknown_error, Error})
    end.

%% put_cdmi_object/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation with cdmi body
%% content type. It parses body as JSON string and gets cdmi data to create file.
%% @end
-spec put_cdmi_object(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_cdmi_object(Req, #state{filepath = Filepath,opts = Opts} = State) ->
    % parse body
    {ok, RawBody, Req0} = opn_cowboy_bridge:apply(cowboy_req, body, [Req]),
    Body = rest_utils:parse_body(RawBody),
    ok = rest_utils:validate_body(Body),

    % prepare body fields
    {CdmiPartialFlag, Req1} = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    RequestedMimetype = proplists:get_value(<<"mimetype">>, Body),
    RequestedValueTransferEncoding = proplists:get_value(<<"valuetransferencoding">>, Body),
    RequestedCopyURI = proplists:get_value(<<"copy">>, Body),
    RequestedMoveURI = proplists:get_value(<<"move">>, Body),
    RequestedUserMetadata = proplists:get_value(<<"metadata">>, Body),
    URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
    Value = proplists:get_value(<<"value">>, Body),
    Range = case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From_, To_} -> {From_,To_};
        false -> undefined
    end,
    RawValue = case Range of
                   undefined -> decode(Value, RequestedValueTransferEncoding);
                   _ -> decode(Value, <<"base64">>)
               end,

    % make sure file is created
    {Operation, OperationAns} =
        case {RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined} ->
                case logical_files_manager:exists(Filepath) of
                    true -> {update, ok};
                    false -> {create, logical_files_manager:create(Filepath)};
                    Error_ -> {create, Error_}
                end;
            {undefined, MoveURI} -> {move, logical_files_manager:mv(utils:ensure_unicode_list(MoveURI),Filepath)};
            {CopyURI, undefined} -> {copy, logical_files_manager:cp(utils:ensure_unicode_list(CopyURI),Filepath)}
        end,

    %check creation result, update value and metadata depending on creation type
    case Operation of
        create ->
            case OperationAns of
                ok ->
                    update_completion_status(Filepath, <<"Processing">>),
                    case logical_files_manager:write(Filepath, 0, RawValue) of
                        Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                            ok;
                        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES ->
                            logical_files_manager:delete(Filepath),
                            throw(?forbidden);
                        Error ->
                            logical_files_manager:delete(Filepath),
                            throw({?write_object_unknown_error, Error})
                    end;
                {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES -> throw(?forbidden);
                Error1 -> throw({?put_object_unknown_error, Error1})
            end,

            % return response
            case logical_files_manager:getfileattr(Filepath) of
                {ok,Attrs} ->
                    update_encoding(Filepath, case RequestedValueTransferEncoding of undefined -> <<"utf-8">>; _ -> RequestedValueTransferEncoding end),
                    update_mimetype(Filepath, RequestedMimetype),
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata),
                    Response = rest_utils:encode_to_json({struct, prepare_object_ans(?default_put_file_opts, State#state{attributes = Attrs})}),
                    Req2 = cowboy_req:set_resp_body(Response, Req1),
                    {true, Req2, State};
                Error2 ->
                    logical_files_manager:delete(Filepath),
                    cdmi_error:error_reply(Req1, State, {?get_attr_unknown_error, Error2})
            end;
        update ->
            Permitted = case RequestedUserMetadata of
                [{<<"cdmi_acl">>, _}] ->
                    case URIMetadataNames of
                        [<<"cdmi_acl">>] -> logical_files_manager:check_file_perm(Filepath, owner);
                        _ -> logical_files_manager:check_file_perm(Filepath, write)
                    end;
                _ -> logical_files_manager:check_file_perm(Filepath, write)
            end,
            case Permitted of
                true -> ok;
                false -> throw(?forbidden)
            end,
            update_completion_status(Filepath, <<"Processing">>),
            update_encoding(Filepath, RequestedValueTransferEncoding),
            update_mimetype(Filepath, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    WriteAns = logical_files_manager:write(Filepath, From, RawValue),
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    case WriteAns of
                        Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) -> {true, Req1, State};
                        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES -> cdmi_error:error_reply(Req, State, ?forbidden);
                        Error -> cdmi_error:error_reply(Req1, State, {?write_object_unknown_error, Error})
                    end;
                undefined when is_binary(Value) ->
                    logical_files_manager:truncate(Filepath, 0),
                    WriteAns = logical_files_manager:write(Filepath, 0, RawValue),
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    case WriteAns of
                        Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) -> {true, Req1, State};
                        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES -> cdmi_error:error_reply(Req, State, ?forbidden);
                        Error -> cdmi_error:error_reply(Req1, State, {?write_object_unknown_error, Error})
                    end;
                undefined ->
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    {true, Req1, State};
                MalformedRange ->
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    cdmi_error:error_reply(Req1, State, {?invalid_range, MalformedRange})
            end;
        Other when Other =:= move orelse Other =:= copy ->
            case OperationAns of
                ok ->
                    update_completion_status(Filepath, <<"Processing">>),
                    update_encoding(Filepath, RequestedValueTransferEncoding),
                    update_mimetype(Filepath, RequestedMimetype),
                    cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata, URIMetadataNames),
                    set_completion_status_according_to_partial_flag(Filepath, CdmiPartialFlag),
                    {true, Req1, State};
                {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES -> cdmi_error:error_reply(Req, State, ?forbidden);
                Error -> cdmi_error:error_reply(Req, State, {?put_object_unknown_error, Error})
            end
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
    {ok, Uuid} = logical_files_manager:get_file_uuid(Filepath),
    [{<<"objectID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, utils:ensure_unicode_binary(filename:basename(Filepath))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentURI">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    ParentURI = utils:ensure_unicode_binary(rest_utils:ensure_path_ends_with_slash(fslogic_path:strip_path_leaf(Filepath))),
    [{<<"parentURI">>, ParentURI} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentID">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentID">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentID">> | Tail], #state{filepath = Filepath} = State) ->
    {ok, Uuid} = logical_files_manager:get_file_uuid(fslogic_path:strip_path_leaf(Filepath)),
    [{<<"parentID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, utils:ensure_unicode_binary(?dataobject_capability_path)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"completionStatus">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"completionStatus">>, get_completion_status(Filepath)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"mimetype">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"mimetype">>, get_mimetype(Filepath)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"metadata">> | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Attrs)} | prepare_object_ans(Tail, State)];
prepare_object_ans([{<<"metadata">>, Prefix} | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Prefix, Attrs)} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"valuetransferencoding">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"valuetransferencoding">>, get_encoding(Filepath)} | prepare_object_ans(Tail, State)];
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
%% @doc @equiv write_body_to_file(Req, State, Offset, true)
-spec write_body_to_file(req(), #state{}, integer()) -> {boolean(), req(), #state{}}.
%% ====================================================================
write_body_to_file(Req, State, Offset) ->
    write_body_to_file(Req, State, Offset, true).

%% write_body_to_file/4
%% ====================================================================
%% @doc Reads request's body and writes it to file obtained from state.
%% This callback return value is compatibile with put requests.
%% @end
-spec write_body_to_file(req(), #state{}, integer(), boolean()) -> {boolean(), req(), #state{}}.
%% ====================================================================
write_body_to_file(Req, #state{filepath = Filepath} = State, Offset, RemoveIfFails) ->
    {Status, Chunk, Req1} = opn_cowboy_bridge:apply(cowboy_req, body, [Req]),
    case logical_files_manager:write(Filepath, Offset, Chunk) of
        Bytes when is_integer(Bytes) ->
            case Status of
                more -> write_body_to_file(Req1, State, Offset + Bytes);
                ok -> {true, Req1, State}
            end;
        Error -> %todo handle write file forbidden
            case RemoveIfFails of
                true -> logical_files_manager:delete(Filepath);
                false -> ok
            end,
            cdmi_error:error_reply(Req1, State, {?write_object_unknown_error, Error})
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
stream_file(Socket, Transport, #state{attributes = #fileattributes{size = Size}} = State, default, Encoding, BufferSize) ->
    stream_file(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize); %default range should remain consistent with parse_object_ans/2 valuerange clause
stream_file(Socket, Transport, #state{filepath = Path} = State, {From, To}, Encoding, BufferSize) ->
    ToRead = To - From + 1,
    case ToRead > BufferSize of
        true ->
            {ok, Data} = logical_files_manager:read(Path, From, BufferSize),
            Transport:send(Socket, encode(Data, Encoding)),
            stream_file(Socket, Transport, State, {From + BufferSize, To}, Encoding, BufferSize);
        false ->
            {ok, Data} = logical_files_manager:read(Path, From, ToRead),
            Transport:send(Socket, encode(Data, Encoding))
    end.

%% encode/2
%% ====================================================================
%% @doc Encodes data according to given ecoding
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
%% ====================================================================
encode(Data, Encoding) when Encoding =:= <<"base64">> ->
    base64:encode(Data);
encode(Data, _) ->
    Data.

%% decode/2
%% ====================================================================
%% @doc Decodes data according to given ecoding
-spec decode(Data :: binary(), Encoding :: binary()) -> binary().
%% ====================================================================
decode(undefined,_Encoding) ->
    <<>>;
decode(Data, Encoding) when Encoding =:= <<"base64">> ->
    try base64:decode(Data)
    catch _:_ -> throw(?invalid_base64)
    end;
decode(Data, _) ->
    Data.

%% parse_byte_range/1
%% ====================================================================
%% @doc Parses byte ranges from 'Range' http header format to list of erlang range tuples,
%% i. e. <<"1-5,-3">> for a file with length 10 will produce -> [{1,5},{7,9}]
%% @end
-spec parse_byte_range(#state{}, binary() | list()) -> list(Range) | invalid when
    Range :: {From :: integer(), To :: integer()}.
%% ====================================================================
parse_byte_range(State, Range) when is_binary(Range) ->
    Ranges = parse_byte_range(State, binary:split(Range, <<",">>, [global])),
    case lists:member(invalid, Ranges) of
        true -> invalid;
        false -> Ranges
    end;
parse_byte_range(_, []) ->
    [];
parse_byte_range(#state{attributes = #fileattributes{size = Size}} = State, [First | Rest]) ->
    Range = case binary:split(First, <<"-">>, [global]) of
                [<<>>, FromEnd] -> {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
                [From, <<>>] -> {binary_to_integer(From), Size - 1};
                [From_, To] -> {binary_to_integer(From_), binary_to_integer(To)};
                _ -> [invalid]
            end,
    case Range of
        [invalid] -> [invalid];
        {Begin, End} when Begin > End -> [invalid];
        ValidRange -> [ValidRange | parse_byte_range(State, Rest)]
    end.

%% parse_content/1
%% ====================================================================
%% @doc Parses content-type header to mimetype and charset part, if charset is other than utf-8,
%% function returns undefined
%% @end
-spec parse_content(binary()) -> {Mimetype :: binary(), Encoding :: binary() | undefined}.
%% ====================================================================
parse_content(Content) ->
    case binary:split(Content,<<";">>) of
        [RawMimetype, RawEncoding] ->
            case binary:split(utils:trim_spaces(RawEncoding),<<"=">>) of
                [<<"charset">>, <<"utf-8">>] ->
                    {utils:trim_spaces(RawMimetype), <<"utf-8">>};
                _ ->
                    {utils:trim_spaces(RawMimetype), undefined}
            end;
        [RawMimetype] ->
            {utils:trim_spaces(RawMimetype), undefined}
    end.

%% get_mimetype/1
%% ====================================================================
%% @doc Gets mimetype associated with file, returns default value if no mimetype
%% could be found
%% @end
-spec get_mimetype(string()) -> binary().
%% ====================================================================
get_mimetype(Filepath) ->
    case logical_files_manager:get_xattr(Filepath, ?mimetype_xattr_key) of
        {ok, Value} -> Value;
        {logical_file_system_error, ?VENOATTR} -> ?mimetype_default_value
    end.

%% get_encoding/1
%% ====================================================================
%% @doc Gets valuetransferencoding associated with file, returns default value if no valuetransferencoding
%% could be found
%% @end
-spec get_encoding(string()) -> binary().
%% ====================================================================
get_encoding(Filepath) ->
    case logical_files_manager:get_xattr(Filepath, ?encoding_xattr_key) of
        {ok, Value} -> Value;
        {logical_file_system_error, ?VENOATTR} -> ?encoding_default_value
    end.

%% get_completion_status/1
%% ====================================================================
%% @doc Gets completion status associated with file, returns default value if no completion status
%% could be found. The result can be: binary("Complete") | binary("Processing") | binary("Error")
%% @end
-spec get_completion_status(string()) -> binary().
%% ====================================================================
get_completion_status(Filepath) ->
    case logical_files_manager:get_xattr(Filepath, ?completion_status_xattr_key) of
        {ok, Value} -> Value;
        {logical_file_system_error, ?VENOATTR} -> ?completion_status_default_value
    end.

%% update_mimetype/2
%% ====================================================================
%% @doc Updates mimetype associated with file
-spec update_mimetype(string(), binary()) -> ok | no_return().
%% ====================================================================
update_mimetype(_Filepath, undefined) -> ok;
update_mimetype(Filepath, Mimetype) ->
    ok = logical_files_manager:set_xattr(Filepath, ?mimetype_xattr_key, Mimetype).

%% update_encoding/2
%% ====================================================================
%% @doc Updates valuetransferencoding associated with file
-spec update_encoding(string(), binary()) -> ok | no_return().
%% ====================================================================
update_encoding(_Filepath, undefined) -> ok;
update_encoding(Filepath, Encoding) ->
    ok = logical_files_manager:set_xattr(Filepath, ?encoding_xattr_key, Encoding).

%% update_completion_status/2
%% ====================================================================
%% @doc Updates completion status associated with file
-spec update_completion_status(string(), binary()) -> ok | no_return().
%% ====================================================================
update_completion_status(_Filepath, undefined) -> ok;
update_completion_status(Filepath, CompletionStatus)
    when CompletionStatus =:= <<"Complete">> orelse CompletionStatus =:= <<"Processing">> orelse CompletionStatus =:= <<"Error">> ->
    ok = logical_files_manager:set_xattr(Filepath, ?completion_status_xattr_key, CompletionStatus).

%% set_completion_status_according_to_partial_flag/2
%% ====================================================================
%% @doc Updates completion status associated with file,  according to X-CDMI-Partial flag
-spec set_completion_status_according_to_partial_flag(string(), binary()) -> ok | no_return().
%% ====================================================================
set_completion_status_according_to_partial_flag(_Filepath, <<"true">>) -> ok;
set_completion_status_according_to_partial_flag(Filepath, _) ->
    ok = update_completion_status(Filepath, <<"Complete">>).
