%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles cdmi object PUT, GET and DELETE requests
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_object_handler).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/http_status.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    resource_exists/2, is_authorized/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_cdmi/2, get_binary/2, put_cdmi/2, put_binary/2, error_wrong_path/2]).

%% the default json response for get/put cdmi_object will contain this entities,
%% they can be choosed selectively by appending '?name1;name2' list to the
%% request url
-define(DEFAULT_GET_FILE_OPTS,
    [
        <<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>,
        <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>,
        <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>,
        <<"valuerange">>, <<"value">>
    ]
).
-define(DEFAULT_PUT_FILE_OPTS,
    [
        <<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>,
        <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>,
        <<"metadata">>, <<"mimetype">>
    ]
).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), maps:map()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), maps:map()) -> {boolean(), req(), maps:map()}.
malformed_request(Req, State) ->
    cdmi_arg_parser:malformed_request(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {boolean(), req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), maps:map()) -> {boolean(), req(), maps:map()}.
resource_exists(Req, State) ->
    cdmi_existence_checker:object_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, #{cdmi_version := undefined} = State) ->
    {[
        {<<"application/binary">>, get_binary}
    ], Req, State};
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-object">>, get_cdmi}
    ], Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, #{cdmi_version := undefined} = State) ->
    {[
        {'*', put_binary}
    ], Req, State};
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi},
        {<<"application/cdmi-container">>, error_wrong_path}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, #{path := Path, auth := Auth} = State) ->
    ok = onedata_file_api:unlink(Auth, {path, Path}),
    {true, Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Cowboy callback function
%% Handles GET requests for file, returning file content as response body.
%% @end
%%--------------------------------------------------------------------
-spec get_binary(req(), maps:map()) -> {term(), req(), maps:map()}.
get_binary(Req, #{auth := Auth, attributes := #file_attr{size = Size, uuid = FileGUID}} = State) ->
    % prepare response
    {Ranges, Req1} = cdmi_arg_parser:get_ranges(Req, Size),
    Mimetype = cdmi_metadata:get_mimetype(Auth, {guid, FileGUID}),
    Req2 = cowboy_req:set_resp_header(<<"content-type">>, Mimetype, Req1),
    HttpStatus =
        case Ranges of
            undefined -> ?HTTP_OK;
            _ -> ?PARTIAL_CONTENT
        end,

    % prepare stream
    StreamSize = cdmi_streamer:binary_stream_size(Ranges, Size),
    StreamFun = cdmi_streamer:stream_binary(State, Ranges),

    % reply
    {ok, Req3} = apply(cowboy_req, reply, [HttpStatus, [], {StreamSize, StreamFun}, Req2]),
    {halt, Req3, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(req(), maps:map()) -> {term(), req(), maps:map()}.
get_cdmi(Req, State = #{options := Opts, auth := Auth, attributes := #file_attr{size = Size, uuid = FileGUID}}) ->
    NonEmptyOpts = utils:ensure_defined(Opts, [], ?DEFAULT_GET_FILE_OPTS),
    Answer = cdmi_object_answer:prepare(NonEmptyOpts, State#{options := NonEmptyOpts}),

    case maps:get(<<"value">>, Answer, undefined) of
        {range, Range} ->
            % prepare response
            BodyWithoutValue = maps:remove(<<"value">>, Answer),
            ValueTransferEncoding = cdmi_metadata:get_encoding(Auth, {guid, FileGUID}),
            JsonBodyWithoutValue = json_utils:encode_map(BodyWithoutValue),
            JsonBodyPrefix =
                case BodyWithoutValue of
                    [] -> <<"{\"value\":\"">>;
                    _ ->
                        <<(erlang:binary_part(JsonBodyWithoutValue, 0, byte_size(JsonBodyWithoutValue) - 1))/binary, ",\"value\":\"">>
                end,
            JsonBodySuffix = <<"\"}">>,

            % prepare stream
            StreamSize = cdmi_streamer:cdmi_stream_size(Range, Size, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix),
            StreamFun = cdmi_streamer:stream_cdmi(State, Range, ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix),

            % reply
            {{stream, StreamSize, StreamFun}, Req, State};
        undefined ->
            Response = json_utils:encode_map(Answer),
            {Response, Req, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(req(), maps:map()) -> {term(), req(), maps:map()}.
put_binary(ReqArg, State = #{auth := Auth, path := Path}) ->
    % prepare request data
    {Content, Req0} = cowboy_req:header(<<"content-type">>, ReqArg, <<"application/octet-stream">>),
    {CdmiPartialFlag, Req} = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    {Mimetype, Encoding} = cdmi_arg_parser:parse_content(Content),
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    case onedata_file_api:stat(Auth, {path, Path}) of
        {error, ?ENOENT} ->
            {ok, FileGUID} = onedata_file_api:create(Auth, Path, DefaultMode),
            cdmi_metadata:update_mimetype(Auth, {guid, FileGUID}, Mimetype),
            cdmi_metadata:update_encoding(Auth, {guid, FileGUID}, Encoding),
            {ok, FileHandle} = onedata_file_api:open(Auth, {path, Path}, write),
            cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGUID}, <<"Processing">>),
            {ok, Req1} = cdmi_streamer:write_body_to_file(Req, 0, FileHandle),
            onedata_file_api:fsync(FileHandle),
            onedata_file_api:release(FileHandle),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                Auth, {path, Path}, CdmiPartialFlag
            ),
            {true, Req1, State};
        {ok, #file_attr{uuid = FileGUID, size = Size}} ->
            cdmi_metadata:update_mimetype(Auth, {guid, FileGUID}, Mimetype),
            cdmi_metadata:update_encoding(Auth, {guid, FileGUID}, Encoding),
            {RawRange, Req1} = cowboy_req:header(<<"content-range">>, Req),
            case RawRange of
                undefined ->
                    {ok, FileHandle} = onedata_file_api:open(Auth, {guid, FileGUID}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGUID}, <<"Processing">>),
                    ok = onedata_file_api:truncate(FileHandle, 0),
                    {ok, Req2} = cdmi_streamer:write_body_to_file(Req1, 0, FileHandle),
                    onedata_file_api:fsync(FileHandle),
                    onedata_file_api:release(FileHandle),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                        Auth, {guid, FileGUID}, CdmiPartialFlag
                    ),
                    {true, Req2, State};
                _ ->
                    {Length, Req2} = cowboy_req:body_length(Req1),
                    case cdmi_arg_parser:parse_byte_range(RawRange, Size) of
                        [{From, To}] when Length =:= undefined orelse Length =:= To - From + 1 ->
                            {ok, FileHandle} = onedata_file_api:open(Auth, {guid, FileGUID}, write),
                            cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGUID}, <<"Processing">>),
                            {ok, Req3} = cdmi_streamer:write_body_to_file(Req2, From, FileHandle),
                            onedata_file_api:fsync(FileHandle),
                            onedata_file_api:release(FileHandle),
                            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                                Auth, {guid, FileGUID}, CdmiPartialFlag
                            ),
                            {true, Req3, State};
                        _ ->
                            throw(?ERROR_INVALID_RANGE)
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi(req(), maps:map()) -> {term(), req(), maps:map()}.
put_cdmi(Req, #{path := Path, options := Opts, auth := Auth} = State) ->
    % parse body
    {ok, Body, Req0} = cdmi_arg_parser:parse_body(Req),

    % prepare necessary data
    {CdmiPartialFlag, Req1} = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    RequestedMimetype = maps:get(<<"mimetype">>, Body, undefined),
    RequestedValueTransferEncoding = maps:get(<<"valuetransferencoding">>, Body, undefined),
    RequestedCopyURI = maps:get(<<"copy">>, Body, undefined),
    RequestedMoveURI = maps:get(<<"move">>, Body, undefined),
    RequestedUserMetadata = maps:get(<<"metadata">>, Body, undefined),
    URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
    Value = maps:get(<<"value">>, Body, undefined),
    Range = get_range(Opts),
    RawValue = cdmi_encoder:decode(Value, RequestedValueTransferEncoding, Range),
    RawValueSize = byte_size(RawValue),
    Attrs = get_attr(Auth, Path),
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),

    % create object using create/cp/mv
    {ok, OperationPerformed} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {ok, _} = onedata_file_api:create(Auth, Path, DefaultMode),
                {ok, created};
            {#file_attr{}, undefined, undefined} ->
                {ok, none};
            {undefined, CopyURI, undefined} ->
                ok = onedata_file_api:cp(Auth, {path, filepath_utils:ensure_begins_with_slash(CopyURI)}, Path),
                {ok, copied};
            {undefined, undefined, MoveURI} ->
                ok = onedata_file_api:mv(Auth, {path, filepath_utils:ensure_begins_with_slash(MoveURI)}, Path),
                {ok, moved}
        end,

    % update value and metadata depending on creation type
    case OperationPerformed of
        created ->
            {ok, FileHandler} = onedata_file_api:open(Auth, {path, Path}, write),
            cdmi_metadata:update_cdmi_completion_status(Auth, {path, Path}, <<"Processing">>),
            {ok, _, RawValueSize} = onedata_file_api:write(FileHandler, 0, RawValue),
            onedata_file_api:fsync(FileHandler),
            onedata_file_api:release(FileHandler),

            % return response
            {ok, NewAttrs = #file_attr{uuid = FileGUID}} = onedata_file_api:stat(Auth, {path, Path}),
            cdmi_metadata:update_encoding(Auth, {guid, FileGUID}, utils:ensure_defined(
                RequestedValueTransferEncoding, undefined, <<"utf-8">>
            )),
            cdmi_metadata:update_mimetype(Auth, {guid, FileGUID}, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Auth, {guid, FileGUID}, RequestedUserMetadata),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {guid, FileGUID}, CdmiPartialFlag),
            Answer = cdmi_object_answer:prepare(?DEFAULT_PUT_FILE_OPTS, State#{attributes => NewAttrs}),
            Response = json_utils:encode_map(Answer),
            Req2 = cowboy_req:set_resp_body(Response, Req1),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {path, Path}, CdmiPartialFlag),
            {true, Req2, State};
        CopiedOrMoved when CopiedOrMoved =:= copied orelse CopiedOrMoved =:= moved ->
            cdmi_metadata:update_encoding(Auth, {path, Path}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(Auth, {path, Path}, RequestedMimetype),
            cdmi_metadata:update_cdmi_completion_status(Auth, {path, Path}, <<"Processing">>),
            cdmi_metadata:update_user_metadata(Auth, {path, Path}, RequestedUserMetadata, URIMetadataNames),
            {true, Req1, State};
        none ->
            cdmi_metadata:update_encoding(Auth, {path, Path}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(Auth, {path, Path}, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Auth, {path, Path}, RequestedUserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    {ok, FileHandler} = onedata_file_api:open(Auth, {path, Path}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {path, Path}, <<"Processing">>),
                    {ok, _, RawValueSize} = onedata_file_api:write(FileHandler, From, RawValue),
                    onedata_file_api:fsync(FileHandler),
                    onedata_file_api:release(FileHandler),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {path, Path}, CdmiPartialFlag),
                    {true, Req1, State};
                undefined when is_binary(Value) ->
                    {ok, FileHandler} = onedata_file_api:open(Auth, {path, Path}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {path, Path}, <<"Processing">>),
                    ok = onedata_file_api:truncate(FileHandler, 0),
                    {ok, _, RawValueSize} = onedata_file_api:write(FileHandler, 0, RawValue),
                    onedata_file_api:fsync(FileHandler),
                    onedata_file_api:release(FileHandler),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {path, Path}, CdmiPartialFlag),
                    {true, Req1, State};
                undefined ->
                    {true, Req1, State};
                _MalformedRange ->
                    throw(?ERROR_INVALID_RANGE)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with cdmi-object content type, which indicates that request has
%% wrong path as it ends with '/'
%% @end
%%--------------------------------------------------------------------
-spec error_wrong_path(req(), maps:map()) -> no_return().
error_wrong_path(_Req, _State) ->
    throw(?ERROR_WRONG_PATH).

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc Same as lists:keyfind/3, but returns Default when key is undefined
%%--------------------------------------------------------------------
-spec get_range(Opts :: list()) -> {non_neg_integer(), non_neg_integer()} | undefined.
get_range(Opts) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From_, To_} -> {From_, To_};
        false -> undefined
    end.

%%--------------------------------------------------------------------
%% @doc Gets attributes of file, returns undefined when file does not exist
%%--------------------------------------------------------------------
-spec get_attr(onedata_auth_api:auth(), onedata_file_api:file_path()) ->
    onedata_file_api:file_attributes() | undefined.
get_attr(Auth, Path) ->
    case onedata_file_api:stat(Auth, {path, Path}) of
        {ok, Attrs} -> Attrs;
        {error, ?ENOENT} -> undefined
    end.
