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
-include("http/rest.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([terminate/3, allowed_methods/2, malformed_request/2,
    resource_exists/2, is_authorized/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_cdmi/2, get_binary/2, put_cdmi/2, put_binary/2, error_wrong_path/2,
    error_no_version/2]).

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
%% @equiv pre_handler:terminate/3
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @equiv pre_handler:allowed_methods/2
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:malformed_request/2
%% @end
%%--------------------------------------------------------------------
-spec malformed_request(req(), maps:map()) -> {boolean(), req(), maps:map()}.
malformed_request(Req, State) ->
    cdmi_arg_parser:malformed_request(Req, State).

%%--------------------------------------------------------------------
%% @equiv pre_handler:is_authorized/2
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {boolean(), req(), maps:map()}.
is_authorized(Req, State) ->
    rest_auth:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @equiv pre_handler:resource_exists/2
%% @end
%%--------------------------------------------------------------------
-spec resource_exists(req(), maps:map()) -> {boolean(), req(), maps:map()}.
resource_exists(Req, State) ->
    cdmi_existence_checker:object_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @equiv pre_handler:content_types_provided/2
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, #{cdmi_version := undefined} = State) ->
    {[
        {<<"application/binary">>, get_binary},
        {<<"application/cdmi-object">>, error_no_version}
    ], Req, State};
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-object">>, get_cdmi},
        {<<"application/binary">>, get_binary}
    ], Req, State}.


%%--------------------------------------------------------------------
%% @equiv pre_handler:content_types_accepted/2
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, #{cdmi_version := undefined} = State) ->
    {[
        {<<"application/cdmi-object">>, error_no_version},
        {<<"application/cdmi-container">>, error_no_version},
        {'*', put_binary}
    ], Req, State};
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi},
        {<<"application/cdmi-container">>, error_wrong_path},
        {'*', put_binary}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:delete_resource/2
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, #{path := Path, auth := Auth} = State) ->
    ok = lfm:unlink(Auth, {path, Path}, false),
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
get_binary(Req, #{auth := Auth, attributes := #file_attr{size = Size, guid = FileGuid}} = State) ->
    % prepare response
    {Ranges, Req1} = cdmi_arg_parser:get_ranges(Req, Size),
    Mimetype = cdmi_metadata:get_mimetype(Auth, {guid, FileGuid}),
    Req2 = cowboy_req:set_resp_header(<<"content-type">>, Mimetype, Req1),
    HttpStatus =
        case Ranges of
            undefined -> ?HTTP_200_OK;
            _ -> ?HTTP_206_PARTIAL_CONTENT
        end,
    Req3 = cdmi_streamer:stream_binary(HttpStatus, Req2, State, Size, Ranges),
    {stop, Req3, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(req(), maps:map()) -> {term(), req(), maps:map()}.
get_cdmi(Req, State = #{options := Opts, auth := Auth, attributes := #file_attr{size = Size, guid = FileGuid}}) ->
    NonEmptyOpts = utils:ensure_defined(Opts, [], ?DEFAULT_GET_FILE_OPTS),
    Answer = cdmi_object_answer:prepare(NonEmptyOpts, State#{options := NonEmptyOpts}),

    case maps:get(<<"value">>, Answer, undefined) of
        {range, Range} ->
            % prepare response
            BodyWithoutValue = maps:remove(<<"value">>, Answer),
            ValueTransferEncoding = cdmi_metadata:get_encoding(Auth, {guid, FileGuid}),
            JsonBodyWithoutValue = json_utils:encode(BodyWithoutValue),
            JsonBodyPrefix =
                case BodyWithoutValue of
                    #{} = Map when map_size(Map) =:= 0 -> <<"{\"value\":\"">>;
                    _ ->
                        <<(erlang:binary_part(JsonBodyWithoutValue, 0, byte_size(JsonBodyWithoutValue) - 1))/binary, ",\"value\":\"">>
                end,
            JsonBodySuffix = <<"\"}">>,

            Req2 = cdmi_streamer:stream_cdmi(Req, State, Size, Range,
                ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix
            ),
            {stop, Req2, State};
        undefined ->
            Response = json_utils:encode(Answer),
            {Response, Req, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(req(), maps:map()) -> {term(), req(), maps:map()}.
put_binary(Req, State = #{auth := Auth, path := Path}) ->
    % prepare request data
    Content = cowboy_req:header(<<"content-type">>, Req, <<"application/octet-stream">>),
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req),
    {Mimetype, Encoding} = cdmi_arg_parser:parse_content(Content),
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    case lfm:stat(Auth, {path, Path}) of
        {error, ?ENOENT} ->
            {ok, FileGuid} = lfm:create(Auth, Path, DefaultMode),
            cdmi_metadata:update_mimetype(Auth, {guid, FileGuid}, Mimetype),
            cdmi_metadata:update_encoding(Auth, {guid, FileGuid}, Encoding),
            {ok, FileHandle} = lfm:open(Auth, {path, Path}, write),
            cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGuid}, <<"Processing">>),
            {ok, Req1} = cdmi_streamer:write_body_to_file(Req, 0, FileHandle),
            lfm:fsync(FileHandle),
            lfm:release(FileHandle),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                Auth, {path, Path}, CdmiPartialFlag
            ),
            {true, Req1, State};
        {ok, #file_attr{guid = FileGuid, size = Size}} ->
            cdmi_metadata:update_mimetype(Auth, {guid, FileGuid}, Mimetype),
            cdmi_metadata:update_encoding(Auth, {guid, FileGuid}, Encoding),
            RawRange = cowboy_req:header(<<"content-range">>, Req),
            case RawRange of
                undefined ->
                    {ok, FileHandle} = lfm:open(Auth, {guid, FileGuid}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGuid}, <<"Processing">>),
                    ok = lfm:truncate(Auth, {guid, FileGuid}, 0),
                    {ok, Req2} = cdmi_streamer:write_body_to_file(Req, 0, FileHandle),
                    lfm:fsync(FileHandle),
                    lfm:release(FileHandle),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                        Auth, {guid, FileGuid}, CdmiPartialFlag
                    ),
                    {true, Req2, State};
                _ ->
                    Length = cowboy_req:body_length(Req),
                    case cdmi_arg_parser:parse_content_range(RawRange, Size) of
                        {{From, To}, _ExpectedSize} when Length =:= undefined orelse Length =:= To - From + 1 ->
                            {ok, FileHandle} = lfm:open(Auth, {guid, FileGuid}, write),
                            cdmi_metadata:update_cdmi_completion_status(Auth, {guid, FileGuid}, <<"Processing">>),
                            {ok, Req3} = cdmi_streamer:write_body_to_file(Req, From, FileHandle),
                            lfm:fsync(FileHandle),
                            lfm:release(FileHandle),
                            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                                Auth, {guid, FileGuid}, CdmiPartialFlag
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
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
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
    {ok, OperationPerformed, Guid} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {ok, NewGuid} = lfm:create(Auth, Path, DefaultMode),
                {ok, created, NewGuid};
            {#file_attr{guid = NewGuid}, undefined, undefined} ->
                {ok, none, NewGuid};
            {undefined, CopyURI, undefined} ->
                {ok, NewGuid} = lfm:cp(Auth, {path, filepath_utils:ensure_begins_with_slash(CopyURI)}, Path),
                {ok, copied, NewGuid};
            {undefined, undefined, MoveURI} ->
                {ok, NewGuid} = lfm:mv(Auth, {path, filepath_utils:ensure_begins_with_slash(MoveURI)}, Path),
                {ok, moved, NewGuid}
        end,

    % update value and metadata depending on creation type
    case OperationPerformed of
        created ->
            {ok, FileHandler} = lfm:open(Auth, {guid, Guid}, write),
            cdmi_metadata:update_cdmi_completion_status(Auth, {guid, Guid}, <<"Processing">>),
            {ok, _, RawValueSize} = lfm:write(FileHandler, 0, RawValue),
            lfm:fsync(FileHandler),
            lfm:release(FileHandler),

            % return response
            cdmi_metadata:update_encoding(Auth, {guid, Guid}, utils:ensure_defined(
                RequestedValueTransferEncoding, undefined, <<"utf-8">>
            )),
            cdmi_metadata:update_mimetype(Auth, {guid, Guid}, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Auth, {guid, Guid}, RequestedUserMetadata),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {guid, Guid}, CdmiPartialFlag),
            Answer = cdmi_object_answer:prepare(?DEFAULT_PUT_FILE_OPTS, State#{guid => Guid}),
            Response = json_utils:encode(Answer),
            Req2 = cowboy_req:set_resp_body(Response, Req0),
            {true, Req2, State};
        CopiedOrMoved when CopiedOrMoved =:= copied orelse CopiedOrMoved =:= moved ->
            cdmi_metadata:update_encoding(Auth, {guid, Guid}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(Auth, {guid, Guid}, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Auth, {guid, Guid}, RequestedUserMetadata, URIMetadataNames),
            Answer = cdmi_object_answer:prepare(?DEFAULT_PUT_FILE_OPTS, State#{guid => Guid}),
            Response = json_utils:encode(Answer),
            Req2 = cowboy_req:set_resp_body(Response, Req0),
            {true, Req2, State};
        none ->
            cdmi_metadata:update_encoding(Auth, {guid, Guid}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(Auth, {guid, Guid}, RequestedMimetype),
            cdmi_metadata:update_user_metadata(Auth, {guid, Guid}, RequestedUserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    {ok, FileHandler} = lfm:open(Auth, {guid, Guid}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {guid, Guid}, <<"Processing">>),
                    {ok, _, RawValueSize} = lfm:write(FileHandler, From, RawValue),
                    lfm:fsync(FileHandler),
                    lfm:release(FileHandler),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {guid, Guid}, CdmiPartialFlag),
                    {true, Req0, State};
                undefined when is_binary(Value) ->
                    {ok, FileHandler} = lfm:open(Auth, {guid, Guid}, write),
                    cdmi_metadata:update_cdmi_completion_status(Auth, {guid, Guid}, <<"Processing">>),
                    ok = lfm:truncate(Auth, {guid, Guid}, 0),
                    {ok, _, RawValueSize} = lfm:write(FileHandler, 0, RawValue),
                    lfm:fsync(FileHandler),
                    lfm:release(FileHandler),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(Auth, {guid, Guid}, CdmiPartialFlag),
                    {true, Req0, State};
                undefined ->
                    {true, Req0, State};
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

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with cdmi content type, without CDMI version given
%% @end
%%--------------------------------------------------------------------
-spec error_no_version(req(), maps:map()) -> no_return().
error_no_version(_Req, _State) ->
    throw(?ERROR_NO_VERSION_GIVEN).

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Same as lists:keyfind/3, but returns Default when key is undefined
%% @end
%%--------------------------------------------------------------------
-spec get_range(Opts :: list()) -> {non_neg_integer(), non_neg_integer()} | undefined.
get_range(Opts) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From_, To_} -> {From_, To_};
        false -> undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets attributes of file, returns undefined when file does not exist
%% @end
%%--------------------------------------------------------------------
-spec get_attr(rest_auth:auth(), file_meta:path()) -> #file_attr{} | undefined.
get_attr(Auth, Path) ->
    case lfm:stat(Auth, {path, Path}) of
        {ok, Attrs} -> Attrs;
        {error, ?ENOENT} -> undefined
    end.
