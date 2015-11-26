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
-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include("modules/http_worker/rest/http_status.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    resource_exists/2, is_authorized/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_cdmi/2, put/2, get_binary/2]).

%% the default json response for get/put cdmi_object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the request url
-define(DEFAULT_GET_FILE_OPTS, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>, <<"valuerange">>, <<"value">>]).
-define(DEFAULT_PUT_FILE_OPTS, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), #{}} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}}.
malformed_request(Req, State) ->
    cdmi_arg_parser:malformed_request(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    rest_auth:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #{}) -> {boolean(), req(), #{}}.
resource_exists(Req, State) ->
    cdmi_existence_checker:object_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
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
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
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
-spec get_binary(req(), #{}) -> {term(), req(), #{}}.
get_binary(Req, #{attributes := #file_attr{size = Size, mimetype = Mimetype}} = State) ->
    % prepare response
    {Ranges, Req1} = cdmi_arg_parser:get_ranges(Req, State),
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
-spec get_cdmi(req(), #{}) -> {term(), req(), #{}}.
get_cdmi(Req, State = #{options := Opts, auth := Auth, path := Path, attributes := #file_attr{size = Size, encoding = Encoding}}) ->
    NonEmptyOpts = case Opts of [] -> ?DEFAULT_GET_FILE_OPTS; _ -> Opts end,
    DirCdmi = cdmi_object_answer:prepare(NonEmptyOpts, State),

    case proplists:get_value(<<"value">>, DirCdmi) of
        {range, Range} ->
            % prepare response
            BodyWithoutValue = proplists:delete(<<"value">>, DirCdmi),
            ValueTransferEncoding = cdmi_object_answer:encoding_to_valuetransferencoding(Encoding),
            JsonBodyWithoutValue = json:encode({struct, BodyWithoutValue}),
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
            Response = json:encode({struct, DirCdmi}),
            {Response, Req, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put(req(), #{}) -> {term(), req(), #{}}.
put(Req, State) ->
    {true, Req, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================