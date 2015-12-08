%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles cdmi container PUT, GET and DELETE requests
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_container_handler).
-author("Tomasz Lichon").

-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-define(DEFAULT_GET_DIR_OPTS, [<<"objectType">>, <<"objectID">>,
    <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>,
    <<"completionStatus">>, <<"metadata">>, <<"childrenrange">>, <<"children">>]).

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    is_authorized/2, resource_exists/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_cdmi/2, put_cdmi/2, put_binary/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(cowboy_req:req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
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
    cdmi_existence_checker:container_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_cdmi}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-container">>, put_cdmi},
        {'*', put_binary}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
delete_resource(Req, State) ->
    {true, Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Handles GET with "application/cdmi-container" content-type
%%--------------------------------------------------------------------
-spec get_cdmi(req(), #{}) -> {term(), req(), #{}}.
get_cdmi(Req, #{options := Options} = State) ->
    NewOptions = utils:ensure_defined(Options, [], ?DEFAULT_GET_DIR_OPTS),
    DirCdmi = cdmi_container_answer:prepare(NewOptions, State#{options := NewOptions}),
    Response = json_utils:encode({struct, DirCdmi}),
    {Response, Req, State}.

%%--------------------------------------------------------------------
%% @doc Handles PUT with "application/cdmi-container" content-type
%%--------------------------------------------------------------------
-spec put_cdmi(req(), #{}) -> {term(), req(), #{}}.
put_cdmi(_, #{cdmi_version := undefined}) ->
    throw(?no_version_given);
put_cdmi(Req, State = #{auth := Auth, path := Path, options := Opts}) ->
    {ok, Body, Req1} = cdmi_arg_parser:parse_body(Req),
    Attrs = get_attr(Auth, Path),

    % create dir using mkdir/cp/mv
    RequestedCopyURI = proplists:get_value(<<"copy">>, Body),
    RequestedMoveURI = proplists:get_value(<<"move">>, Body),
    {ok, OperationPerformed} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {onedata_file_api:mkdir(Auth, Path), created};
            {#file_attr{}, undefined, undefined} ->
                {ok, none};
            {undefined, CopyURI, undefined} ->
                {onedata_file_api:cp({path, CopyURI}, Path), copied};
            {undefined, undefined, MoveURI} ->
                {onedata_file_api:mv({path, MoveURI}, Path), moved}
        end,

    %update metadata and return result
    RequestedUserMetadata = proplists:get_value(<<"metadata">>, Body),
    case OperationPerformed of
        none ->
            URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
            ok = cdmi_metadata:update_user_metadata(Auth, {path, Path}, RequestedUserMetadata, URIMetadataNames),
            {true, Req1, State};
        _  ->
            {ok, NewAttrs = #file_attr{uuid = Uuid}} = onedata_file_api:stat(Auth, {path, Path}),
            ok = cdmi_metadata:update_user_metadata(Auth, {uuid, Uuid}, RequestedUserMetadata),
            Answer = cdmi_container_answer:prepare(?DEFAULT_GET_DIR_OPTS, State#{attributes => NewAttrs, opts => ?DEFAULT_GET_DIR_OPTS}),
            Response = json_utils:encode(Answer),
            Req2 = cowboy_req:set_resp_body(Response, Req1),
            {true, Req2, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(req(), #{}) -> {term(), req(), #{}}.
put_binary(Req, State = #{auth := Auth, path := Path}) ->
    ok = onedata_file_api:mkdir(Auth, Path),
    {true, Req, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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