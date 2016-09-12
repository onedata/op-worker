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

-include("http/http_common.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
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
-export([get_cdmi/2, put_cdmi/2, put_binary/2, error_wrong_path/2]).

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
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), maps:map()) -> {boolean(), req(), maps:map()}.
resource_exists(Req, State) ->
    cdmi_existence_checker:container_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_cdmi}
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
        {<<"application/cdmi-container">>, put_cdmi},
        {<<"application/cdmi-object">>, error_wrong_path}

    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State = #{auth := Auth, path := Path}) ->
    ok = onedata_file_api:rm_recursive(Auth, {path, Path}),
    {true, Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Handles GET with "application/cdmi-container" content-type
%%--------------------------------------------------------------------
-spec get_cdmi(req(), maps:map()) -> {term(), req(), maps:map()}.
get_cdmi(Req, #{options := Options} = State) ->
    NonEmptyOpts = utils:ensure_defined(Options, [], ?DEFAULT_GET_DIR_OPTS),
    Answer = cdmi_container_answer:prepare(NonEmptyOpts, State#{options := NonEmptyOpts}),

    Response =
        case proplists:get_value(<<"metadata">>, Answer) of
            undefined ->
                json_utils:encode_map(maps:from_list(Answer));
            Metadata ->
                case proplists:get_value(<<"cdmi_acl">>, Metadata) of
                    undefined ->
                        json_utils:encode_map(maps:put(<<"metadata">>, maps:from_list(Metadata), maps:from_list(Answer)));
                    Acl ->
                        AclMap = lists:map(fun maps:from_list/1, Acl),
                        MetaMap = maps:put(<<"cdmi_acl">>, AclMap , maps:from_list(Metadata)),
                        json_utils:encode_map(maps:put(<<"metadata">>, MetaMap, maps:from_list(Answer)))
                end
        end,
    {Response, Req, State}.



%%--------------------------------------------------------------------
%% @doc Handles PUT with "application/cdmi-container" content-type
%%--------------------------------------------------------------------
-spec put_cdmi(req(), maps:map()) -> {term(), req(), maps:map()}.
put_cdmi(_, #{cdmi_version := undefined}) ->
    throw(?ERROR_NO_VERSION_GIVEN);
put_cdmi(Req, State = #{auth := Auth, path := Path, options := Opts}) ->
    {ok, Body, Req1} = cdmi_arg_parser:parse_body(Req),
    Attrs = get_attr(Auth, Path),

    % create dir using mkdir/cp/mv
    RequestedCopyURI = proplists:get_value(<<"copy">>, Body),
    RequestedMoveURI = proplists:get_value(<<"move">>, Body),
    {ok, OperationPerformed} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {ok, _} = onedata_file_api:mkdir(Auth, Path),
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

    %update metadata and return result
    RequestedUserMetadata = proplists:get_value(<<"metadata">>, Body),
    case OperationPerformed of
        none ->
            URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
            ok = cdmi_metadata:update_user_metadata(Auth, {path, Path}, RequestedUserMetadata, URIMetadataNames),
            {true, Req1, State};
        _ ->
            {ok, NewAttrs = #file_attr{uuid = FileGUID}} = onedata_file_api:stat(Auth, {path, Path}),
            ok = cdmi_metadata:update_user_metadata(Auth, {guid, FileGUID}, RequestedUserMetadata),
            Answer = cdmi_container_answer:prepare(?DEFAULT_GET_DIR_OPTS, State#{attributes => NewAttrs, options => ?DEFAULT_GET_DIR_OPTS}),
            Response =
                case proplists:get_value(<<"metadata">>, Answer) of
                    undefined ->
                        json_utils:encode_map(maps:from_list(Answer));
                    Metadata ->
                        case proplists:get_value(<<"cdmi_acl">>, Metadata) of
                            undefined ->
                                json_utils:encode_map(maps:put(<<"metadata">>, maps:from_list(Metadata), maps:from_list(Answer)));
                            Acl ->
                                AclMap = lists:map(fun maps:from_list/1, Acl),
                                MetaMap = maps:put(<<"cdmi_acl">>, AclMap , maps:from_list(Metadata)),
                                json_utils:encode_map(maps:put(<<"metadata">>, MetaMap, maps:from_list(Answer)))
                        end
                end,
            Req2 = cowboy_req:set_resp_body(Response, Req1),
            {true, Req2, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(req(), maps:map()) -> {term(), req(), maps:map()}.
put_binary(Req, State = #{auth := Auth, path := Path}) ->
    {ok, _} = onedata_file_api:mkdir(Auth, Path),
    {true, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with cdmi-object content type, which indicates that request has
%% wrong path as it ends with '/'
%% @end
%%--------------------------------------------------------------------
-spec error_wrong_path(req(), maps:map()) -> no_return().
error_wrong_path(_Req, _State) ->
    throw(?ERROR_WRONG_PATH).

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
