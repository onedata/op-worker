%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_handler).
-author("Piotr Ociepka").
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/cdmi.hrl").
-include("http/rest.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% cowboy rest handler API
-export([
    init/2,
    allowed_methods/2,
    malformed_request/2,
    is_authorized/2,
    resource_exists/2,
    content_types_accepted/2,
    content_types_provided/2,

    error_no_version/2,
    error_wrong_path/2,
    get_cdmi_capability/2,
    get_cdmi_container/2,
    get_cdmi_dataobject/2,
    get_binary_dataobject/2,
    put_cdmi_container/2,
    put_binary_container/2,
    put_cdmi_dataobject/2,
    put_binary_dataobject/2,
    delete_resource/2
]).
%% Test API
-export([get_supported_version/1]).

-type cdmi_resource() ::
    {capabilities, root | container | dataobject} |
    container |
    dataobject.
-type cdmi_req() :: #cdmi_req{}.

-export_type([cdmi_resource/0, cdmi_req/0]).


-define(CDMI_VERSION_HEADER, <<"x-cdmi-specification-version">>).

%% Proplist that provides mapping between objectid and capability path
-define(CAPABILITY_ID_TO_PATH, [
    {?ROOT_CAPABILITY_ID, filename:absname(<<"/", (?ROOT_CAPABILITY_PATH)/binary>>)},
    {?CONTAINER_CAPABILITY_ID, filename:absname(<<"/", (?CONTAINER_CAPABILITY_PATH)/binary>>)},
    {?DATAOBJECT_CAPABILITY_ID, filename:absname(<<"/", (?DATAOBJECT_CAPABILITY_PATH)/binary>>)}
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Initialize the state for this request and resolves resource it's
%% trying to access (capability, container or dataobject).
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), by_id | by_path) ->
    {cowboy_rest, cowboy_req:req(), cdmi_req()} |
    {ok, cowboy_req:req(), undefined}.
init(Req, ReqTypeResolutionMethod) ->
    try
        CdmiReq = case ReqTypeResolutionMethod of
            by_id ->
                resolve_resource_by_id(Req);
            by_path ->
                {Path, _} = cdmi_path:get_path(Req),
                resolve_resource_by_path(Path)
        end,
        {cowboy_rest, Req, CdmiReq}
    catch
        throw:Err ->
            ErrorResp = rest_translator:error_response(Err),
            {ok, send_response(ErrorResp, Req), undefined};
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {ok, NewReq, undefined}
    end.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of allowed methods.
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(cowboy_req:req(), cdmi_req()) ->
    {[binary()], cowboy_req:req(), cdmi_req()}.
allowed_methods(Req, #cdmi_req{resource = {capabilities, _}} = CdmiReq) ->
    {[<<"GET">>], Req, CdmiReq};
allowed_methods(Req, #cdmi_req{resource = container} = CdmiReq) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, CdmiReq};
allowed_methods(Req, #cdmi_req{resource = dataobject} = CdmiReq) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Checks whether request options (in query string) are malformed or
%% cdmi version not specified in case of capability request.
%% @end
%%--------------------------------------------------------------------
-spec malformed_request(cowboy_req:req(), cdmi_req()) ->
    {stop | boolean(), cowboy_req:req(), cdmi_req()}.
malformed_request(Req, #cdmi_req{resource = Type} = CdmiReq) ->
    ReqVer = cowboy_req:header(?CDMI_VERSION_HEADER, Req),
    try {get_supported_version(ReqVer), parse_qs(cowboy_req:qs(Req)), Type} of
        {undefined, _, {capabilities, _}} ->
            ErrorResp = rest_translator:error_response(
                ?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])
            ),
            {stop, send_response(ErrorResp, Req), CdmiReq};
        {Version, Options, _} ->
            {false, Req, CdmiReq#cdmi_req{
                version = Version,
                options = Options
            }}
    catch
        throw:Err ->
            ErrorResp = rest_translator:error_response(Err),
            {stop, send_response(ErrorResp, Req), CdmiReq};
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, CdmiReq}
    end.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Returns whether the user is authorized to perform the action.
%% CDMI requests, beside capabilities ones (?NOBODY authentication is
%% associated with them at init/2 fun), require concrete user
%% authentication.
%% NOTE: The name and description of this function is actually misleading;
%% 401 Unauthorized is returned when there's been an *authentication* error,
%% and 403 Forbidden is returned when the already-authenticated client
%% is unauthorized to perform an operation.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(cowboy_req:req(), cdmi_req()) ->
    {stop | true | {false, binary()}, cowboy_req:req(), cdmi_req()}.
is_authorized(Req, #cdmi_req{client = undefined} = CdmiReq) ->
    try http_auth:authenticate(Req) of
        {ok, ?USER = Client} ->
            {true, Req, CdmiReq#cdmi_req{client = Client}};
        {ok, ?NOBODY} ->
            {stop, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), CdmiReq};
        {error, not_found} ->
            {stop, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), CdmiReq};
        {error, Reason} ->
            ?debug("Authentication error in ~p due to: ~p", [?MODULE, Reason]),
            {{false, <<"authentication_error">>}, Req, CdmiReq}
    catch
        throw:Err ->
            ErrorResp = rest_translator:error_response(Err),
            {stop, send_response(ErrorResp, Req), CdmiReq};
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, CdmiReq}
    end;
is_authorized(Req, CdmiReq) ->
    {true, Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @equiv Cowboy callback function.
%% Checks existence of container or dataobject resources (capabilities always
%% exist because they are virtual resources).
%% @end
%%--------------------------------------------------------------------
-spec resource_exists(cowboy_req:req(), cdmi_req()) ->
    {stop | boolean(), cowboy_req:req(), cdmi_req()}.
resource_exists(Req, #cdmi_req{resource = {capabilities, _}} = CdmiReq) ->
    {true, Req, CdmiReq};
resource_exists(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    resource = Type
} = CdmiReq) ->
    case lfm:stat(SessionId, {path, Path}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE} = Attr} when Type == container ->
            {true, Req, CdmiReq#cdmi_req{file_attrs = Attr}};
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} when Type == dataobject ->
            redirect_to_container(Req, CdmiReq);
        {ok, Attr = #file_attr{type = ?REGULAR_FILE_TYPE}} when Type == dataobject ->
            {true, Req, CdmiReq#cdmi_req{file_attrs = Attr}};
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} when Type == container ->
            redirect_to_dataobject(Req, CdmiReq);
        {ok, #file_attr{type = ?SYMLINK_TYPE}} ->
            {false, Req, CdmiReq};
        {error, ?ENOENT} ->
            {false, Req, CdmiReq};
        {error, Errno} ->
            ErrorResp = rest_translator:error_response(?ERROR_POSIX(Errno)),
            {stop, send_response(ErrorResp, Req), CdmiReq}
    end.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of content-types the resource accepts.
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(cowboy_req:req(), cdmi_req()) ->
    {Value, cowboy_req:req(), cdmi_req()} when
    Value :: [{binary() | {Type, SubType, Params}, AcceptResource}],
    Type :: binary(),
    SubType :: binary(),
    Params :: '*' | [{binary(), binary()}],
    AcceptResource :: atom().
content_types_accepted(Req, #cdmi_req{resource = container, version = undefined} = CdmiReq) ->
    {[
        {<<"application/cdmi-container">>, error_no_version},
        {<<"application/cdmi-object">>, error_no_version},
        {'*', put_binary_container}
    ], Req, CdmiReq};
content_types_accepted(Req, #cdmi_req{resource = container} = CdmiReq) ->
    {[
        {<<"application/cdmi-container">>, put_cdmi_container},
        {<<"application/cdmi-object">>, error_wrong_path},
        {'*', put_binary_container}

    ], Req, CdmiReq};
content_types_accepted(Req, #cdmi_req{resource = dataobject, version = undefined} = CdmiReq) ->
    {[
        {<<"application/cdmi-object">>, error_no_version},
        {<<"application/cdmi-container">>, error_no_version},
        {'*', put_binary_dataobject}
    ], Req, CdmiReq};
content_types_accepted(Req, #cdmi_req{resource = dataobject} = CdmiReq) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi_dataobject},
        {<<"application/cdmi-container">>, error_wrong_path},
        {'*', put_binary_dataobject}
    ], Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of content-types the resource provides.
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(cowboy_req:req(), cdmi_req()) ->
    {[{ContentType :: binary(), Method :: atom()}], cowboy_req:req(), cdmi_req()}.
content_types_provided(Req, #cdmi_req{resource = {capabilities, _}} = CdmiReq) ->
    {[
        {<<"application/cdmi-capability">>, get_cdmi_capability}
    ], Req, CdmiReq};
content_types_provided(Req, #cdmi_req{resource = container, version = undefined} = CdmiReq) ->
    {[
        {<<"application/cdmi-container">>, error_no_version}
    ], Req, CdmiReq};
content_types_provided(Req, #cdmi_req{resource = container} = CdmiReq) ->
    {[
        {<<"application/cdmi-container">>, get_cdmi_container}
    ], Req, CdmiReq};
content_types_provided(Req, #cdmi_req{resource = dataobject, version = undefined} = CdmiReq) ->
    {[
        {<<"application/binary">>, get_binary_dataobject},
        {<<"application/cdmi-object">>, error_no_version}
    ], Req, CdmiReq};
content_types_provided(Req, #cdmi_req{resource = dataobject} = CdmiReq) ->
    {[
        {<<"application/binary">>, get_binary_dataobject},
        {<<"application/cdmi-object">>, get_cdmi_dataobject}
    ], Req, CdmiReq}.


error_no_version(_, _) ->
    ok.


error_wrong_path(_, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function (as content_types_provided).
%% Returns requested capabilities.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_capability(cowboy_req:req(), cdmi_req()) ->
    {binary(), cowboy_req:req(), cdmi_req()}.
get_cdmi_capability(Req, #cdmi_req{
    resource = {capabilities, CapType},
    options = Options
} = CdmiReq) ->
    NonEmptyOpts = utils:ensure_defined(
        Options, [], ?DEFAULT_CAPABILITIES_OPTIONS
    ),
    Capabilities = case CapType of
        root -> cdmi_capabilities:root_capabilities(NonEmptyOpts);
        container -> cdmi_capabilities:container_capabilities(NonEmptyOpts);
        dataobject -> cdmi_capabilities:dataobject_capabilities(NonEmptyOpts)
    end,
    {json_utils:encode(Capabilities), Req, CdmiReq}.


get_cdmi_container(_, _) ->
    ok.


get_cdmi_dataobject(_, _) ->
    ok.


get_binary_dataobject(_, _) ->
    ok.


put_cdmi_container(_, _) ->
    ok.


put_binary_container(_, _) ->
    ok.


put_cdmi_dataobject(_, _) ->
    ok.


put_binary_dataobject(_, _) ->
    ok.


delete_resource(_, _) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves request type (capability, container or dataobject) using specified
%% absolute path (it must begin with /).
%% @end
%%--------------------------------------------------------------------
-spec resolve_resource_by_id(cowboy_req:req()) -> cdmi_req().
resolve_resource_by_id(Req) ->
    ObjectId = cowboy_req:binding(id, Req),
    Guid = case catch file_id:objectid_to_guid(ObjectId) of
        {ok, Id} ->
            Id;
        _Error ->
            throw(?ERROR_BAD_VALUE_IDENTIFIER(<<"file_id">>))
    end,

    {Cl, BasePath} = case proplists:get_value(ObjectId, ?CAPABILITY_ID_TO_PATH) of
        undefined ->
            case http_auth:authenticate(Req) of
                {ok, ?USER(_UserId, SessionId) = Client} ->
                    case lfm:get_file_path(SessionId, Guid) of
                        {ok, FilePath} ->
                            {Client, FilePath};
                        {error, Errno} ->
                            throw(?ERROR_POSIX(Errno))
                    end;
                _ ->
                    throw(?ERROR_UNAUTHORIZED)
            end;
        CapabilityPath ->
            {?NOBODY, CapabilityPath}
    end,

    {Path, _} = cdmi_path:get_path_of_id_request(Req),

    % concatenate BasePath and Path to FullPath
    FullPath = case BasePath of
        <<"/">> -> Path;
        _ -> <<BasePath/binary, Path/binary>>
    end,

    CdmiReq = resolve_resource_by_path(FullPath),
    CdmiReq#cdmi_req{client = Cl}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves resource accessed (capability, container or dataobject) using
%% specified absolute path (it must begin with /).
%% For capability requests ?NOBODY authorization is additionally added.
%% @end
%%--------------------------------------------------------------------
-spec resolve_resource_by_path(file_meta:path()) -> cdmi_req().
resolve_resource_by_path(<<"/", Path/binary>> = FullPath) ->
    case Path of
        ?ROOT_CAPABILITY_PATH ->
            #cdmi_req{
                client = ?NOBODY,
                resource = {capabilities, root}
            };
        ?CONTAINER_CAPABILITY_PATH ->
            #cdmi_req{
                client = ?NOBODY,
                resource = {capabilities, container}
            };
        ?DATAOBJECT_CAPABILITY_PATH ->
            #cdmi_req{
                client = ?NOBODY,
                resource = {capabilities, dataobject}
            };
        _ ->
            CdmiReq = case filepath_utils:ends_with_slash(FullPath) of
                true -> #cdmi_req{resource = container};
                false -> #cdmi_req{resource = dataobject}
            end,
            CdmiReq#cdmi_req{file_path = FullPath}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extract the CDMI version from request arguments string.
%% @end
%%--------------------------------------------------------------------
-spec get_supported_version(list() | binary()) ->
    binary() | undefined.
get_supported_version(undefined) ->
    undefined;
get_supported_version([]) ->
    throw(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]));
get_supported_version([<<"1.1.1">> | _Rest]) ->
    <<"1.1.1">>;
get_supported_version([<<"1.1">> | _Rest]) ->
    <<"1.1.1">>;
get_supported_version([_Version | Rest]) ->
    get_supported_version(Rest);
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
    VersionList = lists:map(fun utils:trim_spaces/1, binary:split(VersionBinary, <<",">>, [global])),
    get_supported_version(VersionList).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling simple and range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc;ddd:fff") will return
%% [binary(aaa),{binary(bbb),1,2},binary(ccc),{binary(ddd),binary(fff)}]
%% @end
%%--------------------------------------------------------------------
-spec parse_qs(binary()) ->
    [binary() | {binary(), binary()} | {binary(), integer(), integer()}].
parse_qs(<<>>) ->
    [];
parse_qs(QueryString) ->
    lists:map(
        fun
            (Opt) when is_binary(Opt) ->
                case binary:split(Opt, <<":">>) of
                    [SimpleOpt] ->
                        SimpleOpt;
                    [SimpleOpt, Range] ->
                        case binary:split(Range, <<"-">>) of
                            [SimpleVal] ->
                                {SimpleOpt, SimpleVal};
                            [FromBin, ToBin] ->
                                try
                                    From = binary_to_integer(FromBin),
                                    To = binary_to_integer(ToBin),
                                    {SimpleOpt, From, To}
                                catch
                                    _:_ ->
                                        throw(?ERROR_BAD_DATA(<<"query string">>))
                                end;
                            _ ->
                                throw(?ERROR_BAD_DATA(<<"query string">>))
                        end;
                    _ ->
                        throw(?ERROR_BAD_DATA(<<"query string">>))
                end;
            (_Other) ->
                throw(?ERROR_BAD_DATA(<<"query string">>))
        end,
        binary:split(QueryString, <<";">>, [global])
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Redirect this request to the same url but without trailing '/'.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to_dataobject(cowboy_req:req(), cdmi_req()) ->
    {stop, cowboy_req:req(), cdmi_req()}.
redirect_to_dataobject(Req, #cdmi_req{file_path = Path} = CdmiReq) ->
    redirect_to(Req, CdmiReq, binary_part(Path, {0, byte_size(Path) - 1})).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Redirect this request to the same url but with trailing '/'.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to_container(cowboy_req:req(), cdmi_req()) ->
    {stop, cowboy_req:req(), cdmi_req()}.
redirect_to_container(Req, #cdmi_req{file_path = Path} = CdmiReq) ->
    redirect_to(Req, CdmiReq, <<Path/binary, "/">>).


%% @private
-spec redirect_to(cowboy_req:req(), cdmi_req(), Path :: binary()) ->
    {stop, cowboy_req:req(), cdmi_req()}.
redirect_to(Req, CdmiReq, Path) ->
    Qs = cowboy_req:qs(Req),
    Hostname = cowboy_req:header(<<"host">>, Req),
    Location = case Qs of
        <<"">> -> <<"https://", Hostname/binary, "/cdmi", Path/binary>>;
        _ -> <<"https://", Hostname/binary, "/cdmi", Path/binary, "?", Qs/binary>>
    end,
    Headers = #{<<"location">> => Location},
    NewReq = cowboy_req:reply(?HTTP_301_MOVED_PERMANENTLY, Headers, Req),
    {stop, NewReq, CdmiReq}.


%% @private
-spec send_response(#rest_resp{}, cowboy_req:req()) -> cowboy_req:req().
send_response(#rest_resp{code = Code, headers = Headers, body = Body}, Req) ->
    RespBody = case Body of
        {binary, Bin} -> Bin;
        Map -> json_utils:encode(Map)
    end,
    cowboy_req:reply(Code, Headers, RespBody, Req).
