%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles cdmi object/container PUT, GET and DELETE requests
%% @end
%% ===================================================================
-module(cdmi_handler).
-author("Tomasz Lichon").

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_error.hrl").

%% Callbacks
-export([init/3, rest_init/2, resource_exists/2, malformed_request/2, allowed_methods/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).
%% Content type routing functions
-export([get_cdmi_container/2, get_cdmi_object/2, get_binary/2, get_cdmi_capability/2]).
-export([put_cdmi_container/2, put_cdmi_object/2, put_binary/2]).

%% ====================================================================
%% Cowboy rest callbacks
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy callback function
%% Imposes a cowboy upgrade protocol to cowboy_rest - this module is
%% now treated as REST module by cowboy.
%% @end
-spec init(any(), any(), any()) -> {upgrade, protocol, cowboy_rest}.
%% ====================================================================
init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

%% rest_init/2
%% ====================================================================
%% @doc Cowboy callback function
%% Called right after protocol upgrade to init the request context.
%% Will shut down the connection if the peer doesn't provide a valid
%% proxy certificate.
%% @end
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
%% ====================================================================
rest_init(ReqArg, _Opt) ->
    try
        {ok, Identity, Req} = rest_utils:verify_peer_cert(ReqArg),
        ok = rest_utils:prepare_context(Identity),
        {Method, Req1} = cowboy_req:method(Req),
        {PathInfo, Req2} = cowboy_req:path_info(Req1),
        {Url, Req3} = cowboy_req:path(Req2),
        {RawOpts, Req4} = cowboy_req:qs(Req3),
        Path = case PathInfo == [] of
                   true -> "/";
                   false -> gui_str:binary_to_unicode_list(rest_utils:join_to_path(PathInfo))
               end,
        HandlerModule = cdmi_routes:route(PathInfo, Url),
        {ok, Req4, #state{method = Method, filepath = Path, opts = parse_opts(RawOpts), handler_module = HandlerModule}}
    catch
        _Type:{badmatch,{error, Error}} -> {ok, ReqArg, {error, Error}};
        _Type:{badmatch,Error} -> {ok, ReqArg, {error,Error}};
        _Type:Error -> {ok, ReqArg, {error,Error}}
    end.

%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed.
%% @end
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
%% ====================================================================
% Some errors could have been detected in do_init/2. If so, State contains
% an {error, Type} tuple. These errors shall be handled here,
% because cowboy doesn't allow returning errors in rest_init.
allowed_methods(Req, {error,Error}) ->
    case Error of
        {?user_unknown, DnString} -> cdmi_error:error_reply(Req, undefined, {?user_unknown, DnString});
        _ when Error =:= ?no_certificate_chain_found orelse Error =:= ?invalid_cert orelse Error =:= ?invalid_token ->
            cdmi_error:error_reply(Req, undefined, Error);
        _ -> cdmi_error:error_reply(Req, undefined, {?state_init_error, Error})
    end;
allowed_methods(Req, #state{handler_module = Handler} = State) ->
    Handler:allowed_methods(Req,State).

%% malformed_request/2
%% ====================================================================
%% @doc Cowboy callback function
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
malformed_request(Req, #state{handler_module = Handler} = State) ->
    try
        % check cdmi version
        {CdmiVersionList, Req1} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
        CdmiVersion = get_supported_version(CdmiVersionList),

        % set response header
        Req2 = case CdmiVersion of
                   undefined -> Req1;
                   _ -> cowboy_req:set_resp_header(<<"x-cdmi-specification-version">>, CdmiVersion, Req1)
               end,

        Handler:malformed_request(Req2,State#state{cdmi_version = CdmiVersion})
    catch
        throw:{halt,ErrReq,ErrState}  -> {halt,ErrReq,ErrState};
        throw:?unsupported_version -> cdmi_error:error_reply(Req, State, ?unsupported_version);
        _Type:Error -> cdmi_error:error_reply(Req, State, {?malformed_request, Error})
    end.

%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by Filepath exists.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, #state{handler_module = Handler} = State) ->
    Handler:resource_exists(Req,State).

%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, #state{handler_module = Handler} = State) ->
    Handler:content_types_provided(Req,State).

%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, #state{handler_module = Handler} = State) ->
    Handler:content_types_accepted(Req,State).

%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{handler_module = Handler} = State) ->
    Handler:delete_resource(Req,State).

%% ====================================================================
%% Content type routing functions
%% ====================================================================


%% ====================================================================
%% This functions are needed by cowboy for registration in
%% content_types_accepted/content_types_provided methods and simply delegates
%% their responsibility to adequate handler modules
%% ====================================================================
get_cdmi_container(Req,State = #state{handler_module = Handler}) ->
    Handler:get_cdmi_container(Req,State).
get_binary(Req,State = #state{handler_module = Handler}) ->
    Handler:get_binary(Req,State).
get_cdmi_object(Req,State = #state{handler_module = Handler}) ->
    Handler:get_cdmi_object(Req,State).
get_cdmi_capability(Req,State = #state{handler_module = Handler}) ->
    Handler:get_cdmi_capability(Req,State).
put_cdmi_container(Req,State = #state{handler_module = Handler}) ->
    Handler:put_cdmi_container(Req,State).
put_binary(Req,State = #state{handler_module = Handler}) ->
    Handler:put_binary(Req,State).
put_cdmi_object(Req,State = #state{handler_module = Handler}) ->
    try Handler:put_cdmi_object(Req,State)
    catch _:?invalid_base64 -> cdmi_error:error_reply(Req, State, ?invalid_base64)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% parse_opts/1
%% ====================================================================
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling simple and range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc;ddd:fff") will return
%% [binary(aaa),{binary(bbb),1,2},binary(ccc),{binary(ddd),binary(fff)}]
%% @end
-spec parse_opts(binary()) -> [binary() | {binary(), binary()} | {binary(), From :: integer(), To :: integer()}].
%% ====================================================================
parse_opts(<<>>) ->
    [];
parse_opts(RawOpts) ->
    Opts = binary:split(RawOpts, <<";">>, [global]),
    lists:map(
        fun(Opt) ->
            case binary:split(Opt, <<":">>) of
                [SimpleOpt] -> SimpleOpt;
                [SimpleOpt, Range] ->
                    case binary:split(Range, <<"-">>) of
                        [SimpleVal] -> {SimpleOpt, SimpleVal};
                        [From, To] ->
                            {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
                    end
            end
        end,
        Opts
    ).

%% get_supported_version/1
%% ====================================================================
%% @doc Finds supported version in coma-separated binary of client versions,
%% throws exception when no version is supported
%% @end
-spec get_supported_version(binary() | list() | undefined ) -> binary() | undefined | no_return().
%% ====================================================================
get_supported_version(undefined) -> undefined;
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
    VersionList = lists:map(fun rest_utils:trim_spaces/1, binary:split(VersionBinary,<<",">>,[global])),
    get_supported_version(VersionList);
get_supported_version([]) -> throw(?unsupported_version);
get_supported_version([<<"1.0.2">> | _Rest]) -> <<"1.0.2">>;
get_supported_version([<<"1.0.1">> | _Rest]) -> <<"1.0.1">>;
get_supported_version([_Version | Rest]) -> get_supported_version(Rest).

