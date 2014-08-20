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

%% Callbacks
-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).
%% Content type routing functions
-export([get_cdmi_container/2, get_cdmi_object/2, get_binary/2]).
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
rest_init(Req, _Opt) ->
    try
        {ok, DnString} = rest_utils:verify_peer_cert(Req),
        ok = rest_utils:prepare_context(DnString),
        {Method, _} = cowboy_req:method(Req),
        {PathInfo, _} = cowboy_req:path_info(Req),
        {Url, _} = cowboy_req:path(Req),
        {RawOpts, _} = cowboy_req:qs(Req),
        {CdmiVersionList, _} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
        CdmiVersion = get_supported_version(CdmiVersionList),
        Req2 = case CdmiVersion of
                   undefined -> Req;
                   Version -> cowboy_req:set_resp_header(<<"x-cdmi-specification-version">>,Version,Req)
               end,
        Path = case PathInfo == [] of
                   true -> "/";
                   false -> gui_str:binary_to_unicode_list(rest_utils:join_to_path(PathInfo))
               end,
        HandlerModule = cdmi_routes:route(PathInfo, Url),
        {ok, Req2, #state{method = Method, filepath = Path, opts = parse_opts(RawOpts), cdmi_version = CdmiVersion, handler_module = HandlerModule}}
    catch
        _Type:{badmatch,{error, Error}} -> {ok, Req, {error, Error}};
        _Type:{badmatch,Error} -> {ok, Req, {error,Error}};
        _Type:Error -> {ok, Req, {error,Error}}
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
    NewReq = case Error of
                 {user_unknown, DnString} -> rest_utils:reply_with_error(Req, error, ?error_user_unknown, [DnString], ?error_unauthorized_code);
                 unsupported_version -> rest_utils:reply_with_error(Req, error, ?error_cdmi_version_unsupported, [], ?error_bad_request_code);
                 _ -> rest_utils:reply_with_error(Req, error, ?error_bad_request, [],?error_bad_request_code)
             end,
    {halt, NewReq, error};
allowed_methods(Req, #state{handler_module = Handler} = State) ->
    Handler:allowed_methods(Req,State).

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
content_types_provided(Req, #state{handler_module = Handler} = State) -> %todo handle non-cdmi types
    Handler:content_types_provided(Req,State).

%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, #state{handler_module = Handler} = State) -> %todo handle noncdmi dir put
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
put_cdmi_container(Req,State = #state{handler_module = Handler}) ->
    Handler:put_cdmi_container(Req,State).
put_binary(Req,State = #state{handler_module = Handler}) ->
    Handler:put_binary(Req,State).
put_cdmi_object(Req,State = #state{handler_module = Handler}) ->
    Handler:put_cdmi_object(Req,State).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% parse_opts/1
%% ====================================================================
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc") will return [binary(aaa),{binary(bbb),1,2},binary(ccc)]
%% @end
-spec parse_opts(binary()) -> [binary() | {binary(), From :: integer(), To :: integer()}].
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
                    [From, To] = binary:split(Range, <<"-">>),
                    {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
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
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
    VersionList = lists:map(fun trim_spaces/1, binary:split(VersionBinary,<<",">>,[global])),
    get_supported_version(VersionList);
get_supported_version(VersionList) when is_list(VersionList) ->
    case lists:member(<<"1.0.2">>,VersionList) of
        true -> <<"1.0.2">>;
        false ->
            ?error("Request with unsupported cdmi version list: ~p",[VersionList]),
            throw(unsupported_version)
    end;
get_supported_version(undefined) -> undefined.

%% trim_spaces/1
%% ====================================================================
%% @doc trims spaces from front and end of given binary
-spec trim_spaces(binary()) -> binary().
%% ====================================================================
trim_spaces(Binary) when is_binary(Binary) ->
    list_to_binary(string:strip(binary_to_list(Binary), both, $ )).
