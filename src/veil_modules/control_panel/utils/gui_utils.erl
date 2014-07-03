%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains miscellaneous functions used commonly
%% in GUI modules.
%% @end
%% ===================================================================

-module(gui_utils).
-include_lib("ibrowse/include/ibrowse.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Initialization of n2o settings and cleanup
-export([init_n2o_ets_and_envs/3, cleanup_n2o/1]).

% Convenience functions to manipulate response headers
-export([cowboy_ensure_header/3, onrequest_adjust_headers/1]).

% Functions used to perform secure server-server http requests
-export([https_get/2, https_post/3]).

%% Maximum redirects to follow when doing http request
-define(max_redirects, 5).
%% Maximum depth of CA cert analize
-define(ca_cert_max_depth, 11).


%% ====================================================================
%% API functions
%% ====================================================================

%% init_n2o_ets_and_envs/3
%% @doc Initializes all environment settings required by n2o and creates
%% required ets tables. Should be called before starting a cowboy listener for
%% n2o GUI.
%% @end
-spec init_n2o_ets_and_envs(GuiPort :: integer(), RoutingModule :: atom(), SessionLogicModule :: atom()) -> ok.
%% ====================================================================
init_n2o_ets_and_envs(GuiPort, RoutingModule, SessionLogicModule) ->
    % Transition port - the same as gui port
    ok = application:set_env(n2o, transition_port, GuiPort),
    % Custom route handler
    ok = application:set_env(n2o, route, RoutingModule),
    % Custom session handler for n2o
    ok = application:set_env(n2o, session, gui_session_handler),
    % Custom session logic handler for gui_session_handler
    ok = application:set_env(veil_cluster_node, session_logic_module, SessionLogicModule),

    SessionLogicModule:init(),

    % Ets tables needed by n2o
    ets:new(actions, [set, named_table, {keypos, 1}, public]),
    ets:new(globals, [set, named_table, {keypos, 1}, public]),
    ets:new(caching, [set, named_table, {keypos, 1}, public]),
    ets:insert(globals, {onlineusers, 0}),
    ok.


%% cleanup_n2o/1
%% @doc Cleans up n2o setup, such as ets tables.
%% Should be called after stopping a cowboy listener for
%% n2o GUI.
%% @end
-spec cleanup_n2o(SessionLogicModule :: atom()) -> ok.
%% ====================================================================
cleanup_n2o(SessionLogicModule) ->
    SessionLogicModule:cleanup(),
    ets:delete(actions),
    ets:delete(globals),
    ets:delete(caching),
    ok.


%% cowboy_ensure_header/3
%% ====================================================================
%% @doc Sets a response header, but prevents duplicate entries. Header must
%% be normalized to lowercase (e. g. content-type and not Content-Type)
%% @end
-spec cowboy_ensure_header(Name :: binary(), Value :: binary(), Req :: req()) -> req().
%% ====================================================================
cowboy_ensure_header(Name, Value, Req) when is_binary(Name) and is_binary(Value) ->
    Req2 = cowboy_req:delete_resp_header(Name, Req),
    cowboy_req:set_resp_header(Name, Value, Req2).


%% onrequest_adjust_headers/1
%% ====================================================================
%% @doc Callback hook for cowboy to modify response headers for HTTPS GUI.
%% Those headers improve security of https connection.
%% @end
-spec onrequest_adjust_headers(Req :: req()) -> req().
%% ====================================================================
onrequest_adjust_headers(Req) ->
    Req2 = cowboy_req:set_resp_header(<<"Strict-Transport-Security">>, <<"max-age=31536000; includeSubDomains">>, Req),
    cowboy_req:set_resp_header(<<"X-Frame-Options">>, <<"SAMEORIGIN">>, Req2).


%% https_get/2
%% ====================================================================
%% @doc Performs a HTTPS GET. Host is verified according to locally installed CA certs
%% (path is provided in environment variable). Only if connection is secure,
%% the request is performed.
%% @end
-spec https_get(URLBin :: binary() | string(), ReqHeadersBin :: [{binary() | string(), binary() | string()}]) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
https_get(URLBin, ReqHeadersBin) ->
    URL = gui_str:to_list(URLBin),
    ReqHeaders = lists:map(
        fun({Key, Value}) ->
            {gui_str:to_list(Key), gui_str:to_list(Value)}
        end, ReqHeadersBin),
    perform_request(URL, ReqHeaders, get, "", ?max_redirects).


%% https_post/3
%% ====================================================================
%% @doc Performs a HTTPS POST. Host is verified according to locally installed CA certs
%% (path is provided in environment variable). Only if connection is secure,
%% the request is performed.
%% @end
-spec https_post(URLBin :: binary() | string(), ReqHeadersBin :: [{binary() | string(), binary() | string()}], Body :: binary() | string()) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
https_post(URLBin, ReqHeadersBin, Body) ->
    URL = gui_str:to_list(URLBin),
    ReqHeaders = lists:map(
        fun({Key, Value}) ->
            {gui_str:to_list(Key), gui_str:to_list(Value)}
        end, ReqHeadersBin),
    %% 0 max redirects, according to RFC post requests should not be redirected
    perform_request(URL, ReqHeaders, post, gui_str:to_list(Body), 0).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% perform_request/4
%% ====================================================================
%% @doc Performs a HTTPS request with given args.
%% @end
-spec perform_request(URL :: string(), ReqHeaders :: [{string(), string()}], Method :: atom(), Body :: binary(), Redirects :: integer()) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
perform_request(URL, ReqHeaders, Method, Body, Redirects) ->
    try
%%         {ok, {_, _, Domain, _, _, _}} = http_uri:parse(URL),
%%         There is a bug in Erlang 17.0, that has been fixed in 17.1 (which is not yet released).
%%         The bug makes the ssl gen_server crash on tls handshake, so for now https connections are off the table
%%         http://erlang.org/pipermail/erlang-questions/2014-April/078654.html
%%         curl will be used until 17.1 is released
%%         case ibrowse:send_req(URL, ReqHeaders, Method, Body, [{response_format, binary}, {ssl_options, ssl_opts(Domain)}]) of
        case do_curl(URL, ReqHeaders, Method, Body) of
            {ok, Rcode, RespHeaders, ResponseBody}
                when (Rcode =:= "301" orelse Rcode =:= "302" orelse Rcode =:= "303" orelse Rcode =:= "307") andalso Redirects > 0 ->
                % Code in {301, 302, 303, 307} - we are being redirected
                case get_redirect_url(URL, RespHeaders) of
                    undefined -> ResponseBody;
                    URL -> ResponseBody;
                    NewURL -> perform_request(NewURL, ReqHeaders, Method, Body, Redirects - 1)
                end;

            {ok, "200", _, ResponseBody} ->
                % Answer ok
                {ok, ResponseBody};

            {error, {conn_failed, {error, {tls_alert, "certificate unknown"}}}} ->
                % Host authenticity cannot be confirmed
                {error, unknown_cert};

            {error, Other} ->
                {error, Other};

            Other ->
                {error, Other}
        end
    catch
        _:M ->
            {error, M}
    end.


%% do_curl/4
%% ====================================================================
%% @doc
%% Temporary alternative for erlang ssl (which does not work in 17.0)
%% @end
-spec do_curl(URL :: string(), ReqHeaders :: string(), Method :: string(), Body :: string()) -> term().
%% ====================================================================
do_curl(URL, ReqHeaders, Method, Body) ->
    MethodString = case Method of
                       get -> " -X GET ";
                       post -> " -X POST "
                   end,
    BodyString = case Body of
                     [] -> "";
                     _ -> " -d \"" ++ Body ++ "\" "
                 end,
    HeadersString = lists:foldl(
        fun({Key, Value}, Acc) ->
            Acc ++ " -H \"" ++ Key ++ ": " ++ Value ++ "\""
        end, " ", ReqHeaders),
    CurlCommand = "curl -sL" ++ MethodString ++ URL ++ BodyString ++ HeadersString,
    Res = os:cmd(CurlCommand),
    case Res of
        "" -> {error, curl_failed};
        "\n" -> {error, curl_failed};
        RespBody -> {ok, "200", [], gui_str:to_binary(RespBody)}
    end.


%% get_redirect_url/1
%% ====================================================================
%% @doc
%% Retrieves redirect URL from a HTTP response.
%% @end
-spec get_redirect_url(OldURL :: string(), Headers :: list()) -> string().
%% ====================================================================
get_redirect_url(OldURL, Headers) ->
    Location = proplists:get_value("location", Headers, proplists:get_value("Location", Headers)),
    case Location of
        "http://" ++ _ -> Location;
        "https://" ++ _ -> Location;
        [$/ | _] = Location ->
            #url{protocol = Protocol, host = Host, port = Port} = ibrowse_lib:parse_url(OldURL),
            PortFrag = case {Protocol, Port} of
                           {http, 80} -> "";
                           {https, 443} -> "";
                           _ -> ":" ++ integer_to_list(Port)
                       end,
            atom_to_list(Protocol) ++ "://" ++ Host ++ PortFrag ++ Location;
        _ -> undefined
    end.

%% Unused function
%% %% ssl_opts/1
%% %% ====================================================================
%% %% @doc Returns list of ssl opts for secure connection.
%% %% @end
%% -spec ssl_opts(ReqHostname :: string()) -> [tuple()].
%% %% ====================================================================
%% ssl_opts(ReqHostname) ->
%%     VerifyFun =
%%         fun(_, {bad_cert, _}, RequestedHostname) ->
%%             {unknown, RequestedHostname};
%%
%%             (_, {extension, _}, RequestedHostname) ->
%%                 {unknown, RequestedHostname};
%%
%%             (_, valid, RequestedHostname) ->
%%                 {valid, RequestedHostname};
%%
%%             (Cert, valid_peer, RequestedHostname) ->
%%                 % If peer is valid, make sure one of domain names contained in cert matches our requested adress
%%                 #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Extensions}} = Cert,
%%                 AllowedHostnames = lists:foldl(
%%                     fun(#'Extension'{extnID = ExtID, extnValue = ExtVal}, Acc) ->
%%                         case ExtID of
%%                             ?'id-ce-subjectAltName' ->
%%                                 Acc ++ lists:map(
%%                                     fun({dNSName, DNSName}) ->
%%                                         % Create regexps from allowed domain names, to later match them against requested address
%%                                         ReplacedDots = re:replace(DNSName, "\\.", "\\\\.", [global, {return, list}]),
%%                                         _ReplacedWildcards = re:replace(ReplacedDots, "\\*", ".*", [global, {return, list}])
%%                                     end, ExtVal);
%%                             _ ->
%%                                 Acc
%%                         end
%%                     end, [], Extensions),
%%
%%                 Valid = lists:foldl(
%%                     fun(RegExp, Acc) ->
%%                         case re:run(RequestedHostname, RegExp) of
%%                         % At least one domain name matched, the peer is indeed valid
%%                             {match, _} -> valid;
%%                             _ -> Acc
%%                         end
%%                     end, unknown, AllowedHostnames),
%%                 {Valid, RequestedHostname}
%%         end,
%%
%%     CaCertFileAtom = case application:get_env(veil_cluster_node, root_cacert_file) of
%%                          {ok, Val} -> Val;
%%                          _ -> throw("root_cacert_file env missing")
%%                      end,
%%     % Return ssl opts for a secure connection
%%     [
%%         {verify, verify_peer},
%%         {cacertfile, atom_to_list(CaCertFileAtom)},
%%         {verify_fun, {VerifyFun, ReqHostname}},
%%         {depth, ?ca_cert_max_depth}
%%     ].
