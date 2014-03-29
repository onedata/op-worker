%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles REST requests by routing them to proper rest module.
%% @end
%% ===================================================================

-module(rest_handler).

-include_lib("public_key/include/public_key.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("err.hrl").

-record(state, {
    version = <<"latest">> :: binary(),
    method = <<"GET">> :: binary(),
    handler_module = undefined :: atom(),
    resource_id = undefined :: binary()
}).

-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2, get_resource/2]).
-export([content_types_accepted/2, delete_resource/2, handle_urlencoded_data/2, handle_json_data/2, handle_multipart_data/2]).


%% ====================================================================
%% API functions
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
rest_init(Req, _Opts) ->
    {OtpCert, Certs} = try
        {ok, PeerCert} = ssl:peercert(cowboy_req:get(socket, Req)),
        {ok, {Serial, Issuer}} = public_key:pkix_issuer_id(PeerCert, self),
        [{_, [TryOtpCert | TryCerts], _}] = ets:lookup(gsi_state, {Serial, Issuer}),
        {TryOtpCert, TryCerts}
                       catch
                           _:_ ->
                               ?error("[REST] Peer connected but cerificate chain was not found. Please check if GSI validation is enabled."),
                               erlang:error(invalid_cert)
                       end,

    case gsi_handler:call(gsi_nif, verify_cert_c,
        [public_key:pkix_encode('OTPCertificate', OtpCert, otp),                    %% peer certificate
            [public_key:pkix_encode('OTPCertificate', Cert, otp) || Cert <- Certs], %% peer CA chain
            [DER || [DER] <- ets:match(gsi_state, {{ca, '_'}, '$1', '_'})],         %% cluster CA store
            [DER || [DER] <- ets:match(gsi_state, {{crl, '_'}, '$1', '_'})]]) of    %% cluster CRL store
        {ok, 1} ->
            {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
            {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
            {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
            {ok, _Req, _State} = do_init(Req, DnString);
        {ok, 0, Errno} ->
            ?info("[REST] Peer ~p was rejected due to ~p error code", [OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject, Errno]),
            erlang:error({gsi_error_code, Errno});
        {error, Reason} ->
            ?error("[REST] GSI peer verification callback error: ~p", [Reason]),
            erlang:error(Reason);
        Other ->
            ?error("[REST] GSI verification callback returned unknown response ~p", [Other]),
            erlang:error({gsi_unknown_response, Other})
    end.


%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed, based on version specified in URI.
%% Will call methods_and_version_info/1 from rest_module_behaviour.
%% @end
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, #state{version = Version, handler_module = Mod} = State) ->
    {MethodsVersionInfo, Req2} = Mod:methods_and_versions_info(Req),
    RequestedVersion = case Version of
                           <<"latest">> ->
                               {Ver, _} = lists:last(MethodsVersionInfo),
                               Ver;
                           Ver ->
                               Ver
                       end,
    % Check if requested version is supported
    case proplists:get_value(RequestedVersion, MethodsVersionInfo, undefined) of
        undefined ->
            NewReq = reply_with_error(Req2, warning, ?error_version_unsupported, [binary_to_list(RequestedVersion)]),
            {halt, NewReq, State};
        AllowedMethods ->
            {Method, _} = cowboy_req:method(Req2),
            % Check if requested method is allowed
            case lists:member(Method, AllowedMethods) of
                false ->
                    ErrorRec = ?report_warning(?error_method_unsupported, [binary_to_list(Method)]),
                    NewReq = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), Req2),
                    {AllowedMethods, NewReq, State};
                true ->
                    % Check if content-type is acceptable (for PUT or POST)
                    case (Method =/= <<"POST">> andalso Method =/= <<"PUT">>) orelse (content_type_supported(Req2)) of
                        false ->
                            NewReq = reply_with_error(Req2, warning, ?error_media_type_unsupported, []),
                            {halt, NewReq, State};
                        true ->
                            {AllowedMethods, Req2, State#state{version = RequestedVersion}}
                    end
            end
    end;

% Some errors could have been detected in do_init/2. If so, State contains
% an {error, Type} tuple. These errors shall be handled here,
% because cowboy doesn't allow returning errors in rest_init.
allowed_methods(Req, {error, Type}) ->
    NewReq = case Type of
                 path_invalid -> reply_with_error(Req, warning, ?error_path_invalid, []);
                 {user_unknown, DnString} -> reply_with_error(Req, error, ?error_user_unknown, [DnString])
             end,
    {halt, NewReq, error}.


%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided. "application/json" is default.
%% It can be changed later by gui_utils:cowboy_ensure_header/3.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, State) ->
    {[{<<"application/json">>, get_resource}], Req, State}.


%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by URI exists.
%% Will call exists/3 from rest_module_behaviour.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, #state{handler_module = undefined} = State) ->
    {false, Req, State};

resource_exists(Req, #state{resource_id = undefined} = State) ->
    {true, Req, State};

resource_exists(Req, #state{version = Version, handler_module = Mod, resource_id = Id} = State) ->
    {Exists, NewReq} = Mod:exists(Req, Version, Id),
    case Exists of
        false ->
            ErrorRec = ?report_warning(?error_not_found, [binary_to_list(Id)]),
            Req2 = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), NewReq),
            {false, Req2, State};
        true ->
            {true, NewReq, State}
    end
.


%% get_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests. 
%% Will call get/3 from rest_module_behaviour.
%% @end
-spec get_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_resource(Req, #state{version = Version, handler_module = Mod, resource_id = Id} = State) ->
    {Answer, Req2} = Mod:get(Req, Version, Id),
    % process_callback_answer/2 cannot be used here as cowboy expects other returned values
    {Resp, NewReq} = case Answer of
                         ok ->
                             {halt, Req2};
                         {body, ResponseBody} ->
                             {ResponseBody, Req2};
                         {stream, Size, Fun, ContentType} ->
                             Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, Req2),
                             {{stream, Size, Fun}, Req3};
                         error ->
                             {ok, Req3} = cowboy_req:reply(500, Req2),
                             {halt, Req3};
                         {error, ErrorDesc} ->
                             Req3 = cowboy_req:set_resp_body(ErrorDesc, Req2),
                             {ok, Req4} = cowboy_req:reply(500, Req3),
                             {halt, Req4}
                     end,
    {Resp, NewReq, State}.


%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what 
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, handle_urlencoded_data},
        {{<<"application">>, <<"json">>, '*'}, handle_json_data},
        {{<<"multipart">>, <<"form-data">>, '*'}, handle_multipart_data}
    ], Req, State}.


%% handle_urlencoded_data/2
%% ====================================================================
%% @doc Function handling "application/x-www-form-urlencoded" requests.
%% @end
-spec handle_urlencoded_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_urlencoded_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {ok, Data, Req2} = cowboy_req:body_qs(Req),
    {Result, NewReq} = handle_data(Req2, Mod, Version, Id, Data),
    {Result, NewReq, State}.


%% handle_json_data/2
%% ====================================================================
%% @doc Function handling "application/json" requests.
%% @end
-spec handle_json_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_json_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {ok, Binary, Req2} = cowboy_req:body(Req),
    Data = case Binary of
               <<"">> ->
                   <<"">>;
               _ ->
                   case rest_utils:decode_from_json(Binary) of
                       {_Type, Struct} -> Struct;
                       Other -> Other
                   end
           end,
    {Result, NewReq} = handle_data(Req2, Mod, Version, Id, Data),
    {Result, NewReq, State}.


%% handle_multipart_data/2
%% ====================================================================
%% @doc Function handling "multipart/form-data" requests.
%% @end
-spec handle_multipart_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_multipart_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {Result, NewReq} = case erlang:function_exported(Mod, handle_multipart_data, 4) of
                           true ->
                               {Method, _} = cowboy_req:method(Req),
                               {Answer, Req2} = Mod:handle_multipart_data(Req, Version, Method, Id),
                               process_callback_answer(Answer, Req2);
                           false ->
                               {false, Req}
                       end,
    {Result, NewReq, State}.


%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests. 
%% Will call delete/2 from rest_module_behaviour.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {Answer, Req2} = Mod:delete(Req, Version, Id),
    {Result, Req3} = process_callback_answer(Answer, Req2),
    {Result, Req3, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% handle_data/5
%% ====================================================================
%% Handles POST or PUT requests.
%% Will call post/2 or put/3 from rest_module_behaviour.
%% @end
-spec handle_data(req(), atom(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
handle_data(Req, Mod, Version, Id, Data) ->
    {Answer, Req2} = case cowboy_req:method(Req) of
                         {<<"POST">>, _} ->
                             Mod:post(Req, Version, Id, Data);
                         {<<"PUT">>, _} ->
                             Mod:put(Req, Version, Id, Data)
                     end,
    process_callback_answer(Answer, Req2).


%% do_init/2
%% ====================================================================
%% Initializes request context after the peer has been validated. Checks if user
%% exists in the database and requested URI is supported.
%% @end
-spec do_init(req(), string()) -> {ok, req(), #state{} | {error, term()}}.
%% ====================================================================
do_init(Req, DnString) ->
    Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, <<"application/json">>, Req),
    case user_logic:get_user({dn, DnString}) of
        {ok, _} ->
            put(user_id, DnString),
            ?info("[REST] Peer connected using certificate with subject: ~p ~n", [DnString]),
            {PathInfo, _} = cowboy_req:path_info(Req2),
            case rest_routes:route(PathInfo) of
                undefined ->
                    {ok, Req2, {error, path_invalid}};
                {Module, Id} ->
                    {Method, _} = cowboy_req:method(Req2),
                    {Version, _} = cowboy_req:binding(version, Req2), % :version in cowboy router
                    Req3 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req2),
                    {ok, Req3, #state{version = Version, handler_module = Module, method = Method, resource_id = Id}}
            end;
        _ ->
            {ok, Req2, {error, {user_unknown, DnString}}}
    end.


%% process_callback_answer/1
%% ====================================================================
%% Unifies replying from PUT / POST / DELETE requests - first argument is response
%% from a callback, that should conform to rules specified in rest_module_behaviour
%% @end
-spec process_callback_answer(term(), req()) -> {true | false, req()}.
%% ====================================================================
process_callback_answer(Answer, Req) ->
    case Answer of
        ok ->
            {true, Req};
        {body, ResponseBody} ->
            NewReq = cowboy_req:set_resp_body(ResponseBody, Req),
            {true, NewReq};
        {stream, Size, Fun, ContentType} ->
            Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, Req),
            Req3 = cowboy_req:set_resp_body_fun(Size, Fun, Req2),
            {true, Req3};
        error ->
            {false, Req};
        {error, ErrorDesc} ->
            Req2 = cowboy_req:set_resp_body(ErrorDesc, Req),
            {false, Req2}
    end.


%% reply_with_error/2
%% ====================================================================
%% Replies with 500 error cose, content-type set to application/json and
%% an error message
%% @end
-spec reply_with_error(req(), atom(), {string(), string()}, list()) -> req().
%% ====================================================================
reply_with_error(Req, Severity, ErrorDesc, Args) ->
    ErrorRec = case Severity of
                   warning -> ?report_warning(ErrorDesc, Args);
                   error -> ?report_error(ErrorDesc, Args);
                   alert -> ?report_alert(ErrorDesc, Args)
               end,
    Req2 = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), Req),
    {ok, Req3} = cowboy_req:reply(500, Req2),
    Req3.


%% content_type_supported/2
%% ====================================================================
%% Checks if request content-type is supported by rest modules.
%% @end
-spec content_type_supported(req()) -> boolean().
%% ====================================================================
content_type_supported(Req) ->
    {CTA, _, _} = content_types_accepted(Req, []),
    {Ans, ContentType, _} = cowboy_req:parse_header(<<"content-type">>, Req),
    case Ans of
        ok ->
            lists:foldl(
                fun({{Type, Subtype, _}, _}, Acc) ->
                    case ContentType of
                        {Type, Subtype, _} -> Acc orelse true;
                        _ -> Acc orelse false
                    end
                end, false, CTA);
        _ ->
            false
    end.