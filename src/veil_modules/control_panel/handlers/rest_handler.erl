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
-include("logging.hrl").

-record(state, {handler_module = undefined :: atom(), version = <<"latest">> :: binary(), resource_id = undefined :: binary()}).

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
    catch _:_->
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
            ?info("[REST] Peer connected using certificate with subject: ~p ~n", [DnString]),
            put(user_id, DnString),
            {ok, _NewReq, _State} = do_init(Req);
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


%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by URL exists.
%% Will call exists/2 from rest_module_behaviour.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, #state{handler_module = undefined} = State) -> 
    {false, Req, State};

resource_exists(Req, #state{resource_id = undefined} = State) -> 
    {true, Req, State};

resource_exists(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = S) -> 
    {Exists, NewReq} = Mod:exists(Req, Version, Id),
    {Exists, NewReq, S}.


%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed for request URL.
%% Will call allowed_methods/2 from rest_module_behaviour.
%% @end
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) -> 
    {Methods, NewReq} = Mod:allowed_methods(Req, Version, Id),
    {Methods, NewReq, State}.


%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided for the request.
%% Will call content_types_provided/1|2 from rest_module_behaviour.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) -> 
    {ContentTypes, NewRew} = case Id of
        undefined -> Mod:content_types_provided(Req, Version);
        _ -> Mod:content_types_provided(Req, Version, Id)
    end,
    ContentTypesProvided = lists:zip(ContentTypes, lists:duplicate(length(ContentTypes), get_resource)),
    {ContentTypesProvided, NewRew, State}.


%% get_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests. 
%% Will call get/1|2 from rest_module_behaviour.
%% @end
-spec get_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_resource(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {Resp, NewReq} = case Id of
        undefined -> Mod:get(Req, Version);
        _ -> Mod:get(Req, Version, Id)
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
        {<<"application/x-www-form-urlencoded">>, handle_urlencoded_data},
        {<<"application/json">>, handle_json_data},
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
    Data = case rest_utils:decode_from_json(Binary) of 
        {_Type, Struct} -> Struct; 
        Other -> Other 
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
            Mod:handle_multipart_data(Req, Version, Method, Id);
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
    {Result, NewReq} = Mod:delete(Req, Version, Id),
    {Result, NewReq, State}.


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
    {_Result, _NewReq} = case Mod:validate(Req, Version, Id, Data) of
        {true, Req2} -> case cowboy_req:method(Req) of 
                {<<"POST">>, _} -> 
                    Mod:post(Req2, Version, Id, Data);
                {<<"PUT">>, _} -> 
                    Mod:put(Req2, Version, Id, Data);
                _ ->
                    {false, Req2}
            end;
        {false, Req2} -> {false, Req2} 
    end.


%% do_init/1
%% ====================================================================
%% Initializes request context after the peer has been validated.
%% @end
-spec do_init(req()) -> {ok, req(), #state{}}.
%% ====================================================================
do_init(Req) ->
    {Version, _} = cowboy_req:binding(version, Req), % :version in cowboy router
    {PathInfo, _} = cowboy_req:path_info(Req),
    {Module, Id} = case rest_routes:route(PathInfo) of 
        undefined -> {undefined, undefined}; 
        {Mod, ID} -> {Mod, ID} 
    end,
    Req2 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req),
    {ok, Req2, #state{handler_module = Module, version = Version, resource_id = Id}}. 

