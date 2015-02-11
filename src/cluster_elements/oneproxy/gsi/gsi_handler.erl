%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Konrad Zemek
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module manages GSI validation
%%% @end
%%%-------------------------------------------------------------------
-module(gsi_handler).
-author("Rafal Slota").
-author("Konrad Zemek").

-include("global_definitions.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% How many slave nodes that loads GSI NIF has to be started
-define(GSI_SLAVE_COUNT, 2).

%% Proxy Certificate Extension ID
-define(PROXY_CERT_EXT, {1, 3, 6, 1, 5, 5, 7, 1, 14}).

-deprecated([proxy_subject/1]).

%% API
-export([init/0, get_certs_from_req/2, verify_callback/3, load_certs/1, update_crls/1, proxy_subject/1, call/3,
    is_proxy_certificate/1, find_eec_cert/3, get_ca_certs/0, get_ca_certs_from_all_cert_dirs/0, strip_self_signed_ca/1, get_ciphers/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes GSI Handler. This method should be called once, before using any other method from this module.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    case application:get_env(?APP_NAME, node_type) of
        {ok, ccm} ->
            throw(ccm_node); % ccm node doesn't have socket interface, so GSI would be useless
        _ -> ok
    end,
    ?info("GSI Handler module is starting"),
    ets:new(gsi_state, [{read_concurrency, true}, public, ordered_set, named_table]),
    {ok, CADir} = application:get_env(?APP_NAME, ca_dir),
    {SSPid, _Ref} = spawn_monitor(fun() -> start_slaves(?GSI_SLAVE_COUNT) end),

    case filelib:is_dir(CADir) of
        true ->
            load_certs(CADir),
            case utils:ensure_running(inets) of
                ok ->
                    update_crls(CADir),
                    catch oneproxy:ca_crl_to_der(oneproxy:get_der_certs_dir()),
                    {ok, CRLUpdateInterval} = application:get_env(?APP_NAME, crl_update_interval),
                    case timer:apply_interval(CRLUpdateInterval, ?MODULE, update_crls, [CADir]) of
                        {ok, _} -> ok;
                        {error, Reason} ->
                            ?error("GSI Handler: Setting CLR auto-update failed (reason: ~p)", [Reason])
                    end;

                {error, Reason} ->
                    ?error("GSI Handler: Cannot update CRLs because inets didn't start (reason: ~p)", [Reason])
            end;

        false ->
            ?error("GSI Handler: Cannot find GSI CA certs dir (~p)", [CADir])
    end,

    receive
        {'DOWN', _, process, SSPid, normal} ->
            ?info("GSI Handler module successfully loaded");
        {'DOWN', _, process, SSPid, Reason1} ->
            ?warning("GSI Handler: slave node loader unknown exit reason: ~p", [Reason1]),
            ?info("GSI Handler module partially loaded")
    after 5000 ->
        ?error("GSI Handler: slave node loader execution timeout, state unknown"),
        ?info("GSI Handler module partially loaded")
    end,

    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns client's certificates based on cowboy's request.
%% If there is not valid GSI session, {error, no_gsi_session} is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_certs_from_req(OneProxyHandler :: pid() | atom(), Req :: term()) ->
    {ok, {OtpCert :: #'OTPCertificate'{}, Chain :: [#'OTPCertificate'{}]}} |
    {error, no_gsi_session} | {error, Reason :: term()}.
get_certs_from_req(OneProxyHandler, Req) ->
    {SessionId, _} = cowboy_req:header(<<"onedata-internal-client-session-id">>, Req),
    case SessionId of
        undefined ->
            {error, no_gsi_session};
        SessionId ->
            case oneproxy:get_session(OneProxyHandler, SessionId) of
                {error, Reason} ->
                    {error, Reason};
                {ok, SessionData} ->
                    ClientCerts = base64:decode(SessionData),
                    [OtpCert | Certs] = lists:reverse([public_key:pkix_decode_cert(DER, otp) || {'Certificate', DER, _} <- public_key:pem_decode(ClientCerts)]),
                    {ok, {OtpCert, Certs}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns all CA certificates (from cacerts dir), extended with ca's from certs dir.
%% Returns list of DER encoded entities.
%%--------------------------------------------------------------------
-spec get_ca_certs_from_all_cert_dirs() -> [binary()] | no_return().
get_ca_certs_from_all_cert_dirs() ->
    {ok, CertDir1} = application:get_env(?APP_NAME, certs_dir),
    CertDir = atom_to_list(CertDir1),
    get_ca_certs() ++ get_ca_certs_from_dir(CertDir).

%%--------------------------------------------------------------------
%% @doc
%% Returns all CA certificates (from cacerts dir) as an list of DER encoded entities
%% @end
%%--------------------------------------------------------------------
-spec get_ca_certs() -> [binary()] | no_return().
get_ca_certs() ->
    {ok, CADir} = application:get_env(?APP_NAME, ca_dir),
    get_ca_certs_from_dir(CADir).

%%--------------------------------------------------------------------
%% @doc
%% Returns all CA certificates from given dir as an list of DER encoded entities
%% @end
%%--------------------------------------------------------------------
-spec get_ca_certs_from_dir(CADir :: string()) -> [binary()] | no_return().
get_ca_certs_from_dir(CADir) ->
    {ok, Files} = file:list_dir(CADir),

    %% Get only files with .pem extension
    CA1 = [{strip_filename_ext(Name), file:read_file(filename:join(CADir, Name))} || Name <- Files, lists:suffix(".pem", Name)],
    CA2 = [lists:map(fun(Y) ->
        {Name, Y} end, public_key:pem_decode(X)) || {Name, {ok, X}} <- CA1],
    _CA2 = [X || {_Name, {'Certificate', X, not_encrypted}} <- lists:flatten(CA2)].

%%--------------------------------------------------------------------
%% @doc
%% Returns given list of certificates but without self-signed ones.
%% @end
%%--------------------------------------------------------------------
-spec strip_self_signed_ca([binary()]) -> [binary()] | no_return().
strip_self_signed_ca(DERList) when is_list(DERList) ->
    Stripped = lists:map(
        fun(DER) ->
            OTPCert = public_key:der_decode('OTPCertificate', DER),
            case public_key:pkix_is_self_signed(OTPCert) of
                true -> [];
                false -> [DER]
            end
        end, DERList),
    lists:flatten(Stripped).

%%--------------------------------------------------------------------
%% @doc
%% This method is an registered callback, called foreach peer certificate. <br/>
%% This callback saves whole certificate chain in GSI ETS based state for further use.
%% @end
%%--------------------------------------------------------------------
-spec verify_callback(OtpCert :: #'OTPCertificate'{}, Status :: term(), Certs :: [#'OTPCertificate'{}]) ->
    {valid, UserState :: term()} | {fail, Reason :: term()}.
verify_callback(OtpCert, valid_peer, Certs) ->
    Serial = save_cert_chain([OtpCert | Certs]),
    ?debug("Peer ~p connected", [Serial]),
    {valid, []};
verify_callback(OtpCert, {bad_cert, unknown_ca}, Certs) ->
    save_cert_chain([OtpCert | Certs]),
    {valid, [OtpCert | Certs]};
verify_callback(OtpCert, _IgnoredError, Certs) ->
    case Certs of
        [OtpCert | _] -> {valid, Certs};
        _ -> {valid, [OtpCert | Certs]}
    end.

%% load_certs/1
%% ====================================================================
%% @doc Loads all PEM encoded CA certificates from given directory along with their CRL certificates (if any). <br/>
%%      Note that CRL certificates should also be PEM encoded and the CRL filename should match their CA filename but with '.crl' extension.
%% @end
-spec load_certs(CADir :: string()) -> ok | no_return().
%% ====================================================================
load_certs(CADir) ->
    ?info("GSI Handler: Loading CA certs from dir ~p", [CADir]),
    {ok, Files} = file:list_dir(CADir),
    CA1 = [{strip_filename_ext(Name), file:read_file(filename:join(CADir, Name))} || Name <- Files, lists:suffix(".pem", Name)],
    CRL1 = [{strip_filename_ext(Name), file:read_file(filename:join(CADir, strip_filename_ext(Name) ++ ".crl"))} || Name <- Files, lists:suffix(".pem", Name)],
    CA2 = [lists:map(fun(Y) ->
        {Name, Y} end, public_key:pem_decode(X)) || {Name, {ok, X}} <- CA1],
    CRL2 = [lists:map(fun(Y) ->
        {Name, Y} end, public_key:pem_decode(X)) || {Name, {ok, X}} <- CRL1],

    {Len1, Len2} =
        lists:foldl(
            fun({Name, {Type, X, _}}, {CAs, CRLs}) ->
                case Type of
                    'Certificate' ->
                        case catch public_key:pkix_issuer_id(X, self) of
                            {ok, {SerialNumber, Issuer}} ->
                                ets:insert(gsi_state, {{ca, {SerialNumber, Issuer}}, X, Name}),
                                {CAs + 1, CRLs};
                            _ ->
                                case catch public_key:pkix_issuer_id(X, other) of
                                    {ok, {SerialNumber, Issuer}} ->
                                        ets:insert(gsi_state, {{ca, {SerialNumber, Issuer}}, X, Name}),
                                        {CAs + 1, CRLs};
                                    _ -> {CAs, CRLs}
                                end
                        end;

                    'CertificateList' ->
                        CertificateList = public_key:der_decode(Type, X),
                        #'CertificateList'{tbsCertList = TBSCertList} = CertificateList,
                        #'TBSCertList'{issuer = Issuer} = TBSCertList,
                        ets:insert(gsi_state, {{crl, Issuer}, X, Name}),
                        {CAs, CRLs + 1};

                    _ -> {CAs, CRLs}
                end
            end, {0, 0}, lists:flatten(CA2 ++ CRL2)),
    ?info("GSI Handler: ~p CA and ~p CRL certs successfully loaded", [Len1, Len2]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates CRL certificates based on their distribution point (x509 CA extension).
%% @end
%%--------------------------------------------------------------------
-spec update_crls(CADir :: string()) -> ok | no_return().
update_crls(CADir) ->
    ?info("GSI Handler: Updating CLRs from distribution points"),
    CAs = [{public_key:pkix_decode_cert(X, otp), Name} || [X, Name] <- ets:match(gsi_state, {{ca, '_'}, '$1', '$2'})],
    CAsAndDPs = [{OtpCert, get_dp_url(OtpCert), Name} || {OtpCert, Name} <- CAs],
    utils:pforeach(fun(X) -> update_crl(CADir, X) end, CAsAndDPs),
    load_certs(CADir).

%%--------------------------------------------------------------------
%% @doc
%% Returns subject of given certificate.
%% If proxy certificate is given, EEC subject is returned.
%% @end
%% @deprecated The function shall not be used when proxy is not directly signed by EEC (which you can't be sure about without x509 chain). <br/>
%% Use {@link gsi_handler:find_eec_cert/3} instead and get EEC's subject (if you have the whole certificate chain available).
%% @end
%%--------------------------------------------------------------------
-spec proxy_subject(OtpCert :: #'OTPCertificate'{}) -> {rdnSequence, [#'AttributeTypeAndValue'{}]}.
proxy_subject(OtpCert = #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{} = TbsCert}) ->
    Subject = TbsCert#'OTPTBSCertificate'.subject,
    case is_proxy_certificate(OtpCert) of
        true -> %% Delete last 'common name' attribute, because its proxy-specific
            {rdnSequence, Attrs} = Subject,
            Attrs1 = lists:keydelete(?'id-at-commonName', 2, lists:reverse(Attrs)),
            {rdnSequence, lists:reverse(Attrs1)};
        false ->
            Subject
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves whole given certificate chain in GSI ETS based state for further use. <br/>
%% EEC certificate pkix_issuer_id is used as ETS key for new entry. <br/>
%% Saved chain will is scheduled to removal when EEC certificate expires
%% @end
%%--------------------------------------------------------------------
-spec save_cert_chain([OtpCert :: #'OTPCertificate'{}]) -> Serial :: integer().
save_cert_chain([OtpCert | Certs]) ->
    {ok, {Serial, Issuer}} = public_key:pkix_issuer_id(OtpCert, self),
    case ets:lookup(gsi_state, {Serial, Issuer}) of
        [{_, _, TRef1}] -> timer:cancel(TRef1);
        _ -> ok
    end,
    TBSCert = OtpCert#'OTPCertificate'.tbsCertificate,
    {'Validity', _NotBeforeStr, NotAfterStr} = TBSCert#'OTPTBSCertificate'.validity,
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    NotAfter = time_str_2_gregorian_sec(NotAfterStr),
    {ok, TRef} = timer:apply_after(timer:seconds(NotAfter - Now), ets, delete, [gsi_state, {Serial, Issuer}]),
    ets:insert(gsi_state, {{Serial, Issuer}, [OtpCert | Certs], TRef}),
    Serial.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes Count slave nodes. See {@link initialize_node/1}.
%% @end
%%--------------------------------------------------------------------
-spec start_slaves(Count :: non_neg_integer()) -> [term()].
start_slaves(Count) when Count >= 0 ->
    [initialize_node(list_to_atom(atom_to_list(get_node_name()) ++ "_gsi" ++ integer_to_list(N))) || N <- lists:seq(1, Count)].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes slave node with given NodeName. Starts it and loads NIF library. <br/>
%% If NIF load fails, slave node is stopped.
%% @end
%%--------------------------------------------------------------------
-spec initialize_node(NodeName :: atom()) -> term().
initialize_node(NodeName) when is_atom(NodeName) ->
    ?info("Trying to start GSI slave node: ~p @ ~p", [NodeName, get_host()]),
    case slave:start(get_host(), NodeName, make_code_path() ++ " -setcookie \"" ++ atom_to_list(erlang:get_cookie()) ++ "\"", no_link, erl) of
        {error, {already_running, Node}} ->
            ?info("GSI slave node ~p is already running", [Node]),
            ets:insert(gsi_state, {node, NodeName});
        {ok, Node} ->
            ?info("GSI slave node ~p started", [Node]),
            ets:insert(gsi_state, {node, NodeName});
        {error, Reason} ->
            ?error("Could not start GSI slave node ~p @ ~p due to error: ~p", [NodeName, get_host(), Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls apply(Module, Method, Args) on one of started slave nodes.
%% If slave node is down, initializes restart procedure and tries to use another node. <br/>
%% However is all nodes are down, error is returned and GSI action is interrupted (e.g. peer verification fails).
%% @end
%%--------------------------------------------------------------------
-spec call(Module :: atom(), Method :: atom(), Args :: [term()]) -> ok | no_return().
call(Module, Method, Args) ->
    case ets:info(gsi_state) of
        undefined -> error(gsi_handler_not_loaded);
        _ -> ok
    end,
    Nodes = ets:lookup(gsi_state, node),
    call(Module, Method, Args, Nodes).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% See {@link call/3}
%% @end
%%--------------------------------------------------------------------
-spec call(Module :: atom(), Method :: atom(), Args :: [term()], [Node :: atom()]) -> Response :: term().
call(_Module, _Method, _Args, []) ->
    spawn(fun() -> start_slaves(?GSI_SLAVE_COUNT) end),
    ?error("No GSI slave nodes. Trying to reinitialize module"),
    {error, verification_nodes_down};
call(Module, Method, Args, [{node, NodeName} | OtherNodes]) ->
    case rpc:call(get_node(NodeName), Module, Method, Args) of
        {badrpc, {'EXIT', Exit}} -> {'EXIT', Exit};
        {badrpc, Reason} ->
            spawn(fun() -> initialize_node(NodeName) end),
            ?error("GSI slave node ~p is down (reason ~p). Trying to reinitialize node", [get_node(NodeName), Reason]),
            call(Module, Method, Args, OtherNodes);
        Res -> Res
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get ciphers that are available on server.
%% @end
%%--------------------------------------------------------------------
-spec get_ciphers() -> Ciphers :: [{KeyExchange :: atom(), Cipther :: atom(), Hash :: atom()}].
get_ciphers() ->
    lists:filter(
        fun
            ({_, des_cbc, _}) -> false;
            ({_, _, _}) -> true
        end, ssl:cipher_suites()).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles CRL update process for given CRL certificate. <br/>
%% This method gets already prepared URLs and destination file name.
%% @end
%%--------------------------------------------------------------------
-spec update_crl(CADir :: string(), {OtpCert :: #'OTPCertificate'{}, [URLs :: string()], Name :: string()}) -> ok | {error, no_dp}.
update_crl(_CADir, {_OtpCert, [], _Name}) ->
    {error, no_dp};
update_crl(CADir, {OtpCert, [URL | URLs], Name}) ->
    case httpc:request(get, {URL, []}, [{timeout, 10000}], [{body_format, binary}, {full_result, false}]) of
        {ok, {200, Binary}} ->
            case binary_to_crl(Binary) of
                {true, CertList} ->
                    PubKey = get_pubkey(OtpCert),
                    #'CertificateList'{tbsCertList = TBSCertList, signature = {0, Signature}, signatureAlgorithm = AlgorithmIdent} = CertList,
                    #'AlgorithmIdentifier'{algorithm = Oid} = AlgorithmIdent,
                    {DigestType, _} = public_key:pkix_sign_types(Oid),
                    Der = public_key:der_encode('TBSCertList', TBSCertList),

                    case public_key:verify(Der, DigestType, Signature, PubKey) of
                        true ->
                            ?info("GSI Handler: Saving new CLR of ~p", [Name]),
                            PemEntry = public_key:pem_entry_encode('CertificateList', CertList),
                            Data = public_key:pem_encode([PemEntry]),
                            Filename = filename:join(CADir, Name ++ ".crl"),
                            case file:write_file(Filename, Data) of
                                ok ->
                                    file:change_mode(Filename, 8#644);

                                {error, Reason} ->
                                    ?error("GSI Handler: Failed to save CLR of ~p (path: ~p, reason: ~p)", [Name, Filename, Reason])
                            end,
                            ok;

                        false ->
                            ?warning("GSI Handler: CLR of ~p retrieved from ~p couldn't be verified", [Name, URL]),
                            update_crl(CADir, {OtpCert, URLs, Name})
                    end;

                false ->
                    ?warning("GSI Handler: CLR of ~p retrieved from ~p couldn't be decoded", [Name, URL]),
                    update_crl(CADir, {OtpCert, URLs, Name})
            end;

        {ok, {Status, _}} ->
            ?warning("GSI Handler: Updating CLR of ~p from URL ~p failed (status: ~p)", [Name, URL, Status]),
            update_crl(CADir, {OtpCert, URLs, Name});

        {error, Reason} ->
            ?warning("GSI Handler: Updating CLR of ~p from URL ~p failed (reason: ~p)", [Name, URL, Reason]),
            update_crl(CADir, {OtpCert, URLs, Name})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a der or pem encoded binary CRL to a CertificateList record.
%% @end
%%--------------------------------------------------------------------
-spec binary_to_crl(Binary :: binary()) -> {true, #'CertificateList'{}} | false.
binary_to_crl(Binary) ->
    try
        try
            {true, public_key:der_decode('CertificateList', Binary)}
        catch error:_ ->
            [{'CertificateList', Entry, not_encrypted} | _] = public_key:pem_decode(Binary),
            {true, public_key:der_decode('CertificateList', Entry)}
        end
    catch
        error:_ -> false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves public key from an OTP Certificate.
%% @end
%%--------------------------------------------------------------------
-spec get_pubkey(OtpCert :: #'OTPCertificate'{}) ->
    Key :: public_key:public_key().
get_pubkey(OtpCert) ->
    #'OTPCertificate'{tbsCertificate = TbsCert} = OtpCert,
    #'OTPTBSCertificate'{subjectPublicKeyInfo = PKI} = TbsCert,
    #'OTPSubjectPublicKeyInfo'{subjectPublicKey = Key} = PKI,
    Key.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns current erlang VM host name (as atom).
%% @end
%%--------------------------------------------------------------------
-spec get_host() -> Host :: atom().
get_host() ->
    Node = atom_to_list(node()),
    [_, Host] = string:tokens(Node, "@"),
    list_to_atom(Host).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns current erlang VM node name (as atom).
%% @end
%%--------------------------------------------------------------------
-spec get_node_name() -> NodeName :: atom().
get_node_name() ->
    Node = atom_to_list(node()),
    [Name, _] = string:tokens(Node, "@"),
    list_to_atom(Name).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns 'NodeName@get_host()' atom. Basically it uses given NodeName to generate full node spec (using current host name)
%% @end
%%--------------------------------------------------------------------
-spec get_node(NodeName :: atom()) -> Node :: atom().
get_node(NodeName) ->
    list_to_atom(atom_to_list(NodeName) ++ "@" ++ atom_to_list(get_host())).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns current code path string, formatted as erlang slave node argument.
%% @end
%%--------------------------------------------------------------------
-spec make_code_path() -> string().
make_code_path() ->
    lists:foldl(fun(Node, Path) -> " -pa " ++ Node ++ Path end,
        [], code:get_path()).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extracts from given OTP certificate list of distribution point's HTTP URLs (based on x509 DP extension)
%% @end
%%--------------------------------------------------------------------
-spec get_dp_url(OtpCert :: #'OTPCertificate'{}) -> [URL :: string()].
get_dp_url(OtpCert = #'OTPCertificate'{}) ->
    Ext = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions,
    try Ext of
        List when is_list(List) ->
            DPs = lists:flatten([X || #'Extension'{extnValue = X} <- Ext, is_list(X)]),
            GNames = [GenNames || #'DistributionPoint'{distributionPoint = {fullName, GenNames}} <- DPs],
            [URL || {uniformResourceIdentifier, URL} <- lists:flatten(GNames), lists:prefix("http", URL)];
        _ ->
            []
    catch
        Type:Reason ->
            ?error("Cannot read extensions from certificate ~p due to ~p: ~p", [OtpCert, Type, Reason]),
            []
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Strips extension from given filename.
%% @end
%%--------------------------------------------------------------------
-spec strip_filename_ext(FileName :: string()) -> FileName :: string().
strip_filename_ext(FileName) when is_list(FileName) ->
    filename:rootname(FileName).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks is given OTP Certificate has an proxy extension or looks like legacy proxy.
%% 'maybe' is returned for proxy legacy proxy certificates since, there's no way to be sure about it
%% 'true' is returned only for RFC compliant Proxy Certificates.
%% @end
%%--------------------------------------------------------------------
-spec is_proxy_certificate(OtpCert :: #'OTPCertificate'{}) -> boolean() | maybe.
is_proxy_certificate(OtpCert = #'OTPCertificate'{}) ->
    Ext = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions,
    Subject = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject,
    {rdnSequence, Attrs} = Subject,

    %% Get all subject's RDNs
    FlatAttrs = [Attr || #'AttributeTypeAndValue'{} = Attr <- lists:flatten(Attrs)],
    ReversedAttrs = lists:reverse(FlatAttrs),

    %% Check for legacy proxy and use the result as init status for RFC check
    LegacyStatus =
        case ReversedAttrs of
            [#'AttributeTypeAndValue'{type = ?'id-at-commonName', value = {_, "proxy"}} | _] ->
                maybe;
            _ -> false
        end,

    %% Proceed with RFC proxy check
    case Ext of
        Exts when is_list(Exts) ->
            lists:foldl(
                fun(#'Extension'{extnID = ?PROXY_CERT_EXT}, _) ->
                    true;
                    (_, AccIn) ->
                        AccIn
                end, LegacyStatus, Ext);
        _ ->
            false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given proxy certificate returns its EEC
%% @end
%%--------------------------------------------------------------------
-spec find_eec_cert(CurrentOtp :: #'OTPCertificate'{}, Chain :: [#'OTPCertificate'{}], IsProxy :: boolean()) -> {ok, #'OTPCertificate'{}} | no_return().
find_eec_cert(CurrentOtp, Chain, maybe) ->
    ?warning("Processing non RFC compliant Proxy Certificate with subject: ~p", [CurrentOtp#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject]),
    find_eec_cert(CurrentOtp, Chain, true);
find_eec_cert(CurrentOtp, Chain, true) ->
    false = public_key:pkix_is_self_signed(CurrentOtp),
    {ok, NextCert} =
        lists:foldl(fun(_, {ok, Found}) -> {ok, Found};
            (Cert, NotFound) ->
                case public_key:pkix_is_issuer(CurrentOtp, Cert) of
                    true -> {ok, Cert};
                    false -> NotFound
                end end,
            no_cert, Chain),
    find_eec_cert(NextCert, Chain, is_proxy_certificate(NextCert));
find_eec_cert(CurrentOtp, _Chain, false) ->
    {ok, CurrentOtp}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @see pubkey_cert:time_str_2_gregorian_sec/1
%% @end
%%--------------------------------------------------------------------
-spec time_str_2_gregorian_sec(TimeStr :: term()) -> integer().
time_str_2_gregorian_sec({utcTime, [Y1, Y2, M1, M2, D1, D2, H1, H2, M3, M4, S1, S2, Z]}) ->
    case list_to_integer([Y1, Y2]) of
        N when N >= 70 ->
            time_str_2_gregorian_sec({generalTime,
                [$1, $9, Y1, Y2, M1, M2, D1, D2,
                    H1, H2, M3, M4, S1, S2, Z]});
        _ ->
            time_str_2_gregorian_sec({generalTime,
                [$2, $0, Y1, Y2, M1, M2, D1, D2,
                    H1, H2, M3, M4, S1, S2, Z]})
    end;
time_str_2_gregorian_sec({_, [Y1, Y2, Y3, Y4, M1, M2, D1, D2, H1, H2, M3, M4, S1, S2, $Z]}) ->
    Year = list_to_integer([Y1, Y2, Y3, Y4]),
    Month = list_to_integer([M1, M2]),
    Day = list_to_integer([D1, D2]),
    Hour = list_to_integer([H1, H2]),
    Min = list_to_integer([M3, M4]),
    Sec = list_to_integer([S1, S2]),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}).
