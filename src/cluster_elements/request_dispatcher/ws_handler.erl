%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module forwards requests from socket to dispatcher.
%% @end
%% ===================================================================

%% TODO
%% Zrobić tak, żeby odpowiadając atomem nie trzeba było wysyłać do handlera
%% rekordu atom tylko, żeby handler sam przepakował ten atom do rekordu

-module(ws_handler).
-include("registered_names.hrl").
-include("messages_white_list.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("logging_pb.hrl").
-include("remote_file_management_pb.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/cluster_rengine/cluster_rengine.hrl").
-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include_lib("public_key/include/public_key.hrl").

%% Holds state of websocket connection. peer_dn field contains DN of certificate of connected peer.
-record(handler_state, {peer_serial, dispatcher_timeout, fuse_id = "", connection_id = "",
    peer_type = user, %% user | provider
    provider_id = <<>>, %% only valid if peer_type = provider
    peer_dn, access_token, user_global_id %% only valid if peer_type = user
}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-ifdef(TEST).
-export([decode_clustermsg_pb/1, encode_answer/2, encode_answer/3, encode_answer/5, encode_answer/6]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Switches protocol to WebSocket
-spec init(Proto :: term(), Req :: term(), Opts :: term()) -> {upgrade, protocol, cowboy_websocket}.
%% ====================================================================
init(_Proto, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.


%% websocket_init/3
%% ====================================================================
%% @doc Cowboy's webscoket_init callback. Initialize connection, proceed with TLS-GSI authentication. <br/>
%%      If GSI validation fails, connection will be closed. <br/>
%%      Currently validation is handled by Globus NIF library loaded on erlang slave nodes.
-spec websocket_init(TransportName :: atom(), Req :: term(), Opts :: list()) -> {ok, Req :: term(), State :: term()} | {shutdown, Req :: term()}.
%% ====================================================================
websocket_init(TransportName, Req, _Opts) ->
    ?debug("WebSocket connection received. Transport: ~p", [TransportName]),

    {ClientSubjectDN, _} = cowboy_req:header(<<"onedata-internal-client-subject-dn">>, Req),
    {SessionId, _} = cowboy_req:header(<<"onedata-internal-client-session-id">>, Req),
    ?debug("New connection with SessionId ~p, ClientSubjectDN: ~p", [SessionId, ClientSubjectDN]),

    {ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),
    InitCtx = #handler_state{dispatcher_timeout = DispatcherTimeout},

    case gsi_handler:get_certs_from_req(?ONEPROXY_DISPATCHER, Req) of
        {ok, {OtpCert, Certs}} ->
            {ok, {Serial, _Issuer}} = public_key:pkix_issuer_id(OtpCert, self),
            InitCtx2 = InitCtx#handler_state{peer_serial = Serial},
            {ok, Req, setup_connection(InitCtx2, OtpCert, Certs, auth_handler:is_provider(OtpCert))};
        {error, _} ->
            {GRUID, Req2}  = cowboy_req:header(<<"global-user-id">>, Req),
            {Secret, Req3} = cowboy_req:header(<<"authentication-secret">>, Req2),

            case is_binary(GRUID) and is_binary(Secret) of
                true ->
                    case auth_handler:authenticate_user_by_secret(GRUID, Secret) of
                        {true, AccessToken} ->
                            State = InitCtx#handler_state{peer_type = user,
                                user_global_id = GRUID, access_token = AccessToken},
                            {ok, Req3, State};

                        false ->
                            ?info("Peer rejected: token authentication credentials verification failed"),
                            {shutdown, Req3}
                    end;

                false ->
                    ?info("Peer rejected: no peer certificate"),
                    {shutdown, Req3}
            end
    end.


%% setup_connection/4
%% ====================================================================
%% @doc Setup handler's state with peer info (i.e. peer_type, provider_id | peer_dn).
%% @end
-spec setup_connection(InitCtx :: #handler_state{}, OtpCert :: #'OTPCertificate'{}, Certs :: [#'OTPCertificate'{}], IsProvider :: boolean()) ->
    InitializedCTX :: #handler_state{}.
%% ====================================================================
setup_connection(InitCtx, OtpCert, Certs, false) ->
    {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
    ?debug("Peer connected using certificate with subject: ~p ~n", [gsi_handler:proxy_subject(EEC)]),
    {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
    {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
    InitCtx#handler_state{peer_dn = DnString, peer_type = user};
setup_connection(InitCtx, OtpCert, _Certs, true) ->
    ProviderId = auth_handler:get_provider_id(OtpCert),
    ?info("Provider ~p connected.", [ProviderId]),
    InitCtx#handler_state{provider_id = ProviderId, peer_type = provider}.

%% websocket_handle/3
%% ====================================================================
%% @doc Cowboy's websocket_handle callback. Binary data was received on socket. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_handle({Type :: atom(), Data :: term()}, Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_handle({binary, Data}, Req, #handler_state{peer_type = PeerType} = State) ->
    try
        Request =
            case PeerType of
                provider -> decode_providermsg_pb(Data);
                user     -> decode_clustermsg_pb(Data)
            end,
        ?debug("Received request: ~p", [Request]),

        handle(Req, Request, State) %% Decode ClusterMsg and handle it
    catch
        wrong_message_format                            -> {reply, {binary, encode_answer(wrong_message_format)}, Req, State};
        {wrong_internal_message_type, MsgId2}           -> {reply, {binary, encode_answer(wrong_internal_message_type, MsgId2)}, Req, State};
        {message_not_supported, MsgId2}                 -> {reply, {binary, encode_answer(message_not_supported, MsgId2)}, Req, State};
        {handshake_error, _HError, MsgId2}              -> {reply, {binary, encode_answer(handshake_error, MsgId2)}, Req, State};
        {no_credentials, MsgId2}                        -> {reply, {binary, encode_answer(no_credentials, MsgId2)}, Req, State};
        {no_user_found_error, _HError, MsgId2}          -> {reply, {binary, encode_answer(no_user_found_error, MsgId2)}, Req, State};
        {cert_confirmation_required, UserLogin, MsgId2} -> {reply, {binary, encode_answer(cert_confirmation_required, MsgId2, UserLogin)}, Req, State};
        {cert_denied_by_user, MsgId2}                   -> {reply, {binary, encode_answer(cert_denied_by_user, MsgId2)}, Req, State};
        {AtomError, MsgId2} when is_atom(AtomError)     -> {reply, {binary, encode_answer(AtomError, MsgId2)}, Req, State};
        _:Reason ->
            ?error_stacktrace("WSHandler failed due to: ~p", [Reason]),
            {reply, {binary, encode_answer(ws_handler_error)}, Req, State}
    end;
websocket_handle({Type, Data}, Req, State) ->
    ?warning("Unknown WebSocket request. Type: ~p, Payload: ~p", [Type, Data]),
    {ok, Req, State}.

%% Internal websocket_handle method implementation
%% Handle Handshake request - FUSE ID negotiation
handle(Req, {_, Answer_decoder_name, ProtocolVersion,
    #handshakerequest{hostname = Hostname, variable = Vars, cert_confirmation = CertConfirmation} = HReq, MsgId, Answer_type, _AccessToken},
    #handler_state{peer_dn = DnString, user_global_id = GRUID} = State) ->

    ?debug("Handshake request: ~p", [HReq]),
    NewFuseId = genFuseId(HReq),
    UID = case DnString =:= undefined of %% Fetch user's ID
        false ->
            case user_logic:get_user({dn, DnString}) of
                {ok, #db_document{uuid = UID1}} ->
                    UID1;
                {error, Error} ->
                    case user_logic:get_user({unverified_dn, DnString}) of
                        {ok, #db_document{uuid = UID1} = UserDoc} ->
                            {{OpenIdProvider, UserName}, _} = user_logic:get_login_with_uid(UserDoc),
                            Login = utils:ensure_list(OpenIdProvider) ++ "_" ++ utils:ensure_list(UserName),
                            case CertConfirmation of
                                #handshakerequest_certconfirmation{login = Login, result = Result} ->
                                    % Remove the DN from unverified DNs as it has been confirmed or declined
                                    {ok, UserDoc2} = user_logic:update_unverified_dn_list(UserDoc, user_logic:get_unverified_dn_list(UserDoc) -- [DnString]),
                                    case Result of
                                        false ->
                                            ?alert("Private key owner denied having added a certificate with DN: ~p (added by ~p)", [DnString, Login]),
                                            throw({cert_denied_by_user, MsgId});
                                        true ->
                                            {ok, _} = user_logic:update_dn_list(UserDoc2, user_logic:get_dn_list(UserDoc2) ++ [DnString]),
                                            ?debug("User ~p confirmed a certificate with DN: ~p", [Login, DnString]),
                                            UID1
                                    end;
                                _ ->
                                    ?debug("Handshake request is missing confirmation of certificate with DN: ~p. Denying connection.", [DnString]),
                                    throw({cert_confirmation_required, Login, MsgId})
                            end;
                        {error, _} ->
                            ?error("oneclient handshake failed. User (DN: ~p) data is not available due to DAO error: ~p", [DnString, Error]),
                            throw({no_user_found_error, Error, MsgId})
                    end
            end;

        true ->
            case user_logic:get_user({global_id, utils:ensure_list(GRUID)}) of
                {ok, #db_document{uuid = UID1}} ->
                    UID1;
                {error, Error} ->
                    ?error("oneclient handshake failed. User (GRUID: ~p) data is not available due to DAO error: ~p", [GRUID, Error]),
                    throw({no_user_found_error, Error, MsgId})
            end
    end,

    %% Env Vars list. Entry format: {Name :: atom(), value :: string()}
    EnvVars = [{list_to_atom(string:to_lower(Name)), Value} || #handshakerequest_envvariable{name = Name, value = Value} <- Vars],

    %% Save received data to DB
    FuseEnv = #db_document{uuid = NewFuseId, record = #fuse_session{uid = UID, hostname = Hostname, env_vars = EnvVars}},
    case dao_lib:apply(dao_cluster, save_fuse_session, [FuseEnv], ProtocolVersion) of
        {ok, _} -> ok;
        {error, Error1} ->
            ?error("oneclient handshake failed. Cannot save FUSE env variables (~p) due to DAO error: ~p", [FuseEnv, Error1]),
            throw({handshake_error, Error1, MsgId})
    end,

    %% Update connection state with new FUSE_ID and send it to client
    NewState = State#handler_state{fuse_id = NewFuseId},
    {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #handshakeresponse{fuse_id = NewFuseId})}, Req, NewState};

%% Handle HandshakeACK message - set FUSE ID used in this session, register connection
handle(Req, {_Task, Answer_decoder_name, ProtocolVersion, #handshakeack{fuse_id = NewFuseId}, MsgId, Answer_type, {_GlobalId, _TokenHash}}, #handler_state{peer_dn = DnString, user_global_id = GRUID} = State) ->
    UserKey = case DnString =:= undefined of
        true  -> {global_id, utils:ensure_list(GRUID)};
        false -> {dn, DnString}
    end,

    {UID, AccessToken, UserGID} = %% Fetch user's ID
        case dao_lib:apply(dao_users, get_user, [UserKey], ProtocolVersion) of
            {ok, #db_document{uuid = UID1, record = #user{access_token = AccessToken1, global_id = UserGID1}}} ->
                {UID1, AccessToken1, UserGID1};
            {error, Error} ->
                ?error("oneclient handshake failed. User ~p data is not available due to DAO error: ~p", [UserKey, Error]),
                throw({no_user_found_error, Error, MsgId})
        end,

    %% Refresh user's access token and schedule future refreshes
    gen_server:call(?Dispatcher_Name, {control_panel, 1, {request_refresh, {uuid, UID}, {fuse, self()}}}),

    %% Fetch session data (using FUSE ID)
    case dao_lib:apply(dao_cluster, get_fuse_session, [NewFuseId], ProtocolVersion) of
        {ok, #db_document{uuid = SessID, record = #fuse_session{uid = UID}}} ->
            %% Save connection's location (node and pid) to DB or crash, sice failure leaves no way of recovering
            {ok, ConnID} = dao_lib:apply(dao_cluster, save_connection_info, [#connection_info{session_id = SessID, controlling_node = node(), controlling_pid = self()}], ProtocolVersion),

            %% Double check if session is valid. We cant leave any connections with invalid session ID. Zombies are bad. Really, really bad.
            case dao_lib:apply(dao_cluster, get_fuse_session, [NewFuseId, {stale, update_before}], ProtocolVersion) of
                {ok, _} -> ok;  %% Everything is fine, just continue
                {error, Reason} ->      %% Session has been destroyed, let client know that it's invalidated
                    ?info("Session has beed deleted (error: ~p) while HandshakeACK was in progress. Closing the connection.", [Reason]),
                    dao_lib:apply(dao_cluster, remove_connection_info, [ConnID], ProtocolVersion),  %% Cleanup...
                    throw({invalid_fuse_id, MsgId})                                                 %% ...and crash
            end,

            %% Session data found, and its user ID matches -> send OK status and update current connection state
            ?debug("User ~p assigned FUSE ID ~p to the connection (PID: ~p)", [UserKey, NewFuseId, self()]),
            {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VOK})}, Req,
                        State#handler_state{fuse_id = NewFuseId, connection_id = ConnID, access_token = utils:ensure_binary(AccessToken), user_global_id = utils:ensure_binary(UserGID)}};

        {ok, #db_document{record = #fuse_session{uid = OtherUID}}} ->
            %% Current user does not match session owner
            ?warning("User ~p tried to access someone else's session (fuse ID: ~p, session owner UID: ~p)", [UserKey, NewFuseId, OtherUID]),
            throw({invalid_fuse_id, MsgId});

        {error, Error1} ->
            ?error("Cannot use fuseID ~p due to dao error: ~p", [NewFuseId, Error1]),
            throw({invalid_fuse_id, MsgId})
    end;

%% Handle other messages
handle(Req, {push, FuseID, {Msg, MsgId, DecoderName1, MsgType}}, #handler_state{peer_type = provider} = State) ->
    ?debug("Got push msg for ~p: ~p ~p ~p", [FuseID, Msg, DecoderName1, MsgType]),
    request_dispatcher:send_to_fuse(utils:ensure_list(FuseID), Msg, DecoderName1),
    {reply, {binary, encode_answer(ok, MsgId)}, Req, State};
handle(Req, {pull, FuseID, CLM}, #handler_state{peer_type = provider, provider_id = ProviderId} = State) ->
    ?debug("Got pull msg: ~p from ~p", [CLM, FuseID]),
    handle(Req, CLM, State#handler_state{fuse_id = utils:ensure_list( fslogic_context:gen_global_fuse_id(ProviderId, FuseID) )});
handle(Req, {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, {GlobalId, TokenHash}} = _CLM,
        #handler_state{peer_type = PeerType, provider_id = ProviderId, peer_dn = DnString, dispatcher_timeout = DispatcherTimeout, fuse_id = FuseID,
                      access_token = SessionAccessToken, user_global_id = SessionUserGID} = State) ->
    %% Check if received message requires FuseId
    MsgType = case Msg of
                  M0 when is_tuple(M0) -> erlang:element(1, M0); %% Record
                  M1 when is_atom(M1) -> atom                   %% Atom
              end,
    case {FuseID, lists:member(MsgType, ?SessionDependentMessages)} of
        {[], false} -> ok;                             % Message doesn't require FuseId
        {[], true} -> throw({invalid_fuse_id, MsgId}); % Message requires FuseId which is not present
        {FID, _} when is_list(FID) -> ok                               % FuseId is present
    end,

    {UserGID, AccessToken} =
        case get({GlobalId, TokenHash}) of
            <<CachedToken/binary>> ->
                {GlobalId, CachedToken};
            _ ->
                case {SessionUserGID, SessionAccessToken} of
                    {undefined, _} ->
                        case auth_handler:authenticate_user_by_secret(GlobalId, TokenHash) of
                            {true, AccessToken1} ->
                                %% Cache AccessToken for the user
                                put({GlobalId, TokenHash}, AccessToken1),

                                {GlobalId, AccessToken1};
                            false ->
                                {undefined, undefined}
                        end;
                    {<<SessionUserGID/binary>>, undefined} ->
                        auth_handler:get_access_token(SessionUserGID);
                    {<<SessionUserGID/binary>>, <<SessionAccessToken/binary>>} ->
                        {SessionUserGID, SessionAccessToken}
                end
        end,

    case DnString =/= undefined orelse UserGID =/= undefined of
        true  -> ok;
        false -> throw({no_credentials, MsgId})
    end,

    PeerId =
        case PeerType of
            provider ->
                {provider_id, ProviderId};
            _ ->
                {gruid, UserGID}
        end,
    Request = case Msg of
                  CallbackMsg when is_record(CallbackMsg, channelregistration) ->
                      #worker_request{peer_id = PeerId, subject = DnString, request =
                      #callback{fuse = FuseID, pid = self(), node = node(), action = channelregistration}, access_token = {UserGID, AccessToken}};
                  CallbackMsg2 when is_record(CallbackMsg2, channelclose) ->
                      #worker_request{peer_id = PeerId, subject = DnString, request =
                      #callback{fuse = FuseID, pid = self(), node = node(), action = channelclose}, access_token = {UserGID, AccessToken}};
                  _ ->
                      #worker_request{peer_id = PeerId, subject = DnString, request = Msg, fuse_id = FuseID, access_token = {UserGID, AccessToken}}
              end,

    case Answer_type of
        undefined ->
            case Msg of
                ack ->
                    gen_server:call(?Dispatcher_Name, {node_chosen_for_ack, {Task, ProtocolVersion, Request, MsgId, FuseID}});
                _ ->
                    gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Request}})
            end,
            {ok, Req, State};
        _ ->
            try
                Pid = self(),
                Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Pid, {MsgId, Answer_type, Answer_decoder_name}, Request}}),
                case Ans of
                    ok -> {ok, Req, State};
                    Other -> {reply, {binary, encode_answer(Other, MsgId)}, Req, State}
                end
            catch
                _:_ -> {reply, {binary, encode_answer(dispatcher_error, MsgId)}, Req, State}
            end
    end.


%% websocket_info/3
%% ====================================================================
%% @doc Cowboy's webscoket_info callback. Erlang message received. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_info(Msg :: term(), Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_info({new_access_token, AccessToken}, Req, State) ->
    {ok, Req, State#handler_state{access_token = AccessToken}};
websocket_info({Pid, get_session_id}, Req, State) ->
    Pid ! {ok, State#handler_state.fuse_id}, %% Response with assigned FuseID, when cluster asks
    {ok, Req, State};
websocket_info({Pid, shutdown}, Req, State) -> %% Handler internal shutdown request - close the connection
    Pid ! ok,
    {shutdown, Req, State};
websocket_info({worker_answer, {MsgId, AnswerType, AnswerDecoder}, Answer}, Req, State) ->
    {reply, {binary, encode_answer(ok, MsgId, AnswerType, AnswerDecoder, Answer)}, Req, State};
websocket_info({ResponsePid, Message, MessageDecoder, MsgID}, Req, State) ->
    encode_and_send({ResponsePid, Message, MessageDecoder, MsgID}, -1, Req, State);
websocket_info({with_ack, ResponsePid, Message, MessageDecoder, MsgID}, Req, State) ->
    encode_and_send({ResponsePid, Message, MessageDecoder, MsgID}, MsgID, Req, State);
websocket_info(_Msg, Req, State) ->
    ?warning("Unknown WebSocket PUSH request. Message: ~p", [_Msg]),
    {ok, Req, State}.

%% websocket_terminate/3
%% ====================================================================
%% @doc Cowboy's webscoket_info callback. Connection was closed. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_terminate(Reason :: term(), Req, State) -> ok
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_terminate(_Reason, _Req, State) ->
    #handler_state{peer_serial = _Serial, connection_id = ConnID, peer_dn = DN, user_global_id = GRUID, peer_type = PeerType} = State,
    ?debug("WebSocket connection  terminate for peer ~p with reason: ~p", [_Serial, _Reason]),
    dao_lib:apply(dao_cluster, remove_connection_info, [ConnID], 1),        %% Cleanup connection info.

    case PeerType of
        user ->
            UserIdentification =
                if
                    DN =/= undefined -> {dn, DN};
                    GRUID =/= undefined -> {global_id, GRUID}
                end,

            gen_server:call(?Dispatcher_Name, {control_panel, 1, {fuse_session_close, UserIdentification, self()}});

        provider ->
            ok
    end,

    gen_server:cast(?Node_Manager_Name, {delete_callback_by_pid, self()}),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% encode_and_send/4
%% ====================================================================
%% @doc encode message and send to client. There is difference between MsgId and MessageIdForClient.
%% MsgId is value that will be send back to caller and MessageIdForClient is value generated inside ws_handler
%% and is useful when sending message to client with ack.
-spec encode_and_send(Msg :: term(), MessageIdForClient :: integer(), Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
encode_and_send({ResponsePid, Message, MessageDecoder, MsgID}, MessageIdForClient, Req, State) ->
    try
        [MessageType | _] = tuple_to_list(Message),
        AnsRecord = encode_answer_record(push, MessageIdForClient, atom_to_list(MessageType), MessageDecoder, Message, []),
        case list_to_atom(AnsRecord#answer.answer_status) of
            push ->
                ResponsePid ! {self(), MsgID, ok},
                {reply, {binary, erlang:iolist_to_binary(communication_protocol_pb:encode_answer(AnsRecord))}, Req, State};
            Other ->
                ResponsePid ! {self(), MsgID, Other},
                {ok, Req, State}
        end
    catch
        Type:Error ->
            ?error("Ranch handler callback error for message ~p, error: ~p:~p", [Message, Type, Error]),
            ResponsePid ! {self(), MsgID, handler_error},
            {ok, Req, State}
    end.

%% decode_clustermsg_pb/1
%% ====================================================================
%% @doc Decodes the clustermsg message using protocol buffers records_translator.
-spec decode_clustermsg_pb(MsgBytes :: binary()) -> Result when
    Result :: {ModuleName, Msg, MsgId, Answer_type, {GlobalId, TokenHash}} | no_return(),
    ModuleName :: atom(),
    Msg :: term(),
    MsgId :: integer(),
    Answer_type :: string(),
    GlobalId :: binary(),
    TokenHash :: binary().
%% ====================================================================
decode_clustermsg_pb(MsgBytes) ->
    DecodedBytes = try
        communication_protocol_pb:decode_clustermsg(MsgBytes)
                   catch
                       _:_ -> throw(wrong_message_format)
                   end,

    #clustermsg{module_name = ModuleName, message_type = Message_type, message_decoder_name = Message_decoder_name, answer_type = Answer_type,
        answer_decoder_name = Answer_decoder_name, protocol_version = Prot_version, message_id = MsgId, input = Bytes,
        token_hash = TokenHash, global_user_id = GlobalId} = DecodedBytes,

    {Decoder, DecodingFun, ModuleNameAtom} = try
      {list_to_existing_atom(Message_decoder_name ++ "_pb"), list_to_existing_atom("decode_" ++ Message_type), list_to_existing_atom(ModuleName)}
    catch
      _:_ -> throw({message_not_supported, MsgId})
    end,

    Msg =
        try
            erlang:apply(Decoder, DecodingFun, [Bytes])
        catch
            _:_ -> throw({wrong_internal_message_type, MsgId})
        end,

    TranslatedMsg = try
      records_translator:translate(Msg, Message_decoder_name)
                    catch
                      _:message_not_supported -> throw({message_not_supported, MsgId})
                    end,
    {ModuleNameAtom, Answer_decoder_name, Prot_version, TranslatedMsg, MsgId, Answer_type, {GlobalId, TokenHash}}.

%% decode_answer_pb/1
%% ====================================================================
%% @doc Decodes the answer message using protocol buffers records_translator.
-spec decode_answer_pb(MsgBytes :: binary()) -> Result when
    Result :: {Msg, MsgId, DecoderName, MsgType},
    Msg :: term(),
    MsgId :: integer(),
    DecoderName :: string(),
    MsgType :: string().
%% ====================================================================
decode_answer_pb(MsgBytes) ->
    DecodedBytes = try
        communication_protocol_pb:decode_answer(MsgBytes)
                   catch
                       _:_ -> throw(wrong_message_format)
                   end,

    #answer{message_type = MsgType, worker_answer = Input, message_id = MsgId, message_decoder_name = DecoderName} = DecodedBytes,
    ?info("Decoding answer ~p", [DecodedBytes]),

    DecoderName1 = case DecoderName of
                       [] -> "fuse_messages";
                       _  -> DecoderName
                   end,

    {Decoder, DecodingFun} = try
      {list_to_existing_atom(DecoderName1 ++ "_pb"), list_to_existing_atom("decode_" ++ MsgType)}
    catch
      _:_ -> throw({message_not_supported, MsgId})
    end,

    Msg = try
        erlang:apply(Decoder, DecodingFun, [Input])
          catch
              _:_ -> throw({wrong_internal_message_type, MsgId})
          end,

    TranslatedMsg = try
      records_translator:translate(Msg, DecoderName1)
    catch
      _:message_not_supported -> throw({message_not_supported, MsgId})
    end,
    {TranslatedMsg, MsgId, DecoderName1, MsgType}.


%% decode_providermsg_pb/2
%% ====================================================================
%% @doc Decodes the providermsg message using protocol buffers records_translator. <br/>
%%      In case of 'pull' message - InputMessage is decoded with decode_clustermsg_pb/2. <br/>
%%      In case of 'push' message - InputMessage is decoded with decode_answermsg_pb/2.
%% @end
-spec decode_providermsg_pb(MsgBytes :: binary()) -> Result when
    Result :: {pull | push, FuseId :: binary(), InputMessage :: term()}.
%% ====================================================================
decode_providermsg_pb(MsgBytes) ->
    DecodedBytes = try
        communication_protocol_pb:decode_providermsg(MsgBytes)
                   catch
                       _:_ -> throw(wrong_message_format)
                   end,

    #providermsg{message_type = MsgType, input = Input, fuse_id = FuseID} = DecodedBytes,
    FuseID1 = case FuseID of undefined -> "cluster_fid"; _ -> FuseID end,
    case MsgType of
        "clustermsg" ->
            {pull, FuseID1, decode_clustermsg_pb(Input)};
        "answer" ->
            {push, FuseID1, decode_answer_pb(Input)}
    end.


%% encode_answer/1
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer(Main_Answer) ->
    encode_answer(Main_Answer, 0).

%% encode_answer/2
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), MsgId :: integer()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer(Main_Answer, MsgId) ->
    encode_answer(Main_Answer, MsgId, non, "non", [], []).

%% encode_answer/3
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), MsgId :: integer(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer(Main_Answer, MsgId, ErrorDescription) ->
    encode_answer(Main_Answer, MsgId, non, "non", [], ErrorDescription).

%% encode_answer/5
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer) ->
    encode_answer(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, []).

%% encode_answer/6
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription) ->
    ?debug("Encoding answer ~p ~p ~p ~p ~p ~p", [Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription]),
    Message = encode_answer_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription),
    erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message)).

%% encode_answer_record/6
%% ====================================================================
%% @doc Creates answer record
-spec encode_answer_record(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_answer_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription) ->
    Check = ((Main_Answer =:= ok) and is_atom(Worker_Answer) and (Worker_Answer =:= worker_plug_in_error)),
    Main_Answer2 = case Check of
                       true -> Worker_Answer;
                       false -> Main_Answer
                   end,
    AnswerRecord = case (Main_Answer2 =:= ok) or (Main_Answer2 =:= push) of
                       true -> case AnswerType of
                                   non -> #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId};
                                   _Type ->
                                       try
                                         DecoderName = list_to_existing_atom(Answer_decoder_name ++ "_pb"),
                                         EncodingFun = list_to_existing_atom("encode_" ++ AnswerType),
                                         try
                                             WAns = erlang:apply(DecoderName, EncodingFun, [records_translator:translate_to_record(Worker_Answer)]),
                                             case Main_Answer2 of
                                                 push ->
                                                     #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, message_type = AnswerType, worker_answer = WAns};
                                                 _ ->
                                                     #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, worker_answer = WAns}
                                             end
                                         catch
                                             Type:Error ->
                                                 ?error("Ranch handler error during encoding worker answer: ~p:~p, answer type: ~p, decoder ~p, worker answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Worker_Answer]),
                                                 #answer{answer_status = "worker_answer_encoding_error", message_id = MsgId}
                                         end
                                       catch
                                         _:_ ->
                                           ?error_stacktrace("Wrong decoder ~p or encoding function ~p", [Answer_decoder_name, AnswerType]),
                                           #answer{answer_status = "not_supported_answer_decoder", message_id = MsgId}
                                       end
                               end;
                       false ->
                           try
                               #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId}
                           catch
                               Type:Error ->
                                   ?error("Ranch handler error during encoding main answer: ~p:~p, answer type: ~p, decoder ~p, main answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Main_Answer2]),
                                   #answer{answer_status = "main_answer_encoding_error", message_id = MsgId}
                           end
                   end,
    case ErrorDescription of
        [] -> AnswerRecord;
        _ -> AnswerRecord#answer{error_description = ErrorDescription}
    end.

%% genFuseId/1
%% ====================================================================
%% @doc Generates new fuseId. Returned values will be used as document UUID in DB. All returned values shall be unique.
-spec genFuseId(#handshakerequest{}) -> Result :: nonempty_string().
%% ====================================================================
genFuseId(#handshakerequest{} = _HReq) ->
    dao_helper:gen_uuid().
