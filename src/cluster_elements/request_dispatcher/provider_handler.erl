%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me !
%% @end
%% ===================================================================

-module(provider_handler).
-include("registered_names.hrl").
-include("messages_white_list.hrl").
-include("rtcore_pb.hrl").
-include("dbsync_pb.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/cluster_rengine/cluster_rengine.hrl").
-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include_lib("public_key/include/public_key.hrl").

%% Holds state of websocket connection. peer_dn field contains DN of certificate of connected peer.
-record(handler_state, {peer_serial, dispatcher_timeout, connection_id = "",
    provider_id = <<>>, %% only valid if peer_type = provider
    peer_dn
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
-export([encode_rtresponse/2, encode_rtresponse/3, encode_rtresponse/5, encode_rtresponse/6]).
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
    ?info("Provider's WebSocket connection received. Transport: ~p", [TransportName]),

    {ClientSubjectDN, _} = cowboy_req:header(<<"onedata-internal-client-subject-dn">>, Req),
    {SessionId, _} = cowboy_req:header(<<"onedata-internal-client-session-id">>, Req),
    ?debug("New connection with SessionId ~p, ClientSubjectDN: ~p", [SessionId, ClientSubjectDN]),

    {ok, DispatcherTimeout} = application:get_env(oneprovider_node, dispatcher_timeout),
    InitCtx = #handler_state{dispatcher_timeout = DispatcherTimeout},

    case gsi_handler:get_certs_from_req(?ONEPROXY_DISPATCHER, Req) of
        {ok, {OtpCert, Certs}} ->
            {ok, {Serial, _Issuer}} = public_key:pkix_issuer_id(OtpCert, self),
            InitCtx2 = InitCtx#handler_state{peer_serial = Serial},
            {ok, Req, setup_connection(InitCtx2, OtpCert, Certs, auth_handler:is_provider(OtpCert))};
        {error, _} ->
            ?info("Provider rejected: no peer certificate"),
            {shutdown, Req}
    end.


%% setup_connection/4
%% ====================================================================
%% @doc Setup handler's state with peer info (i.e. peer_type, provider_id | peer_dn).
%% @end
-spec setup_connection(InitCtx :: #handler_state{}, OtpCert :: #'OTPCertificate'{}, Certs :: [#'OTPCertificate'{}], IsProvider :: boolean()) ->
    InitializedCTX :: #handler_state{}.
%% ====================================================================
setup_connection(InitCtx, OtpCert, Certs, false) ->
    error(invalid_provider);
setup_connection(InitCtx, OtpCert, _Certs, true) ->
    ProviderId = auth_handler:get_provider_id(OtpCert),
    ?info("Provider ~p connected.", [ProviderId]),
    InitCtx#handler_state{provider_id = ProviderId}.

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
websocket_handle({binary, Data}, Req, #handler_state{} = State) ->
    try
        Request = decode_rtrequest_pb(Data),
        ?debug("Received request: ~p", [Request]),

        handle(Req, Request, State) %% Decode ClusterMsg and handle it
    catch
        wrong_message_format                            -> {reply, {binary, encode_rtresponse(wrong_message_format)}, Req, State};
        {wrong_internal_message_type, MsgId2}           -> {reply, {binary, encode_rtresponse(wrong_internal_message_type, MsgId2)}, Req, State};
        {message_not_supported, MsgId2}                 ->
            ?info("Dafuq ~p", [erlang:get_stacktrace()]),
            {reply, {binary, encode_rtresponse(message_not_supported, MsgId2)}, Req, State};
        {handshake_error, _HError, MsgId2}              -> {reply, {binary, encode_rtresponse(handshake_error, MsgId2)}, Req, State};
        {no_credentials, MsgId2}                        -> {reply, {binary, encode_rtresponse(no_credentials, MsgId2)}, Req, State};
        {no_user_found_error, _HError, MsgId2}          -> {reply, {binary, encode_rtresponse(no_user_found_error, MsgId2)}, Req, State};
        {cert_confirmation_required, UserLogin, MsgId2} -> {reply, {binary, encode_rtresponse(cert_confirmation_required, MsgId2, UserLogin)}, Req, State};
        {cert_denied_by_user, MsgId2}                   -> {reply, {binary, encode_rtresponse(cert_denied_by_user, MsgId2)}, Req, State};
        {AtomError, MsgId2} when is_atom(AtomError)     -> {reply, {binary, encode_rtresponse(AtomError, MsgId2)}, Req, State};
        _:Reason ->
            ?error_stacktrace("WSHandler failed due to: ~p", [Reason]),
            {reply, {binary, encode_rtresponse(ws_handler_error)}, Req, State}
    end;
websocket_handle({Type, Data}, Req, State) ->
    ?warning("Unknown WebSocket request. Type: ~p, Payload: ~p", [Type, Data]),
    {ok, Req, State}.

%% Handle other messages
%% handle(Req, {push, FuseID, {Msg, MsgId, DecoderName1, MsgType}}, #handler_state{peer_type = provider} = State) ->
%%     ?debug("Got push msg for ~p: ~p ~p ~p", [FuseID, Msg, DecoderName1, MsgType]),
%%     request_dispatcher:send_to_fuse(utils:ensure_list(FuseID), Msg, DecoderName1),
%%     {reply, {binary, encode_rtresponse(ok, MsgId)}, Req, State};
%% handle(Req, {pull, FuseID, CLM}, #handler_state{peer_type = provider, provider_id = ProviderId} = State) ->
%%     ?debug("Got pull msg: ~p from ~p", [CLM, FuseID]),
%%     handle(Req, CLM, State#handler_state{fuse_id = utils:ensure_list( fslogic_context:gen_global_fuse_id(ProviderId, FuseID) )});
handle(Req, {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type} = _RTR,
    #handler_state{peer_dn = DnString, dispatcher_timeout = DispatcherTimeout, provider_id = ProviderId} = State) ->
    %% Check if received message requires FuseId
    MsgType = case Msg of
                  M0 when is_tuple(M0) -> erlang:element(1, M0); %% Record
                  M1 when is_atom(M1) -> atom                   %% Atom
              end,

    Request = #worker_request{peer_id = {provider_id, ProviderId}, subject = DnString, request = Msg},

    case Synch of
        true ->
            try
                Pid = self(),
                Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Pid, MsgId, Request}}),
                case Ans of
                    ok ->
                        receive
                            {worker_answer, MsgId, Ans2} ->
                                {reply, {binary, encode_rtresponse(Ans, MsgId, Answer_type, Answer_decoder_name, Ans2)}, Req, State}
                        after DispatcherTimeout ->
                            {reply, {binary, encode_rtresponse(dispatcher_timeout, MsgId)}, Req, State}
                        end;
                    Other -> {reply, {binary, encode_rtresponse(Other, MsgId)}, Req, State}
                end
            catch
                _:_ -> {reply, {binary, encode_rtresponse(dispatcher_error, MsgId)}, Req, State}
            end;
        false ->
            try
                case Msg of
                    ack ->
                        gen_server:call(?Dispatcher_Name, {node_chosen_for_ack, {Task, ProtocolVersion, Request, MsgId, undefined}}),
                        {ok, Req, State};
                    _ ->
                        Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Request}}),
                        {reply, {binary, encode_rtresponse(Ans, MsgId)}, Req, State}
                end
            catch
                _:_ -> {reply, {binary, encode_rtresponse(dispatcher_error, MsgId)}, Req, State}
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
websocket_info({Pid, shutdown}, Req, State) -> %% Handler internal shutdown request - close the connection
    Pid ! ok,
    {shutdown, Req, State};
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
    #handler_state{peer_serial = _Serial, connection_id = ConnID, peer_dn = DN} = State,
    ?debug("WebSocket connection  terminate for peer ~p with reason: ~p", [_Serial, _Reason]),
    dao_lib:apply(dao_cluster, remove_connection_info, [ConnID], 1),        %% Cleanup connection info.

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
        AnsRecord = encode_rtresponse_record(push, MessageIdForClient, atom_to_list(MessageType), MessageDecoder, Message, []),
        case list_to_atom(AnsRecord#rtresponse.answer_status) of
            push ->
                ResponsePid ! {self(), MsgID, ok},
                {reply, {binary, erlang:iolist_to_binary(rtcore_pb:encode_rtresponse(AnsRecord))}, Req, State};
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

%% decode_rtrequest_pb/1
%% ====================================================================
%% @doc Decodes the clustermsg message using protocol buffers records_translator.
-spec decode_rtrequest_pb(MsgBytes :: binary()) -> Result when
    Result :: {Synch, ModuleName, Msg, MsgId, Answer_type, {GlobalId, TokenHash}} | no_return(),
    Synch :: boolean(),
    ModuleName :: atom(),
    Msg :: term(),
    MsgId :: integer(),
    Answer_type :: string(),
    GlobalId :: binary(),
    TokenHash :: binary().
%% ====================================================================
decode_rtrequest_pb(MsgBytes) ->
    DecodedBytes = try
                       rtcore_pb:decode_rtrequest(MsgBytes)
                   catch
                       _:_ -> throw(wrong_message_format)
                   end,

    #rtrequest{module_name = ModuleName, message_type = Message_type, message_decoder_name = Message_decoder_name, answer_type = Answer_type,
        answer_decoder_name = Answer_decoder_name, synch = Synch, protocol_version = Prot_version, message_id = MsgId, input = Bytes} = DecodedBytes,

    {Decoder, DecodingFun, ModuleNameAtom} = try
                                                 {list_to_existing_atom(Message_decoder_name ++ "_pb"), list_to_existing_atom("decode_" ++ Message_type), list_to_existing_atom(ModuleName)}
                                             catch
                                                 _:_ ->
                                                     ?info("OMG: ~p ~p ~p", [Message_decoder_name, Message_type, ModuleName]),
                                                     throw({message_not_supported, MsgId})
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
    {Synch, ModuleNameAtom, Answer_decoder_name, Prot_version, TranslatedMsg, MsgId, Answer_type}.

%% decode_rtresponse_pb/1
%% ====================================================================
%% @doc Decodes the answer message using protocol buffers records_translator.
-spec decode_rtresponse_pb(MsgBytes :: binary()) -> Result when
    Result :: {Msg, MsgId, DecoderName, MsgType},
    Msg :: term(),
    MsgId :: integer(),
    DecoderName :: string(),
    MsgType :: string().
%% ====================================================================
decode_rtresponse_pb(MsgBytes) ->
    DecodedBytes = try
                       rtcore_pb:decode_rtresponse(MsgBytes)
                   catch
                       _:_ -> throw(wrong_message_format)
                   end,

    #rtresponse{message_type = MsgType, worker_answer = Input, message_id = MsgId, message_decoder_name = DecoderName} = DecodedBytes,
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


%% encode_rtresponse/1
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_rtresponse(Main_Answer :: atom()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse(Main_Answer) ->
    encode_rtresponse(Main_Answer, 0).

%% encode_rtresponse/2
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_rtresponse(Main_Answer :: atom(), MsgId :: integer()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse(Main_Answer, MsgId) ->
    encode_rtresponse(Main_Answer, MsgId, non, "non", [], []).

%% encode_rtresponse/3
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_rtresponse(Main_Answer :: atom(), MsgId :: integer(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse(Main_Answer, MsgId, ErrorDescription) ->
    encode_rtresponse(Main_Answer, MsgId, non, "non", [], ErrorDescription).

%% encode_rtresponse/5
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_rtresponse(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer) ->
    encode_rtresponse(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, []).

%% encode_rtresponse/6
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_rtresponse(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription) ->
    ?debug("Encoding answer ~p ~p ~p ~p ~p ~p", [Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription]),
    Message = encode_rtresponse_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription),
    erlang:iolist_to_binary(rtcore_pb:encode_rtresponse(Message)).

%% encode_rtresponse_record/6
%% ====================================================================
%% @doc Creates answer record
-spec encode_rtresponse_record(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term(), ErrorDescription :: term()) -> Result when
    Result :: binary().
%% ====================================================================
encode_rtresponse_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer, ErrorDescription) ->
    Check = ((Main_Answer =:= ok) and is_atom(Worker_Answer) and (Worker_Answer =:= worker_plug_in_error)),
    Main_Answer2 = case Check of
                       true -> Worker_Answer;
                       false -> Main_Answer
                   end,
    AnswerRecord = case (Main_Answer2 =:= ok) or (Main_Answer2 =:= push) of
                       true -> case AnswerType of
                                   non -> #rtresponse{answer_status = atom_to_list(Main_Answer2), message_id = MsgId};
                                   _Type ->
                                       try
                                           DecoderName = list_to_existing_atom(Answer_decoder_name ++ "_pb"),
                                           EncodingFun = list_to_existing_atom("encode_" ++ AnswerType),
                                           try
                                               WAns = erlang:apply(DecoderName, EncodingFun, [records_translator:translate_to_record(Worker_Answer)]),
                                               case Main_Answer2 of
                                                   push ->
                                                       #rtresponse{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, message_type = AnswerType, worker_answer = WAns};
                                                   _ ->
                                                       #rtresponse{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, worker_answer = WAns}
                                               end
                                           catch
                                               Type:Error ->
                                                   ?error("Ranch handler error during encoding worker answer: ~p:~p, answer type: ~p, decoder ~p, worker answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Worker_Answer]),
                                                   #rtresponse{answer_status = "worker_rtresponse_encoding_error", message_id = MsgId}
                                           end
                                       catch
                                           _:_ ->
                                               ?error_stacktrace("Wrong decoder ~p or encoding function ~p", [Answer_decoder_name, AnswerType]),
                                               #rtresponse{answer_status = "not_supported_rtresponse_decoder", message_id = MsgId}
                                       end
                               end;
                       false ->
                           try
                               #rtresponse{answer_status = atom_to_list(Main_Answer2), message_id = MsgId}
                           catch
                               Type:Error ->
                                   ?error("Ranch handler error during encoding main answer: ~p:~p, answer type: ~p, decoder ~p, main answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Main_Answer2]),
                                   #rtresponse{answer_status = "main_rtresponse_encoding_error", message_id = MsgId}
                           end
                   end,
    case ErrorDescription of
        [] -> AnswerRecord;
        _ -> AnswerRecord#rtresponse{error_description = ErrorDescription}
    end.
