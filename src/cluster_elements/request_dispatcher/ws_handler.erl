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

-module(ws_handler).
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("logging.hrl").
-include_lib("public_key/include/public_key.hrl").

%% Holds state of websocket connection. peer_eec field contains #otp_certificate of connected peer.
-record(hander_state, {peer_eec, peer_serial, dispatcher_timeout}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-ifdef(TEST).
-export([decode_protocol_buffer/1, encode_answer/1, encode_answer/4]).
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
    {ok, PeerCert} = ssl:peercert(cowboy_req:get(socket, Req)),
    {ok, {Serial, Issuer}} = public_key:pkix_issuer_id(PeerCert, self),
    {ok, DispatcherTimeout} = application:get_env(veil_cluster_node, dispatcher_timeout),

    case ets:lookup(gsi_state, {Serial, Issuer}) of
        [{_, [OtpCert | Certs], _}]    ->
            case gsi_handler:call(gsi_nif, verify_cert_c,
                [public_key:pkix_encode('OTPCertificate', OtpCert, otp),                    %% peer certificate
                    [public_key:pkix_encode('OTPCertificate', Cert, otp) || Cert <- Certs], %% peer CA chain
                    [DER || [DER] <- ets:match(gsi_state, {{ca, '_'}, '$1', '_'})],         %% cluster CA store
                    [DER || [DER] <- ets:match(gsi_state, {{crl, '_'}, '$1', '_'})]]) of    %% cluster CRL store
                {ok, 1} ->
                    {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
                    ?info("Peer connected using certificate with subject: ~p ~n", [gsi_handler:proxy_subject(EEC)]),
                    {ok, Req, #hander_state{peer_eec = EEC, peer_serial = Serial, dispatcher_timeout = DispatcherTimeout}};
                {ok, 0, Errno} ->
                    ?info("Peer ~p was rejected due to ~p error code", [OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject, Errno]),
                    {shutdown, Req};
                {error, Reason} ->
                    ?error("GSI peer verification callback error: ~p", [Reason]),
                    {shutdown, Req};
                Other ->
                    ?error("GSI verification callback returned unknown response ~p", [Other]),
                    {shutdown, Req}
            end;
        _->
            ?error("Peer was conected but cerificate chain was not found. Please check if GSI validation is enabled."),
            {shutdown, Req}
    end.

%% websocket_handle/3
%% ====================================================================
%% @doc Cowboy's webscoket_handle callback. Binary data was received on socket. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_handle({Type :: atom(), Data :: term()}, Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
        Req :: term(),
        State :: #hander_state{}.
%% ====================================================================
websocket_handle({binary, Data}, Req, #hander_state{peer_eec = EEC, dispatcher_timeout = DispatcherTimeout} = State) ->
    try
        {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, Answer_type} = decode_protocol_buffer(Data),
        {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
        {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
        Request = #veil_request{subject = DnString, request = Msg},
        case Synch of
            true ->
                try
                    Pid = self(),
                    Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Pid, Request}}),
                    case Ans of
                        ok ->
                            receive
                                Ans2 -> {reply, {binary, encode_answer(Ans, Answer_type, Answer_decoder_name, Ans2)}, Req, State}
                            after DispatcherTimeout ->
                                {reply, {binary, encode_answer(dispatcher_timeout)}, Req, State}
                            end;
                        Other -> {reply, {binary, encode_answer(Other)}, Req, State}
                    end
                catch
                    _:_ -> {reply, {binary, encode_answer(dispatcher_error)}, Req, State}
                end;
            false ->
                try
                    Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Request}}),
                    {reply, {binary, encode_answer(Ans)}, Req, State}
                catch
                    _:_ -> {reply, {binary, encode_answer(dispatcher_error)}, Req, State}
                end
        end
    catch
        wrong_message_format -> {reply, {binary, encode_answer(wrong_message_format)}, Req, State};
        wrong_internal_message_type -> {reply, {binary, encode_answer(wrong_internal_message_type)}, Req, State};
        _:_ -> {reply, {binary, encode_answer(ranch_handler_error)}, Req, State}
    end;
websocket_handle({Type, Data}, Req, State) ->
    ?warning("Unknown WebSocket request. Type: ~p, Payload: ~p", [Type, Data]),
    {ok, Req, State}.


%% websocket_info/3
%% ====================================================================
%% @doc Cowboy's webscoket_info callback. Erlang message received. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_info(Msg :: term(), Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
        Req :: term(),
        State :: #hander_state{}.
%% ====================================================================
websocket_info(Msg, Req, State) ->
    %% Example: Send Msg to socket
    %% TODO: PUSH channel
    {reply, {binary, Msg}, Req, State}; %% Send Msg (has to be binary !) back to connected client
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
        State :: #hander_state{}.
%% ====================================================================
websocket_terminate(_Reason, _Req, #hander_state{peer_serial = _Serial} = _State) ->
    ?debug("WebSocket connection  terminate for peer ~p with reason: ~p", [_Serial, _Reason]),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% decode_protocol_buffer/1
%% ====================================================================
%% @doc Decodes the message using protocol buffers records_translator.
-spec decode_protocol_buffer(MsgBytes :: binary()) -> Result when
  Result ::  {Synch, ModuleName, Msg, Answer_type},
  Synch :: boolean(),
  ModuleName :: atom(),
  Msg :: term(),
  Answer_type :: string().
%% ====================================================================
decode_protocol_buffer(MsgBytes) ->
  DecodedBytes = try
    communication_protocol_pb:decode_clustermsg(MsgBytes)
  catch
    _:_ -> throw(wrong_message_format)
  end,

  #clustermsg{module_name = ModuleName, message_type = Message_type, message_decoder_name = Message_decoder_name, answer_type = Answer_type,
    answer_decoder_name = Answer_decoder_name, synch = Synch, protocol_version = Prot_version, input = Bytes} = DecodedBytes,

  Msg = try
    erlang:apply(list_to_atom(Message_decoder_name ++ "_pb"), list_to_atom("decode_" ++ Message_type), [Bytes])
  catch
    _:_ -> throw(wrong_internal_message_type)
  end,

  {Synch, list_to_atom(ModuleName), Answer_decoder_name, Prot_version, records_translator:translate(Msg, Message_decoder_name), Answer_type}.


%% encode_answer/1
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom()) -> Result when
  Result ::  binary().
%% ====================================================================
encode_answer(Main_Answer) ->
  encode_answer(Main_Answer, non, "non", []).

%% encode_answer/4
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
  Result ::  binary().
%% ====================================================================
encode_answer(Main_Answer, AnswerType, Answer_decoder_name, Worker_Answer) ->
  Check = ((Main_Answer =:= ok) and is_atom(Worker_Answer) and (Worker_Answer =:= worker_plug_in_error)),
  Main_Answer2 = case Check of
     true -> Worker_Answer;
     false -> Main_Answer
  end,
  Message = case Main_Answer2 of
    ok -> case AnswerType of
      non -> #answer{answer_status = atom_to_list(Main_Answer2)};
      _Type ->
        try
          WAns = erlang:apply(list_to_atom(Answer_decoder_name ++ "_pb"), list_to_atom("encode_" ++ AnswerType), [records_translator:translate_to_record(Worker_Answer)]),
          #answer{answer_status = atom_to_list(Main_Answer2), worker_answer = WAns}
        catch
          Type:Error ->
            lager:error("Ranch handler error during encoding worker answer: ~p:~p, answer type: ~s, decoder ~s, worker answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Worker_Answer]),
            #answer{answer_status = "worker_answer_encoding_error"}
        end
    end;
    _Other ->
      try
        #answer{answer_status = atom_to_list(Main_Answer2)}
      catch
        Type:Error ->
          lager:error("Ranch handler error during encoding main answer: ~p:~p, main answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Main_Answer2]),
          #answer{answer_status = "main_answer_encoding_error"}
      end
  end,
  erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message)).
