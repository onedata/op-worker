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
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").
-include("veil_modules/dao/dao.hrl").
-include_lib("public_key/include/public_key.hrl").

%% Holds state of websocket connection. peer_dn field contains DN of certificate of connected peer.
-record(hander_state, {peer_dn, peer_serial, dispatcher_timeout, fuse_id = "", connection_id = ""}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-ifdef(TEST).
-export([decode_protocol_buffer/2, encode_answer/2, encode_answer/5, checkMessage/2]).
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
    [{_, [OtpCert | Certs], _}] ->
      case gsi_handler:call(gsi_nif, verify_cert_c,
        [public_key:pkix_encode('OTPCertificate', OtpCert, otp),                    %% peer certificate
          [public_key:pkix_encode('OTPCertificate', Cert, otp) || Cert <- Certs], %% peer CA chain
          [DER || [DER] <- ets:match(gsi_state, {{ca, '_'}, '$1', '_'})],         %% cluster CA store
          [DER || [DER] <- ets:match(gsi_state, {{crl, '_'}, '$1', '_'})]]) of    %% cluster CRL store
        {ok, 1} ->
          {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
          ?debug("Peer connected using certificate with subject: ~p ~n", [gsi_handler:proxy_subject(EEC)]),
          {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
          {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
          {ok, Req, #hander_state{peer_dn = DnString, peer_serial = Serial, dispatcher_timeout = DispatcherTimeout}};
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
    _ ->
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
websocket_handle({binary, Data}, Req, #hander_state{peer_dn = DnString} = State) ->
  try
    handle(Req, decode_protocol_buffer(Data, DnString), State) %% Decode ClusterMsg and handle it
  catch
    wrong_message_format -> {reply, {binary, encode_answer(wrong_message_format)}, Req, State};
    {wrong_internal_message_type, MsgId2} ->
      {reply, {binary, encode_answer(wrong_internal_message_type, MsgId2)}, Req, State};
    {message_not_supported, MsgId2} -> {reply, {binary, encode_answer(message_not_supported, MsgId2)}, Req, State};
    {handshake_error, _HError, MsgId2} -> {reply, {binary, encode_answer(handshake_error, MsgId2)}, Req, State};
    {no_user_found_error, _HError, MsgId2} -> {reply, {binary, encode_answer(no_user_found_error, MsgId2)}, Req, State};
    {AtomError, MsgId2} when is_atom(AtomError) -> {reply, {binary, encode_answer(AtomError, MsgId2)}, Req, State};
    _:_ -> {reply, {binary, encode_answer(ws_handler_error)}, Req, State}
  end;
websocket_handle({Type, Data}, Req, State) ->
  ?warning("Unknown WebSocket request. Type: ~p, Payload: ~p", [Type, Data]),
  {ok, Req, State}.

handle(Req, {_, _, Answer_decoder_name, ProtocolVersion, #storagetestrequest{type = "create_storage_test_file", storage_id = StorageId} = SReq, MsgId, Answer_type}, #hander_state{peer_dn = DnString} = State) ->
  ?debug("Create storage test file request: ~p", [SReq]),
  lager:info("=========> Create storage test file request: ~p", [SReq]),
  Login = %% Fetch user's login
  case dao_lib:apply(dao_users, get_user, [{dn, DnString}], ProtocolVersion) of
    {ok, #veil_document{record = #user{login = UserLogin}}} ->
      UserLogin;
    {error, Error} ->
      ?error("VeilClient storage test file creation failed. User ~p data is not available due to DAO error: ~p", [DnString, Error]),
      throw({no_user_found_error, Error, MsgId})
  end,
  Length = 20,
  {A, B, C} = now(),
  random:seed(A, B, C),
  Text = list_to_binary(lists:foldl(fun(_, Acc) -> [random:uniform(93) + 33 | Acc] end, [], lists:seq(1, Length))),
  %% Delete storage test file after one minute
  DeleteStorageTestFile = fun(StorageHelperInfo, Path) ->
    timer:sleep(60 * 1000),
    storage_files_manager:delete(StorageHelperInfo, Path)
  end,
  case dao_lib:apply(dao_vfs, get_storage, [{id, StorageId}], ProtocolVersion) of
    {ok, #veil_document{record = StorageInfo}} ->
      StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
      case create_storage_test_file(StorageHelperInfo, Login) of
        {ok, Path} ->
          spawn(fun() -> DeleteStorageTestFile(StorageHelperInfo, Path) end),
          case storage_files_manager:write(StorageHelperInfo, Path, Text) of
            Length ->
              {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "ok", relative_path = Path, text = Text})}, Req, State};
            _ ->
              {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "error"})}, Req, State}
          end;
        _ ->
          {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "error"})}, Req, State}
      end;
    _ ->
      {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "error"})}, Req, State}
  end;

handle(Req, {_, _, Answer_decoder_name, ProtocolVersion, #storagetestrequest{type = "storage_test_file_modified", storage_id = StorageId, relative_path = Path, text = ExpectedText} = SReq, MsgId, Answer_type}, State) ->
  ?debug("Storage test file modified request: ~p", [SReq]),
  lager:info("=======> Storage test file modified request: ~p", [SReq]),
  case dao_lib:apply(dao_vfs, get_storage, [{id, StorageId}], ProtocolVersion) of
    {ok, #veil_document{record = StorageInfo}} ->
      StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
      case storage_files_manager:read(StorageHelperInfo, Path, 0, length(ExpectedText)) of
        {ok, Bytes} ->
          Answer = case binary_to_list(Bytes) of
                     ExpectedText -> "ok";
                     _ -> "error"
                   end,
          storage_files_manager:delete(StorageHelperInfo, Path),
          {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = Answer})}, Req, State};
        _ ->
          {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "error"})}, Req, State}
      end;
    _ ->
      {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #storagetestresponse{answer = "error"})}, Req, State}
  end;

handle(Req, {_, _, Answer_decoder_name, ProtocolVersion, #clientstorageinfo{storage_info = ClientStorageInfo} = SReq, MsgId, Answer_type}, #hander_state{fuse_id = FuseId} = State) ->
  ?debug("Client storage info: ~p", [SReq]),
  lager:info("===========> Got client storage info: ~p", [ClientStorageInfo]),
  case dao_lib:apply(dao_cluster, get_fuse_session, [FuseId], ProtocolVersion) of
    {ok, #veil_document{record = #fuse_session{env_vars = EnvVars} = FuseSession} = FuseSessionDoc} ->
      case proplists:get_value(group_id, EnvVars) of
        undefined ->
          lager:info("===========> Fuse group undefined."),
          case get_fuse_group_name(ClientStorageInfo, ProtocolVersion) of
            {new_fuse_group, Name} ->
              lager:info("===========> New fuse group created."),
              try
                ok = add_fuse_group_to_storages(Name, ClientStorageInfo, ProtocolVersion),
                ok = save_fuse_group_name_in_client_session(Name, FuseSession, FuseSessionDoc, ProtocolVersion),
                {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VOK})}, Req, State}
              catch
                _:_ ->
                  {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VEREMOTEIO})}, Req, State}
              end;
            {existing_fuse_group, Name} ->
              lager:info("===========> Existing fuse group reused."),
              try
                ok = save_fuse_group_name_in_client_session(Name, FuseSession, FuseSessionDoc, ProtocolVersion),
                {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VOK})}, Req, State}
              catch
                _:_ ->
                  {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VEREMOTEIO})}, Req, State}
              end;
            error ->
              lager:info("===========> Error while getting fuse group name."),
              {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VEREMOTEIO})}, Req, State}
          end;
        _ ->
          {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VOK})}, Req, State}
      end;
    _ ->
      {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VEREMOTEIO})}, Req, State}
  end;

%% Internal websocket_handle method implementation
%% Handle Handshake request - FUSE ID negotiation
handle(Req, {_, _, Answer_decoder_name, ProtocolVersion, #handshakerequest{hostname = Hostname, variable = Vars} = HReq, MsgId, Answer_type}, #hander_state{peer_dn = DnString} = State) ->
  ?debug("Handshake request: ~p", [HReq]),
  NewFuseId = gen_fuse_id(HReq),
  UID = %% Fetch user's ID
  case dao_lib:apply(dao_users, get_user, [{dn, DnString}], ProtocolVersion) of
    {ok, #veil_document{uuid = UID1}} ->
      UID1;
    {error, Error} ->
      ?error("VeilClient handshake failed. User ~p data is not available due to DAO error: ~p", [DnString, Error]),
      throw({no_user_found_error, Error, MsgId})
  end,

%% Env Vars list. Entry format: {Name :: atom(), value :: string()}
  EnvVars = [{list_to_atom(string:to_lower(Name)), Value} || #handshakerequest_envvariable{name = Name, value = Value} <- Vars],

%% Save received data to DB
  FuseEnv = #veil_document{uuid = NewFuseId, record = #fuse_session{uid = UID, hostname = Hostname, env_vars = EnvVars}},
  case dao_lib:apply(dao_cluster, save_fuse_session, [FuseEnv], ProtocolVersion) of
    {ok, _} -> ok;
    {error, Error1} ->
      ?error("VeilClient handshake failed. Cannot save FUSE env variables (~p) due to DAO error: ~p", [FuseEnv, Error1]),
      throw({handshake_error, Error1, MsgId})
  end,

%% Update connection state with new FUSE_ID and send it to client
  NewState = State#hander_state{fuse_id = NewFuseId},
  {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #handshakeresponse{fuse_id = NewFuseId})}, Req, NewState};

%% Handle HandshakeACK message - set FUSE ID used in this session, register connection
handle(Req, {_Synch, _Task, Answer_decoder_name, ProtocolVersion, #handshakeack{fuse_id = NewFuseId}, MsgId, Answer_type}, #hander_state{peer_dn = DnString} = State) ->
  UID = %% Fetch user's ID
  case dao_lib:apply(dao_users, get_user, [{dn, DnString}], ProtocolVersion) of
    {ok, #veil_document{uuid = UID1}} ->
      UID1;
    {error, Error} ->
      ?error("VeilClient handshake failed. User ~p data is not available due to DAO error: ~p", [DnString, Error]),
      throw({no_user_found_error, Error, MsgId})
  end,

%% Fetch session data (using FUSE ID)
  case dao_lib:apply(dao_cluster, get_fuse_session, [NewFuseId], ProtocolVersion) of
    {ok, #veil_document{uuid = SessID, record = #fuse_session{uid = UID}}} ->
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
      ?debug("User ~p assigned FUSE ID ~p to the connection (PID: ~p)", [DnString, NewFuseId, self()]),
      {reply, {binary, encode_answer(ok, MsgId, Answer_type, Answer_decoder_name, #atom{value = ?VOK})}, Req, State#hander_state{fuse_id = NewFuseId, connection_id = ConnID}};
    {ok, #veil_document{record = #fuse_session{uid = OtherUID}}} ->
%% Current user does not match session owner
      ?warning("User ~p tried to access someone else's session (fuse ID: ~p, session owner UID: ~p)", [DnString, NewFuseId, OtherUID]),
      throw({invalid_fuse_id, MsgId});
    {error, Error1} ->
      ?error("Cannot use fuseID ~p due to dao error: ~p", [NewFuseId, Error1]),
      throw({invalid_fuse_id, MsgId})
  end;

%% Handle other messages
handle(Req, {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type}, #hander_state{peer_dn = DnString, dispatcher_timeout = DispatcherTimeout, fuse_id = FuseID} = State) ->
%% Check if received message requires FuseId
  MsgType = case Msg of
              M0 when is_tuple(M0) -> erlang:element(1, M0); %% Record
              M1 when is_atom(M1) -> atom                   %% Atom
            end,
  case {FuseID, lists:member(MsgType, ?SessionDependentMessages)} of
    {[], false} -> ok;                              % Message doesn't require FuseId
    {[], true} -> throw({invalid_fuse_id, MsgId}); % Message requires FuseId which is not present
    {FID, _} when is_list(FID) -> ok                               % FuseId is present
  end,

  Request = case Msg of
              CallbackMsg when is_record(CallbackMsg, channelregistration) ->
                #veil_request{subject = DnString, request = #callback{fuse = FuseID, pid = self(), node = node(), action = channelregistration}};
              CallbackMsg2 when is_record(CallbackMsg2, channelclose) ->
                #veil_request{subject = DnString, request = #callback{fuse = FuseID, pid = self(), node = node(), action = channelclose}};
              _ -> #veil_request{subject = DnString, request = Msg, fuse_id = FuseID}
            end,

  case Synch of
    true ->
      try
        Pid = self(),
        Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Pid, MsgId, Request}}),
        case Ans of
          ok ->
            receive
              {worker_answer, MsgId, Ans2} ->
                {reply, {binary, encode_answer(Ans, MsgId, Answer_type, Answer_decoder_name, Ans2)}, Req, State}
            after DispatcherTimeout ->
              {reply, {binary, encode_answer(dispatcher_timeout, MsgId)}, Req, State}
            end;
          Other -> {reply, {binary, encode_answer(Other, MsgId)}, Req, State}
        end
      catch
        _:_ -> {reply, {binary, encode_answer(dispatcher_error, MsgId)}, Req, State}
      end;
    false ->
      try
        Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {Task, ProtocolVersion, Request}}),
        {reply, {binary, encode_answer(Ans, MsgId)}, Req, State}
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
  State :: #hander_state{}.
%% ====================================================================
websocket_info({Pid, get_session_id}, Req, State) ->
  Pid ! {ok, State#hander_state.fuse_id}, %% Response with assigned FuseID, when cluster asks
  {ok, Req, State};
websocket_info({Pid, shutdown}, Req, State) -> %% Handler internal shutdown request - close the connection
  Pid ! ok,
  {shutdown, Req, State};
websocket_info({ResponsePid, Message, MessageDecoder, MsgID}, Req, State) ->
  try
    [MessageType | _] = tuple_to_list(Message),
    AnsRecord = encode_answer_record(push, -1, atom_to_list(MessageType), MessageDecoder, Message),
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
      lager:error("Ranch handler callback error for message ~p, error: ~p:~p", [Message, Type, Error]),
      ResponsePid ! {self(), MsgID, handler_error},
      {ok, Req, State}
  end;
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
websocket_terminate(_Reason, _Req, #hander_state{peer_serial = _Serial, connection_id = ConnID} = _State) ->
  ?debug("WebSocket connection  terminate for peer ~p with reason: ~p", [_Serial, _Reason]),
  dao_lib:apply(dao_cluster, remove_connection_info, [ConnID], 1),        %% Cleanup connection info.
  gen_server:cast(?Node_Manager_Name, {delete_callback_by_pid, self()}),
  ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% decode_protocol_buffer/2
%% ====================================================================
%% @doc Decodes the message using protocol buffers records_translator.
-spec decode_protocol_buffer(MsgBytes :: binary(), DN :: string()) -> Result when
  Result :: {Synch, ModuleName, Msg, MsgId, Answer_type},
  Synch :: boolean(),
  ModuleName :: atom(),
  Msg :: term(),
  MsgId :: integer(),
  Answer_type :: string().
%% ====================================================================
decode_protocol_buffer(MsgBytes, DN) ->
  DecodedBytes = try
    communication_protocol_pb:decode_clustermsg(MsgBytes)
                 catch
                   _:_ -> throw(wrong_message_format)
                 end,

  #clustermsg{module_name = ModuleName, message_type = Message_type, message_decoder_name = Message_decoder_name, answer_type = Answer_type,
    answer_decoder_name = Answer_decoder_name, synch = Synch, protocol_version = Prot_version, message_id = MsgId, input = Bytes} = DecodedBytes,

  Msg = try
    erlang:apply(list_to_atom(Message_decoder_name ++ "_pb"), list_to_atom("decode_" ++ Message_type), [Bytes])
        catch
          _:_ -> throw({wrong_internal_message_type, MsgId})
        end,

  TranslatedMsg = records_translator:translate(Msg, Message_decoder_name),
  case checkMessage(TranslatedMsg, DN) of
    true -> {Synch, list_to_atom(ModuleName), Answer_decoder_name, Prot_version, TranslatedMsg, MsgId, Answer_type};
    false -> throw({message_not_supported, MsgId})
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
  encode_answer(Main_Answer, MsgId, non, "non", []).

%% encode_answer/5
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
  Result :: binary().
%% ====================================================================
encode_answer(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer) ->
  Message = encode_answer_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer),
  erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message)).

%% encode_answer_record/5
%% ====================================================================
%% @doc Creates answer record
-spec encode_answer_record(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
  Result :: binary().
%% ====================================================================
encode_answer_record(Main_Answer, MsgId, AnswerType, Answer_decoder_name, Worker_Answer) ->
  Check = ((Main_Answer =:= ok) and is_atom(Worker_Answer) and (Worker_Answer =:= worker_plug_in_error)),
  Main_Answer2 = case Check of
                   true -> Worker_Answer;
                   false -> Main_Answer
                 end,
  case (Main_Answer2 =:= ok) or (Main_Answer2 =:= push) of
    true -> case AnswerType of
              non -> #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId};
              _Type ->
                try
                  WAns = erlang:apply(list_to_atom(Answer_decoder_name ++ "_pb"), list_to_atom("encode_" ++ AnswerType), [records_translator:translate_to_record(Worker_Answer)]),
                  case Main_Answer2 of
                    push ->
                      #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, message_type = AnswerType, worker_answer = WAns};
                    _ -> #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId, worker_answer = WAns}
                  end
                catch
                  Type:Error ->
                    lager:error("Ranch handler error during encoding worker answer: ~p:~p, answer type: ~s, decoder ~s, worker answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Worker_Answer]),
                    #answer{answer_status = "worker_answer_encoding_error", message_id = MsgId}
                end
            end;
    false ->
      try
        #answer{answer_status = atom_to_list(Main_Answer2), message_id = MsgId}
      catch
        Type:Error ->
          lager:error("Ranch handler error during encoding main answer: ~p:~p, main answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Main_Answer2]),
          #answer{answer_status = "main_answer_encoding_error", message_id = MsgId}
      end
  end.

%% map_dn_to_client_type/1
%% ====================================================================
%% @doc Checks if message can be processed by cluster.
-spec map_dn_to_client_type(DN :: string()) -> UserType when
  UserType :: atom().
%% ====================================================================
map_dn_to_client_type(_DN) ->
  standard_user.

%% checkMessage/2
%% ====================================================================
%% @doc Checks if message can be processed by cluster.
-spec checkMessage(Msg :: term(), DN :: string()) -> Result when
  Result :: boolean().
%% ====================================================================
checkMessage(Msg, DN) when is_atom(Msg) ->
  lists:member(Msg, proplists:get_value(map_dn_to_client_type(DN), ?AtomsWhiteList, []));

checkMessage(Msg, DN) when is_tuple(Msg) ->
  [Record_Type | _] = tuple_to_list(Msg),
  lists:member(Record_Type, proplists:get_value(map_dn_to_client_type(DN), ?MessagesWhiteList, []));

checkMessage(Msg, DN) ->
  lager:warning("Wrong type of message ~p for user ~p", [Msg, DN]),
  false.


%% gen_fuse_id/1
%% ====================================================================
%% @doc Generates new fuseId. Returned values will be used as document UUID in DB. All returned values shall be unique.
-spec gen_fuse_id(#handshakerequest{}) -> Result :: nonempty_string().
%% ====================================================================
gen_fuse_id(#handshakerequest{} = _HReq) ->
  dao_helper:gen_uuid().

%% create_storage_test_file/2
%% ====================================================================
%% @doc Creates storage test file with random filename. If file already exists new name is generated.
-spec create_storage_test_file(StorageHelperInfo :: #storage_helper_info{}, Login :: string()) -> Result when
  Result :: {ok, Path :: string()} | {error, attempts_limit_excceded}.
%% ====================================================================
create_storage_test_file(StorageHelperInfo, Login) ->
  create_storage_test_file(StorageHelperInfo, Login, 20).

%% create_storage_test_file/3
%% ====================================================================
%% @doc Creates storage test file with random filename. If file already exists new name is generated.
-spec create_storage_test_file(StorageHelperInfo :: #storage_helper_info{}, Login :: string(), Attempts :: integer()) -> Result when
  Result :: {ok, Path :: string()} | {error, attempts_limit_excceded}.
%% ====================================================================
create_storage_test_file(_, _, 0) ->
  {error, attempts_limit_exceeded};
create_storage_test_file(StorageHelperInfo, Login, Attempts) ->
  {A, B, C} = now(),
  random:seed(A, B, C),
  Filename = lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, 8)),
  Path = "users/" ++ Login ++ "/" ++ Filename,
  case storage_files_manager:create(StorageHelperInfo, Path) of
    ok -> {ok, Path};
    _ -> create_storage_test_file(StorageHelperInfo, Login, Attempts - 1)
  end.

%% get_fuse_group_name/2
%% ====================================================================
%% @doc Creates storage test file with random filename. If file already exists new name is generated.
-spec get_fuse_group_name(ClientStorageInfo, ProtocolVersion) -> Result when
  ClientStorageInfo :: [{FieldName :: atom(), StorageId :: integer(), Path :: string()}],
  ProtocolVersion :: integer(),
  Result :: {new_group, Name :: string()} | {exist_group, Name :: string()} | error.
%% ====================================================================
get_fuse_group_name(ClientStorageInfo, ProtocolVersion) ->
  try
    NewClientStorageInfo = lists:map(fun({_, StorageId, Path}) -> {StorageId, Path} end, ClientStorageInfo),
    lager:info("===========> Client storage info: ~p.", [NewClientStorageInfo]),
    {ok, Hash} = dao_lib:apply(dao_vfs, gen_fuse_group_name, [NewClientStorageInfo], ProtocolVersion),
    lager:info("===========> Hash: ~p.", [Hash]),
    case dao_lib:apply(dao_vfs, exist_fuse_group_name, [{hash, Hash}], ProtocolVersion) of
      {ok, true} ->
        lager:info("===========> Get fuse exists."),
        {ok, #veil_document{record = #fuse_group_name{name = Name}}} =
          dao_lib:apply(dao_vfs, get_fuse_group_name, [{hash, Hash}], ProtocolVersion),
        lager:info("===========> Get fuse group name (existing): ~p.", [Name]),
        {exist_fuse_group, Name};
      {ok, false} ->
        lager:info("===========> Get fuse dose not exist."),
        {ok, _} = dao_lib:apply(dao_vfs, save_fuse_group_name, [#fuse_group_name{name = Hash, hash = Hash}], ProtocolVersion),
        lager:info("===========> Get fuse group name (new): ~p.", [Hash]),
        {new_fuse_group, Hash};
      _ ->
        lager:info("===========> Get fuse group name: ERROR"),
        error
    end
  catch
    _:_ -> error
  end.

%% save_fuse_group_name_in_client_session/4
%% ====================================================================
%% @doc Saves fuse group name in client session
-spec save_fuse_group_name_in_client_session(Name, FuseSession, FuseSessionDoc, ProtocolVersion) -> Result when
  Name :: string(),
  FuseSession :: #fuse_session{},
  FuseSessionDoc :: #veil_document{},
  ProtocolVersion :: integer(),
  Result :: ok | error.
%% ====================================================================
save_fuse_group_name_in_client_session(Name, FuseSession, FuseSessionDoc, ProtocolVersion) ->
  lager:info("===========> Saving fuse group name in client session."),
  EnvVars = FuseSession#fuse_session.env_vars,
  NewFuseSessionDoc = FuseSessionDoc#veil_document{record = FuseSession#fuse_session{env_vars = [{group_id, Name} | EnvVars]}},
  case dao_lib:apply(dao_cluster, save_fuse_session, [NewFuseSessionDoc], ProtocolVersion) of
    {ok, _} -> ok;
    _ -> error
  end.

%% add_fuse_group_to_storages/3
%% ====================================================================
%% @doc Add fuse group to specified storages
-spec add_fuse_group_to_storages(Name, ClientStorageInfo, ProtocolVersion) -> Result when
  Name :: string(),
  ClientStorageInfo :: [{FieldName :: atom(), StorageId :: integer(), Path :: string()}],
  ProtocolVersion :: integer(),
  Result :: ok | error.
%% ====================================================================
add_fuse_group_to_storages(Name, ClientStorageInfo, ProtocolVersion) ->
  lager:info("===========> Adding fuse group name to storages."),
  lists:foreach(fun({_, StorageId, Root}) ->
    FuseGroup = #fuse_group_info{name = Name, storage_helper = #storage_helper_info{name = "DirectIO", init_args = [Root]}},
    case dao_lib:apply(dao_vfs, get_storage, [{id, StorageId}], ProtocolVersion) of
      {ok, #veil_document{record = #storage_info{fuse_groups = FuseGroups} = StorageInfo} = StorageInfoDoc} ->
        case dao_lib:apply(dao_vfs, save_storage, [StorageInfoDoc#veil_document{record = StorageInfo#storage_info{fuse_groups = [FuseGroup | FuseGroups]}}], ProtocolVersion) of
          {ok, _} -> ok;
          _ -> error
        end;
      _ -> error
    end
  end, ClientStorageInfo).