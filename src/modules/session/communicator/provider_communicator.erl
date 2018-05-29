%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API between providers.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_communicator).
-author("Rafal Slota").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([send/2, send/3, stream/3, stream/4, send_async/2, communicate/2,
    communicate/3, communicate_async/2, communicate_async/3, ensure_connected/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv send(Msg, SessId, 1)
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
send(Msg, Ref) ->
    provider_communicator:send(Msg, Ref, 2).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the server using connection pool associated with server's
%% session or chosen connection. No reply is expected. Waits until message is
%% sent. If an error occurs retries specified number of attempts unless session
%% has been deleted in the meantime or connection does not exist.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #client_message{} | term(), Ref :: connection:ref(),
    Retry :: non_neg_integer() | infinity) -> ok | {error, Reason :: term()}.
send(#client_message{} = Msg, Ref, Retry) when Retry > 1; Retry == infinity ->
    ensure_connected(Ref),
    case connection:send(Msg, Ref) of
        ok -> ok;
        {error, _} ->
            timer:sleep(?SEND_RETRY_DELAY),
            case Retry of
                infinity -> provider_communicator:send(Msg, Ref, Retry);
                _ -> provider_communicator:send(Msg, Ref, Retry - 1)
            end
    end;
send(#client_message{} = Msg, Ref, 1) ->
    ensure_connected(Ref),
    connection:send(Msg, Ref);
send(Msg, Ref, Retry) ->
    provider_communicator:send(#client_message{message_body = Msg}, Ref, Retry).


%%--------------------------------------------------------------------
%% @doc
%% @equiv stream(StmId, Msg, SessId, 1)
%% @end
%%--------------------------------------------------------------------
-spec stream(StmId :: sequencer:stream_id(), Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
stream(StmId, Msg, Ref) ->
    provider_communicator:stream(StmId, Msg, Ref, 1).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the server using connection pool associated with server's
%% session or chosen connection. No reply is expected. Waits until message is
%% sent. If an error occurs retries specified number of attempts unless session
%% has been deleted in the meantime or connection does not exist.
%% @end
%%--------------------------------------------------------------------
-spec stream(StmId :: sequencer:stream_id(), Msg :: #client_message{} | term(), Ref :: session:id(),
    Retry :: non_neg_integer() | infinity) -> ok | {error, Reason :: term()}.
stream(StmId, #client_message{} = Msg, Ref, Retry) when Retry > 1; Retry == infinity ->
    ensure_connected(Ref),
    case sequencer:send_message(Msg, StmId, Ref) of
        ok -> ok;
        {error, _} ->
            timer:sleep(?SEND_RETRY_DELAY),
            case Retry of
                infinity ->
                    provider_communicator:stream(StmId, Msg, Ref, Retry);
                _ ->
                    provider_communicator:stream(StmId, Msg, Ref, Retry - 1)
            end
    end;
stream(StmId, #client_message{} = Msg, Ref, 1) ->
    ensure_connected(Ref),
    sequencer:send_message(Msg, StmId, Ref);
stream(StmId, Msg, Ref, Retry) ->
    provider_communicator:stream(StmId, #client_message{message_body = Msg}, Ref, Retry).

%%--------------------------------------------------------------------
%% @doc
%% Similar to communicator:send/2, but does not wait until message is sent.
%% Always returns 'ok' for non-empty connection pool or existing connection.
%% @end
%%--------------------------------------------------------------------
-spec send_async(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
send_async(#client_message{} = Msg, Ref) ->
    ensure_connected(Ref),
    connection:send_async(Msg, Ref);
send_async(Msg, Ref) ->
    send_async(#client_message{message_body = Msg}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the provider and waits for a reply.
%% @equiv communicate(Msg, Ref, 2)
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    {ok, #server_message{}} | {error, timeout} | {error, Reason :: term()}.
communicate(Msg, Ref) ->
    communicate(Msg, Ref, 2).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the provider and waits for a reply. Retries if needed.
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #client_message{} | term(), Ref :: connection:ref(),
    non_neg_integer()) ->
    {ok, #server_message{}} | {error, timeout} | {error, Reason :: term()}.
communicate(#client_message{} = ClientMsg, Ref, Retires) ->
    case {communicate_async(ClientMsg, Ref, self()), Retires} of
        {{ok, MsgId}, 1} ->
            receive_server_message(MsgId);
        {{ok, MsgId}, _} ->
            case receive_server_message(MsgId) of
                {ok, _} = Ans -> Ans;
                _ -> communicate(ClientMsg, Ref, Retires - 1)
            end;
        {Error, 1} ->
            Error;
        _ ->
            communicate(ClientMsg, Ref, Retires - 1)
    end;
communicate(Msg, Ref, Retires) ->
    communicate(#client_message{message_body = Msg}, Ref, Retires).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message and expects to handle a reply (with generated message ID)
%% by default worker associated with the reply type.
%% @equiv communicate_async(Msg, SessId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    {ok, #message_id{}} | {error, Reason :: term()}.
communicate_async(#client_message{} = ClientMsg, Ref) ->
    communicate_async(ClientMsg, Ref, undefined);
communicate_async(Msg, Ref) ->
    communicate_async(#client_message{message_body = Msg}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Sends server message to client, identified by session ID.
%% This function overrides message ID of request. When 'Recipient' is undefined,
%% the answer will be routed to default handler worker. Otherwise the client
%% answer will be send to ReplyPid process as: #server_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #client_message{} | term(), Ref :: connection:ref(),
    Recipient :: pid() | undefined) -> {ok, #message_id{}} | {error, Reason :: term()}.
communicate_async(#client_message{message_id = undefined} = Msg, Ref, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    communicate_async(Msg#client_message{message_id = MsgId}, Ref, Recipient);
communicate_async(#client_message{message_id = MsgId} = Msg, Ref, _Recipient) ->
    DoSend = case Msg of
        #client_message{message_stream = #message_stream{stream_id = StmId}} when is_integer(StmId) ->
            fun() -> stream(StmId, Msg, Ref, 2) end;
        _ ->
            fun() -> send(Msg, Ref, 2) end
    end,
    case DoSend() of
        ok -> {ok, MsgId};
        {error, Reason} -> {error, Reason}
    end;
communicate_async(Msg, Ref, Recipient) ->
    communicate_async(#client_message{message_body = Msg}, Ref, Recipient).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Ensures that there is at least one outgoing connection for given session.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected(session:id() | pid()) ->
    ok | no_return().
ensure_connected(Conn) when is_pid(Conn) ->
    ok;
ensure_connected(SessId) ->
    case session:get_random_connection(SessId, true) of
        {error, _} ->
            ProviderId = case session:get(SessId) of
                {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia) ->
                    ProxyVia;
                _ ->
                    session_manager:session_id_to_provider_id(SessId)
            end,

            case oneprovider:get_id() of
                ProviderId ->
                    ?warning("Provider attempted to connect to itself, skipping connection."),
                    erlang:error(connection_loop_detected);
                _ ->
                    ok
            end,

            {ok, Domain} = provider_logic:get_domain(ProviderId),
            {ok, IPs} = inet:getaddrs(binary_to_list(Domain), inet),
            IPBinaries = lists:map(fun(IP) ->
                list_to_binary(inet:ntoa(IP))
            end, IPs),
            lists:foreach(
                fun(IPBinary) ->
                    Port = https_listener:port(),
                    critical_section:run([?MODULE, ProviderId, SessId], fun() ->
                        % check once more to prevent races
                        case session:get_random_connection(SessId, true) of
                            {error, _} ->
                                outgoing_connection:start(ProviderId, SessId,
                                    Domain, IPBinary, Port, ranch_ssl, timer:seconds(5));
                            _ ->
                                ensure_connected(SessId)
                        end
                    end)
                end, IPBinaries),
            ok;
        {ok, Pid} ->
            case utils:process_info(Pid, initial_call) of
                undefined ->
                    ok = session:remove_connection(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives reply from other provider
%% @end
%%--------------------------------------------------------------------
-spec receive_server_message(MsgId :: #message_id{}) ->
    {ok, #server_message{}} | {error, timeout} | {error, Reason :: term()}.
receive_server_message(MsgId) ->
    Timeout = 3 * router:get_processes_check_interval(),
    receive
        #server_message{message_id = MsgId,
            message_body = #processing_status{code = 'IN_PROGRESS'}} ->
            receive_server_message(MsgId);
        #server_message{message_id = MsgId} = ServerMsg ->
            {ok, ServerMsg}
    after
        Timeout ->
            {error, timeout}
    end.
