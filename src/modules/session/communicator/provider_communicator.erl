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
-export([send/2, send/3, stream/3, stream/4, send_async/2, communicate/2, communicate_async/2,
    communicate_async/3, ensure_connected/1]).

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
    provider_communicator:send(Msg, Ref, 1).

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
                infinity -> provider_communicator:stream(StmId, Msg, Ref, Retry);
                _ -> provider_communicator:stream(StmId, Msg, Ref, Retry - 1)
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
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    {ok, #server_message{}} | {error, timeout} | {error, Reason :: term()}.
communicate(#client_message{} = ClientMsg, Ref) ->
    {ok, MsgId} = communicate_async(ClientMsg, Ref, self()),
    receive
        #server_message{message_id = MsgId} = ServerMsg -> {ok, ServerMsg}
    after
        ?DEFAULT_REQUEST_TIMEOUT ->
            {error, timeout}
    end;
communicate(Msg, Ref) ->
    communicate(#client_message{message_body = Msg}, Ref).

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
communicate_async(#client_message{} = Msg, Ref, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    NewMsg = Msg#client_message{message_id = MsgId},
    DoSend = case NewMsg of
        #client_message{message_stream = #message_stream{stream_id = StmId}} when is_integer(StmId) ->
            fun() -> stream(StmId, NewMsg, Ref, 2) end;
        _ ->
            fun() -> send(NewMsg, Ref, 2) end
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
            {ok, URLs} = provider_logic:get_urls(ProviderId),
            lists:foreach(
                fun(URL) ->
                    {ok, Port} = application:get_env(?APP_NAME, provider_protocol_handler_port),
                    connection:start_link(SessId, URL, Port, ranch_ssl, timer:seconds(5))
                end, URLs),
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
