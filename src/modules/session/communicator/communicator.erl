%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API between remote client and server.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Krzysztof Trzepla").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send/2, send/3, send_async/2, communicate/2, communicate_async/2,
    communicate_async/3, ensure_sent/2]).

-define(SEND_RETRY_DELAY, timer:seconds(5)).
-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(30)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv send(Msg, SessId, 1)
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | term(), SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
send(Msg, SessId) ->
    communicator:send(Msg, SessId, 1).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client identified by session ID. No reply is expected.
%% Waits until message is sent by one of connections from underlying connection
%% pool. If an error occurs retries specified number of attempts unless session
%% has been deleted in the meantime.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | term(), SessId :: session:id(),
    Retry :: non_neg_integer() | infinity) -> ok | {error, Reason :: term()}.
send(#server_message{} = Msg, SessId, Retry) when Retry > 1; Retry == infinity ->
    case connection:send(Msg, SessId) of
        ok -> ok;
        {error, {not_found, session}} -> {error, {not_found, session}};
        {error, _} ->
            timer:sleep(?SEND_RETRY_DELAY),
            case Retry of
                infinity -> communicator:send(Msg, SessId, Retry);
                _ -> communicator:send(Msg, SessId, Retry - 1)
            end
    end;
send(#server_message{} = Msg, SessId, 1) ->
    connection:send(Msg, SessId);
send(Msg, SessId, Retry) ->
    communicator:send(#server_message{message_body = Msg}, SessId, Retry).

%%--------------------------------------------------------------------
%% @doc
%% Similar to communicator:send/2, but does not wait until message is sent using
%% underlying connection pool. Always returns 'ok' for non-empty connection pool.
%% @end
%%--------------------------------------------------------------------
-spec send_async(Msg :: #server_message{} | term(), SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
send_async(#server_message{} = Msg, SessId) ->
    connection:send_async(Msg, SessId);
send_async(Msg, SessId) ->
    communicator:send_async(#server_message{message_body = Msg}, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client and waits for a reply.
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #server_message{} | term(), SessId :: session:id()) ->
    {ok, #client_message{}} | {error, timeout}.
communicate(#server_message{} = ServerMsg, SessId) ->
    {ok, MsgId} = communicate_async(ServerMsg, SessId, self()),
    receive
        #client_message{message_id = MsgId} = ClientMsg -> {ok, ClientMsg}
    after
        ?DEFAULT_REQUEST_TIMEOUT ->
            {error, timeout}
    end;
communicate(Msg, SessId) ->
    communicate(#server_message{message_body = Msg}, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message and expects to handle a reply (with generated message ID)
%% by default worker associated with the reply type.
%% @equiv communicate_async(Msg, SessId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessId :: session:id()) ->
    {ok, #message_id{}}.
communicate_async(Msg, SessId) ->
    communicate_async(Msg, SessId, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends server message to client, identified by session ID.
%% This function overrides message ID of request. When 'Recipient' is undefined,
%% the answer will be routed to default handler worker. Otherwise the client
%% answer will be send to ReplyPid process as: #client_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{} | term(), SessId :: session:id(),
    Recipient :: pid() | undefined) -> {ok, #message_id{}} | {error, Reason :: term()}.
communicate_async(#server_message{} = Msg, SessId, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    case send(Msg#server_message{message_id = MsgId}, SessId) of
        ok -> {ok, MsgId};
        {error, Reason} -> {error, Reason}
    end;
communicate_async(Msg, SessId, Recipient) ->
    communicate_async(#server_message{message_body = Msg}, SessId, Recipient).

%%--------------------------------------------------------------------
%% @doc
%% Spawns a process that is responsible for sending provided message.
%% The process terminates when the message has been successfully sent or
%% session has been deleted.
%% @end
%%--------------------------------------------------------------------
-spec ensure_sent(Msg :: #server_message{} | term(), SessId :: session:id()) ->
    ok.
ensure_sent(Msg, SessId) ->
    spawn(?MODULE, send, [Msg, SessId, infinity]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
