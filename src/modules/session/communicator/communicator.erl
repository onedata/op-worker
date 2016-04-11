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
-include("timeouts.hrl").

%% API
-export([send/2, send/3, send_async/2, communicate/2, communicate_async/2,
    communicate_async/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv send(Msg, SessId, 1)
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
send(Msg, Ref) ->
    communicator:send(Msg, Ref, 1).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client using connection pool associated with client's
%% session or chosen connection. No reply is expected. Waits until message is
%% sent. If an error occurs retries specified number of attempts unless session
%% has been deleted in the meantime or connection does not exist.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | term(), Ref :: connection:ref(),
    Retry :: non_neg_integer() | infinity) -> ok | {error, Reason :: term()}.
send(#server_message{} = Msg, Ref, Retry) when Retry > 1; Retry == infinity ->
    case connection:send(Msg, Ref) of
        ok -> ok;
        {error, _} ->
            timer:sleep(?SEND_RETRY_DELAY),
            case Retry of
                infinity -> communicator:send(Msg, Ref, Retry);
                _ -> communicator:send(Msg, Ref, Retry - 1)
            end
    end;
send(#server_message{} = Msg, Ref, 1) ->
    connection:send(Msg, Ref);
send(Msg, Ref, Retry) ->
    communicator:send(#server_message{message_body = Msg}, Ref, Retry).

%%--------------------------------------------------------------------
%% @doc
%% Similar to communicator:send/2, but does not wait until message is sent.
%% Always returns 'ok' for non-empty connection pool or existing connection.
%% @end
%%--------------------------------------------------------------------
-spec send_async(Msg :: #server_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
send_async(#server_message{} = Msg, Ref) ->
    connection:send_async(Msg, Ref);
send_async(Msg, Ref) ->
    communicator:send_async(#server_message{message_body = Msg}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client and waits for a reply.
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #server_message{} | term(), Ref :: connection:ref()) ->
    {ok, #client_message{}} | {error, timeout}.
communicate(#server_message{} = ServerMsg, Ref) ->
    {ok, MsgId} = communicate_async(ServerMsg, Ref, self()),
    receive
        #client_message{message_id = MsgId} = ClientMsg -> {ok, ClientMsg}
    after
        ?DEFAULT_REQUEST_TIMEOUT ->
            {error, timeout}
    end;
communicate(Msg, Ref) ->
    communicate(#server_message{message_body = Msg}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message and expects to handle a reply (with generated message ID)
%% by default worker associated with the reply type.
%% @equiv communicate_async(Msg, SessId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, Ref :: connection:ref()) ->
    {ok, #message_id{}}.
communicate_async(Msg, Ref) ->
    communicate_async(Msg, Ref, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends server message to client, identified by session ID.
%% This function overrides message ID of request. When 'Recipient' is undefined,
%% the answer will be routed to default handler worker. Otherwise the client
%% answer will be send to ReplyPid process as: #client_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{} | term(), Ref :: connection:ref(),
    Recipient :: pid() | undefined) -> {ok, #message_id{}} | {error, Reason :: term()}.
communicate_async(#server_message{} = Msg, Ref, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    case send(Msg#server_message{message_id = MsgId}, Ref) of
        ok -> {ok, MsgId};
        {error, Reason} -> {error, Reason}
    end;
communicate_async(Msg, Ref, Recipient) ->
    communicate_async(#server_message{message_body = Msg}, Ref, Recipient).

%%%===================================================================
%%% Internal functions
%%%===================================================================
