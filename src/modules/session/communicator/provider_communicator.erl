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
-module(provider_communicator).
-author("Krzysztof Trzepla").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send/2, send/3, send_async/2, communicate/2, communicate_async/2,
    communicate_async/3]).

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
    case Ref of
        Pid when is_pid(Pid) ->
            ok;
        SessId ->
            ensure_connected(SessId)
    end,
    case connection:send(Msg, Ref) of
        ok -> ok;
        {error, _} ->
            timer:sleep(?SEND_RETRY_DELAY),
            case Retry of
                infinity -> communicator:send(Msg, Ref, Retry);
                _ -> communicator:send(Msg, Ref, Retry - 1)
            end
    end;
send(#client_message{} = Msg, Ref, 1) ->
    case Ref of
        Pid when is_pid(Pid) ->
            ok;
        SessId ->
            ensure_connected(SessId)
    end,
    connection:send(Msg, Ref);
send(Msg, Ref, Retry) ->
    provider_communicator:send(#client_message{message_body = Msg}, Ref, Retry).

%%--------------------------------------------------------------------
%% @doc
%% Similar to communicator:send/2, but does not wait until message is sent.
%% Always returns 'ok' for non-empty connection pool or existing connection.
%% @end
%%--------------------------------------------------------------------
-spec send_async(Msg :: #client_message{} | term(), Ref :: connection:ref()) ->
    ok | {error, Reason :: term()}.
send_async(#client_message{} = Msg, Ref) ->
    connection:send_async(Msg, Ref);
send_async(Msg, Ref) ->
    communicator:send_async(#client_message{message_body = Msg}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client and waits for a reply.
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
    case send(Msg#client_message{message_id = MsgId#message_id{issuer = client}}, Ref) of
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
-spec ensure_connected(session:id()) ->
    ok | no_return().
ensure_connected(SessId) ->
    ProviderId = session_manager:session_id_to_provider_id(SessId),
    case session:get_random_connection(SessId) of
        {error, _} ->
            {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
            lists:foreach(
                fun(URL) ->
                    Port = 5556,
                    connection:start_link(SessId, URL, Port, ranch_ssl2, timer:seconds(5))
                end, URLs),
            ok;
        {ok, Pid} ->
            case process_info(Pid) of
                undefined ->
                    ok = session:remove_connection(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.
