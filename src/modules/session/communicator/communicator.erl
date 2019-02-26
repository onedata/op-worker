%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API between remote client/provider
%%% and server.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Michal Wrzeszcz").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("timeouts.hrl").

%%% API - convenience functions
-export([
    send_to_client/2, send_to_client/3,
    cast_to_provider/2,
    send_to_provider/2, send_to_provider/3,
    stream_to_provider/3,
    communicate_with_provider/2,
    communicate_with_provider/3
]).

-type retries() :: non_neg_integer() | infinity.
-type message() :: #client_message{} | #server_message{}.
-type generic_message() :: tuple().


%%%===================================================================
%%% API - convenience functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_client(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(session:id(), generic_message()) ->
    ok | {ok | message_id:id()} | {error, Reason :: term()}.
send_to_client(SessionId, Msg) ->
    communicator:send_to_client(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to client. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% Exception to this is empty connection pool error,
%% which fails call immediately.
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(session:id(), generic_message(), retries()) ->
    ok | {ok | message_id:id()} | {error, Reason :: term()}.
send_to_client(SessionId, #server_message{} = Msg, 0) ->
    connection_manager:send_sync(SessionId, Msg);
send_to_client(SessionId, #server_message{} = Msg, Retries) ->
    case connection_manager:send_sync(SessionId, Msg) of
        {error, no_connections} = EmptyConnectionPoolError ->
            EmptyConnectionPoolError;
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_client(SessionId, Msg, retries_left(Retries));
        Ans ->
            Ans
    end;
send_to_client(SessionId, Msg, RetriesLeft) ->
    ServerMsg = #server_message{message_body = Msg},
    send_to_client(SessionId, ServerMsg, RetriesLeft).


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_provider(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_message()) ->
    ok | {ok | message_id:id()} | {error, Reason :: term()}.
send_to_provider(SessionId, Msg) ->
    communicator:send_to_provider(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_message(), retries()) ->
    ok | {ok | message_id:id()} | {error, Reason :: term()}.
send_to_provider(SessionId, #client_message{} = Msg, 0) ->
    session_connections:ensure_connected(SessionId),
    connection_manager:send_sync(SessionId, Msg);
send_to_provider(SessionId, #client_message{} = Msg, Retries) ->
    session_connections:ensure_connected(SessionId),
    case connection_manager:send_sync(SessionId, Msg) of
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_provider(SessionId, Msg, retries_left(Retries));
        Ans ->
            Ans
    end;
send_to_provider(SessionId, Msg, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    send_to_provider(SessionId, ClientMsg, Retries).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to provider. Does not wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec cast_to_provider(session:id(), generic_message()) -> ok | {error, term()}.
cast_to_provider(SessionId, #client_message{} = Msg) ->
    session_connections:ensure_connected(SessionId),
    case connection_manager:send_async(SessionId, Msg) of
        {error, empty_connection_pool} ->
            timer:sleep(?SEND_RETRY_DELAY),
            cast_to_provider(SessionId, Msg);
        {error, _Reason} = Error ->
            Error;
        _ ->
            ok
    end;
cast_to_provider(SessionId, Msg) ->
    send_to_provider(SessionId, #client_message{message_body = Msg}).


%%--------------------------------------------------------------------
%% @doc
%% Sends stream message to provider.
%% @end
%%--------------------------------------------------------------------
-spec stream_to_provider(session:id(), generic_message(), sequencer:stream_id()) ->
    ok | {ok | message_id:id()} | {error, Reason :: term()}.
stream_to_provider(SessionId, Msg, StmId) ->
    session_connections:ensure_connected(SessionId),
    sequencer:send_message(Msg, StmId, SessionId).


%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_message()) ->
    {ok, message()} | {error, Reason :: term()}.
communicate_with_provider(SessionId, #client_message{} = Msg) ->
    session_connections:ensure_connected(SessionId),
    % TODO retry is necessary because proc tries to send msg
    % when conn is connecting (upgrading_protocol, performing_handshake)
    case connection_manager:communicate(SessionId, Msg) of
        {error, no_connections} ->
            timer:sleep(?SEND_RETRY_DELAY),
            communicate_with_provider(SessionId, Msg);
        {error, _Reason} = Error ->
            Error;
        Res ->
            Res
    end;
communicate_with_provider(SessionId, Msg) ->
    communicate_with_provider(SessionId, #client_message{message_body = Msg}).


%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_message(), pid()) ->
    {ok | message_id:id()} | {error, Reason :: term()}.
communicate_with_provider(SessionId, #client_message{} = Msg0, Recipient) ->
    {MsgId, Msg} = protocol_utils:maybe_set_msg_id(Msg0, Recipient),
    case Msg of
        #client_message{message_stream = #message_stream{stream_id = StmId}}
            when is_integer(StmId) ->
            case communicator:stream_to_provider(SessionId, Msg, StmId) of
                ok ->
                    {ok, MsgId};
                Error ->
                    Error
            end;
        _ ->
            communicator:send_to_provider(SessionId, Msg, 0)
    end;
communicate_with_provider(SessionId, Msg, Recipient) ->
    ClientMsg = #client_message{message_body = Msg},
    communicate_with_provider(SessionId, ClientMsg, Recipient).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec retries_left(retries()) -> retries().
retries_left(infinity) -> infinity;
retries_left(Num) -> Num - 1.


%%%%--------------------------------------------------------------------
%%%% @private
%%%% @doc
%%%% Forwards message to connection.
%%%% @end
%%%%--------------------------------------------------------------------
%%-spec forward_to_connection_proc(message(), session:id(), Async :: boolean()) ->
%%    ok | {error, Reason :: term()}.
%%forward_to_connection_proc(Msg, Ref, true) when is_pid(Ref) ->
%%    Ref ! {send_async, Msg},
%%    ok;
%%forward_to_connection_proc(Msg, Ref, false) when is_pid(Ref) ->
%%    try
%%        Ref ! {send_sync, self(), Msg},
%%        receive
%%            {result, Resp} ->
%%                Resp
%%        after
%%            ?DEFAULT_REQUEST_TIMEOUT ->
%%                {error, timeout}
%%        end
%%    catch
%%        _:Reason -> {error, Reason}
%%    end;
%%forward_to_connection_proc(Msg, SessionId, Async) ->
%%    MsgWithProxyInfo = case Async of
%%        true -> Msg;
%%        false -> protocol_utils:fill_proxy_info(Msg, SessionId)
%%    end,
%%    case session_connections:get_random_connection(SessionId) of
%%        {ok, Con} -> forward_to_connection_proc(MsgWithProxyInfo, Con, Async);
%%        {error, Reason} -> {error, Reason}
%%    end.
