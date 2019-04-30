%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API with remote client/provider.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Bartosz Walkowicz").

-include("timeouts.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([
    send_to_oneclient/2, send_to_oneclient/3,

    send_to_provider/2, send_to_provider/3, send_to_provider/4,
    communicate_with_provider/2, communicate_with_provider/3,
    stream_to_provider/4
]).

% Pid of process that should receive response to send message.
% It is part of MsgId, so if left undefined no new MsgId will
% generated for given message.
-type recipient_pid() :: undefined | pid().
-type retries() :: non_neg_integer() | infinity.

-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().
% Generic message can by either client_message, server_message or any
% struct that can be used as `message_body` for the first two.
-type generic_message() :: client_message() | server_message() | tuple().

-type error() :: {error, Reason :: term()}.

%%%===================================================================
%%% API - convenience functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_oneclient(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_oneclient(session:id(), generic_message()) -> ok | error().
send_to_oneclient(SessionId, Msg) ->
    % called as such to allow mocking in tests
    communicator:send_to_oneclient(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to client. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% Exception to this is lack of valid connections, that is
%% no_connections error which fails call immediately.
%% @end
%%--------------------------------------------------------------------
-spec send_to_oneclient(session:id(), generic_message(), retries()) ->
    ok | error().
send_to_oneclient(SessionId, #server_message{} = Msg0, Retries) ->
    Msg1 = clproto_utils:fill_effective_session_info(Msg0, SessionId),
    send_to_oneclient_internal(SessionId, Msg1, Retries);
send_to_oneclient(SessionId, Msg, RetriesLeft) ->
    ServerMsg = #server_message{message_body = Msg},
    send_to_oneclient(SessionId, ServerMsg, RetriesLeft).


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_provider(SessionId, Msg, undefined).
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_message()) ->
    ok | {ok | clproto_message_id:id()} | error().
send_to_provider(SessionId, Msg) ->
    send_to_provider(SessionId, Msg, undefined).


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_provider(SessionId, Msg, RecipientPid, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_message(), undefined | pid()) ->
    ok | {ok | clproto_message_id:id()} | error().
send_to_provider(SessionId, Msg, RecipientPid) ->
    send_to_provider(SessionId, Msg, RecipientPid, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_message(), recipient_pid(),
    retries()) -> ok | {ok | clproto_message_id:id()} | error().
send_to_provider(SessionId, #client_message{} = Msg0, RecipientPid, Retries) ->
    {MsgId, Msg1} = maybe_set_msg_id(Msg0, RecipientPid),
    Msg2 = clproto_utils:fill_effective_session_info(Msg1, SessionId),
    case {send_to_provider_internal(SessionId, Msg2, Retries), RecipientPid} of
        {ok, undefined} ->
            ok;
        {ok, _} ->
            {ok, MsgId};
        {{error, no_connections}, _} ->
            ?ERROR_NO_CONNECTION_TO_PEER_PROVIDER;
        {Error, _} ->
            Error
    end;
send_to_provider(SessionId, Msg, RecipientPid, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    send_to_provider(SessionId, ClientMsg, RecipientPid, Retries).


%%--------------------------------------------------------------------
%% @doc
%% @equiv communicate_with_provider(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_message()) ->
    {ok | message()} | error().
communicate_with_provider(SessionId, Msg) ->
    communicate_with_provider(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider and awaits answer. In case of errors keeps
%% retrying until either message is sent or no more retries are left.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_message(), retries()) ->
    {ok, message()} | error().
communicate_with_provider(SessionId, #client_message{} = Msg0, Retries) ->
    Msg1 = clproto_utils:fill_effective_session_info(Msg0, SessionId),
    case communicate_with_provider_internal(SessionId, Msg1, Retries) of
        {ok, _} = Ans ->
            Ans;
        {error, no_connections} ->
            ?ERROR_NO_CONNECTION_TO_PEER_PROVIDER;
        Error ->
            Error
    end;
communicate_with_provider(SessionId, Msg, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    communicate_with_provider(SessionId, ClientMsg, Retries).


%%--------------------------------------------------------------------
%% @doc
%% Sends stream message to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec stream_to_provider(session:id(), generic_message(),
    sequencer:stream_id(), recipient_pid()) ->
    ok | {ok | clproto_message_id:id()} | error().
stream_to_provider(SessionId, #client_message{} = Msg0, StmId, RecipientPid) ->
    {MsgId, Msg} = maybe_set_msg_id(Msg0, RecipientPid),
    case {stream_to_provider_internal(SessionId, Msg, StmId, 1), RecipientPid} of
        {ok, undefined} ->
            ok;
        {ok, _} ->
            {ok, MsgId};
        {Error, _} ->
            Error
    end;
stream_to_provider(SessionId, Msg, StreamId, RecipientPid) ->
    ClientMsg = #client_message{message_body = Msg},
    stream_to_provider(SessionId, ClientMsg, StreamId, RecipientPid).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec send_to_oneclient_internal(session:id(), server_message(), retries()) ->
    ok | error().
send_to_oneclient_internal(SessionId, Msg, 0) ->
    connection_api:send(SessionId, Msg);
send_to_oneclient_internal(SessionId, Msg, Retries) ->
    case connection_api:send(SessionId, Msg) of
        ok ->
            ok;
        {error, no_connections} = NoConnectionsError ->
            NoConnectionsError;
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_oneclient_internal(SessionId, Msg, decrement_retries(Retries))
    end.


%% @private
-spec send_to_provider_internal(session:id(), client_message(), retries()) ->
    ok | error().
send_to_provider_internal(SessionId, Msg, 0) ->
    connection_api:send(SessionId, Msg);
send_to_provider_internal(SessionId, Msg, Retries) ->
    case connection_api:send(SessionId, Msg) of
        ok ->
            ok;
        {error, not_found} ->
            session_connections:ensure_connected(SessionId),
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_provider_internal(SessionId, Msg, decrement_retries(Retries));
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_provider_internal(SessionId, Msg, decrement_retries(Retries))
    end.


%% @private
-spec communicate_with_provider_internal(session:id(), client_message(),
    retries()) -> {ok, message()} | error().
communicate_with_provider_internal(SessionId, Msg, 0) ->
    connection_api:communicate(SessionId, Msg);
communicate_with_provider_internal(SessionId, Msg, Retries) ->
    case connection_api:communicate(SessionId, Msg) of
        {ok, _Response} = Ans ->
            Ans;
        {error, not_found} ->
            session_connections:ensure_connected(SessionId),
            timer:sleep(?SEND_RETRY_DELAY),
            RetriesLeft = decrement_retries(Retries),
            communicate_with_provider_internal(SessionId, Msg, RetriesLeft);
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            RetriesLeft = decrement_retries(Retries),
            communicate_with_provider_internal(SessionId, Msg, RetriesLeft)
    end.


%% @private
-spec stream_to_provider_internal(session:id(), client_message(),
    sequencer:stream_id(), retries()) -> {ok, message()} | error().
stream_to_provider_internal(SessionId, Msg, StmId, 0) ->
    sequencer:send_message(Msg, StmId, SessionId);
stream_to_provider_internal(SessionId, Msg, StmId, Retries) ->
    case sequencer:send_message(Msg, StmId, SessionId) of
        ok ->
            ok;
        {error, not_found} ->
            session_connections:ensure_connected(SessionId),
            timer:sleep(?SEND_RETRY_DELAY),
            RetriesLeft = decrement_retries(Retries),
            stream_to_provider_internal(SessionId, Msg, StmId, RetriesLeft);
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            RetriesLeft = decrement_retries(Retries),
            stream_to_provider_internal(SessionId, Msg, StmId, RetriesLeft)
    end.


%% @private
-spec decrement_retries(retries()) -> retries().
decrement_retries(infinity) -> infinity;
decrement_retries(Num) -> Num - 1.


%% @private
-spec maybe_set_msg_id(message(), recipient_pid()) ->
    {undefined | clproto_message_id:id(), message()}.
maybe_set_msg_id(#client_message{message_id = undefined} = Msg, undefined) ->
    {undefined, Msg};
maybe_set_msg_id(#client_message{message_id = undefined} = Msg, Recipient) ->
    {ok, MsgId} = clproto_message_id:generate(Recipient),
    {MsgId, Msg#client_message{message_id = MsgId}};
maybe_set_msg_id(#client_message{message_id = MsgId} = Msg, _Recipient) ->
    {MsgId, Msg}.
