%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Public API for sending messages via connections of given session.
%%% @end
%%%-------------------------------------------------------------------
-module(connection_api).
-author("Bartosz Walkowicz").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send/2, send/3, send_via_any/2]).

-type message() :: #client_message{} | #server_message{}.


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv send(SessionId, Msg, []).
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), message()) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg) ->
    send(SessionId, Msg, []).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer. In case of errors during sending tries other
%% session connections until message is send or no more available
%% connections remains.
%% Exceptions to this are encoding errors which immediately fails call.
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), message(), ExcludedCons :: [pid()]) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg, ExcludedCons) ->
    case send_msg_excluding_connections(SessionId, Msg, ExcludedCons) of
        ok ->
            ok;
        {error, no_connections} = NoConsError ->
            ?debug("Failed to send msg to ~p due to lack of available "
                   "connections", [SessionId]),
            NoConsError;
        Error ->
            ?error("Failed to send msg ~s to peer ~p due to: ~p", [
                clproto_utils:msg_to_string(Msg), SessionId, Error
            ]),
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Tries to send given message via any of specified connections.
%% @end
%%--------------------------------------------------------------------
-spec send_via_any(message(), [pid()]) -> ok | {error, term()}.
send_via_any(_Msg, []) ->
    {error, no_connections};
send_via_any(Msg, [Conn]) ->
    connection:send_msg(Conn, Msg);
send_via_any(Msg, [Conn | Cons]) ->
    case connection:send_msg(Conn, Msg) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_conn_type} = WrongConnError ->
            WrongConnError;
        _Error ->
            send_via_any(Msg, Cons)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec send_msg_excluding_connections(session:id(), message(),
    ExcludedCons :: [pid()]) -> ok | {error, term()}.
send_msg_excluding_connections(SessionId, Msg, ExcludedCons) ->
    case session_connections:list(SessionId) of
        {ok, AllCons} ->
            Cons = utils:random_shuffle(AllCons -- ExcludedCons),
            send_via_any(Msg, Cons);
        Error ->
            Error
    end.
