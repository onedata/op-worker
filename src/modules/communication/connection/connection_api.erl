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
-export([send/2, send/3, send/4, send_via_any/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv send(SessionId, Msg, []).
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), communicator:message()) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg) ->
    send(SessionId, Msg, []).


%%--------------------------------------------------------------------
%% @doc
%% @equiv send(SessionId, Msg, ExcludedCons, false).
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), communicator:message(), ExcludedCons :: [pid()]) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg, ExcludedCons) ->
    send(SessionId, Msg, ExcludedCons, false).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer. In case of errors during sending tries other
%% session connections until message is send or no more available
%% connections remains.
%% Exceptions to this are encoding errors which immediately fails call.
%% Additionally logs eventual errors if LogErrors is set to true.
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), communicator:message(), ExcludedCons :: [pid()],
    boolean()) -> ok | {error, Reason :: term()}.
send(SessionId, Msg, ExcludedCons, LogErrors) ->
    case send_msg_excluding_connections(SessionId, Msg, ExcludedCons) of
        ok ->
            ok;
        Error ->
            LogErrors andalso log_sending_msg_error(SessionId, Msg, Error),
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% TODO VFS-6364 refactor proxy session - session restart should be on this level
%% Tries to send given message via any of specified connections.
%% @end
%%--------------------------------------------------------------------
-spec send_via_any(communicator:message(), EffSessId :: session:id(), [pid()]) ->
    ok | {error, term()}.
send_via_any(_Msg, EffSessId, []) ->
    session_manager:restart_session_if_dead(EffSessId),
    {error, no_connections};
send_via_any(Msg, EffSessId, [Conn]) ->
    case connection:send_msg(Conn, Msg) of
        {error, no_connection} = Error ->
            session_manager:restart_session_if_dead(EffSessId),
            Error;
        Result ->
            Result
    end;
send_via_any(Msg, EffSessId, [Conn | Cons]) ->
    case connection:send_msg(Conn, Msg) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_conn_type} = WrongConnError ->
            WrongConnError;
        {error, no_connection} ->
            session_manager:restart_session_if_dead(EffSessId),
            send_via_any(Msg, EffSessId, Cons);
        _Error ->
            send_via_any(Msg, EffSessId, Cons)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec send_msg_excluding_connections(session:id(), communicator:message(),
    ExcludedCons :: [pid()]) -> ok | {error, term()}.
send_msg_excluding_connections(SessionId, Msg, ExcludedCons) ->
    case session_connections:list(SessionId) of
        {ok, EffSessId, AllCons} ->
            Cons = lists_utils:shuffle(AllCons -- ExcludedCons),
            send_via_any(Msg, EffSessId, Cons);
        Error ->
            Error
    end.


%% @private
-spec log_sending_msg_error(session:id(), communicator:message(),
    {error, term()}) -> ok.
log_sending_msg_error(SessionId, _Msg, {error, no_connections}) ->
    ?debug("Failed to send msg to ~p due to lack of available "
           "connections", [SessionId]);
log_sending_msg_error(SessionId, Msg, Error) ->
    ?error("Failed to send msg ~s to peer ~p due to: ~p", [
        clproto_utils:msg_to_string(Msg), SessionId, Error
    ]).
