%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incomming client messages.
%%% @end
%%%-------------------------------------------------------------------
-module(router).
-author("Tomasz Lichon").

-include("proto_internal/oneclient/message_id.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/ping_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([preroute_message/2, route_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec preroute_message(Msg :: #client_message{}, SessId :: session:id()) ->
    ok | {ok, #server_message{}} | {error, term()}.
preroute_message(#client_message{message_stream = undefined} = Msg, _SessId) ->
    router:route_message(Msg);
preroute_message(Msg, SessId) ->
    sequencer_manager:route_message(Msg, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate handler, this function should never throw
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{}) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_message(Msg = #client_message{message_id = undefined}) ->
    route_and_ignore_answer(Msg);
route_message(Msg = #client_message{message_id = #message_id{issuer = server,
    recipient = undefined}}) ->
    route_and_ignore_answer(Msg);
route_message(Msg = #client_message{message_id = #message_id{issuer = server,
    recipient = Pid}}) ->
    Pid ! Msg,
    ok;
route_message(Msg = #client_message{message_id = #message_id{issuer = client}}) ->
    route_and_send_answer(Msg).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker and return ok
%% @end
%%--------------------------------------------------------------------
-spec route_and_ignore_answer(#client_message{}) -> ok.
route_and_ignore_answer(#client_message{session_id = SessionId,
    message_body = #read_event{} = Evt}) ->
    event_manager:emit(Evt, SessionId);
route_and_ignore_answer(#client_message{session_id = SessionId,
    message_body = #write_event{} = Evt}) ->
    event_manager:emit(Evt, SessionId);
route_and_ignore_answer(Msg = #client_message{}) ->
    ?info("route_and_ignore_answer(~p)", [Msg]),
    ok. %todo

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) -> ok.
route_and_send_answer(#client_message{message_id = Id,
    message_body = #ping{}}) ->
    protocol_handler:cast(self(), {send, #server_message{message_id = Id,
        message_body = #pong{}}}), %todo send through communicator
    ok;
route_and_send_answer(Msg = #client_message{}) ->
    ?info("route_and_send_answer(~p)", [Msg]),
    ok. %todo
