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

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
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
    ?debug("preroute_message fuse msg ~p", [Msg]),
    router:route_message(Msg);
preroute_message(Msg, SessId) ->
    ?debug("preroute_message fuse msg ~p", [Msg]),
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
    message_body = #event{event = Evt}}) ->
    event_manager:emit(Evt, SessionId);
% Message that updates the #auth{} record in given session (originates from
% #'Token' client message).
route_and_ignore_answer(#client_message{session_id = SessionId,
    message_body = #auth{} = Auth}) ->
    % This function performs an async call to session manager worker.
    ok = session_manager:update_session_auth(SessionId, Auth).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) -> ok.
route_and_send_answer(#client_message{message_id = Id,
    message_body = #ping{data = Data}, session_id = SessId}) ->
    Pong = #server_message{message_id = Id, message_body = #pong{data = Data}},
    communicator:send(Pong, SessId);
route_and_send_answer(#client_message{message_id = Id,
    message_body = #get_protocol_version{}, session_id = SessId}) ->
    ProtoV = #server_message{message_id = Id, message_body = #protocol_version{}},
    communicator:send(ProtoV, SessId);
route_and_send_answer(#client_message{message_id = Id, session_id = SessId,
    message_body = #fuse_request{fuse_request = FuseRequest}}) ->
    ?debug("Fuse request ~p", [FuseRequest]),
    spawn(fun() ->
        FuseResponse = worker_proxy:call(fslogic_worker, {fuse_request, SessId, FuseRequest}),
        ?debug("Fuse response ~p", [FuseResponse]),
        communicator:send(#server_message{
            message_id = Id, message_body = FuseResponse
        }, SessId)
    end),
    ok.
