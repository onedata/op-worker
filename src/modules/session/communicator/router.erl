%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming client messages.
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
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([preroute_message/2, route_message/1, route_proxy_message/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Route messages that were send to remote-proxy session on different provider.
%% @end
%%--------------------------------------------------------------------
-spec route_proxy_message(Msg :: #client_message{}, TargetSessionId :: session:id()) -> ok.
route_proxy_message(#client_message{message_body = #events{events = Evts}} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    lists:foreach(fun(#event{} = Evt) -> event:emit(Evt, TargetSessionId) end, Evts),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec preroute_message(Msg :: #client_message{} | #server_message{}, SessId :: session:id()) ->
    ok | {ok, #server_message{}} | {error, term()}.
preroute_message(#client_message{message_body = #message_request{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #message_acknowledgement{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #end_of_message_stream{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #message_stream_reset{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_stream = undefined} = Msg, _SessId) ->
    router:route_message(Msg);
preroute_message(#server_message{message_stream = undefined} = Msg, _SessId) ->
    router:route_message(Msg);
preroute_message(Msg, SessId) ->
    sequencer:route_message(Msg, SessId).

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
route_message(Msg = #server_message{message_id = #message_id{issuer = client,
    recipient = Pid}}) when is_pid(Pid) ->
    Pid ! Msg,
    ok;
route_message(Msg = #server_message{message_id = #message_id{issuer = client,
    recipient = Pid}}) ->
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
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #event{} = Evt}) ->
    event:emit(Evt, SessId),
    ok;
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #events{events = Evts}}) ->
    lists:foreach(fun(#event{} = Evt) -> event:emit(Evt, SessId) end, Evts),
    ok;
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #subscription{} = Sub}) ->
    event:subscribe(event_utils:inject_event_stream_definition(Sub), SessId),
    ok;
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #subscription_cancellation{} = SubCan}) ->
    event:unsubscribe(SubCan, SessId),
    ok;
% Message that updates the #auth{} record in given session (originates from
% #'Token' client message).
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #auth{} = Auth}) ->
    % This function performs an async call to session manager worker.
    {ok, SessId} = session:update(SessId, #{auth => Auth}),
    ok;
route_and_ignore_answer(#client_message{session_id = SessId,
    message_body = #fuse_request{} = FuseRequest}) ->
    ok = worker_proxy:cast(fslogic_worker, {fuse_request, SessId, FuseRequest}).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_and_send_answer(#client_message{message_id = Id,
    message_body = #ping{data = Data}}) ->
    {ok, #server_message{message_id = Id, message_body = #pong{data = Data}}};
route_and_send_answer(#client_message{message_id = Id,
    message_body = #get_protocol_version{}}) ->
    {ok, #server_message{message_id = Id, message_body = #protocol_version{}}};
route_and_send_answer(#client_message{message_id = Id, session_id = SessId,
    message_body = #get_configuration{}}) ->
    spawn(fun() ->
        Configuration = fuse_config_manager:get_configuration(),
        communicator:send(#server_message{
            message_id = Id, message_body = Configuration
        }, SessId)
    end),
    ok;
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = SessId,
    message_body = #fuse_request{} = FuseRequest}) ->
    Connection = self(),
    ?debug("Fuse request: ~p ~p", [FuseRequest, SessId]),
    spawn(fun() ->
        FuseResponse = worker_proxy:call(fslogic_worker, {fuse_request, effective_session_id(Msg), FuseRequest}),
        ?debug("Fuse response: ~p", [FuseResponse]),
        communicator:send(#server_message{
            message_id = Id, message_body = FuseResponse
        }, Connection)
    end),
    ok;
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = _SessId,
    message_body = #proxyio_request{} = ProxyIORequest}) ->
    ?debug("ProxyIO request ~p", [ProxyIORequest]),
    Connection = self(),
    spawn(fun() ->
        ProxyIOResponse = worker_proxy:call(fslogic_worker,
            {proxyio_request, effective_session_id(Msg), ProxyIORequest}),

        ?debug("ProxyIO response ~p", [ProxyIOResponse]),
        communicator:send(#server_message{message_id = Id,
            message_body = ProxyIOResponse}, Connection)
    end),
    ok;
route_and_send_answer(#client_message{message_id = Id, session_id = SessId,
    message_body = #dbsync_request{} = DBSyncRequest}) ->
    ?debug("DBSync request ~p", [DBSyncRequest]),
    Connection = self(),
    spawn(fun() ->
        DBSyncResponse = worker_proxy:call(dbsync_worker,
            {dbsync_request, SessId, DBSyncRequest}),

        ?debug("DBSync response ~p", [DBSyncResponse]),
        communicator:send(#server_message{message_id = Id,
            message_body = DBSyncResponse}, Connection)
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns session's ID that shall be used for given message.
%% @end
%%--------------------------------------------------------------------
-spec effective_session_id(#client_message{}) ->
    session:id().
effective_session_id(#client_message{session_id = SessionId, proxy_session_id = undefined}) ->
    SessionId;
effective_session_id(#client_message{session_id = _SessionId, proxy_session_id = ProxySessionId}) ->
    ProxySessionId.