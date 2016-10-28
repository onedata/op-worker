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
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([preroute_message/2, route_message/1, route_proxy_message/2]).
-export([effective_session_id/1]).

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
preroute_message(#server_message{message_body = #message_request{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #message_acknowledgement{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #end_of_message_stream{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #message_stream_reset{}} = Msg, SessId) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_stream = undefined} = Msg, _SessId) ->
    router:route_message(Msg);
preroute_message(#client_message{message_body = #subscription{}} = Msg, SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
preroute_message(#client_message{message_body = #subscription_cancellation{}} = Msg, SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
preroute_message(#server_message{message_stream = undefined} = Msg, _SessId) ->
    router:route_message(Msg);
preroute_message(Msg, SessId) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end.

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
route_and_ignore_answer(#client_message{message_body = #event{} = Evt} = Msg) ->
    event:emit(Evt, effective_session_id(Msg)),
    ok;
route_and_ignore_answer(#client_message{message_body = #events{events = Evts}} = Msg) ->
    lists:foreach(fun(#event{} = Evt) -> event:emit(Evt, effective_session_id(Msg)) end, Evts),
    ok;
route_and_ignore_answer(#client_message{message_body = #subscription{} = Sub} = Msg) ->
    case session_manager:is_provider_session_id(effective_session_id(Msg)) of
        true -> ok; %% Do not route subscriptions from other providers (route only subscriptions from users)
        false ->
            event:subscribe(event_utils:inject_event_stream_definition(Sub), effective_session_id(Msg)),
            ok
    end;
route_and_ignore_answer(#client_message{message_body = #subscription_cancellation{} = SubCan} = Msg) ->
    case session_manager:is_provider_session_id(effective_session_id(Msg)) of
        true -> ok; %% Do not route subscription_cancellations from other providers
        false ->
            event:unsubscribe(SubCan, effective_session_id(Msg)),
            ok
    end;
% Message that updates the #token_auth{} record in given session (originates from
% #'Token' client message).
route_and_ignore_answer(#client_message{message_body = #token_auth{} = Auth} = Msg) ->
    % This function performs an async call to session manager worker.
    {ok, SessId} = session:update(effective_session_id(Msg), #{auth => Auth}),
    ok;
route_and_ignore_answer(#client_message{message_body = #fuse_request{} = FuseRequest} = Msg) ->
    ok = worker_proxy:cast(fslogic_worker, {fuse_request, effective_session_id(Msg), FuseRequest}).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_and_send_answer(#client_message{session_id = OriginSessId, message_id = MsgId,
    message_body = #flush_events{} = FlushMsg} = Msg) ->
    event:flush(FlushMsg#flush_events{notify =
        fun(Result) ->
            communicator:send(Result#server_message{message_id = MsgId}, OriginSessId)
        end
    }, effective_session_id(Msg)),
    ok;
route_and_send_answer(#client_message{message_id = Id,
    message_body = #ping{data = Data}}) ->
    {ok, #server_message{message_id = Id, message_body = #pong{data = Data}}};
route_and_send_answer(#client_message{message_id = Id,
    message_body = #get_protocol_version{}}) ->
    {ok, #server_message{message_id = Id, message_body = #protocol_version{}}};
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = OriginSessId,
    message_body = #get_configuration{}}) ->
    spawn(fun() ->
        Configuration = fuse_config_manager:get_configuration(effective_session_id(Msg)),
        communicator:send(#server_message{
            message_id = Id, message_body = Configuration
        }, OriginSessId)
    end),
    ok;
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = OriginSessId,
    message_body = #fuse_request{} = FuseRequest}) ->
    ?debug("Fuse request: ~p ~p", [FuseRequest, effective_session_id(Msg)]),
    spawn(fun() ->
        FuseResponse = worker_proxy:call(fslogic_worker, {fuse_request, effective_session_id(Msg), FuseRequest}),
        ?debug("Fuse response: ~p", [FuseResponse]),
        communicator:send(#server_message{
            message_id = Id, message_body = FuseResponse
        }, OriginSessId)
    end),
    ok;
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = OriginSessId,
    message_body = #provider_request{} = ProviderRequest}) ->
    ?debug("Provider request ~p ~p", [ProviderRequest, effective_session_id(Msg)]),
    spawn(fun() ->
        ProviderResponse = worker_proxy:call(fslogic_worker,
            {provider_request, effective_session_id(Msg), ProviderRequest}),
        ?debug("Provider response ~p", [ProviderResponse]),
        communicator:send(#server_message{message_id = Id,
            message_body = ProviderResponse}, OriginSessId)
    end),
    ok;
route_and_send_answer(Msg = #client_message{message_id = Id, session_id = OriginSessId,
    message_body = #proxyio_request{} = ProxyIORequest}) ->
    ?debug("ProxyIO request ~p", [ProxyIORequest]),
    spawn(fun() ->
        ProxyIOResponse = worker_proxy:call(fslogic_worker,
            {proxyio_request, effective_session_id(Msg), ProxyIORequest}),

        ?debug("ProxyIO response ~p", [ProxyIOResponse]),
        communicator:send(#server_message{message_id = Id,
            message_body = ProxyIOResponse
        }, OriginSessId)
    end),
    ok;
route_and_send_answer(#client_message{message_id = Id, session_id = OriginSessId,
    message_body = #dbsync_request{} = DBSyncRequest} = Msg) ->
    ?debug("DBSync request ~p", [DBSyncRequest]),
    spawn(fun() ->
        DBSyncResponse = worker_proxy:call(dbsync_worker,
            {dbsync_request, effective_session_id(Msg), DBSyncRequest}),

        ?debug("DBSync response ~p", [DBSyncResponse]),
        communicator:send(#server_message{message_id = Id,
            message_body = DBSyncResponse}, OriginSessId)
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
