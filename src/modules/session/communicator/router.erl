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
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([preroute_message/2, route_message/1, route_proxy_message/2]).
-export([effective_session_id/1]).

-define(TIMEOUT, 30000).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Route messages that were send to remote-proxy session on different provider.
%% @end
%%--------------------------------------------------------------------
-spec route_proxy_message(Msg :: #client_message{}, TargetSessionId :: session:id()) -> ok.
route_proxy_message(#client_message{message_body = #events{events = Events}} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    lists:foreach(fun(Evt) ->
        event:emit(Evt, TargetSessionId)
    end, Events);
route_proxy_message(#client_message{message_body = #event{} = Evt} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    event:emit(Evt, TargetSessionId),
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
-spec route_message(Msg :: #client_message{} | #server_message{}) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_message(Msg = #client_message{message_id = undefined}) ->
    route_and_ignore_answer(Msg);
route_message(Msg = #client_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            route_and_ignore_answer(Msg);
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            route_and_send_answer(Msg)
    end;
route_message(Msg = #server_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            ok;
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns session's ID that shall be used for given message.
%% @end
%%--------------------------------------------------------------------
-spec effective_session_id(#client_message{}) ->
    session:id().
effective_session_id(#client_message{session_id = SessionId, proxy_session_id = undefined}) ->
    SessionId;
effective_session_id(#client_message{proxy_session_id = ProxySessionId}) ->
    ProxySessionId.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
            event:subscribe(Sub, effective_session_id(Msg)),
            ok
    end;
route_and_ignore_answer(#client_message{message_body = #subscription_cancellation{} = SubCan} = Msg) ->
    case session_manager:is_provider_session_id(effective_session_id(Msg)) of
        true -> ok; %% Do not route subscription_cancellations from other providers
        false ->
            event:unsubscribe(SubCan, effective_session_id(Msg)),
            ok
    end;
% Message that updates the #macaroon_auth{} record in given session (originates from
% #'Token' client message).
route_and_ignore_answer(#client_message{message_body = Auth} = Msg)
    when is_record(Auth, macaroon_auth) orelse is_record(Auth, token_auth) ->
    % This function performs an async call to session manager worker.
    {ok, _} = session:update(effective_session_id(Msg), fun(Session = #session{}) ->
        {ok, Session#session{auth = Auth}}
    end),
    ok;
route_and_ignore_answer(#client_message{message_body = #fuse_request{} = FuseRequest} = Msg) ->
    ok = worker_proxy:cast(fslogic_worker, {fuse_request, effective_session_id(Msg), FuseRequest});
route_and_ignore_answer(ClientMsg = #client_message{
    message_body = #dbsync_message{message_body = Msg}
}) ->
    ok = worker_proxy:cast(
        dbsync_worker, {dbsync_message, effective_session_id(ClientMsg), Msg}
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = MsgId,
    message_body = FlushMsg = #flush_events{}
}) ->
    event:flush(FlushMsg#flush_events{notify =
        fun(Result) ->
            % TODO - not used by client, why custom mechanism
            communicator:send(Result#server_message{message_id = MsgId}, OriginSessId)
        end
    }, effective_session_id(Msg)),
    ok;
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = #ping{data = Data}
}) ->
    {ok, #server_message{message_id = Id, message_body = #pong{data = Data}}};
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = #get_protocol_version{}
}) ->
    {ok, #server_message{message_id = Id, message_body = #protocol_version{}}};
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = #get_configuration{}
}) ->
    route_and_supervise(fun() ->
        Configuration = storage_req:get_configuration(effective_session_id(Msg)),
        {ok, #server_message{message_id = Id, message_body = Configuration}}
    end, get_heartbeat_fun(Id, OriginSessId), Id);
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = FuseRequest = #fuse_request{
         fuse_request = #file_request{
             context_guid = FileGuid,
             file_request = Req
        }}
}) when is_record(Req, open_file) orelse is_record(Req, release) ->
    Node = consistent_hasing:get_node(FileGuid),
    {ok, FuseResponse} = worker_proxy:call({fslogic_worker, Node},
        {fuse_request, effective_session_id(Msg), FuseRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, OriginSessId)}),
    {ok, #server_message{message_id = Id, message_body = FuseResponse}};
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = FuseRequest = #fuse_request{}
}) ->
    {ok, FuseResponse} = worker_proxy:call(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, OriginSessId)}),
    {ok, #server_message{message_id = Id, message_body = FuseResponse}};
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = ProviderRequest = #provider_request{}
}) ->
    {ok, ProviderResponse} = worker_proxy:call(fslogic_worker,
        {provider_request, effective_session_id(Msg), ProviderRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, OriginSessId)}),
    {ok, #server_message{message_id = Id, message_body = ProviderResponse}};
route_and_send_answer(#client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = Request = #get_remote_document{}
}) ->
    route_and_supervise(fun() ->
        Response = datastore_remote_driver:handle(Request),
        {ok, #server_message{message_id = Id, message_body = Response}}
    end, get_heartbeat_fun(Id, OriginSessId), Id);
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}) ->
    Node = consistent_hasing:get_node(FileGuid),
    {ok, ProxyIOResponse} = worker_proxy:call({fslogic_worker, Node},
        {proxyio_request, effective_session_id(Msg), ProxyIORequest},
        {?TIMEOUT, get_heartbeat_fun(Id, OriginSessId)}),
    {ok, #server_message{message_id = Id, message_body = ProxyIOResponse}};
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = Id,
    message_body = #dbsync_request{} = DBSyncRequest
}) ->
    {ok, DBSyncResponse} = worker_proxy:call(dbsync_worker,
        {dbsync_request, effective_session_id(Msg), DBSyncRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, OriginSessId)}),
    {ok, #server_message{message_id = Id, message_body = DBSyncResponse}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes function that handles message and asynchronously wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec route_and_supervise(fun(() -> term()), fun(() -> ok), message_id:id()) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_and_supervise(Fun, TimeoutFun, Id) ->
    Master = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        Ans = Fun(),
        Master ! {slave_ans, Ref, Ans}
    end),
    receive_loop(Ref, Pid, TimeoutFun, Id).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for worker asynchronous process answer.
%% @end
%%--------------------------------------------------------------------
-spec receive_loop(reference(), pid(), fun(() -> ok), message_id:id()) ->
    ok | {ok, #server_message{}} | {error, term()}.
receive_loop(Ref, Pid, TimeoutFun, Id) ->
    receive
        {slave_ans, Ref, Ans} ->
            Ans
    after
        ?TIMEOUT ->
            case erlang:is_process_alive(Pid) of
                true ->
                    TimeoutFun(),
                    receive_loop(Ref, Pid, TimeoutFun, Id);
                _ ->
                    {ok, #server_message{message_id = Id,
                        message_body = #processing_status{code = 'ERROR'}}}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides function that sends heartbeat.
%% @end
%%--------------------------------------------------------------------
-spec get_heartbeat_fun(message_id:id(), session:id()) -> fun(() -> ok).
get_heartbeat_fun(MsgId, OriginSessId) ->
    {Sock, Transport} = get(heartbeat_info),
    fun() ->
        connection:send_server_message(Sock, Transport,
            #server_message{message_id = MsgId,
                message_body = #processing_status{code = 'IN_PROGRESS'}
            }),
        ok
    end.