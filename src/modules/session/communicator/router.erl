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

-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API
-export([preroute_message/4, route_message/1, route_message/3,
    route_proxy_message/2]).
-export([effective_session_id/1]).

-export([init_counters/0, init_report/0]).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_COUNTERS, [events, event]).
-define(EXOMETER_HISTOGRAM_COUNTERS, [events_length]).
-define(EXOMETER_DEFAULT_TIME_SPAN, 600000).

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
    ?update_counter(?EXOMETER_NAME(events)),
    ?update_counter(?EXOMETER_NAME(events_length), length(Events)),
    lists:foreach(fun(Evt) ->
        event:emit(Evt, TargetSessionId)
    end, Events);
route_proxy_message(#client_message{message_body = #event{} = Evt} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    ?update_counter(?EXOMETER_NAME(event)),
    event:emit(Evt, TargetSessionId),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec preroute_message(Msg :: #client_message{} | #server_message{},
    SessId :: session:id(), Sock :: ssl:socket(), Transport :: module()) ->
    ok | {ok, #server_message{}} | {error, term()}.
preroute_message(#client_message{message_body = #message_request{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #message_acknowledgement{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #end_of_message_stream{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_body = #message_stream_reset{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #message_request{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #message_acknowledgement{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #end_of_message_stream{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#server_message{message_body = #message_stream_reset{}} = Msg,
    SessId, _Sock, _Transport) ->
    sequencer:route_message(Msg, SessId);
preroute_message(#client_message{message_stream = undefined} = Msg,
    _SessId, Sock, Transport) ->
    router:route_message(Msg, Sock, Transport);
preroute_message(#client_message{message_body = #subscription{}} = Msg,
    SessId, _Sock, _Transport) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
preroute_message(#client_message{message_body = #subscription_cancellation{}} = Msg,
    SessId, _Sock, _Transport) ->
    case session_manager:is_provider_session_id(SessId) of
        true ->
            ok;
        false ->
            sequencer:route_message(Msg, SessId)
    end;
preroute_message(#server_message{message_stream = undefined} = Msg,
    _SessId, Sock, Transport) ->
    router:route_message(Msg, Sock, Transport);
preroute_message(Msg, SessId, _Sock, _Transport) ->
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
route_message(Msg) ->
    route_message(Msg, undefined, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate handler, this function should never throw
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{} | #server_message{},
    Sock :: ssl:socket() | undefined, Transport :: module() | undefined) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_message(Msg = #client_message{message_id = undefined}, _Sock, _Transport) ->
    route_and_ignore_answer(Msg);
route_message(Msg = #client_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}, Sock, Transport) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            route_and_ignore_answer(Msg);
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            route_and_send_answer(Msg, Sock, Transport)
    end;
route_message(Msg = #server_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}, _Sock, _Transport) ->
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
%%% Exometer API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    TimeSpan = application:get_env(?APP_NAME,
        exometer_events_time_span, ?EXOMETER_DEFAULT_TIME_SPAN),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    Counters2 = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), histogram, TimeSpan}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_counters(Counters ++ Counters2).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    Reports2 = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_reports(Reports ++ Reports2).

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
-spec route_and_send_answer(#client_message{}, Sock :: ssl:socket() | undefined,
    Transport :: module() | undefined) ->
    ok | {ok, #server_message{}} | {error, term()}.
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = MsgId,
    message_body = FlushMsg = #flush_events{}
}, _Sock, _Transport) ->
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
}, _Sock, _Transport) ->
    {ok, #server_message{message_id = Id, message_body = #pong{data = Data}}};
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = #get_protocol_version{}
}, _Sock, _Transport) ->
    {ok, #server_message{message_id = Id, message_body = #protocol_version{}}};
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = #get_configuration{}
}, Sock, Transport) ->
    route_and_supervise(fun() ->
        Configuration = storage_req:get_configuration(effective_session_id(Msg)),
        {ok, #server_message{message_id = Id, message_body = Configuration}}
    end, get_heartbeat_fun(Id, Sock, Transport), Id);
route_and_send_answer(Msg = #client_message{
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            file_request = #storage_file_created{}
        }}
}, _Sock, _Transport) ->
    ok = worker_proxy:cast(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest}),
    ok;
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{
         fuse_request = #file_request{
             context_guid = FileGuid,
             file_request = Req
        }}
}, Sock, Transport) when is_record(Req, open_file) orelse is_record(Req, release) ->
    Node = consistent_hasing:get_node(FileGuid),
    {ok, FuseResponse} = worker_proxy:call({fslogic_worker, Node},
        {fuse_request, effective_session_id(Msg), FuseRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, Sock, Transport)}),
    {ok, #server_message{message_id = Id, message_body = FuseResponse}};
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{}
}, Sock, Transport) ->
    {ok, FuseResponse} = worker_proxy:call(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, Sock, Transport)}),
    {ok, #server_message{message_id = Id, message_body = FuseResponse}};
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProviderRequest = #provider_request{}
}, Sock, Transport) ->
    {ok, ProviderResponse} = worker_proxy:call(fslogic_worker,
        {provider_request, effective_session_id(Msg), ProviderRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, Sock, Transport)}),
    {ok, #server_message{message_id = Id, message_body = ProviderResponse}};
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = Request = #get_remote_document{}
}, Sock, Transport) ->
    route_and_supervise(fun() ->
        Response = datastore_remote_driver:handle(Request),
        {ok, #server_message{message_id = Id, message_body = Response}}
    end, get_heartbeat_fun(Id, Sock, Transport), Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}, Sock, Transport) ->
    Node = consistent_hasing:get_node(FileGuid),
    {ok, ProxyIOResponse} = worker_proxy:call({fslogic_worker, Node},
        {proxyio_request, effective_session_id(Msg), ProxyIORequest},
        {?TIMEOUT,  get_heartbeat_fun(Id, Sock, Transport)}),
    {ok, #server_message{message_id = Id, message_body = ProxyIOResponse}};
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = #dbsync_request{} = DBSyncRequest
}, Sock, Transport) ->
    {ok, DBSyncResponse} = worker_proxy:call(dbsync_worker,
        {dbsync_request, effective_session_id(Msg), DBSyncRequest},
        {?TIMEOUT, get_heartbeat_fun(Id, Sock, Transport)}),
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
-spec get_heartbeat_fun(message_id:id(), Sock :: ssl:socket() | undefined,
    Transport :: module() | undefined) -> fun(() -> ok).
get_heartbeat_fun(MsgId, Sock, Transport) ->
    case Sock of
        undefined ->
            fun() -> ok end;
        _ ->
            fun() ->
                outgoing_connection:send_server_message(Sock, Transport,
                    #server_message{message_id = MsgId,
                        message_body = #processing_status{code = 'IN_PROGRESS'}
                    }),
                ok
            end
    end.