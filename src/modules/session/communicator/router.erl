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
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").

%% API
-export([preroute_message/2, route_message/1]).
-export([effective_session_id/1]).
-export([save_delegation/3, process_ans/3,
    check_processes/4, get_processes_check_interval/0,
    get_heartbeat_msg/1, get_error_msg/1]).

-define(TIMEOUT, timer:seconds(10)).

-type delegation() :: {message_id:id(), pid(), reference()}.
-type delegate_ans() :: {wait, delegation()}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec preroute_message(Msg :: #client_message{} | #server_message{},
    SessId :: session:id()) -> ok | {ok, #server_message{}} | delegate_ans() |
    {error, term()}.
preroute_message(Msg, SessId) ->
    case stream_router:route_message(Msg, SessId) of
        direct_message ->
            router:route_message(Msg);
        Ans ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate handler, this function should never throw
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{} | #server_message{}) ->
    ok | {ok, #server_message{}} | delegate_ans() | {error, term()}.
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

%%--------------------------------------------------------------------
%% @doc
%% Provides heartbeat message.
%% @end
%%--------------------------------------------------------------------
-spec get_heartbeat_msg(message_id:id()) -> #server_message{}.
get_heartbeat_msg(MsgId) ->
    #server_message{message_id = MsgId,
        message_body = #processing_status{code = 'IN_PROGRESS'}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Provides error message.
%% @end
%%--------------------------------------------------------------------
-spec get_error_msg(message_id:id()) -> #server_message{}.
get_error_msg(MsgId) ->
    #server_message{message_id = MsgId,
        message_body = #processing_status{code = 'ERROR'}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Saves informantion about asynchronous waiting for answer.
%% @end
%%--------------------------------------------------------------------
-spec save_delegation(delegation(), map(), map()) -> {map(), map()}.
save_delegation(Delegation, WaitMap, Pids) ->
    {Id, Pid, Ref} = Delegation,
    WaitMap2 = maps:put(Ref, Id, WaitMap),
    Pids2 = maps:put(Ref, Pid, Pids),
    {WaitMap2, Pids2}.

%%--------------------------------------------------------------------
%% @doc
%% Processes answer and returns message to be sent.
%% @end
%%--------------------------------------------------------------------
-spec process_ans(term(), map(), map()) ->
    {#server_message{}, map(), map()} | wrong_message.
process_ans(ReceivedAns, WaitMap, Pids) ->
    case ReceivedAns of
        {slave_ans, Ref, Ans} ->
            process_ans(Ref, Ans, WaitMap, Pids);
        #worker_answer{id = Ref, response = {ok, Ans}} ->
            process_ans(Ref, Ans, WaitMap, Pids);
        #worker_answer{id = Ref, response = ErrorAns} ->
            process_ans(Ref, {process_error, ErrorAns}, WaitMap, Pids);
        _ ->
            wrong_message
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks processes that handle requests and sends heartbeats.
%% @end
%%--------------------------------------------------------------------
-spec check_processes(map(), map(), fun((message_id:id()) -> ok),
    fun((message_id:id()) -> ok)) -> {map(), map()}.
check_processes(Pids, WaitMap, TimeoutFun, ErrorFun) ->
    {Pids2, Errors} = maps:fold(fun
        (Ref, {Pid, not_alive}, {Acc1, Acc2}) ->
            ?error("Router: process ~p connected with ref ~p is not alive",
                [Pid, Ref]),
            ErrorFun(maps:get(Ref, WaitMap)),
            {Acc1, [Ref | Acc2]};
        (Ref, Pid, {Acc1, Acc2}) ->
            case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
                true ->
                    TimeoutFun(maps:get(Ref, WaitMap)),
                    {maps:put(Ref, Pid, Acc1), Acc2};
                _ ->
                    % Wait with error for another heartbeat
                    % (possible race heartbeat/answer)
                    TimeoutFun(maps:get(Ref, WaitMap)),
                    {maps:put(Ref, {Pid, not_alive}, Acc1), Acc2}
            end
    end, {#{}, []}, Pids),
    WaitMap2 = lists:foldl(fun(Ref, Acc) ->
        maps:remove(Ref, Acc)
    end, WaitMap, Errors),
    {Pids2, WaitMap2}.

%%--------------------------------------------------------------------
%% @doc
%% Returns interval between checking of processes that handle requests.
%% @end
%%--------------------------------------------------------------------
-spec get_processes_check_interval() -> non_neg_integer().
get_processes_check_interval() ->
    application:get_env(?APP_NAME, router_processes_check_interval, ?TIMEOUT).

%%%===================================================================
%%% Internal functions
%%%===================================================================

route_and_ignore_answer(#client_message{message_body = #fuse_request{} = FuseRequest} = Msg) ->
    ok = worker_proxy:cast(fslogic_worker, {fuse_request, effective_session_id(Msg), FuseRequest});
route_and_ignore_answer(ClientMsg = #client_message{
    message_body = #dbsync_message{message_body = Msg}
}) ->
    ok = worker_proxy:cast(
        dbsync_worker, {dbsync_message, effective_session_id(ClientMsg), Msg}
    );
% Message that updates the #macaroon_auth{} record in given session (originates from
% #'Macaroon' client message).
route_and_ignore_answer(#client_message{message_body = #macaroon_auth{} = Auth} = Msg) ->
    % This function performs an async call to session manager worker.
    {ok, _} = session:update(effective_session_id(Msg), fun(Session = #session{}) ->
        {ok, Session#session{auth = Auth}}
    end),
    ok;
route_and_ignore_answer(ClientMsg) ->
    event_router:route_and_ignore_answer(ClientMsg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) ->
    ok | {ok, #server_message{}} | delegate_ans() | {error, term()}.
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
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            file_request = #storage_file_created{}
        }}
}) ->
    ok = worker_proxy:cast(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest}),
    ok;
route_and_send_answer(#client_message{
    message_id = Id = #message_id{issuer = ProviderId},
    message_body = #generate_rtransfer_conn_secret{secret = PeerSecret}
}) ->
    MySecret = rtransfer_config:generate_secret(ProviderId, PeerSecret),
    Response = #rtransfer_conn_secret{secret = MySecret},
    {ok, #server_message{message_id = Id, message_body = Response}};
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = #get_rtransfer_nodes_ips{}
}) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    IpsAndPorts = lists:map(fun(Node) ->
        {{_,_,_,_} = IP, Port} = rpc:call(Node, rtransfer_config, get_local_ip_and_port, []),
        #ip_and_port{ip = IP, port = Port}
    end, Nodes),
    Response = #rtransfer_nodes_ips{nodes = IpsAndPorts},
    {ok, #server_message{message_id = Id, message_body = Response}};
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = #get_configuration{}
}) ->
    route_and_supervise(fun() ->
        storage_req:get_configuration(effective_session_id(Msg))
    end, Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            context_guid = FileGuid,
            file_request = Req
        }}
}) when is_record(Req, open_file) orelse
    is_record(Req, open_file_with_extended_info) orelse is_record(Req, release) ->
    delegate(fun() ->
        Node = consistent_hasing:get_node(fslogic_uuid:guid_to_uuid(FileGuid)),
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor({fslogic_worker, Node},
            {fuse_request, effective_session_id(Msg), FuseRequest}, Ref),
        {Pid, Ref}
    end, Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{}
}) ->
    delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(fslogic_worker,
            {fuse_request, effective_session_id(Msg), FuseRequest}, Ref),
        {Pid, Ref}
    end, Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProviderRequest = #provider_request{}
}) ->
    delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(fslogic_worker,
            {provider_request, effective_session_id(Msg), ProviderRequest}, Ref),
        {Pid, Ref}
    end, Id);
route_and_send_answer(#client_message{
    message_id = Id,
    message_body = Request = #get_remote_document{}
}) ->
    route_and_supervise(fun() ->
        datastore_remote_driver:handle(Request)
    end, Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}) ->
    delegate(fun() ->
        Node = consistent_hasing:get_node(fslogic_uuid:guid_to_uuid(FileGuid)),
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor({fslogic_worker, Node},
            {proxyio_request, effective_session_id(Msg), ProxyIORequest}, Ref),
        {Pid, Ref}
    end, Id);
route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = #dbsync_request{} = DBSyncRequest
}) ->
    delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(dbsync_worker,
            {dbsync_request, effective_session_id(Msg), DBSyncRequest}, Ref),
        {Pid, Ref}
    end, Id);
route_and_send_answer(Msg) ->
    event_router:route_and_send_answer(Msg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes function that handles message and asynchronously wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec route_and_supervise(fun(() -> {pid(), reference()}), message_id:id()) ->
    delegate_ans() | {ok, #server_message{}}.
route_and_supervise(Fun, Id) ->
    Fun2 = fun() ->
        Master = self(),
        Ref = make_ref(),
        Pid = spawn(fun() ->
            try
                Ans = Fun(),
                Master ! {slave_ans, Ref, Ans}
            catch
                _:E ->
                    ?error_stacktrace("Route_and_supervise error: ~p for "
                    "message id ~p", [E, Id]),
                    Master ! {slave_ans, Ref, #processing_status{code = 'ERROR'}}
            end
        end),
        {Pid, Ref}
    end,
    delegate(Fun2, Id).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes function that handles message returns information needed for
%% asynchronous waiting for answer.
%% @end
%%--------------------------------------------------------------------
-spec delegate(fun(() -> {pid(), reference()}), message_id:id()) ->
    delegate_ans() | {ok, #server_message{}}.
delegate(Fun, Id) ->
    try
        {Pid, Ref} = Fun(),
        case is_pid(Pid) of
            true ->
                {wait, {Id, Pid, Ref}};
            ErrorPid ->
                ?error("Router error: ~p for message id ~p", [ErrorPid, Id]),
                {ok, get_error_msg(Id)}
        end
    catch
        _:E ->
            ?error_stacktrace("Router error: ~p for message id ~p", [E, Id]),
            {ok, get_error_msg(Id)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes answer and returns message to be sent.
%% @end
%%--------------------------------------------------------------------
-spec process_ans(reference(), term(), map(), map()) ->
    {#server_message{}, map(), map()} | wrong_message.
process_ans(Ref, {process_error, ErrorAns}, WaitMap, Pids) ->
    case maps:get(Ref, WaitMap, undefined) of
        undefined ->
            wrong_message;
        Id ->
            ?error("Router wrong answer: ~p for message id ~p", [ErrorAns, Id]),
            WaitMap2 = maps:remove(Ref, WaitMap),
            Pids2 = maps:remove(Ref, Pids),
            {get_error_msg(Id), WaitMap2, Pids2}
    end;
process_ans(Ref, Ans, WaitMap, Pids) ->
    case maps:get(Ref, WaitMap, undefined) of
        undefined ->
            wrong_message;
        Id ->
            Return = #server_message{message_id = Id, message_body = Ans},
            WaitMap2 = maps:remove(Ref, WaitMap),
            Pids2 = maps:remove(Ref, Pids),
            {Return, WaitMap2, Pids2}
    end.
