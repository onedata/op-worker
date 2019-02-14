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

%% API
-export([route_message/2]).
-export([effective_session_id/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it through sequencer,
%% otherwise routes it directly
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{} | #server_message{},
    connection_manager:return_address()) ->
    ok | {ok, #server_message{}} |
    async_request_manager:delegate_ans() | {error, term()}.
route_message(Msg, ReturnAddr) ->
    case stream_router:is_stream_message(Msg) of
        true ->
            stream_router:route_message(Msg);
        false ->
            route_direct_message(Msg, ReturnAddr);
        ignore ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns session's ID that shall be used for given message.
%% @end
%%--------------------------------------------------------------------
-spec effective_session_id(#client_message{}) -> session:id().
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
%% Route message to adequate handler, this function should never throw
%% @end
%%--------------------------------------------------------------------
-spec route_direct_message(Msg :: #client_message{} | #server_message{},
    connection_manager:return_address()) ->
    ok | {ok, #server_message{}} | async_request_manager:delegate_ans() |
    {error, term()}.
route_direct_message(Msg = #client_message{message_id = undefined}, _) ->
    route_and_ignore_answer(Msg);
route_direct_message(Msg = #client_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}, ReturnAddr) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            route_and_ignore_answer(Msg);
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            route_and_send_answer(Msg, ReturnAddr)
    end;
route_direct_message(Msg = #server_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}}, _) ->
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
%% @private
%% @doc
%% Route message to adequate handler and ignores answer.
%% @end
%%--------------------------------------------------------------------
-spec route_and_ignore_answer(Msg :: #client_message{}) -> ok.
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
    event_router:route_message(ClientMsg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{},
    connection_manager:return_address()) ->
    ok | {ok, #server_message{}} |
    async_request_manager:delegate_ans() | {error, term()}.
route_and_send_answer(#client_message{
    message_id = MsgId,
    message_body = #ping{data = Data}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #pong{data = Data}
    }};

route_and_send_answer(#client_message{
    message_id = MsgId,
    message_body = #get_protocol_version{}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #protocol_version{}
    }};

route_and_send_answer(Msg = #client_message{
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            file_request = #storage_file_created{}
        }}
}, _) ->
    ok = worker_proxy:cast(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest}
    );

route_and_send_answer(#client_message{
    message_id = #message_id{issuer = ProviderId} = MsgId,
    message_body = #generate_rtransfer_conn_secret{secret = PeerSecret}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #rtransfer_conn_secret{
            secret = rtransfer_config:generate_secret(ProviderId, PeerSecret)
        }
    }};

route_and_send_answer(#client_message{
    message_id = MsgId,
    message_body = #get_rtransfer_nodes_ips{}
}, _) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    IpsAndPorts = lists:map(fun(Node) ->
        {{_,_,_,_} = IP, Port} = rpc:call(
            Node, rtransfer_config, get_local_ip_and_port, []
        ),
        #ip_and_port{ip = IP, port = Port}
    end, Nodes),

    {ok, #server_message{
        message_id = MsgId,
        message_body = #rtransfer_nodes_ips{nodes = IpsAndPorts}
    }};

route_and_send_answer(Msg = #client_message{
    message_id = MsgId,
    message_body = #get_configuration{}
}, ReturnAddr) ->
    connection_manager:route_and_supervise(fun() ->
        storage_req:get_configuration(effective_session_id(Msg))
    end, MsgId, ReturnAddr);

route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            context_guid = FileGuid,
            file_request = Req
        }}
}, ReturnAddr) when is_record(Req, open_file) orelse
    is_record(Req, open_file_with_extended_info) orelse is_record(Req, release) ->
    connection_manager:delegate(fun() ->
        Node = consistent_hasing:get_node(fslogic_uuid:guid_to_uuid(FileGuid)),
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor({fslogic_worker, Node},
            {fuse_request, effective_session_id(Msg), FuseRequest},
            ReturnAddr, {Ref, Id}
        ),
        {Pid, Ref}
    end, Id, ReturnAddr);

route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = FuseRequest = #fuse_request{}
}, ReturnAddr) ->
    connection_manager:delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(fslogic_worker,
            {fuse_request, effective_session_id(Msg), FuseRequest},
            ReturnAddr, {Ref, Id}
        ),
        {Pid, Ref}
    end, Id, ReturnAddr);

route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProviderRequest = #provider_request{}
}, ReturnAddr) ->
    connection_manager:delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(fslogic_worker,
            {provider_request, effective_session_id(Msg), ProviderRequest},
            ReturnAddr, {Ref, Id}
        ),
        {Pid, Ref}
    end, Id, ReturnAddr);

route_and_send_answer(#client_message{
    message_id = Id,
    message_body = Request = #get_remote_document{}
}, ReturnAddr) ->
    connection_manager:route_and_supervise(fun() ->
        datastore_remote_driver:handle(Request)
    end, Id, ReturnAddr);

route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}, ReturnAddr) ->
    connection_manager:delegate(fun() ->
        Node = consistent_hasing:get_node(fslogic_uuid:guid_to_uuid(FileGuid)),
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor({fslogic_worker, Node},
            {proxyio_request, effective_session_id(Msg), ProxyIORequest},
            ReturnAddr, {Ref, Id}
        ),
        {Pid, Ref}
    end, Id, ReturnAddr);

route_and_send_answer(Msg = #client_message{
    message_id = Id,
    message_body = #dbsync_request{} = DBSyncRequest
}, ReturnAddr) ->
    connection_manager:delegate(fun() ->
        Ref = make_ref(),
        Pid = worker_proxy:cast_and_monitor(dbsync_worker,
            {dbsync_request, effective_session_id(Msg), DBSyncRequest},
            ReturnAddr, {Ref, Id}
        ),
        {Pid, Ref}
    end, Id, ReturnAddr);

route_and_send_answer(Msg, _) ->
    event_router:route_message(Msg).
