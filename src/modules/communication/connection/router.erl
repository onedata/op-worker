%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming messages.
%%% @end
%%%-------------------------------------------------------------------
-module(router).
-author("Tomasz Lichon").

-include("proto/common/clproto_message_id.hrl").
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

%% API
-export([effective_session_id/1]).
-export([route_message/2]).

-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns session's ID that shall be used for given message.
%% @end
%%--------------------------------------------------------------------
-spec effective_session_id(client_message()) -> session:id().
effective_session_id(#client_message{
    session_id = SessionId,
    effective_session_id = undefined
}) ->
    SessionId;
effective_session_id(#client_message{effective_session_id = EffSessionId}) ->
    EffSessionId.


%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it through sequencer,
%% otherwise routes it directly
%% @end
%%--------------------------------------------------------------------
-spec route_message(message(), async_request_manager:reply_to()) ->
    ok | {ok, server_message()} | {error, term()}.
route_message(Msg, ReplyTo) ->
    case stream_router:is_stream_message(Msg) of
        true ->
            stream_router:route_message(Msg);
        false ->
            route_direct_message(Msg, ReplyTo);
        ignore ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Route message to adequate handler, this function should never throw
%% @end
%%--------------------------------------------------------------------
-spec route_direct_message(message(), async_request_manager:reply_to()) ->
    ok | {ok, server_message()} | {error, term()}.
route_direct_message(#client_message{message_id = undefined} = Msg, _) ->
    route_and_ignore_answer(Msg);
route_direct_message(#client_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}} = Msg, ReplyTo) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            route_and_ignore_answer(Msg);
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            answer_or_delegate(Msg, ReplyTo)
    end;
route_direct_message(#server_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}} = Msg, _) ->
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
route_and_ignore_answer(#client_message{
    message_body = #fuse_request{} = FuseRequest
} = Msg) ->
    Req = {fuse_request, effective_session_id(Msg), FuseRequest},
    ok = worker_proxy:cast(fslogic_worker, Req);
route_and_ignore_answer(ClientMsg = #client_message{
    message_body = #dbsync_message{message_body = Msg}
}) ->
    Req = {dbsync_message, effective_session_id(ClientMsg), Msg},
    ok = worker_proxy:cast(dbsync_worker, Req);
% Message that updates the #macaroon_auth{} record in given session
% (originates from #'Macaroon' client message).
route_and_ignore_answer(#client_message{
    message_body = #macaroon_auth{} = Auth
} = Msg) ->
    EffSessionId = effective_session_id(Msg),
    % This function performs an async call to session manager worker.
    {ok, _} = session:update(EffSessionId, fun(Session = #session{}) ->
        {ok, Session#session{auth = Auth}}
    end),
    ok;
route_and_ignore_answer(ClientMsg) ->
    event_router:route_message(ClientMsg).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Answers the request directly if possible. If not and request
%% matches known requests, delegates it to the right worker together with
%% reply address. Otherwise delegates it to event_router.
%% @end
%%--------------------------------------------------------------------
-spec answer_or_delegate(client_message(), connection_api:reply_to()) ->
    ok | {ok, server_message()} | {error, term()}.
answer_or_delegate(#client_message{
    message_id = MsgId,
    message_body = #ping{data = Data}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #pong{data = Data}
    }};

answer_or_delegate(#client_message{
    message_id = MsgId,
    message_body = #get_protocol_version{}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #protocol_version{}
    }};

answer_or_delegate(#client_message{
    message_id = #message_id{issuer = ProviderId} = MsgId,
    message_body = #generate_rtransfer_conn_secret{secret = PeerSecret}
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = #rtransfer_conn_secret{
            secret = rtransfer_config:generate_secret(ProviderId, PeerSecret)
        }
    }};

answer_or_delegate(#client_message{
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

answer_or_delegate(Msg = #client_message{
    message_body = FuseRequest = #fuse_request{
        fuse_request = #file_request{
            file_request = #storage_file_created{}
        }}
}, _) ->
    % TODO VFS-5331
    ok = worker_proxy:cast(fslogic_worker,
        {fuse_request, effective_session_id(Msg), FuseRequest}
    );

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = #get_configuration{}
}, ReplyTo) ->
    delegate_request(proc, fun() ->
        storage_req:get_configuration(effective_session_id(Msg))
    end, MsgId, ReplyTo);

answer_or_delegate(#client_message{
    message_id = MsgId,
    message_body = Request = #get_remote_document{}
}, ReplyTo) ->
    delegate_request(proc, fun() ->
        datastore_remote_driver:handle(Request)
    end, MsgId, ReplyTo);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = FuseRequest = #fuse_request{}
}, ReplyTo) ->
    Req = {fuse_request, effective_session_id(Msg), FuseRequest},
    delegate_request(fslogic_worker, Req, MsgId, ReplyTo);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = ProviderRequest = #provider_request{}
}, ReplyTo) ->
    Req = {provider_request, effective_session_id(Msg), ProviderRequest},
    delegate_request(fslogic_worker, Req, MsgId, ReplyTo);

answer_or_delegate(Msg = #client_message{
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}, ReplyTo) ->
    Node = read_write_req:get_proxyio_node(fslogic_uuid:guid_to_uuid(FileGuid)),
    Req = {proxyio_request, effective_session_id(Msg), ProxyIORequest},
    delegate_request({fslogic_worker, Node}, Req, Id, ReplyTo);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = #dbsync_request{} = DBSyncRequest
}, ReplyTo) ->
    Req = {dbsync_request, effective_session_id(Msg), DBSyncRequest},
    delegate_request(dbsync_worker, Req, MsgId, ReplyTo);

answer_or_delegate(Msg, _) ->
    event_router:route_message(Msg).


%% @private
-spec delegate_request(async_request_manager:worker_ref(), Req :: term(),
    clproto_message_id:id(), async_request_manager:reply_to()) ->
    ok | {ok, server_message()}.
delegate_request(WorkerRef, Req, MsgId, ReplyTo) ->
    async_request_manager:delegate_and_supervise(
        WorkerRef, Req, MsgId, ReplyTo
    ).