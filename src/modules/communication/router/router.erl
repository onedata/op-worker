%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to route received messages.
%%% @end
%%%-------------------------------------------------------------------
-module(router).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([effective_session_id/1]).
-export([build_rib/1, route_message/2]).

%% Routing Information Base (RIB in short) is a structure containing necessary
%% information for routing messages. It should be created by connection process
%% right after its start and used in following calls to router.
-record(rib, {
    % Used by worker processes to send responses to delegated requests.
    % This field is set only for incoming connection and left as undefined
    % for outgoing one (it is used for sending requests not handling and
    % responding so it does not even have async_request_manager).
    respond_via = undefined :: undefined | async_request_manager:respond_via(),
    session_type :: session:type()
}).

-type rib() ::#rib{}.
-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().

-export_type([rib/0]).

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
%% Builds RIB for calling connection process. It should be then cached
%% by connection process and used in subsequent calls to router.
%% @end
%%--------------------------------------------------------------------
-spec build_rib(session:id()) -> rib() | no_return().
build_rib(SessionId) ->
    {ok, #document{
        value = #session{
            type = SessionType,
            async_request_manager = AsyncReqManager
        }}
    } = session:get(SessionId),

    RespondVia = case is_pid(AsyncReqManager) of
        true ->
            {self(), AsyncReqManager, SessionId};
        false when SessionType /= provider_incoming andalso SessionType /= fuse ->
            undefined
    end,

    #rib{respond_via = RespondVia, session_type = SessionType}.


%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it through sequencer,
%% otherwise routes it directly
%% @end
%%--------------------------------------------------------------------
-spec route_message(message(), rib()) ->
    ok | {ok, server_message()} | {error, term()}.
route_message(Msg, RIB) ->
    case stream_router:is_stream_message(Msg, RIB#rib.session_type) of
        true ->
            stream_router:route_message(Msg);
        false ->
            route_direct_message(Msg, RIB);
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
-spec route_direct_message(message(), rib()) ->
    ok | {ok, server_message()} | {error, term()}.
route_direct_message(#client_message{message_id = undefined} = Msg, _) ->
    route_and_ignore_answer(Msg);
route_direct_message(#client_message{message_id = #message_id{
    issuer = Issuer,
    recipient = Recipient
}} = Msg, RIB) ->
    case oneprovider:is_self(Issuer) of
        true when Recipient =:= undefined ->
            route_and_ignore_answer(Msg);
        true ->
            Pid = binary_to_term(Recipient),
            Pid ! Msg,
            ok;
        false ->
            answer_or_delegate(Msg, RIB)
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
    message_body = FuseRequest = #fuse_request{fuse_request = #file_request{context_guid = ContextGuid}}
} = Msg) ->
    Req = {fuse_request, effective_session_id(Msg), FuseRequest},
    worker_proxy:cast(fslogic_ref_by_context_guid(ContextGuid), Req);
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
% Message that updates the auth_manager:token_credentials() in given session
% (originates from #'Macaroon' client message).
route_and_ignore_answer(#client_message{
    message_body = #client_tokens{
        access_token = AccessToken,
        consumer_token = ConsumerToken
    }
} = Msg) ->
    incoming_session_watcher:update_credentials(
        effective_session_id(Msg),
        AccessToken, ConsumerToken
    ),
    ok;
route_and_ignore_answer(#client_message{message_body = #close_session{}} = Msg) ->
    incoming_session_watcher:report_session_close(effective_session_id(Msg)),
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
-spec answer_or_delegate(client_message(), rib()) ->
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
    IpsAndPorts = lists:map(fun(Node) ->
        {{_,_,_,_} = IP, Port} = rpc:call(
            Node, rtransfer_config, get_local_ip_and_port, []
        ),
        #ip_and_port{ip = IP, port = Port}
    end, consistent_hashing:get_all_nodes()),

    {ok, #server_message{
        message_id = MsgId,
        message_body = #rtransfer_nodes_ips{nodes = IpsAndPorts}
    }};

answer_or_delegate(#client_message{
    message_id = MsgId,
    message_body = #provider_rpc_call{} = ProviderRpcCall
}, _) ->
    {ok, #server_message{
        message_id = MsgId,
        message_body = provider_rpc_worker:exec(ProviderRpcCall)
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
}, RIB) ->
    delegate_request(proc, fun() ->
        storage_req:get_configuration(effective_session_id(Msg))
    end, MsgId, RIB);

answer_or_delegate(#client_message{
    message_id = MsgId,
    message_body = Request = #get_remote_document{key = Key}
}, RIB) ->
    delegate_request({proc, Key}, fun() ->
        datastore_remote_driver:handle(Request)
    end, MsgId, RIB);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = FuseRequest = #fuse_request{fuse_request = #file_request{context_guid = ContextGuid}}
}, RIB) ->
    Req = {fuse_request, effective_session_id(Msg), FuseRequest},
    delegate_request(fslogic_ref_by_context_guid(ContextGuid), Req, MsgId, RIB);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = FuseRequest = #fuse_request{}
}, RIB) ->
    Req = {fuse_request, effective_session_id(Msg), FuseRequest},
    delegate_request(fslogic_worker, Req, MsgId, RIB);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = ProviderRequest = #provider_request{context_guid = ContextGuid}
}, RIB) ->
    Req = {provider_request, effective_session_id(Msg), ProviderRequest},
    delegate_request(fslogic_ref_by_context_guid(ContextGuid), Req, MsgId, RIB);

answer_or_delegate(Msg = #client_message{
    message_id = Id,
    message_body = ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
    }
}, RIB) ->
    Node = read_write_req:get_proxyio_node(file_id:guid_to_uuid(FileGuid)),
    Req = {proxyio_request, effective_session_id(Msg), ProxyIORequest},
    delegate_request({fslogic_worker, Node}, Req, Id, RIB);

answer_or_delegate(Msg = #client_message{
    message_id = MsgId,
    message_body = #dbsync_request{} = DBSyncRequest
}, RIB) ->
    Req = {dbsync_request, effective_session_id(Msg), DBSyncRequest},
    delegate_request(dbsync_worker, Req, MsgId, RIB);

answer_or_delegate(Msg, _) ->
    event_router:route_message(Msg).


%% @private
-spec delegate_request(async_request_manager:worker_ref(), Req :: term(),
    clproto_message_id:id(), rib()) -> ok | {ok, server_message()}.
delegate_request(WorkerRef, Req, MsgId, RIB) ->
    async_request_manager:delegate_and_supervise(
        WorkerRef, Req, MsgId, RIB#rib.respond_via
    ).

%% @private
-spec fslogic_ref_by_context_guid(file_id:file_guid()) -> {id, module(), datastore:key()}.
fslogic_ref_by_context_guid(ContextGuid) ->
    {id, fslogic_worker, file_id:guid_to_uuid(ContextGuid)}.