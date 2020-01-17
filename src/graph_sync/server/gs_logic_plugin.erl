%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model implements gs_logic_plugin_behaviour and is called by gs_server
%%% to handle application specific Graph Sync logic.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_logic_plugin).
-author("Bartosz Walkowicz").

-behaviour(gs_logic_plugin_behaviour).

-include("middleware/middleware.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

%% API
-export([verify_handshake_auth/2]).
-export([client_connected/2, client_disconnected/2]).
-export([verify_auth_override/2]).
-export([is_authorized/5]).
-export([handle_rpc/4]).
-export([handle_graph_request/6]).
-export([is_subscribable/1, is_type_supported/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback verify_handshake_auth/2.
%% @end
%%--------------------------------------------------------------------
-spec verify_handshake_auth(gs_protocol:client_auth(), ip_utils:ip()) ->
    {ok, aai:auth()} | errors:error().
verify_handshake_auth(undefined, _) ->
    {ok, ?NOBODY};
verify_handshake_auth(nobody, _) ->
    {ok, ?NOBODY};
verify_handshake_auth({token, AccessToken}, PeerIp) ->
    TokenAuth = auth_manager:build_token_auth(
        AccessToken, undefined,
        PeerIp, graphsync, disallow_data_access_caveats
    ),
    case http_auth:authenticate(TokenAuth) of
        {ok, ?USER = Auth} ->
            {ok, Auth};
        {ok, ?NOBODY} ->
            ?ERROR_UNAUTHORIZED;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback client_connected/2.
%% @end
%%--------------------------------------------------------------------
-spec client_connected(aai:auth(), gs_server:conn_ref()) ->
    ok.
client_connected(?USER(_UserId, SessionId), ConnectionRef) ->
    session_connections:register(SessionId, ConnectionRef);
client_connected(_, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback client_disconnected/2.
%% @end
%%--------------------------------------------------------------------
-spec client_disconnected(aai:auth(), gs_server:conn_ref()) ->
    ok.
client_disconnected(?USER(_UserId, SessionId), ConnectionRef) ->
    session_connections:deregister(SessionId, ConnectionRef);
client_disconnected(_, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback verify_auth_override/2.
%% @end
%%--------------------------------------------------------------------
-spec verify_auth_override(aai:auth(), gs_protocol:auth_override()) ->
    {ok, aai:auth()} | errors:error().
verify_auth_override(_, _) ->
    ?ERROR_UNAUTHORIZED.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback is_authorized/5.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(aai:auth(), gs_protocol:auth_hint(),
    gri:gri(), gs_protocol:operation(), gs_protocol:versioned_entity()) ->
    {true, gri:gri()} | false.
is_authorized(Auth, AuthHint, GRI, Operation, VersionedEntity) ->
    OpReq = #op_req{
        auth = Auth,
        operation = Operation,
        gri = GRI,
        auth_hint = AuthHint
    },
    middleware:is_authorized(OpReq, VersionedEntity).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback handle_rpc/4.
%% @end
%%--------------------------------------------------------------------
-spec handle_rpc(gs_protocol:protocol_version(), aai:auth(),
    gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
handle_rpc(_, Auth, RpcFun, Data) ->
    gs_rpc:handle(Auth, RpcFun, Data).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback handle_graph_request/6.
%% @end
%%--------------------------------------------------------------------
-spec handle_graph_request(aai:auth(), gs_protocol:auth_hint(),
    gri:gri(), gs_protocol:operation(), gs_protocol:data(),
    gs_protocol:versioned_entity()) -> gs_protocol:graph_request_result().
handle_graph_request(Auth, AuthHint, GRI, Operation, Data, VersionedEntity) ->
    OpReq = #op_req{
        auth = Auth,
        operation = Operation,
        gri = GRI,
        data = Data,
        auth_hint = AuthHint,
        return_revision = true
    },
    middleware:handle(OpReq, VersionedEntity).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback is_subscribable/1.
%% NOTE: prototype implementation without subscribables
%% @end
%%--------------------------------------------------------------------
-spec is_subscribable(gri:gri()) -> boolean().
is_subscribable(_) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_logic_plugin_behaviour} callback is_type_supported/1.
%% @end
%%--------------------------------------------------------------------
-spec is_type_supported(gri:gri()) -> boolean().
is_type_supported(#gri{type = op_provider}) -> true;
is_type_supported(#gri{type = op_space}) -> true;
is_type_supported(#gri{type = op_user}) -> true;
is_type_supported(#gri{type = op_group}) -> true;
is_type_supported(#gri{type = op_file}) -> true;
is_type_supported(#gri{type = op_replica}) -> true;
is_type_supported(#gri{type = op_transfer}) -> true;
is_type_supported(#gri{type = _}) -> false.
