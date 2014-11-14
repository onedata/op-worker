%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements worker_plugin_behaviour to provide
%% gateway functionality (transfer of files between data centers).
%% @end
%% ===================================================================

-module(gateway).
-author("Konrad Zemek").
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include("registered_names.hrl").

-export([init/1, handle/2, cleanup/0]).
-export([compute_request_hash/1]).


-ifdef(TEST).
-define(NOTIFICATION_STATE, notification_state).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================


%% init/1
%% ====================================================================
%% @doc Initialize the module, starting all necessary services.
%% @see worker_plugin_behaviour
-spec init(Args :: term()) -> ok | {error, Error :: any()}.
%% ====================================================================
-ifdef(TEST).
init(_Args) ->
  ets:new(?NOTIFICATION_STATE, [named_table, public, set]),
  [].
-else.
init(_Args) ->
    {ok, GwPort} = application:get_env(?APP_Name, gateway_listener_port),
    {ok, GwProxyPort} = application:get_env(?APP_Name, gateway_proxy_port),
    {ok, Cert} = application:get_env(?APP_Name, global_registry_provider_cert_path),
    {ok, Acceptors} = application:get_env(?APP_Name, gateway_acceptor_number),
    {ok, NICs} = application:get_env(?APP_Name, gateway_network_interfaces),

    LocalServerPort = oneproxy:get_local_port(GwPort),
    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, Acceptors, ranch_tcp,
        [{port, LocalServerPort}], gateway_protocol_handler, []),

    OpPid = spawn_link(fun() -> oneproxy:start_proxy(GwProxyPort, Cert, verify_none) end),
    register(gw_oneproxy_outgoing, OpPid),

    OpPid2 = spawn_link(fun() -> oneproxy:start_rproxy(GwPort, LocalServerPort, Cert, verify_none, no_http) end),
    register(gw_oneproxy_incoming, OpPid2),

    %% @TODO: On supervisor's exit we should be able to reinitialize the module.
	{ok, _} = gateway_dispatcher_supervisor:start_link(NICs),
    ok.
-endif.


%% handle/2
%% ====================================================================
%% @doc Handle a message.
%% @see worker_plugin_behaviour
-spec handle(ProtocolVersion :: term(), Request :: term()) ->
    {ok, Ans :: term()} | {error, Error :: any()}.
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();


handle(_ProtocolVersion, {node_lifecycle_notification, Node, Module, Action, Pid}) ->
  handle_node_lifecycle_notification(Node, Module, Action, Pid),
  ok;

handle(_ProtocolVersion, node_lifecycle_get_notification) ->
  node_lifecycle_get_notification();


handle(_ProtocolVersion, _Msg) ->
    ?log_call(_Msg),
    ok.


%% handle_node_lifecycle_notification/4
%% ====================================================================
%% @doc Handles lifecycle calls
-spec handle_node_lifecycle_notification(Node :: list(), Module :: atom(), Action :: atom(), Pid :: pid()) -> ok.
%% ====================================================================
-ifdef(TEST).
handle_node_lifecycle_notification(Node, Module, Action, Pid) ->
  case ets:lookup(?NOTIFICATION_STATE, node_lifecycle_notification) of
    [{_, L}] -> ets:insert(?NOTIFICATION_STATE, {node_lifecycle_notification, [{Node, Module, Action, Pid}|L]});
    _ -> ets:insert(?NOTIFICATION_STATE, {node_lifecycle_notification, [{Node, Module, Action, Pid}]})
  end,
  ok.
-else.
handle_node_lifecycle_notification(_Node, _Module, _Action, _Pid) ->
  ok.
-endif.

%% node_lifecycle_get_notification/0
%% ====================================================================
%% @doc Handles test calls.
-spec node_lifecycle_get_notification() -> ok | term().
%% ====================================================================
-ifdef(TEST).
node_lifecycle_get_notification() ->
  Notification = ets:lookup(?NOTIFICATION_STATE, node_lifecycle_notification),
  {ok, {node_lifecycle, Notification}}.
-else.
node_lifecycle_get_notification() ->
  ok.
-endif.


%% cleanup/0
%% ====================================================================
%% @doc Cleanup any state associated with the module.
%% @see worker_plugin_behaviour
-spec cleanup() -> ok | {error, Error :: any()}.
%% ====================================================================
cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    exit(gw_oneproxy_outgoing, shutdown),
    exit(gw_oneproxy_incoming, shutdown),
    exit(?GATEWAY_DISPATCHER_SUPERVISOR, shutdown).


%% compute_request_hash/1
%% ====================================================================
%% @doc Computes a sha256 hash of an encoded protobuf #filerequest
-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
%% ====================================================================
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).


%% ====================================================================
%% Internal functions
%% ====================================================================
