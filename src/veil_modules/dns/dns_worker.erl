%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements {@link worker_plugin_behaviour} to provide
%% functionality of resolution ipv4 addresses for given worker name.
%% @end
%% ===================================================================

-module(dns_worker).
-behaviour(worker_plugin_behaviour).
-include("veil_modules/dns/dns_worker.hrl").
-include("registered_names.hrl").
-include("supervision_macros.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0, env_dependencies/0, start_listening/0]).

env_dependencies() ->
	[{dns_port, integer}, {dns_response_ttl, integer}, {dispatcher_timeout, integer},
		{dns_tcp_acceptor_pool_size, integer}, {dns_tcp_timeout, integer},
		{dns_port, integer}
	].

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1.
%% @end
%% ====================================================================
-spec init(Args :: term()) -> Result when
	Result :: #dns_worker_state{} | {error, Error},
	Error :: term().
%% ====================================================================
init([]) ->
	{ok, _Pid} = proc_lib:start(?MODULE, start_listening, [], 500),
    #dns_worker_state{};

init(InitialState) when is_record(InitialState, dns_worker_state) ->
	InitialState;

init(test) ->
	#dns_worker_state{};

init(_) ->
	throw(unknown_initial_state).


%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Calling handle(_, ping) returns pong.
%% Calling handle(_, get_version) returns current version of application.
%% Calling handle(_. {update_state, _}) updates plugin state.
%% Calling handle(_, {get_worker, Name}) returns list of ipv4 addresses of workers with specified name.
%% @end
%% ====================================================================
-spec handle(ProtocolVersion :: term(), Request) -> Result when
	Request :: ping | get_version |
	{update_state, [{atom(),  [inet:ip4_address()]}]} |
	{get_worker, atom()},
	Result :: ok | {ok, Response} | {error, Error} | pong | Version,
	Response :: [inet:ip4_address()],
	Version :: term(),
	Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
	pong;

handle(_ProtocolVersion, get_version) ->
	node_manager:check_vsn();

handle(_ProtocolVersion, {update_state, ModulesToNodes}) ->
  ModulesToNodes2 = lists:map(fun ({Module, Nodes}) ->
    GetLoads = fun({Node, V}) ->
      {Node, V, V}
    end,
    {Module, lists:map(GetLoads, Nodes)}
  end, ModulesToNodes),
	New_DNS_State = #dns_worker_state{workers_list = ModulesToNodes2},
	ok = gen_server:call(?MODULE, {updatePlugInState, New_DNS_State});

handle(_ProtocolVersion, {get_worker, Module}) ->
	DNS_State = gen_server:call(?MODULE, getPlugInState),
	WorkerList = DNS_State#dns_worker_state.workers_list,
	Result = proplists:get_value(Module, WorkerList, []),

  PrepareResult = fun({Node, V, C}, {TmpAns, TmpWorkers}) ->
    TmpAns2 = case C of
      V -> [Node | TmpAns];
      _ -> TmpAns
    end,
    C2 = case C of
      1 -> V;
      _ -> C - 1
    end,
    {TmpAns2, [{Node, V, C2} | TmpWorkers]}
  end,
  {Result2, WorkerList2} = lists:foldl(PrepareResult, {[], []}, Result),

  New_DNS_State = #dns_worker_state{workers_list = WorkerList2},
  ok = gen_server:call(?MODULE, {updatePlugInState, New_DNS_State}),
	{ok, Result2};

handle(ProtocolVersion, Msg) ->
	throw({unsupported_request, ProtocolVersion, Msg}).

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%% ====================================================================
-spec cleanup() -> Result when
	Result :: ok.
%% ====================================================================
cleanup() ->
	SafelyExecute = fun (Invoke, Log) ->
		try
			Invoke()
		catch
			_:Error -> lager:warning(Log, [Error])
		end
	end,

	SafelyExecute(fun () -> ok = supervisor:terminate_child(?Supervisor_Name, ?DNS_UDP),
			supervisor:delete_child(?Supervisor_Name, ?DNS_UDP)
		end,
		"Error stopping dns udp listener, status ~p"),

	SafelyExecute(fun () -> ok = ranch:stop_listener(dns_tcp_listener)
		end,
		"Error stopping dns tcp listener, status ~p"),
	ok.


%% start_listening/0
%% ====================================================================
%% @doc Starts dns listeners and terminates dns_worker process in case of error.
%% @end
%% ====================================================================
-spec start_listening() -> ok.
%% ====================================================================
start_listening() ->
	try
		{ok, DNSPort} = application:get_env(?APP_Name, dns_port),
		{ok, DNSResponseTTL} = application:get_env(?APP_Name, dns_response_ttl),
		{ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),
		{ok, TcpAcceptorPool} = application:get_env(?APP_Name, dns_tcp_acceptor_pool_size),
		{ok, TcpTimeout} = application:get_env(?APP_Name, dns_tcp_timeout),
		{ok, DNSPort} = application:get_env(?APP_Name, dns_port),
		UDP_Child = ?Sup_Child(?DNS_UDP, dns_udp_handler, permanent, [DNSPort, DNSResponseTTL, DispatcherTimeout]),
		DNS_TCP_Transport_Options =  [{packet, 2}, {dispatcher_timeout, DispatcherTimeout}, {dns_response_ttl, DNSResponseTTL},
			{dns_tcp_timeout, TcpTimeout}, {keepalive, true}],

		proc_lib:init_ack({ok, self()}),
		{ok, Pid} = supervisor:start_child(?Supervisor_Name, UDP_Child),
		start_tcp_listener(TcpAcceptorPool, DNSPort, DNS_TCP_Transport_Options, Pid)
	catch
		_:Reason -> lager:warning("Error during starting listeners, ~p", [Reason]),
			Result = gen_server:call({global, ?CCM}, {stop_worker, node(), ?MODULE}),
			lager:info("Terminating ~p with status ~p", [?MODULE, Result])

	end,
	ok.


%% start_tcp_listener/4
%% ====================================================================
%% @doc Starts tcp listener and handles case when udp listener has already started, but
%% tcp listener is unable to do so.
%% @end
%% ====================================================================
-spec start_tcp_listener(AcceptorPool, Port, TransportOpts, Pid) -> ok when
	AcceptorPool :: pos_integer(),
	Port :: pos_integer(),
	TransportOpts :: list(),
	Pid :: pid().
%% ====================================================================
start_tcp_listener(AcceptorPool, Port, TransportOpts, Pid) ->
	try
		{ok, _} = ranch:start_listener(dns_tcp_listener, AcceptorPool, ranch_tcp, [{port, Port}],
			dns_ranch_tcp_handler, TransportOpts)
	catch
		_:RanchError -> ok = supervisor:terminate_child(?Supervisor_Name, Pid),
			supervisor:delete_child(?Supervisor_Name, Pid),
			throw(RanchError)
	end,
	ok.