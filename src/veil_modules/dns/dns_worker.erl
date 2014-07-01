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
-include("logging.hrl").

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


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Calling handle(_, ping) returns pong.
%% Calling handle(_, get_version) returns current version of application.
%% Calling handle(_. {update_state, _}) updates plugin state.
%% Calling handle(_, {get_worker, Name}) returns list of ipv4 addresses of workers with specified name.
%% @end
%% ====================================================================
-spec handle(ProtocolVersion :: term(), Request) -> Result when
	Request :: ping | healthcheck | get_version |
	{update_state, list(), list()} |
	{get_worker, atom()} |
  get_nodes,
	Result :: ok | {ok, Response} | {error, Error} | pong | Version,
	Response :: [inet:ip4_address()],
	Version :: term(),
	Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
	pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
	node_manager:check_vsn();

handle(_ProtocolVersion, {update_state, ModulesToNodes, NLoads, AvgLoad}) ->
  ?info("DNS state update: ~p", [{ModulesToNodes, NLoads, AvgLoad}]),
  try
    ModulesToNodes2 = lists:map(fun ({Module, Nodes}) ->
      GetLoads = fun({Node, V}) ->
        {Node, V, V}
      end,
      {Module, lists:map(GetLoads, Nodes)}
    end, ModulesToNodes),
    New_DNS_State = #dns_worker_state{workers_list = ModulesToNodes2, nodes_list = NLoads, avg_load = AvgLoad},
    case gen_server:call(?MODULE, {updatePlugInState, New_DNS_State}) of
      ok ->
        ok;
      UpdateError ->
        ?error("DNS update error: ~p", [UpdateError]),
        udpate_error
    end
  catch
    E1:E2 ->
      ?error("DNS update error: ~p:~p", [E1, E2]),
      udpate_error
  end;

handle(_ProtocolVersion, {get_worker, Module}) ->
  try
    DNS_State = gen_server:call(?MODULE, getPlugInState),
    WorkerList = DNS_State#dns_worker_state.workers_list,
    NodesList = DNS_State#dns_worker_state.nodes_list,
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
    {Result2, ModuleWorkerList} = lists:foldl(PrepareResult, {[], []}, Result),

    PrepareState = fun({M, Workers}, TmpWorkersList) ->
      case M =:= Module of
        true -> [{M, ModuleWorkerList} | TmpWorkersList];
        false -> [{M, Workers} | TmpWorkersList]
      end
    end,
    NewWorkersList = lists:foldl(PrepareState, [], WorkerList),

    New_DNS_State = DNS_State#dns_worker_state{workers_list = NewWorkersList},

    case gen_server:call(?MODULE, {updatePlugInState, New_DNS_State}) of
      ok ->
        random:seed(now()),
        Result3 = make_ans_random(Result2),
        case Module of
          control_panel ->
            {ok, Result3};
          _ ->
            create_ans(Result3, NodesList)
        end;
      UpdateError ->
        ?error("DNS get_worker error: ~p", [UpdateError]),
        {error, dns_update_state_error}
    end
  catch
    E1:E2 ->
      ?error("DNS get_worker error: ~p:~p", [E1, E2]),
      {error, dns_get_worker_error}
  end;

handle(_ProtocolVersion, get_nodes) ->
  try
    DNS_State = gen_server:call(?MODULE, getPlugInState),
    NodesList = DNS_State#dns_worker_state.nodes_list,
    AvgLoad = DNS_State#dns_worker_state.avg_load,

    case AvgLoad of
      0 ->
        Res = make_ans_random(lists:map(
          fun({Node, _}) ->
            Node
          end, NodesList)),
        {ok, Res};
      _ ->
        random:seed(now()),
        ChooseNodes = fun({Node, NodeLoad}, TmpAns) ->
          case is_number(NodeLoad) and (NodeLoad > 0) of
            true ->
              Ratio = AvgLoad / NodeLoad,
              case Ratio >= random:uniform() of
                true ->
                  [Node | TmpAns];
                false ->
                  TmpAns
              end;
            false ->
              [Node | TmpAns]
          end
        end,
        Result = lists:foldl(ChooseNodes, [], NodesList),
        Result2 = make_ans_random(Result),
        create_ans(Result2, NodesList)
    end
  catch
    E1:E2 ->
      ?error("DNS get_nodes error: ~p:~p", [E1, E2]),
      {error, get_nodes}
  end;

handle(ProtocolVersion, Msg) ->
  ?warning("Wrong request: ~p", [Msg]),
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
  spawn(fun() -> clear_children_and_listeners() end),
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

    try
      supervisor:delete_child(?Supervisor_Name, ?DNS_UDP),
      ?debug("DNS UDP child has existed")
    catch
      _:_ -> ok
    end,

    try
      ranch:stop_listener(dns_tcp_listener),
      ?debug("dns_tcp_listener has existed")
    catch
      _:_ -> ok
    end,

		{ok, Pid} = supervisor:start_child(?Supervisor_Name, UDP_Child),
		start_tcp_listener(TcpAcceptorPool, DNSPort, DNS_TCP_Transport_Options, Pid)
	catch
		_:Reason -> ?error("DNS Error during starting listeners, ~p", [Reason]),
			gen_server:cast({global, ?CCM}, {stop_worker, node(), ?MODULE}),
			?info("Terminating ~p", [?MODULE])
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
      ?error("Start DNS TCP listener error, ~p", [RanchError]),
			throw(RanchError)
	end,
	ok.

%% clear_children_and_listeners/0
%% ====================================================================
%% @doc Clears listeners and created children
%% @end
%% ====================================================================
-spec clear_children_and_listeners() -> ok.
%% ====================================================================
clear_children_and_listeners() ->
  SafelyExecute = fun (Invoke, Log) ->
    try
      Invoke()
    catch
      _:Error -> ?error(Log, [Error])
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

%% create_ans/2
%% ====================================================================
%% @doc Creates answer from results list (adds additional node if needed).
%% @end
%% ====================================================================
-spec create_ans(Result :: list(), NodesList :: list()) -> Ans when
  Ans :: {ok, IPs},
  IPs :: list().
%% ====================================================================
create_ans(Result, NodesList) ->
  case (length(Result) > 1) or (length(NodesList) =< 1)  of
    true -> {ok, Result};
    false ->
      NodeNum = random:uniform(length(NodesList)),
      {NewNode, _} = lists:nth(NodeNum, NodesList),
      case Result =:= [NewNode] of
        true ->
          {NewNode2, _} = lists:nth((NodeNum rem length(NodesList) + 1) , NodesList),
          {ok, Result ++ [NewNode2]};
        false ->
          {ok, Result ++ [NewNode]}
      end
  end.

%% make_ans_random/1
%% ====================================================================
%% @doc Makes order of nodes in answer random.
%% @end
%% ====================================================================
-spec make_ans_random(Result :: list()) -> IPs when
  IPs :: list().
%% ====================================================================
make_ans_random(Result) ->
  Len = length(Result),
  case Len of
    0 -> [];
    1 -> Result;
    _ ->
      NodeNum = random:uniform(Len),
      NewRes = lists:sublist(Result, 1, NodeNum - 1) ++ lists:sublist(Result, NodeNum + 1, Len),
      [lists:nth(NodeNum, Result) | make_ans_random(NewRes)]
  end.
