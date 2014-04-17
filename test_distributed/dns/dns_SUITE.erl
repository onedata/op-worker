%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dns_worker,
%% dns_udp_handler and dns_ranch_tcp_handler modules.
%% It contains unit tests that base on ct.
%% @end
%% ===================================================================

-module(dns_SUITE).
-include_lib("kernel/src/inet_dns.hrl").

-include("nodes_manager.hrl").
-include("registered_names.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([dns_worker_env_test/1, dns_udp_handler_responds_to_dns_queries/1, dns_ranch_tcp_handler_responds_to_dns_queries/1, distributed_test/1]).

%% export nodes' codes
-export([dns_worker_env_test_code/0, ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [dns_worker_env_test, dns_ranch_tcp_handler_responds_to_dns_queries, dns_udp_handler_responds_to_dns_queries, distributed_test].


%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

dns_worker_env_test_code() ->
  assert_all_deps_are_met(?APP_Name, dns_worker),
  ok.

%% ====================================================================
%% Test functions
%% ====================================================================

%% Checks if all necessary variables are declared in application
dns_worker_env_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  {Node, _DNS_Port} = extract_node_and_dns_port(Config),

  ?assertEqual(ok, rpc:call(Node, ?MODULE, dns_worker_env_test_code, [])).

%% Checks if dns_udp_handler responds before and after running dns_worker
dns_udp_handler_responds_to_dns_queries(Config) ->
  prepare_test_and_start_cluster(Config),

  {Node, DNS_Port} = extract_node_and_dns_port(Config),
  Host = get_host(Node),

  {OpenAns, Socket} = gen_udp:open(0, [{active, false}, binary]),
  ?assertEqual(ok, OpenAns),

  Request = create_request(),
  Sender = fun () -> gen_udp:send(Socket, Host, DNS_Port, Request) end,
  Receiver = fun () -> {RcvAns, {_, DNS_Port, Packet}} = gen_udp:recv(Socket, 0, infinity),
                       {RcvAns, Packet}
  end,

  try
    send_message_and_assert_results(Sender, Receiver)
  after
    gen_udp:close(Socket)
  end.


%% Checks if dns_ranch_tcp_handler does not close connection after first response
dns_ranch_tcp_handler_responds_to_dns_queries(Config) ->
  prepare_test_and_start_cluster(Config),

  {Node, DNS_Port} = extract_node_and_dns_port(Config),
  Host = get_host(Node),

  {ConAns, Socket} = gen_tcp:connect(Host, DNS_Port, [{active, false}, binary, {packet, 2}]),
  ?assertEqual(ok, ConAns),

  Request = create_request(),
  Sender = fun () -> gen_tcp:send(Socket, Request) end,
  Receiver = fun () -> gen_tcp:recv(Socket, 0, infinity) end,

  try
    send_message_and_assert_results(Sender, Receiver),

    %% connection should still be valid
    send_message_and_assert_results(Sender, Receiver)
  after
    gen_tcp:close(Socket)
  end.

%% This test checks if dns works well with multi-node cluster
distributed_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,
  DNS_Port = 1308,
  Host = get_host(CCM),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  nodes_manager:wait_for_cluster_init(),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, fslogic}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, fslogic, []}, 1000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  nodes_manager:wait_for_cluster_init(length(NodesUp) - 1),

  {ConAns, Socket} = gen_tcp:connect(Host, DNS_Port, [{active, false}, binary, {packet, 2}]),
  ?assertEqual(ok, ConAns),

  Request = create_request("dns_worker.veilfs.plgrid.pl"),
  Sender = fun () -> gen_tcp:send(Socket, Request) end,

  Request2 = create_request("control_panel.veilfs.plgrid.pl"),
  Sender2 = fun () -> gen_tcp:send(Socket, Request2) end,

  Request3 = create_request("fslogic.veilfs.plgrid.pl"),
  Sender3 = fun () -> gen_tcp:send(Socket, Request3) end,

  Request4 = create_request("veilfs.plgrid.pl"),
  Sender4 = fun () -> gen_tcp:send(Socket, Request4) end,

  Request5 = create_request("www.veilfs.plgrid.pl"),
  Sender5 = fun () -> gen_tcp:send(Socket, Request5) end,

  Request6 = create_request("xxx.cluster.veilfs.plgrid.pl"),
  Sender6 = fun () -> gen_tcp:send(Socket, Request6) end,

  Receiver = fun () -> gen_tcp:recv(Socket, 0, infinity) end,

  try
    send_message_and_assert_results(Sender, Receiver, 2),
    send_message_and_assert_results(Sender2, Receiver),
    send_message_and_assert_results(Sender3, Receiver, 4),
    send_message_and_assert_results(Sender4, Receiver),
    send_message_and_assert_results(Sender5, Receiver),
    send_message_and_assert_results(Sender6, Receiver, any),

    send_message_and_assert_results(Sender, Receiver, 2),
    send_message_and_assert_results(Sender2, Receiver),
    send_message_and_assert_results(Sender3, Receiver, 4),
    send_message_and_assert_results(Sender4, Receiver),
    send_message_and_assert_results(Sender5, Receiver),
    send_message_and_assert_results(Sender6, Receiver, any)
  after
    gen_tcp:close(Socket)
  end.

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(distributed_test, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {control_panel_port, 1350}, {control_panel_redirect_port, 1354}, {rest_port, 8443}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 6666}, {control_panel_port, 1351}, {control_panel_redirect_port, 1355}, {rest_port, 8444}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 7777}, {control_panel_port, 1352}, {control_panel_redirect_port, 1356}, {rest_port, 8445}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 8888}, {control_panel_port, 1353}, {control_panel_redirect_port, 1357}, {rest_port, 8446}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config);

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [Node | _] = NodesUp,

  DNS_Port = 1312,
  Env = [{node_type, ccm_test}, {dispatcher_port, 6666}, {ccm_nodes, [Node]}, {dns_port, DNS_Port}],

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [Env]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{dns_port, DNS_Port}, {nodes, NodesUp}, {assertions, Assertions}], Config).


end_per_testcase(distributed_test, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns);

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).

%% ====================================================================
%% Helping functions
%% ====================================================================

%% Helper function setting test timeout, checking start assertions and waiting for cluster creation
prepare_test_and_start_cluster(Config) ->
  ct:timetrap({seconds, 30}),
  nodes_manager:check_start_assertions(Config),
  start_cluster(Config),
  nodes_manager:wait_for_cluster_init().


%% Helper function for starting cluster
start_cluster(Config) ->
  [Node | _] = ?config(nodes, Config),
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  nodes_manager:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.


%% Helper function extracting node and dns port from test config
extract_node_and_dns_port(Config) ->
  [Node | _] = ?config(nodes, Config),
  DNS_Port = ?config(dns_port, Config),
  {Node, DNS_Port}.


%% Helper function returning host for given node
get_host(Node) ->
  NodeAsList = atom_to_list(Node),
  [_, Host] = string:tokens(NodeAsList, "@"),
  Host.


%% Helper function creating sample binary dns request
create_request() ->
  create_request("dns_worker").

create_request(SupportedDomain) ->
  RequestHeader = #dns_header{id = 1, qr = false, opcode = ?QUERY, rd = 1},
  RequestQuery = #dns_query{domain=SupportedDomain, type=?T_A,class=?C_IN},
  Request = #dns_rec{header=RequestHeader, qdlist=[RequestQuery], anlist=[], nslist=[], arlist=[]},

  inet_dns:encode(Request).


%% Helper function sending request and asserting returned response
send_message_and_assert_results(Sender, Receiver) ->
  send_message_and_assert_results(Sender, Receiver, 1).

send_message_and_assert_results(Sender, Receiver, IPsNum) ->
  SendAns = Sender(),
  ?assertEqual(ok, SendAns),

  {RecvAns, Packet} = Receiver(),
  ?assertEqual(ok, RecvAns),

  {DecodeAns, Response} = inet_dns:decode(Packet),
  ?assertEqual(ok, DecodeAns),

  Header = Response#dns_rec.header,
  ?assertEqual(?NOERROR, Header#dns_header.rcode),

  case IPsNum of
    any ->
      ?assert(length(Response#dns_rec.anlist) >= 1);
    _ ->
      ?assertEqual(IPsNum, length(Response#dns_rec.anlist))
  end.


%% Helper function returning type of expression
type_of(X) when is_integer(X)   -> integer;
type_of(X) when is_float(X)     -> float;
type_of(X) when is_list(X)      -> list;
type_of(X) when is_tuple(X)     -> tuple;
type_of(X) when is_bitstring(X) -> bitstring;
type_of(X) when is_binary(X)    -> binary;
type_of(X) when is_boolean(X)   -> boolean;
type_of(X) when is_function(X)  -> function;
type_of(X) when is_pid(X)       -> pid;
type_of(X) when is_port(X)      -> port;
type_of(X) when is_reference(X) -> reference;
type_of(X) when is_atom(X)      -> atom;
type_of(_X)                     -> unknown.


%% Helper function for asserting that all module dependencies
%% are set and have declared type
assert_all_deps_are_met(Application, Deps) when is_list(Deps) ->
  lists:foreach(fun(Dep) ->
      assert_all_deps_are_met(Application, Dep)
    end, Deps);

assert_all_deps_are_met(Application, {VarName, VarType}) when is_atom(VarName) andalso is_atom(VarType) ->

  {ok, Value} = application:get_env(Application, VarName),
  ?assertEqual(VarType, type_of(Value));

assert_all_deps_are_met(Application, Module) when is_atom(Module) ->

  Dependencies = Module:env_dependencies(),
  assert_all_deps_are_met(Application, Dependencies).

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

ccm_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

ccm_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

worker_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.