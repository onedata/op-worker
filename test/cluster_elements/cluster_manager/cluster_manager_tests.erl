%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).


%% ====================================================================
%% Test generation
%% ====================================================================

update_dns_state_test_() ->
	[
		?_test(update_dns_state__current_worker_host_implementation()),
		?_test(update_dns_state__unresolveable_ip_address()),
		?_test(update_dns_state__empty_list()),
		?_test(update_dns_state__one_non_dns_worker()),
		?_test(update_dns_state__one_dns_worker()),
    ?_test(calculate_worker_load__one_worker()),
		?_test(update_dns_state__many_non_dns_workers()),
		?_test(update_dns_state__many_non_dns_workers_one_dns_worker()),
    ?_test(calculate_worker_load__many_non_dns_workers()),
		?_test(update_dns_state__many_non_dns_workers_many_dns_workers())
	].

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if ccm is resistant to incorrect requests.
wrong_request_test() ->
	application:set_env(?APP_Name, worker_load_memory_size, 1000),
	application:set_env(?APP_Name, hot_swapping_time, 10000),
	application:set_env(?APP_Name, initialization_time, 10),
	application:set_env(?APP_Name, cluster_clontrol_period, 300),

	cluster_manager:start_link(test),

	gen_server:cast({global, ?CCM}, abc),
	Reply = gen_server:call({global, ?CCM}, abc),
	?assert(Reply =:= wrong_request),
	cluster_manager:stop().

%% ====================================================================
%% Functions used by tests
%% ====================================================================

%% Checks if update_dns_state can work with empty list.
update_dns_state__empty_list() ->
  {NodesLoad, AvgLoad} = cluster_manager:calculate_node_load([], medium),
  WorkersLoad = cluster_manager:calculate_worker_load([]),
  Load = cluster_manager:calculate_load(NodesLoad, WorkersLoad),
  cluster_manager:update_dns_state([], Load, AvgLoad).

%% Checks if update_dns_state can work with one dns worker.
update_dns_state__one_dns_worker() ->
	{ok, DNS_Gen_Server} = gen_server_mock:new(),

	OneDnsWorker = [{'node@127.0.0.1', dns_worker, DNS_Gen_Server}],
  Load = [{'node@127.0.0.1', 3, [{dns_worker, 1}]}],
  Avg = 3,

	expect_update_state(DNS_Gen_Server, [{dns_worker, [{{127,0,0,1}, 1}]}]),

	try
		cluster_manager:update_dns_state(OneDnsWorker, Load, Avg)
	after
		assert_expectations_and_stop(DNS_Gen_Server)
	end.

%% Checks if update_dns_state can work with one non dns worker.
update_dns_state__one_non_dns_worker() ->
	{ok, Gen_Server} = gen_server_mock:new(),

	OneNonDnsWorker = [{'node@127.0.0.1', module, Gen_Server}],
  Load = [{'node@127.0.0.1', 3, [{module, 1}]}],
  Avg = 3,

	try
		cluster_manager:update_dns_state(OneNonDnsWorker, Load, Avg)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks workers load calculation
calculate_worker_load__one_worker() ->
  {ok, Gen_Server} = gen_server_mock:new(),
  expect_load_info(Gen_Server),

  try
    Load = cluster_manager:calculate_worker_load([{'node@127.0.0.1', module, Gen_Server}]),
    ?assertEqual([{'node@127.0.0.1', [{module, 1.0}]}], Load)
  after
    assert_expectations_and_stop(Gen_Server)
  end.

%% Checks if update_dns_state can work with many non dns workers.
update_dns_state__many_non_dns_workers() ->
	Gen_Servers = gen_server_mock:new(5),

	Times = lists:seq(10, 50, 10),
	Gen_ServersAndTimes = lists:zip(Gen_Servers, Times),

	ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
		{list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
	end, Gen_ServersAndTimes),

  Load = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
  end, Gen_ServersAndTimes),
  Avg = 3,

	try
		cluster_manager:update_dns_state(ManyNonDnsWorkers, Load, Avg)
	after
		assert_expectations_and_stop(Gen_Servers)
	end.

%% Checks if update_dns_state can work with many non dns workers on same node and one dns worker.
update_dns_state__many_non_dns_workers_one_dns_worker() ->
	Non_DNS_Workers = gen_server_mock:new(5),
	{ok, DNS_Worker} = gen_server_mock:new(),

	Times = lists:seq(10, 50, 10),
	Non_DNS_WorkersAndTimes = lists:zip(Non_DNS_Workers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Non_DNS_WorkersAndTimes),
  Workers = [{'node@192.168.0.60', dns_worker, DNS_Worker} | ManyNonDnsWorkers],

  Load = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
  end, Non_DNS_WorkersAndTimes),
  Load2 = [{'node@192.168.0.60', 3, [{dns_worker, 1}]} | Load],
  Avg = 3,

	Expected_DNS_State = [
		{dns_worker, [{{192,168,0,60}, 1}]},
		{module, [{{192,168,0,50}, 1}, {{192,168,0,40}, 1}, {{192,168,0,30}, 1}, {{192,168,0,20}, 1}, {{192,168,0,10}, 1}]}
	],

	expect_update_state(DNS_Worker, Expected_DNS_State),

	try
		cluster_manager:update_dns_state(Workers, Load2, Avg)
	after
		assert_expectations_and_stop([DNS_Worker | Non_DNS_Workers])
	end.

% Checks workers load calculation
calculate_worker_load__many_non_dns_workers() ->
  Non_DNS_Workers = gen_server_mock:new(5),
  {ok, DNS_Worker} = gen_server_mock:new(),

  Times = lists:seq(10, 50, 10),
  Gen_ServersAndTimes = lists:zip(Non_DNS_Workers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Gen_ServersAndTimes),
  Workers = [{'node@192.168.0.50', dns_worker, DNS_Worker} | ManyNonDnsWorkers],

  expect_load_info([{DNS_Worker, 100} | Gen_ServersAndTimes]),

  ExpectedAns = lists:map(fun({Gen_Server, Load}) ->
    N = list_to_atom("node@192.168.0." ++ integer_to_list(Load)),
    case Load == 50 of
      true -> {N, [{module, 1.0}, {dns_worker, 1.0}]};
      false -> {N, [{module, 1.0}]}
    end
  end, Gen_ServersAndTimes),

  try
    Load = cluster_manager:calculate_worker_load(Workers),
    ?assertEqual(length(ExpectedAns), length(Load)),
    lists:foreach(fun({{ExpectedN, ExpectedM}, {AnsN, AnsM}}) ->
      ?assertEqual(ExpectedN, AnsN),
      case ExpectedN == 'node@192.168.0.50' of
        true ->
          ?assertEqual(length(ExpectedM), length(AnsM)),
          lists:foreach(fun({{ExpectedM2, _}, {AnsM2, _}}) ->
            ?assertEqual(ExpectedM2, AnsM2)
          end, lists:zip(ExpectedM, AnsM));
        false -> ?assertEqual(ExpectedM, AnsM)
      end
    end, lists:zip(ExpectedAns, Load))
  after
    assert_expectations_and_stop([DNS_Worker | Non_DNS_Workers])
  end.

%% Checks if update_dns_state can work with many non dns workers and many dns workers.
update_dns_state__many_non_dns_workers_many_dns_workers() ->
	Non_DNS_Workers = gen_server_mock:new(5),
	DNS_Workers = gen_server_mock:new(5),

	Times = lists:seq(10, 100, 10),
	WorkersAndTimes = lists:zip(Non_DNS_Workers ++ DNS_Workers, Times),

  Workers = lists:map(fun({Gen_Server, Load}) ->
    case Load > 50 of
      true -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), dns_worker, Gen_Server};
      false -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
    end
  end, WorkersAndTimes),

  Load = lists:map(fun({Gen_Server, Load}) ->
    case Load > 50 of
      true -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{dns_worker, 1}]};
      false -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
    end
  end, WorkersAndTimes),
  Avg = 3,

  Expected_DNS_State = [
    {dns_worker, [{{192,168,0,100}, 1}, {{192,168,0,90}, 1}, {{192,168,0,80}, 1}, {{192,168,0,70}, 1}, {{192,168,0,60}, 1}]},
    {module, [{{192,168,0,50}, 1}, {{192,168,0,40}, 1}, {{192,168,0,30}, 1}, {{192,168,0,20}, 1}, {{192,168,0,10}, 1}]}
  ],

  expect_update_state(DNS_Workers, Expected_DNS_State),

  try
    cluster_manager:update_dns_state(Workers, Load, Avg)
  after
    assert_expectations_and_stop(Non_DNS_Workers ++ DNS_Workers)
  end.


%% Checks if update_dns_state can handle unresolveable ip address.
update_dns_state__unresolveable_ip_address() ->
	UnresolveableAddresses = ['node@unresolveable_address', 'node@', '@', '', 'node@256.256.256.1'],

	lists:foreach(fun (NodeName) ->
		update_dns_state_with_wrong_ip_address(NodeName)
	end, UnresolveableAddresses).


%% Checks if update_dns_state can work with current worker host implementation.
update_dns_state__current_worker_host_implementation() ->
	{ok, State} = worker_host:init([dns_worker, test, 100]),
	Ref = erlang:monitor(process, self()),

	{reply, {{MegaS, S, MicroS}, Load}, _} = worker_host:handle_call(getLoadInfo, {self(), Ref}, State),

	?assert(is_integer(MegaS)),
	?assert(is_integer(S)),
	?assert(is_integer(MicroS)),
	?assert(is_integer(Load)),
	erlang:demonitor(Ref).

%% Checks if dispatcher is updated properly
update_dispatcher_test() ->
  request_dispatcher:start_link(),

  N1 = node(),
  Loads = [{'node@127.0.0.1', 1}, {N1, 1}],
  Nodes = ['node@127.0.0.1', N1],
  WorkersList = [{N1, fslogic}, {N1, dao}, {n2, fslogic}, {n3, fslogic}, {n3, dao}, {n4, gateway}, {N1, dns_worker}],
  cluster_manager:update_dispatcher_state(WorkersList, Nodes, 1, Loads, 1),

  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao, rtransfer, dao, dns_worker, dns_worker, gateway, gateway],
  ExpectedAns = [n3, n2, N1, N1, n2, n3, n3, n3, non, N1, N1, N1, n4, n4],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {get_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

%% ====================================================================
%% Helping functions
%% ====================================================================

%% Helper function for testing update_dns_state with wrong ip address.
update_dns_state_with_wrong_ip_address(NodeName) ->
	{ok, DNS_Gen_Server} = gen_server_mock:new(),

  OneDnsWorker = [{NodeName, dns_worker, DNS_Gen_Server}],
  Load = [{NodeName, 3, [{dns_worker, 1}]}],
  Avg = 3,

	expect_update_state(DNS_Gen_Server, []),

	cluster_manager:update_dns_state(OneDnsWorker, Load, Avg),

	assert_expectations_and_stop(DNS_Gen_Server).

%% Helper function for setting getLoadInfo mock expectation.
expect_load_info(Gen_Server, Load) ->
	gen_server_mock:expect_call(Gen_Server, fun(getLoadInfo, _From, State) ->
		{ok, {{0, 0, 1000}, Load}, State}
	end).

%% Helper function for setting getLoadInfo mock expectation.
expect_load_info(Gen_ServersAndTimes) when is_list(Gen_ServersAndTimes) ->
	lists:foreach(fun({Gen_Server, Load}) ->
		expect_load_info(Gen_Server, Load)
	end, Gen_ServersAndTimes);

expect_load_info(Gen_Server) ->
	expect_load_info(Gen_Server, 1000).

%% Helper function for setting update_state mock expectation with specified state.
expect_update_state(Gen_Servers, Expected_State)  when is_list(Gen_Servers) ->
	lists:foreach(fun(Gen_Server) ->
		expect_update_state(Gen_Server, Expected_State)
	end, Gen_Servers);

expect_update_state(Gen_Server, Expected_State) ->
	gen_server_mock:expect_cast(Gen_Server,	fun({asynch, 1, {update_state, Actual_State}}, _State) ->
		?assertEqual(Expected_State, Actual_State)
	end).

%% Helper function - shortcut for gen_server_mock:assert_expectations and gen_server_mock:stop.
assert_expectations_and_stop([]) ->
	ok;

assert_expectations_and_stop([Gen_Server | Gen_Servers]) ->
	try
		assert_expectations_and_stop(Gen_Server)
	after
		assert_expectations_and_stop(Gen_Servers)
	end;

assert_expectations_and_stop(Gen_Server) ->
	try
		gen_server_mock:assert_expectations(Gen_Server)
	after
		gen_server_mock:stop(Gen_Server)
	end.

-endif.