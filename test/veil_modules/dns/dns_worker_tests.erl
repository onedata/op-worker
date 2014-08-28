%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dns_worker module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dns_worker_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("veil_modules/dns/dns_worker.hrl").
-define(SAMPLE_PLUGIN, dns_internal_sample_plugin).
-define(MAX_RESPONSE_TIME, 500).
-endif.

-ifdef(TEST).


%% ====================================================================
%% Test generation
%% ====================================================================


dns_test_() ->
	[
		?_test(updatePluginState_works_with_current_worker_host_implementation()),
		?_test(update_state_works_with_current_worker_host_implementation()),
		?_test(get_worker_works_with_current_worker_host_implementation()),

		{inorder, [
					?_test(get_workers()),
          ?_test(get_workers_mamy_times()),
          ?_test(get_control_panel_mamy_times()),
          ?_test(get_nodes_mamy_times()),
					?_test(update_state())
				  ]
		}
	].

%% ====================================================================
%% Tests
%% ====================================================================

%% Checks if dns worker can handle get_workers message with different initial states
get_workers() ->
	OneModule = {[{sample_module, [{{127,0,0,1}, 1, 1}, {{192,168,0,1}, 1, 1}]}], [{{127,0,0,1}, 3}, {{192,168,0,1}, 3}], 3,
				   sample_module, [{192,168,0,1}, {127,0,0,1}]},

	ModuleWithNoWorkers = {[{sample_module, []}], [], 0,
								sample_module, []},

	NonExistingModule = {[{sample_module, [{{127,0,0,1}, 1, 1}]}], [{{127,0,0,1}, 3}], 3,
						   different_sample_module, []},

	ManyModules = {[{sample_module, [{{127,0,0,1}, 1, 1}]},
					{sample_module2, [{{192,168,0,1}, 1, 1}]}
				   ], [{{127,0,0,1}, 3}, {{192,168,0,1}, 3}], 3,
          sample_module, [{127,0,0,1}, any]},

	TestCases = [OneModule, ModuleWithNoWorkers, NonExistingModule, ManyModules],

	lists:foreach(fun ({InitialState, NLoads, AvgLoad, Module, ExpectedResults}) ->
			get_workers_with_initial_state_and_expected_response(#dns_worker_state{workers_list = InitialState, nodes_list = NLoads, avg_load = AvgLoad},
        [{Module, {ok, ExpectedResults}}])
		end, TestCases).

%% Checks if dns worker can handle get_workers message many times
get_workers_mamy_times() ->
  InitialState = [{sample_module, [{{127,0,0,1}, 3, 3}, {{192,168,0,1}, 1, 1}, {{150,100,0,1}, 2, 2}]}, {sample_module2, [{{127,0,0,1}, 1, 1}]}],
  NLoads = [{{127,0,0,1}, 4}, {{192,168,0,1}, 1}, {{150,100,0,1}, 2}],
  AvgLoad = 7/3,
  TestCases = [{sample_module, {ok, [{150,100,0,1}, {192,168,0,1}, {127,0,0,1}]}}, {sample_module, {ok, [{192,168,0,1}, any]}},
    {sample_module, {ok, [{150,100,0,1}, {192,168,0,1}]}}, {sample_module, {ok, [{127,0,0,1}, {192,168,0,1}]}},
    {sample_module, {ok, [{150,100,0,1}, {192,168,0,1}]}}, {sample_module, {ok, [{192,168,0,1}, any]}},
    {sample_module, {ok, [{150,100,0,1}, {192,168,0,1}, {127,0,0,1}]}}, {sample_module2, {ok, [{127,0,0,1}, any]}}],

  get_workers_with_initial_state_and_expected_response(#dns_worker_state{workers_list = InitialState, nodes_list = NLoads, avg_load = AvgLoad}, TestCases).

%% Checks if dns worker can handle get_workers message for control_panel many times
get_control_panel_mamy_times() ->
  InitialState = [{control_panel, [{{127,0,0,1}, 3, 3}, {{192,168,0,1}, 1, 1}, {{150,100,0,1}, 2, 2}]}, {sample_module2, [{{127,0,0,1}, 1, 1}]}],
  NLoads = [{{127,0,0,1}, 4}, {{192,168,0,1}, 1}, {{150,100,0,1}, 2}],
  AvgLoad = 7/3,
  TestCases = [{control_panel, {ok, [{150,100,0,1}, {192,168,0,1}, {127,0,0,1}]}}, {control_panel, {ok, [{192,168,0,1}]}},
    {control_panel, {ok, [{150,100,0,1}, {192,168,0,1}]}}, {control_panel, {ok, [{127,0,0,1}, {192,168,0,1}]}},
    {control_panel, {ok, [{150,100,0,1}, {192,168,0,1}]}}, {control_panel, {ok, [{192,168,0,1}]}},
    {control_panel, {ok, [{150,100,0,1}, {192,168,0,1}, {127,0,0,1}]}}, {sample_module2, {ok, [{127,0,0,1}, any]}}],

  get_workers_with_initial_state_and_expected_response(#dns_worker_state{workers_list = InitialState, nodes_list = NLoads, avg_load = AvgLoad}, TestCases).

%% Checks if dns worker can handle get_nodes message many times
get_nodes_mamy_times() ->
  InitialState = [{sample_module, [{{127,0,0,1}, 3, 3}, {{192,168,0,1}, 1, 1}, {{150,100,0,1}, 2, 2}]}, {sample_module2, [{{127,0,0,1}, 1, 1}]}],
  NLoads = [{{127,0,0,1}, 4}, {{192,168,0,1}, 1}, {{150,100,0,1}, 2}],
  AvgLoad = 7/3,

  {ok, Worker} = worker_host:start_link(dns_worker, #dns_worker_state{workers_list = InitialState, nodes_list = NLoads, avg_load = AvgLoad}, 1000),
  TestFun = fun(ExpectedResponse) ->
    gen_server:cast(Worker, {synch, 1, get_nodes, non, {proc, self()}}),
    receive_get_nodes_with_default_timeout(ExpectedResponse)
  end,
  try
    for(1, 10, fun() -> TestFun({ok, [{150,100,0,1}, {192,168,0,1}]}) end),

    NewState = [{sample_module, [{{127,0,0,1}, 3}, {{192,168,0,1}, 1}, {{150,100,0,1}, 2}]}, {sample_module2, [{{127,0,0,1}, 1}]}],
    SampleUpdateStateRequest = {update_state, NewState, [{{127,0,0,1}, 1}, {{192,168,0,1}, 1}, {{150,100,0,1}, 1}], 1},
    gen_server:cast(Worker, {synch, 1, SampleUpdateStateRequest, non, {proc, self()}}),
    receive_with_default_timeout(ok),

    for(1, 10, fun() -> TestFun({ok, [{127,0,0,1}, {150,100,0,1}, {192,168,0,1}]}) end),

    SampleUpdateStateRequest2 = {update_state, NewState, [{{127,0,0,1}, 1}, {{192,168,0,1}, 1}, {{150,100,0,1}, 1}], 0},
    gen_server:cast(Worker, {synch, 1, SampleUpdateStateRequest2, non, {proc, self()}}),
    receive_with_default_timeout(ok),

    for(1, 10, fun() -> TestFun({ok, [{127,0,0,1}, {150,100,0,1}, {192,168,0,1}]}) end)
  after
    kill_worker_host_with_timeout(Worker)
  end.

%% Checks if dns worker updates state
update_state() ->
	{ok, Worker} = worker_host:start_link(dns_worker, test, 1000),

	ExpectedState =  [{sample_plugin, [{{127,0,0,1}, 1, 1}]},
					  {sample_plugin2, [{{192,168,0,1}, 1, 1},
              {{192,168,0,2}, 2, 2}]}],

	SampleUpdateStateRequest = {update_state, [{sample_plugin, [{{127,0,0,1}, 1}]},
    {sample_plugin2, [{{192,168,0,1}, 1},
      {{192,168,0,2}, 2}]}], [{{127,0,0,1}, 1}], 1},

	try
		gen_server:cast(Worker, {synch, 1, SampleUpdateStateRequest, non, {proc, self()}}),

		receive_with_default_timeout(ok),

		PluginState = gen_server:call(Worker, getPlugInState),
		?assertEqual(ExpectedState, PluginState#dns_worker_state.workers_list)
	after
		kill_worker_host_with_timeout(Worker)
	end.


%% Checks if updatePluginState used by dns_worker supports current worker_host implementation
updatePluginState_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),
	Ref = erlang:monitor(process, self()),
	From = {self(), Ref},

	SampleDNSState = #dns_worker_state{workers_list = [{sample_plugin, [{127,0,0,1}, {127,0,0,2}]}]},
	try
		{reply, _Response, UpdatedPluginState} = worker_host:handle_call({updatePlugInState, SampleDNSState}, From, State),

		{reply, SampleDNSState, _NewWorkerState} = worker_host:handle_call(getPlugInState, From, UpdatedPluginState)
	after
		erlang:demonitor(Ref)
	end.


%% Checks if update_state request supported by dns_worker can work with current worker_host implementation
update_state_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),

	ok = meck:new(?SAMPLE_PLUGIN),

	try
		SampleUpdateStateRequest = {update_state, [{dns_worker, [{127,0,0,1}, {192,168,0,1}]}], [{{127,0,0,1}, 1}, {{192,168,0,1}, 1}], 1},

		meck:expect(?SAMPLE_PLUGIN, handle, fun(_PluginVersion, ActualRequest) ->
				?assertEqual(SampleUpdateStateRequest, ActualRequest)
			end),

		worker_host:handle_cast({asynch, 1, SampleUpdateStateRequest}, State),
		?assert(meck:validate(?SAMPLE_PLUGIN))
	after
		meck:unload(?SAMPLE_PLUGIN)
	end.


%% Checks if get_worker request supported by dns_worker can work with current worker host implementation
get_worker_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),

	meck:new(?SAMPLE_PLUGIN),

	try
		ExpectedGetWorkerRequest = {get_worker, dns_worker},
		ExpectedAnswer = {ok, [{127,0,0,1}]},
		meck:expect(?SAMPLE_PLUGIN, handle, fun(_PluginVersion, ActualRequest) ->
				?assertEqual(ExpectedGetWorkerRequest, ActualRequest),
				ExpectedAnswer
			end),

		worker_host:handle_cast({synch, 1, ExpectedGetWorkerRequest, non, {proc, self()}}, State),

		receive_with_default_timeout(ExpectedAnswer),

		?assert(meck:validate(?SAMPLE_PLUGIN))
	after
		meck:unload(?SAMPLE_PLUGIN)
	end.


%% ====================================================================
%% Helping functions
%% ====================================================================

%% Receive and check message
receive_with_default_timeout(ExpectedAnswer) ->
  case ExpectedAnswer of
    {ok, ExpectedIPS} ->
      receive
        {ok, IPS} ->
          ?assertEqual(length(ExpectedIPS), length(IPS)),
          lists:foreach(fun(ExpectedIP) ->
            case ExpectedIP of
              any ->
                true;
              _ ->
                ?assert(lists:member(ExpectedIP, IPS))
            end
          end, ExpectedIPS)
      after
        ?MAX_RESPONSE_TIME -> error(timeout)
      end;
    _ ->
      receive
        ExpectedAnswer -> true
      after
        ?MAX_RESPONSE_TIME -> error(timeout)
      end
  end.

%% Receive and check get_nodes message
receive_get_nodes_with_default_timeout(ExpectedAnswer) ->
  case ExpectedAnswer of
    {ok, ExpectedIPS} ->
      receive
        {ok, IPS} ->
          ?assert(length(ExpectedIPS) =< length(IPS)),
          lists:foreach(fun(ExpectedIP) ->
            ?assert(lists:member(ExpectedIP, IPS))
          end, ExpectedIPS)
      after
        ?MAX_RESPONSE_TIME -> error(timeout)
      end;
    _ ->
      receive
        ExpectedAnswer -> true
      after
        ?MAX_RESPONSE_TIME -> error(timeout)
      end
  end.

%% Helping function for checking get_worker request with specified conditions
get_workers_with_initial_state_and_expected_response(InitialState, TestCases) ->
	{ok, Worker} = worker_host:start_link(dns_worker, InitialState, 1000),

  try
    lists:foreach(fun({Module, ExpectedResponse}) ->
        gen_server:cast(Worker, {synch, 1, {get_worker, Module}, non, {proc, self()}}),

        receive_with_default_timeout(ExpectedResponse)
    end, TestCases)
  after
    kill_worker_host_with_timeout(Worker)
  end.

%% Helping function for killing worker host
kill_worker_host_with_timeout(Worker) ->
	exit(Worker, normal),
	receive_with_default_timeout({'EXIT', Worker, normal}).

%% for loop
for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

-endif.
