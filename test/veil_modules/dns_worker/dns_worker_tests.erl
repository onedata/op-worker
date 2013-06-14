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
-include("records.hrl").
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
					?_test(update_state())
				  ]
		}
	].

%% ====================================================================
%% Tests
%% ====================================================================

%% Checks if dns worker can handle get_workers message with different initial states
get_workers() ->
	OneModule = {[{sample_module, [{127,0,0,1}, {192,168,0,1}]}],
				   sample_module, [{127,0,0,1}, {192,168,0,1}]},

	ModuleWithNoWorkers = {[{sample_module, []}],
								sample_module, []},

	NonExistingModule = {[{sample_module, [{127,0,0,1}]}],
						   different_sample_module, []},

	ManyModules = {[{sample_module, [{127,0,0,1}]},
					{sample_module2, [{192,168,0,1}]}
				   ], sample_module, [{127,0,0,1}]},

	TestCases = [OneModule, ModuleWithNoWorkers, NonExistingModule, ManyModules],

	lists:foreach(fun ({InitialState, Module, ExpectedResults}) ->
			get_workers_with_initial_state_and_expected_response(#dns_worker_state{workers_list = InitialState}, Module, {ok, ExpectedResults})
		end, TestCases).


%% Checks if dns worker updates state
update_state() ->
	{ok, Worker} = worker_host:start_link(dns_worker, [], 1000),

	ExpectedState =  [{sample_plugin, [{127,0,0,1}]},
					  {sample_plugin2, [{192,168,0,1},
						                {192,168,0,2}]}],

	SampleUpdateStateRequest = {update_state, ExpectedState},

	gen_server:cast(Worker, {synch, 1, SampleUpdateStateRequest, test, {proc, self()}}),

	receive_with_default_timeout(ok),

	PluginState = gen_server:call(Worker, getPlugInState),
	?assertEqual(ExpectedState, PluginState#dns_worker_state.workers_list),

	kill_worker_host_with_timeout(Worker).


%% Checks if updatePluginState used by dns worker can work with current worker host implementation
updatePluginState_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),
	Ref = erlang:monitor(process, self()),
	From = {self(), Ref},

	SampleDNSState = #dns_worker_state{workers_list = [{sample_plugin, [{127,0,0,1}, {127,0,0,2}]}]},
	{reply, _Response, UpdatedPluginState} = worker_host:handle_call({updatePlugInState, SampleDNSState}, From, State),

	{reply, SampleDNSState, _NewWorkerState} = worker_host:handle_call(getPlugInState, From, UpdatedPluginState),

	erlang:demonitor(Ref).


%% Checks if update_state request supported by dns worker can work with current worker host implementation
update_state_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),

	ok = meck:new(?SAMPLE_PLUGIN),

	try
		SampleUpdateStateRequest = {update_state, [{dns_worker, [{127,0,0,1}, {192,168,0,1}]}]},

		meck:expect(?SAMPLE_PLUGIN, handle, fun(_PluginVersion, ActualRequest) ->
				?assertEqual(SampleUpdateStateRequest, ActualRequest)
			end),

		worker_host:handle_cast({asynch, 1, SampleUpdateStateRequest}, State),
		?assert(meck:validate(?SAMPLE_PLUGIN))
	after
		meck:unload(?SAMPLE_PLUGIN)
	end.


%% Checks if get_worker request supported by dns worker can work with current worker host implementation
get_worker_works_with_current_worker_host_implementation() ->
	{ok, State} = worker_host:init([?SAMPLE_PLUGIN, ok, 100]),

	meck:new(?SAMPLE_PLUGIN),

	try
		SampleGetWorkerRequest = {get_worker, dns_worker},
		ExpectedAnswer = {ok, [{127,0,0,1}]},
		meck:expect(?SAMPLE_PLUGIN, handle, fun(_PluginVersion, ActualRequest) ->
				?assertEqual(SampleGetWorkerRequest, ActualRequest),
				ExpectedAnswer
			end),

		worker_host:handle_cast({synch, 1, SampleGetWorkerRequest, test, {proc, self()}}, State),

		receive_with_default_timeout(ExpectedAnswer),

		?assert(meck:validate(?SAMPLE_PLUGIN))
	after
		meck:unload(?SAMPLE_PLUGIN)
	end.


%% ====================================================================
%% Helping functions
%% ====================================================================

%% Shortcut for receive ... after construction
receive_with_default_timeout(ExpectedAnswer) ->
	receive
		ExpectedAnswer -> true
	after
		?MAX_RESPONSE_TIME -> error(timeout)
	end.

%% Helping function for checking get_worker request with specified conditions
get_workers_with_initial_state_and_expected_response(InitialState, Module, ExpectedResponse) ->
	{ok, Worker} = worker_host:start_link(dns_worker, InitialState, 1000),

	gen_server:cast(Worker, {synch, 1, {get_worker, Module}, test, {proc, self()}}),

	receive_with_default_timeout(ExpectedResponse),
	kill_worker_host_with_timeout(Worker).


%% Helping function for killing worker host
kill_worker_host_with_timeout(Worker) ->
	exit(Worker, normal),
	receive_with_default_timeout({'EXIT', Worker, normal}).

-endif.