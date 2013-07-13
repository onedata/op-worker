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
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/src/inet_dns.hrl").

-export([all/0]).
-export([dns_udp_sup_env_test/1, dns_udp_handler_responds_to_dns_queries/1, dns_ranch_tcp_handler_responds_to_dns_queries/1]).

-include("env_setter.hrl").
-include("registered_names.hrl").

all() -> [dns_udp_sup_env_test, dns_ranch_tcp_handler_responds_to_dns_queries, dns_udp_handler_responds_to_dns_queries].


%% ====================================================================
%% Tests
%% ====================================================================

%% Checks if all necessary variables are declared in application
dns_udp_sup_env_test(_Config) ->
	try
		?INIT_DIST_TEST,
		env_setter:start_test(),
		assert_all_deps_are_met(?APP_Name, dns_worker)
	after
		env_setter:stop_test()
	end.

%% Checks if dns_udp_handler responds before and after running dns_worker
dns_udp_handler_responds_to_dns_queries(_Config) ->
	ct:timetrap({seconds, 30}),
	?INIT_DIST_TEST,
	Port = 6667,
	DNS_Port = 1328,
	UDP_Port = 1400,
	Address = "localhost",
	SupportedDomain = "dns_worker",
	Env = [{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [node()]},
			{dns_port, DNS_Port}, {initialization_time, 1}],

	{ok, Socket} = gen_udp:open(UDP_Port, [{active, false}, binary]),

	RequestHeader = #dns_header{id = 1, qr = false, opcode = ?QUERY, rd = 1},
	RequestQuery = #dns_query{domain=SupportedDomain, type=?T_A,class=?C_IN},
	Request = #dns_rec{header=RequestHeader, qdlist=[RequestQuery], anlist=[], nslist=[], arlist=[]},

	try
		BinRequest = inet_dns:encode(Request),

		env_setter:start_test(),
		env_setter:start_app(Env),

		gen_server:cast(?Node_Manager_Name, do_heart_beat),
		gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    timer:sleep(100),
		gen_server:cast({global, ?CCM}, init_cluster),
		timer:sleep(3000),

		%Both - dns udp handler and dns_worker should work
		MessagingTest = fun () ->
			ok = gen_udp:send(Socket, Address, DNS_Port, BinRequest),
			{ok, {_, DNS_Port, Packet}} = gen_udp:recv(Socket, 0, infinity),
			{ok, Response} = inet_dns:decode(Packet),
			Header = Response#dns_rec.header,
			?NOERROR = Header#dns_header.rcode,
			1 == length(Response#dns_rec.anlist)
		end,

		MessagingTest()
	after
		try
			gen_udp:close(Socket)
		after
			try
				env_setter:stop_app()
			after
				env_setter:stop_test()
			end

		end
	end.

%% Checks if dns_ranch_tcp_handler does not close connection after first response
dns_ranch_tcp_handler_responds_to_dns_queries(_Config) ->
	ct:timetrap({seconds, 30}),
	?INIT_DIST_TEST,
	Port = 6667,
	DNS_Port = 1329,
	Address = "localhost",
	SupportedDomain = "dns_worker",
	Env = [{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [node()]},
		{dns_port, DNS_Port}, {initialization_time, 1}],

	RequestHeader = #dns_header{id = 1, qr = false, opcode = ?QUERY, rd = 1},
	RequestQuery = #dns_query{domain=SupportedDomain, type=?T_A,class=?C_IN},
	Request = #dns_rec{header=RequestHeader, qdlist=[RequestQuery], anlist=[], nslist=[], arlist=[]},

	try
		BinRequest = inet_dns:encode(Request),

		env_setter:start_test(),
		env_setter:start_app(Env),
		timer:sleep(500),

		gen_server:cast(?Node_Manager_Name, do_heart_beat),
		gen_server:cast({global, ?CCM}, {set_monitoring, on}),
        timer:sleep(100),
		gen_server:cast({global, ?CCM}, init_cluster),
		timer:sleep(3000),

		{ok, Socket} = gen_tcp:connect(Address, DNS_Port, [{active, false}, binary, {packet, 2}]),
		try
			SendMessageAndAssertResults = fun () ->
				ok = gen_tcp:send(Socket, BinRequest),
				{ok, Packet} = gen_tcp:recv(Socket, 0, infinity),
				{ok, Response} = inet_dns:decode(Packet),
				Header = Response#dns_rec.header,
				?NOERROR = Header#dns_header.rcode
			end,

			SendMessageAndAssertResults(),

			%% connection should still be valid
			SendMessageAndAssertResults()

		after
			gen_tcp:close(Socket)
		end
	after
		try
			env_setter:stop_app()
		after
			env_setter:stop_test()
		end
	end.

%% ====================================================================
%% Helping functions
%% ====================================================================

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
	VarType = type_of(Value);

assert_all_deps_are_met(Application, Module) when is_atom(Module) ->

	Dependencies = Module:env_dependencies(),
	assert_all_deps_are_met(Application, Dependencies).
