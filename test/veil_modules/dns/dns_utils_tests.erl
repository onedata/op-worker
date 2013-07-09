%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dns_utils module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dns_utils_tests).

-ifdef(TEST).
-include("registered_names.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/src/inet_dns.hrl").
-define(DISPATCHER_TIMEOUT, 500).
-endif.


-ifdef(TEST).


%% ====================================================================
%% Test generation
%% ====================================================================


dns_utils_udp_test_() ->
	[
		?_test(generate_answer__request_with_one_query_and_nearly_truncated_answer(udp)),
		?_test(generate_answer__request_with_one_query_and_truncated_answer(udp))
	].

dns_utils_tcp_test_() ->
	[
		?_test(generate_answer__request_with_one_query_and_nearly_truncated_answer(tcp)),
		?_test(generate_answer__request_with_one_query_and_truncated_answer(tcp))
	].

dns_utils_protocol_agnostic_test_() ->
	[
		?_test(generate_answer__wrong_worker(udp)),
		?_test(generate_answer__unsupported_type(udp)),
		?_test(generate_answer__unsupported_class(udp)),
		?_test(generate_answer__request_with_dispatcher_timeout(udp)),
		?_test(generate_answer__request_with_one_query_and_empty_answer(udp)),
		?_test(generate_answer__request_with_no_query_and_no_answer(udp)),
		?_test(generate_answer__request_with_one_query_and_one_answer(udp)),
		?_test(generate_answer__request_with_one_query_and_multiple_answers(udp)),
		?_test(generate_answer__request_with_multiple_query_and_one_answer(udp)),
		?_test(generate_answer__request_with_multiple_query_and_multiple_answer_number_of_answers_not_match(udp)),
		?_test(generate_answer__request_with_multiple_query_and_multiple_answer_number_of_answers_not_match_wrong_module(udp)),
		?_test(generate_answer__request_with_multiple_query_and_multiple_answer(udp)),
		?_test(generate_answer__request_domain_ending_with_dot(udp))
	].


%% ====================================================================
%% Tests
%% ====================================================================

%% Checks if generate_answer can handle wrong worker name
generate_answer__wrong_worker(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), worker_not_found),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, 60, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response),
		assert_queries(Response, 1),
		assert_not_truncated(Response, 0),
		assert_rcode(Response, ?SERVFAIL),
		assert_no_answers(Response)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle unsupported query type
generate_answer__unsupported_type(Protocol) ->
	generate_answer__with_query_params(Protocol, ?T_AAAA, ?C_IN).

%% Checks if generate_answer can handle unsupported query class
generate_answer__unsupported_class(Protocol) ->
	generate_answer__with_query_params(Protocol, ?T_A, ?C_CHAOS).

%% Checks if generate_answer can handle lack of the answer from dns_worker
generate_answer__request_with_dispatcher_timeout(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	Dispatcher_Timeout = 0,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		gen_server_mock:expect_call(Gen_Server, fun ({dns_worker, 1, _Pid, {get_worker, control_panel}}, _From, State) ->
				{ok, ok, State}     %% we are not sending response to Pid
			end),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, Dispatcher_Timeout, 60, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response),
		assert_not_truncated(Response, 0),
		assert_no_answers(Response),
		assert_queries(Response, 1),
		assert_rcode(Response, ?SERVFAIL)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle one dns query for module which is not currently working
generate_answer__request_with_one_query_and_empty_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, []}),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, 60, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response),
		assert_not_truncated(Response, 0),
		assert_no_answers(Response),
		assert_queries(Response, 1),
		assert_rcode(Response, ?NOERROR)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle one dns query for module working on only one node
generate_answer__request_with_one_query_and_one_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, [{127,0,0,1}]}),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, [{127,0,0,1}]),
		assert_queries(Response, 1),
		assert_rcode(Response, ?NOERROR)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle dns query for module working on multiple nodes
generate_answer__request_with_one_query_and_multiple_answers(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, [{127,0,0,1}, {192,168,0,1}, {172,16,0,1}]}),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, [{127,0,0,1}, {192,168,0,1}, {172,16,0,1}]),
		assert_queries(Response, 1),
		assert_rcode(Response, ?NOERROR)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle empty response
generate_answer__request_with_no_query_and_no_answer(Protocol) ->
	Message = create_request(create_standard_header(), []),
	BinMessage = inet_dns:encode(Message),

	{ok, BinResponse} = dns_utils:generate_answer(BinMessage, dummy_gen_server, 50, dummy_ttl, Protocol),
	{ok, Response} = inet_dns:decode(BinResponse),

	assert_standard_response(Response),
	assert_no_answers(Response),
	assert_queries(Response, 0),
	assert_rcode(Response, ?NOERROR).

%% Checks if generate_answer can handle udp protocol specific response truncation rules
generate_answer__request_with_one_query_and_nearly_truncated_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, lists:duplicate(29, {127,0,0,1})}),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, lists:duplicate(29, {127,0,0,1})),
		assert_rcode(Response, ?NOERROR),
		assert_queries(Response, 1)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle udp protocol specific response truncation rules
generate_answer__request_with_one_query_and_truncated_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, lists:duplicate(30, {127,0,0,1})}),

		Message = create_supported_request(Domain),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		case Protocol of
			udp -> assert_truncated_with_ips(Response, lists:duplicate(29, {127,0,0,1}));
			tcp -> assert_not_truncated_with_ips(Response, lists:duplicate(30, {127,0,0,1}))
		end,
		assert_rcode(Response, ?NOERROR),
		assert_queries(Response, 1)

	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle multiple queries when not every query has answer - the reason is wrong domain name
generate_answer__request_with_multiple_query_and_one_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, [{127,0,0,1}]}),

		WrongQuery = create_supported_query("uknown_module" ++ Domain),
		Message = create_request(create_standard_header(), [create_supported_query(Domain), WrongQuery]),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, lists:duplicate(1, {127,0,0,1})),
		assert_rcode(Response, ?NXDOMAIN),
		assert_queries(Response, 2)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle multiple queries when not every query has answer - the reason is not currently working module
generate_answer__request_with_multiple_query_and_multiple_answer_number_of_answers_not_match(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, lists:duplicate(1, {127,0,0,1})}, 2),
		expect_get_workers(Gen_Server, control_panel, self(), {ok, []}, 1),

		Message = create_supported_request(Domain, 3),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, lists:duplicate(2, {127,0,0,1})),
		assert_rcode(Response, ?NOERROR),
		assert_queries(Response, 3)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle multiple queries when dispatcher returns different responses for the same queries
generate_answer__request_with_multiple_query_and_multiple_answer_number_of_answers_not_match_wrong_module(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, lists:duplicate(1, {127,0,0,1})}, 2),
		expect_get_workers(Gen_Server, control_panel, self(), worker_not_found, 1),

		Message = create_supported_request(Domain, 3),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, lists:duplicate(2, {127,0,0,1})),
		assert_rcode(Response, ?SERVFAIL),
		assert_queries(Response, 3)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle multiple queries when number of queries match number of answers
generate_answer__request_with_multiple_query_and_multiple_answer(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, lists:duplicate(1, {127,0,0,1})}, 3),

		Message = create_supported_request(Domain, 3),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, lists:duplicate(3, {127,0,0,1})),
		assert_rcode(Response, ?NOERROR),
		assert_queries(Response, 3)
	after
		assert_expectations_and_stop(Gen_Server)
	end.

%% Checks if generate_answer can handle domain name ending with a dot
generate_answer__request_domain_ending_with_dot(Protocol) ->
	Domain = "control_panel.xxxx.xxx.xxx",
	ResponseTTL = 60,

	{ok, Gen_Server} = gen_server_mock:new(),

	try
		expect_get_workers(Gen_Server, control_panel, self(), {ok, [{127,0,0,1}]}),

		Message = create_supported_request(Domain ++ "."),
		BinMessage = inet_dns:encode(Message),

		{ok, BinResponse} = dns_utils:generate_answer(BinMessage, Gen_Server, ?DISPATCHER_TIMEOUT, ResponseTTL, Protocol),
		{ok, Response} = inet_dns:decode(BinResponse),

		assert_standard_response(Response, Domain, ResponseTTL),
		assert_not_truncated_with_ips(Response, [{127,0,0,1}]),
		assert_rcode(Response, ?NOERROR),
		assert_queries(Response, 1)
	after
		assert_expectations_and_stop(Gen_Server)
	end.


%% ====================================================================
%% Helping functions
%% ====================================================================

%% Helper function for testing unsupported types and classes
generate_answer__with_query_params(Protocol, Type, Class) ->
	Domain = "control_panel.xxxx.xxx.xxx",

	Message = create_request(create_standard_header(), #dns_query{domain=Domain, type=Type, class=Class}),
	BinMessage = inet_dns:encode(Message),

	{ok, BinResponse} = dns_utils:generate_answer(BinMessage, dummy_gen_server, ?DISPATCHER_TIMEOUT, 60, Protocol),
	{ok, Response} = inet_dns:decode(BinResponse),

	assert_standard_response(Response),
	assert_queries(Response, 1),
	assert_not_truncated(Response, 0),
	assert_rcode(Response, ?NOTIMP),
	assert_no_answers(Response).

%% Helper function for creating header with some default values
create_standard_header() ->
	#dns_header{id = 1, qr = false, opcode = ?QUERY, rd = 1}.

%% Helper function for creating supported query type
create_supported_query(Domain, Count) ->
	lists:duplicate(Count, create_supported_query(Domain)).

%% Helper function for creating supported query type
create_supported_query(Domain) ->
	#dns_query{domain=Domain, type=?T_A,class=?C_IN}.

%% Helper function for creating dns records
create_request(Header, Queries) when is_list(Queries)->
	#dns_rec{header=Header, qdlist=Queries, anlist=[], nslist=[], arlist=[]};

create_request(Header, Query) ->
	create_request(Header, [Query]).

%% Helper function for creating request with supported query type
create_supported_request(Domain, Count) ->
	Queries = create_supported_query(Domain, Count),
	Header = create_standard_header(),
	create_request(Header, Queries).

%% Helper function for creating request with supported query types
create_supported_request(Domain) ->
	Query = create_supported_query(Domain),
	Header = create_standard_header(),
	create_request(Header, Query).

%% Helper function for making standard assertions
assert_standard_response(Response) ->
	assert_type_response(Response),
	assert_id(Response, 1),
	assert_recursion_not_supported(Response).

%% Helper function for making standard assertions
assert_standard_response(Response, Domain, ResponseTTL) ->
	assert_type_response(Response),
	assert_id(Response, 1),
	assert_recursion_not_supported(Response),
	case Response#dns_rec.anlist of
		[] -> assert_authority(Response, false);
		_  -> assert_authority(Response, true),
			  assert_answer_domain(Response, Domain),
			  assert_ttl(Response, ResponseTTL)
	end.

%% Helper function for asserting ip addresses of answers
assert_queries(Response, Count) ->
	?assertMatch(Count, length(Response#dns_rec.qdlist)).

%% Helper function for asserting ip addresses of answers
assert_ips(Response, Addresses) ->
	AnswersIPs = [An#dns_rr.data || An <- Response#dns_rec.anlist],
	?assertMatch(Addresses, AnswersIPs).

%% Helper function for asserting error code
assert_rcode(Response, Rc) ->
	?assertMatch(Rc, Response#dns_rec.header#dns_header.rcode).

%% Helper function for asserting that every answer contains the same domain
assert_answer_domain(Response, Domain) ->
	lists:foreach(fun (ResponseEntry) ->
			?assertMatch(Domain, ResponseEntry#dns_rr.domain)
		end, Response#dns_rec.anlist).

%% Helper function for asserting lack of answers
assert_no_answers(Response) ->
	?assertMatch([], Response#dns_rec.anlist).

%% Helper function for asserting dns message type -- response
assert_type_response(Response) ->
	?assertMatch(true, Response#dns_rec.header#dns_header.qr).

%% Helper function for asserting message id
assert_id(Response, ID) ->
	?assertMatch(ID, Response#dns_rec.header#dns_header.id).

assert_recursion_not_supported(Response) ->
	?assertMatch(false, Response#dns_rec.header#dns_header.ra).

%% Helper function for asserting authority of answer
assert_authority(Response, Value) ->
	?assertMatch(Value, Response#dns_rec.header#dns_header.aa).

%% Helper function for asserting truncated responses
assert_truncated(Response, NumberOfExpectedResponses) ->
	?assertMatch(true, Response#dns_rec.header#dns_header.tc),
	?assertMatch(NumberOfExpectedResponses, length(Response#dns_rec.anlist)).

%% Helper function for asserting not truncated responses
assert_not_truncated(Response, NumberOfExpectedResponses) ->
	?assertMatch(false, Response#dns_rec.header#dns_header.tc),
	?assertMatch(NumberOfExpectedResponses, length(Response#dns_rec.anlist)).

%% Helper function for asserting tc flag and response ips
assert_truncated_with_ips(Response, IPs) ->
	assert_truncated(Response, length(IPs)),
	assert_ips(Response, IPs).

%% Helper function for asserting tc flag and response ips
assert_not_truncated_with_ips(Response, IPs) ->
	assert_not_truncated(Response, length(IPs)),
	assert_ips(Response, IPs).

%% Helper function for asserting ttl in response
assert_ttl(Response, TTL) ->
	lists:foreach(fun(ResponseEntry) ->
			?assertMatch(TTL, ResponseEntry#dns_rr.ttl)
		end, Response#dns_rec.anlist).

%% Helper function for setting get_worker expectation
expect_get_workers(Gen_Server, ExpectedModule, ExpectedResponse_To, Return) ->
	gen_server_mock:expect_call(Gen_Server, fun ({dns_worker, 1, Response_To, {get_worker, Module}}, _From, State) ->
			?assertMatch(ExpectedResponse_To, Response_To),
			?assertMatch(ExpectedModule, Module),
			Ret = case Return of
				worker_not_found -> worker_not_found;
				Other -> ExpectedResponse_To ! Other, ok
			end,
			{ok, Ret, State}
		end).

%% Helper function for setting get_worker expectation
expect_get_workers(Gen_Server, ExpectedModule, ExpectedResponse_To, Return, Times) ->
	lists:foreach(fun (_Index) ->
			expect_get_workers(Gen_Server, ExpectedModule, ExpectedResponse_To, Return)
		end, lists:seq(1, Times)).

%% Helper function for checking expectations
assert_expectations(Gen_Servers) when is_list(Gen_Servers)->
	lists:foreach(fun(Gen_Server) ->
			assert_expectations(Gen_Server)
		end, Gen_Servers);

assert_expectations(Gen_Server) ->
	gen_server_mock:assert_expectations(Gen_Server).


%% Helper function for checking expectations and stopping gen_server
assert_expectations_and_stop(Gen_Servers) when is_list(Gen_Servers) ->
	try
		assert_expectations(Gen_Servers)
	after
		safe_stop(Gen_Servers)
	end;

assert_expectations_and_stop(Gen_Server) ->
	try
		gen_server_mock:assert_expectations(Gen_Server)
	after
		safe_stop(Gen_Server)
	end.


%% Helper function for stopping Gen_server
safe_stop([]) ->
	ok;

safe_stop([Gen_Server | Gen_Servers]) ->
	try
		safe_stop(Gen_Server)
	after
		safe_stop(Gen_Servers)
	end;

safe_stop(Gen_Server) ->
	gen_server_mock:stop(Gen_Server).


-endif.