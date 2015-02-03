%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. lut 2015 16:32
%%%-------------------------------------------------------------------
-module(nagios_handler_tests).
-author("lopiola").

-ifdef(TEST).
-include("registered_names.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(dump(A), io:format(user, "[DUMP]: ~p~n~n", [A])).

-define(NODE_1, 'worker1@host.com').
-define(NODE_2, 'worker2@host.com').

-define(WORKER_1, dns_worker).
-define(WORKER_2, http_worker).
-define(WORKER_3, other_worker).
-define(WORKER_4, another_worker).

-define(TIMEOUT_FOR_THE_TEST, 500).
-define(TOO_LONG_TIME, 1000).

% ClusterState is in form:
% [
%     {Node1, [
%         {node_manager, State1},
%         {request_dispatcher, State2},
%         {Worker1, State3},
%         {Worker2, State4},
%         {Worker3, State5},
%     ]},
%     {Node2, [
%         ...
%     ]}
% ]
% State can be: ok | error | out_of_sync

get_cluster_state_test() ->
    CCM = mock_healthcheck(?CCM, 0, {
        [?NODE_1, ?NODE_2],
        [
            {?NODE_1, ?WORKER_1},
            {?NODE_1, ?WORKER_2},
            {?NODE_2, ?WORKER_3},
            {?NODE_2, ?WORKER_4}
        ], 5}),
    NodeManager1 = mock_healthcheck({?NODE_MANAGER_NAME, ?NODE_1}, 0, {ok, 5}),
    Dispatcher1 = mock_healthcheck({?DISPATCHER_NAME, ?NODE_1}, 0, {ok, 4}),
    NodeManager2 = mock_healthcheck({?NODE_MANAGER_NAME, ?NODE_2}, ?TOO_LONG_TIME, {ok, 5}),
    Dispatcher2 = mock_healthcheck({?DISPATCHER_NAME, ?NODE_2}, 0, {ok, 5}),
    Worker1 = mock_healthcheck(?WORKER_1, ?TOO_LONG_TIME, ok),
    Worker2 = mock_healthcheck(?WORKER_2, 0, ok),
    Worker4 = mock_healthcheck(?WORKER_3, 0, ok),
    Worker3 = mock_healthcheck(?WORKER_4, ?TOO_LONG_TIME, ok),

    ClusterState = nagios_handler:get_cluster_state(?TIMEOUT_FOR_THE_TEST),
    ?assertMatch({?NODE_1, _}, lists:keyfind(?NODE_1, 1, ClusterState)),
    ?assertMatch({?NODE_2, _}, lists:keyfind(?NODE_2, 1, ClusterState)),
    Node1State = proplists:get_value(?NODE_1, ClusterState),
    Node2State = proplists:get_value(?NODE_2, ClusterState),

    ?assertEqual(ok, proplists:get_value(?NODE_MANAGER_NAME, Node1State)),
    ?assertEqual(out_of_sync, proplists:get_value(?DISPATCHER_NAME, Node1State)),
    ?assertEqual(error, proplists:get_value(?WORKER_1, Node1State)),
    ?assertEqual(ok, proplists:get_value(?WORKER_2, Node1State)),

    ?assertEqual(error, proplists:get_value(?NODE_MANAGER_NAME, Node2State)),
    ?assertEqual(ok, proplists:get_value(?DISPATCHER_NAME, Node2State)),
    ?assertEqual(ok, proplists:get_value(?WORKER_3, Node2State)),
    ?assertEqual(error, proplists:get_value(?WORKER_4, Node2State)),

    assert_expectations_and_stop([CCM, NodeManager1, NodeManager2, Dispatcher1, Dispatcher2, Worker1, Worker2, Worker3, Worker4]).


mock_healthcheck(GenServerName, AnswerDelay, MockedResponse) ->
    {ok, GenServer} = gen_server_mock:new(),
    erlang:register(GenServerName, GenServer),
    gen_server_mock:expect_call(GenServer,
        fun(healthcheck, _From, State) ->
            timer:sleep(AnswerDelay),
            {ok, MockedResponse, State}
        end),
    GenServer.


assert_expectations_and_stop([]) ->
    ok;
assert_expectations_and_stop([GenServer | T]) ->
    try
        gen_server_mock:assert_expectations(GenServer)
    after
        gen_server_mock:stop(GenServer)
    end,
    assert_expectations_and_stop(T).

-endif.