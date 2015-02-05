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

-define(NODE_1, 'worker1@host.com').
-define(NODE_2, 'worker2@host.com').
-define(NODE_3, 'worker3@host.com').
-define(NODE_4, 'worker4@host.com').

-define(WORKER_1, dns_worker).
-define(WORKER_2, http_worker).
-define(WORKER_3, other_worker).
-define(WORKER_4, another_worker).

% ClusterStatus is in form:
% {oneprovider_node, Status1, [
%     {Node1, Status2, [
%         {node_manager, Status3},
%         {request_dispatcher, Status4},
%         {Worker1, Status5},
%         {Worker2, Status6},
%         {Worker3, Status7},
%     ]},
%     {Node2, Status8, [
%         ...
%     ]}
% ]}
% Status can be: ok | error | out_of_sync

calculate_cluster_Status_test() ->
    meck:new(nagios_handler, [passthrough]),
    Nodes = [?NODE_1, ?NODE_2, ?NODE_3, ?NODE_4],
    StatusNum = 5,
    NodeManagerStatuses = [
        {?NODE_1, {ok, 4}},
        {?NODE_2, error},
        {?NODE_3, {ok, 5}},
        {?NODE_4, {ok, 5}}
    ],
    DistpatcherStatuses = [
        {?NODE_1, {ok, 4}},
        {?NODE_2, {ok, 5}},
        {?NODE_3, {ok, 4}},
        {?NODE_4, {ok, 5}}
    ],
    WorkerStatuses = [
        {?NODE_1, [{?WORKER_1, error}, {?WORKER_2, ok}]},
        {?NODE_2, [{?WORKER_3, ok}, {?WORKER_4, ok}]},
        {?NODE_3, [{?WORKER_2, ok}, {?WORKER_3, ok}]},
        {?NODE_4, [{?WORKER_1, ok}, {?WORKER_3, ok}]}
    ],

    {ok, ClusterStatus} = nagios_handler:calculate_cluster_status(Nodes, StatusNum, NodeManagerStatuses, DistpatcherStatuses, WorkerStatuses),
    ?assertMatch({?APP_NAME, error, _}, ClusterStatus),
    {?APP_NAME, error, NodeStatuses} = ClusterStatus,
    ?assertMatch({?NODE_1, _, _}, lists:keyfind(?NODE_1, 1, NodeStatuses)),
    ?assertMatch({?NODE_2, _, _}, lists:keyfind(?NODE_2, 1, NodeStatuses)),
    ?assertMatch({?NODE_3, _, _}, lists:keyfind(?NODE_3, 1, NodeStatuses)),
    ?assertMatch({?NODE_4, _, _}, lists:keyfind(?NODE_4, 1, NodeStatuses)),
    {?NODE_1, error, Node1Status} = lists:keyfind(?NODE_1, 1, NodeStatuses),
    {?NODE_2, error, Node2Status} = lists:keyfind(?NODE_2, 1, NodeStatuses),
    {?NODE_3, out_of_sync, Node3Status} = lists:keyfind(?NODE_3, 1, NodeStatuses),
    {?NODE_4, ok, Node4Status} = lists:keyfind(?NODE_4, 1, NodeStatuses),

    ?assertEqual(out_of_sync, proplists:get_value(?NODE_MANAGER_NAME, Node1Status)),
    ?assertEqual(out_of_sync, proplists:get_value(?DISPATCHER_NAME, Node1Status)),
    ?assertEqual(error, proplists:get_value(?WORKER_1, Node1Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_2, Node1Status)),

    ?assertEqual(error, proplists:get_value(?NODE_MANAGER_NAME, Node2Status)),
    ?assertEqual(ok, proplists:get_value(?DISPATCHER_NAME, Node2Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_3, Node2Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_4, Node2Status)),

    ?assertEqual(ok, proplists:get_value(?NODE_MANAGER_NAME, Node3Status)),
    ?assertEqual(out_of_sync, proplists:get_value(?DISPATCHER_NAME, Node3Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_2, Node3Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_3, Node3Status)),

    ?assertEqual(ok, proplists:get_value(?NODE_MANAGER_NAME, Node4Status)),
    ?assertEqual(ok, proplists:get_value(?DISPATCHER_NAME, Node4Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_1, Node4Status)),
    ?assertEqual(ok, proplists:get_value(?WORKER_3, Node4Status)),

    ?assert(meck:validate(nagios_handler)),
    ok = meck:unload(nagios_handler).

-endif.