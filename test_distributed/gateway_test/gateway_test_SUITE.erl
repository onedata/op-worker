%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Tests for gateway oneprovider_module.
%% ===================================================================

-module(gateway_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("gwproto_pb.hrl").

%% the actual application uses GRPCA-generated cert
-define(CERT, "./certs/onedataServerFuse.pem").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([send_request_test/1, notify_all_interested_parties_test/1]).

all() -> [send_request_test, notify_all_interested_parties_test].


%% ====================================================================
%% Test functions
%% ====================================================================


send_request_test(Config) ->
    [Node | _] = ?config(nodes, Config),

    Self = self(),
    spawn_link(fun() ->
        {ok, ListenSocket} = ssl:listen(9999, [{certfile, ?CERT}, {reuseaddr, true},
            {mode, binary}, {active, false}, {packet, 4}]),
        {ok, Socket} = ssl:transport_accept(ListenSocket),
        ok = ssl:ssl_accept(Socket),
        {ok, Data} = ssl:recv(Socket, 0, timer:seconds(10)),
        Self ! {data, Data}
    end),

    FetchRequest = #gw_fetch{file_id = "1234", offset = 2345, size = 3456,
        remote = {{127, 0, 0, 1}, 9999}, notify = [self()], retry = 0},

    timer:sleep(timer:seconds(2)),
    ?assertEqual(ok, rpc:call(Node, gen_server, call, [gateway, {test_call, 1, FetchRequest}])),
    Data = receive {data, D} -> D after timer:seconds(10) -> ?assert(false) end,

    ?assertEqual(#fetchrequest{file_id = "1234", offset = 2345, size = 3456},
                 gwproto_pb:decode_fetchrequest(Data)).


notify_all_interested_parties_test(Config) ->
    [Node | _] = ?config(nodes, Config),

    ?assertEqual(ok, rpc:call(Node, erlang, apply, [fun() ->

        {module, rt_priority_queue} = code:ensure_loaded(rt_priority_queue),
        ok = meck:new(gateway_dispatcher, [passthrough]),
        ok = meck:new(rt_utils, [passthrough]),

        ok = meck:expect(gateway_dispatcher, handle_cast,
            fun
                (#gw_fetch{size = Size, notify = Notify} = Action, State) ->
                    lists:foreach(fun(P) -> P ! {fetch_complete, Size, Action} end, Notify),
                    {noreply, State};
                (A, B) ->
                    meck:passthrough([A, B])
            end),

        ets:new(state, [named_table, public]),
        ets:insert(state, {pushed, 0}),

        ok = meck:expect(rt_utils, push,
            fun(ContainerRef, Block) ->
                ets:update_counter(state, pushed, 1),
                meck:passthrough([ContainerRef, Block])
            end),

        ok = meck:expect(rt_utils, pop,
            fun(ContainerRef) ->
                Pushed = ets:lookup_element(state, pushed, 2),
                case Pushed >= 2 of
                    true  -> meck:passthrough([ContainerRef]);
                    false -> {error, empty}
                end
            end),

        FetchRequest = #gw_fetch{file_id = "id", offset = 0, remote = {{127, 0, 0, 1}, 9999}, retry = 0},

        Self = self(),
        OtherSelf = spawn(fun() -> receive A -> Self ! A end end),

        gen_server:cast(gateway, {asynch, 1, FetchRequest#gw_fetch{size = 100, notify = [Self]}}),
        gen_server:cast(gateway, {asynch, 1, FetchRequest#gw_fetch{size = 200, notify = [OtherSelf]}}),

        Msg1 = receive A -> A after timer:seconds(10) -> timeout end,
        Msg2 = receive B -> B after timer:seconds(10) -> timeout end,

        ?assertEqual(Msg1, Msg2),
        ?assertMatch({fetch_complete, 200, #gw_fetch{file_id = "id", offset = 0, size = 200}}, Msg1),

        {_, _, #gw_fetch{notify = Notify}} = Msg1,
        ?assert(ordsets:is_subset(ordsets:from_list([Self, OtherSelf]), ordsets:from_list(Notify))),

        meck:unload(),
        ok

    end, []])).


%% ====================================================================
%% Helper functions
%% ====================================================================


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================


init_per_testcase(_, Config) ->
    ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    NodesUp = test_node_starter:start_test_nodes(1),
    [Node | _] = NodesUp,

    Port = 6666,
    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp,
        [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [Node]},
            {dns_port, 1317}, {db_nodes, [?DB_NODE]}, {heart_beat, 1},
            {global_registry_provider_cert_path, ?CERT}]]),

    lists:foreach(fun(N) ->
        gen_server:cast({?Node_Manager_Name, N}, do_heart_beat) end, NodesUp),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config)).


end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes).
