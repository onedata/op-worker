%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Tests for rtransfer oneprovider_module.
%% ===================================================================

-module(rtransfer_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/rtransfer/rtransfer.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% the actual application uses GRPCA-generated cert
-define(CERT, "./certs/onedataServerFuse.pem").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([test_gateway_delegation/1, test_retry_transfer/1, test_requester_receives_feedback/1,
         test_requester_assumes_multiple_0_read_is_fine/1]).

all() -> [test_gateway_delegation, test_retry_transfer, test_requester_receives_feedback,
          test_requester_assumes_multiple_0_read_is_fine].


%% ====================================================================
%% Test functions
%% ====================================================================


test_gateway_delegation(Config) ->
    [Node] = ?config(nodes, Config),

    ?assertEqual(ok, rpc:call(Node, erlang, apply,
        [fun() ->
            Self = self(),

            ok = meck:new(gateway, [passthrough]),
            ok = meck:new(gr_providers, [passthrough]),
            ok = meck:expect(gr_providers, get_details, fun(provider, _) -> {ok, #provider_details{urls = [<<"127.0.0.1">>]}} end),
            ok = meck:expect(gateway, handle, fun
                (_, #gw_fetch{}) -> Self ! gateway_received_message;
                (A, B) -> meck:passthrough([A, B])
            end),
            ok = gen_server:call(rtransfer, {test_call, 1, #request_transfer{size = 1, offset = 0, file_id = "id", notify = [self()], provider_id = <<"id">>}}),

            ?assert(receive gateway_received_message -> true after timer:seconds(10) -> false end),
            meck:unload(),
            ok
        end, []])).


test_retry_transfer(Config) ->
    [Node] = ?config(nodes, Config),

    CallResult = rpc:call(Node, erlang, apply,
        [fun() ->
            ok = meck:new(gateway, [passthrough]),
            ok = meck:new(gr_providers, [passthrough]),
            ok = meck:expect(gr_providers, get_details, fun(provider, _) -> {ok, #provider_details{urls = [<<"127.0.0.1">>]}} end),

            ok = meck:expect(gateway, handle, fun
                (_, #gw_fetch{notify = Notify} = Action) ->
                    lists:foreach(fun(P) -> P ! {fetch_error, my_error, Action} end, Notify)
            end),

            ok = gen_server:call(rtransfer, {test_call, 1, #request_transfer{size = 1, offset = 0, file_id = "id", notify = self(), provider_id = <<"id">>}}),

            ?assert(receive {transfer_error, my_error, {"id", 0, 1}} -> true after timer:seconds(10) -> false end),

            {ok, FetchRetryNumber} = application:get_env(?APP_Name, rtransfer_fetch_retry_number),
            ?assertEqual(FetchRetryNumber + 1, meck:num_calls(gateway, handle, ['_', '_'])),
            meck:unload(),
            ok
        end, []]),

    ?assertEqual(ok, CallResult).


test_requester_receives_feedback(Config) ->
    [Node] = ?config(nodes, Config),

    CallResult = rpc:call(Node, erlang, apply,
        [fun() ->
            ok = meck:new(gateway, [passthrough]),
            ok = meck:new(gr_providers, [passthrough]),
            ok = meck:expect(gr_providers, get_details, fun(provider, _) -> {ok, #provider_details{urls = [<<"127.0.0.1">>]}} end),

            ok = meck:expect(gateway, handle, fun
                (_, #gw_fetch{size = Size, notify = Notify} = Action) ->
                    lists:foreach(fun(P) -> P ! {fetch_complete, Size, Action} end, Notify)
            end),

            ok = gen_server:call(rtransfer, {test_call, 1, #request_transfer{size = 10240, offset = 0, file_id = "id", notify = self(), provider_id = <<"id">>}}),

            ?assert(receive {transfer_complete, 10240, {"id", 0, 10240}} -> true after timer:seconds(10) -> false end),
            meck:unload(),
            ok
        end, []]),

    ?assertEqual(ok, CallResult).


test_requester_assumes_multiple_0_read_is_fine(Config) ->
    [Node] = ?config(nodes, Config),

    CallResult = rpc:call(Node, erlang, apply,
        [fun() ->
            ok = meck:new(gateway, [passthrough]),
            ok = meck:new(gr_providers, [passthrough]),
            ok = meck:expect(gr_providers, get_details, fun(provider, _) -> {ok, #provider_details{urls = [<<"127.0.0.1">>]}} end),

            ok = meck:expect(gateway, handle, fun
                (_, #gw_fetch{offset = 0, notify = Notify} = Action) ->
                    lists:foreach(fun(P) -> P ! {fetch_complete, 100, Action} end, Notify);
                (_, #gw_fetch{notify = Notify} = Action) ->
                    lists:foreach(fun(P) -> P ! {fetch_complete, 0, Action} end, Notify)
            end),

            ok = gen_server:call(rtransfer, {test_call, 1, #request_transfer{size = 10240, offset = 0, file_id = "id", notify = self(), provider_id = <<"id">>}}),

            ?assert(receive {transfer_complete, 100, {"id", 0, 10240}} -> true; A -> A after timer:seconds(10) -> false end),
            {ok, FetchRetryNumber} = application:get_env(?APP_Name, rtransfer_fetch_retry_number),
            ?assertEqual(FetchRetryNumber + 2, meck:num_calls(gateway, handle, ['_', '_'])),
            meck:unload(),
            ok
        end, []]),

    ?assertEqual(ok, CallResult).


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

  lists:foreach(fun(N) -> gen_server:cast({?Node_Manager_Name, N}, do_heart_beat) end, NodesUp),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  timer:sleep(1000),

  ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config)).


end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes).
