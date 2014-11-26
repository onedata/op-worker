%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of GR push channel handler.
%% It contains tests that base on ct.
%% @end
%% ===================================================================

-module(gr_push_channel_test_SUITE).

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("gr_communication_protocol_pb.hrl").
-include("gr_messages_pb.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([update_test/1]).

all() ->
 [update_test].

-define(ProtocolVersion, 1).

%% ====================================================================
%% Test functions
%% ====================================================================

%% Tests if not permitted operations can not be executed by fslogic
update_test(Config) ->
%%   NodesUp = ?config(nodes, Config),
%%
%%   Cert = ?COMMON_FILE("peer.pem"),
%%   Team = ?TEST_GROUP,
%%   [Node | _] = NodesUp,
%%
%%   gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
%%   gen_server:cast({global, ?CCM}, {set_monitoring, on}),
%%   test_utils:wait_for_cluster_cast(),
%%   gen_server:cast({global, ?CCM}, init_cluster),
%%   test_utils:wait_for_cluster_init(),
%%
%%   ?ENABLE_PROVIDER(Config),
%%   UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, Team]),



%%   rpc:call(FSLogicNode, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ Team], ?ProtocolVersion]),

  ok.

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [FSLogicNode | _] = NodesUp,

  DB_Node = ?DB_NODE,
  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]}, {heart_beat, 1}]]),

  ?ENABLE_PROVIDER(lists:append([{port, Port}, {nodes, NodesUp}], Config)).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().

%% ====================================================================
%% Helper functions
%% ====================================================================



