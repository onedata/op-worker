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
-include("oneprovider_modules/dao/dao.hrl").
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
  NodesUp = ?config(nodes, Config),

  Cert = ?COMMON_FILE("peer.pem"),
  Team = ?TEST_GROUP,
  [Node | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  ?assertMatch({ok, _}, rpc:call(Node, fslogic_storage, insert_storage, ["DirectIO", ?ARG_TEST_ROOT])),

  ?ENABLE_PROVIDER(Config),
  UserDoc1 = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, Team]),
  UserUUID = UserDoc1#db_document.uuid,






  SpaceName = "xyz",
  Spaces = [list_to_binary(SpaceName)],
  Size = [#spacemodified_size{provider = <<"1">>, size = 1024}, #spacemodified_size{provider = <<"2">>, size = 1000000}],
  M1 = #spacemodified{id = <<"id">>, name = list_to_binary(SpaceName), size = Size},
  M1Bytes = encode_update_request(M1),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M1Bytes])),

  test_utils:wait_for_request_handling(),
  Ans1 = rpc:call(Node, dao_lib, apply, [dao_vfs, get_space_file, ["spaces/" ++ SpaceName], ?ProtocolVersion]),
  ?assertMatch({ok, _},Ans1),
  {_, SpaceDoc} = Ans1,
  SpaceRec = SpaceDoc#db_document.record,
  SpaceDesc = proplists:get_value(?file_space_info_extestion, SpaceRec#file.extensions),
  ?assertEqual("id", SpaceDesc#space_info.space_id),
  ?assertEqual(list_to_binary(SpaceName), SpaceDesc#space_info.name),
  ?assertEqual([{<<"1">>, 1024}, {<<"2">>, 1000000}], SpaceDesc#space_info.size),

  Size2 = [#spacemodified_size{provider = <<"1">>, size = 1024}, #spacemodified_size{provider = <<"2">>, size = 1000}, #spacemodified_size{provider = <<"3">>, size = 50}],
  M1_2 = #spacemodified{id = <<"id">>, name = list_to_binary(SpaceName), size = Size2},
  M1_2Bytes = encode_update_request(M1_2),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M1_2Bytes])),

  test_utils:wait_for_request_handling(),
  Ans1_2 = rpc:call(Node, dao_lib, apply, [dao_vfs, get_space_file, ["spaces/" ++ SpaceName], ?ProtocolVersion]),
  ?assertMatch({ok, _},Ans1_2),
  {_, SpaceDoc2} = Ans1_2,
  SpaceRec2 = SpaceDoc2#db_document.record,
  SpaceDesc2 = proplists:get_value(?file_space_info_extestion, SpaceRec2#file.extensions),
  ?assertEqual("id", SpaceDesc2#space_info.space_id),
  ?assertEqual(list_to_binary(SpaceName), SpaceDesc2#space_info.name),
  ?assertEqual([{<<"1">>, 1024}, {<<"2">>, 1000}, {<<"3">>, 50}], SpaceDesc2#space_info.size),






  M2 = #usermodified{id = list_to_binary(UserUUID), spaces = Spaces},
  M2Bytes = encode_update_request(M2),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M2Bytes])),

  test_utils:wait_for_request_handling(),
  Ans2 = rpc:call(Node, dao_lib, apply, [dao_users, get_user, [{uuid, UserUUID}], ?ProtocolVersion]),
  ?assertMatch({ok, _}, Ans2),
  {_, UserDoc2} = Ans2,
  UserRec = UserDoc2#db_document.record,
  ?assertEqual(Spaces, UserRec#user.spaces),






  M3 = #groupmodified{id = <<"gr_id">>, name = <<"gr1">>},
  M3Bytes = encode_update_request(M3),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M3Bytes])),

  test_utils:wait_for_request_handling(),
  Ans3 = rpc:call(Node, dao_lib, apply, [dao_groups, get_group, ["gr_id"], ?ProtocolVersion]),
  ?assertMatch({ok, _}, Ans3),
  {_, GrDoc} = Ans3,
  GrRec = GrDoc#db_document.record,
  ?assertEqual(<<"gr1">>, GrRec#group_details.name),

  M3_2 = #groupmodified{id = <<"gr_id">>, name = <<"gr1_xyz">>},
  M3_2Bytes = encode_update_request(M3_2),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M3_2Bytes])),

  test_utils:wait_for_request_handling(),
  Ans3_2 = rpc:call(Node, dao_lib, apply, [dao_groups, get_group, ["gr_id"], ?ProtocolVersion]),
  ?assertMatch({ok, _}, Ans3_2),
  {_, GrDoc2} = Ans3_2,
  GrRec2 = GrDoc2#db_document.record,
  ?assertEqual(<<"gr1_xyz">>, GrRec2#group_details.name),





  M4 = #spaceremoved{id = <<"id">>},
  M4Bytes = encode_update_request(M4),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M4Bytes])),

  test_utils:wait_for_request_handling(),
  ?assertMatch({error,file_not_found}, rpc:call(Node, dao_lib, apply, [dao_vfs, get_space_file, ["spaces/" ++ SpaceName], ?ProtocolVersion])),






  M5 = #groupremoved{id = <<"gr_id">>},
  M5Bytes = encode_update_request(M5),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M5Bytes])),

  test_utils:wait_for_request_handling(),
  ?assertMatch({error,{not_found,deleted}}, rpc:call(Node, dao_lib, apply, [dao_groups, get_group, ["gr_id"], ?ProtocolVersion])),





  M6 = #userremoved{id = list_to_binary(UserUUID)},
  M6Bytes = encode_update_request(M6),
  ?assertEqual(ok, rpc:call(Node, gr_channel_handler, send_to_gr_channel, [M6Bytes])),

  test_utils:wait_for_request_handling(),
  ?assertMatch({error,{not_found,deleted}}, rpc:call(Node, dao_lib, apply, [dao_users, get_user, [{uuid, UserUUID}], ?ProtocolVersion])).

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

encode_update_request(Request) ->
  Decoder = "gr_messages",
  Decoder2 = "gr_communication_protocol",
  {ok, Data} = pb:encode(Decoder, Request),
  RequestName = atom_to_list(element(1, Request)),
  MainMessage = #message{protocol_version = ?ProtocolVersion, message_type = RequestName, message_decoder_name = Decoder, input = Data},
  {ok, Data2} = pb:encode(Decoder2, MainMessage),
  Data2.
