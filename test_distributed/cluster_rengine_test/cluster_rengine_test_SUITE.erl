%% ==================================================================
%% @author Michal Sitko
%% @copyright (C) 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ==================================================================
%% @doc:
%%
%% @end
%% ==================================================================
-module(cluster_rengine_test_SUITE).

-include("logging.hrl").
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("veil_modules/cluster_rengine/cluster_rengine.hrl").

%% API
-export([test_event_subscription/1]).

-export([ccm_code1/0, worker_code/0]).

-export([all/0, init_per_testcase/2]).
all() -> [test_event_subscription].

%% @doc This is distributed test of dao_vfs:find_files.
%% It consists of series of dao_vfs:find_files with various file_criteria and comparing result to expected values.
%% Before testing it clears documents that may affect test and after testing it clears created documents.
%% @end
test_event_subscription(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  timer:sleep(500),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, []))
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  timer:sleep(1000),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers),

  StartAdditionalWorker = fun(Node, Module) ->
    case lists:member({Node, Module}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, Module, []}),
        ?assertEqual(ok, StartAns)
    end
  end,

  StartAdditionalWorker(CCM, rule_manager),
  lists:foreach(fun(Node) -> StartAdditionalWorker(Node, cluster_rengine) end, NodesUp),

  WriteEvent = #write_event{user_id = "1234", ans_pid = self()},

%%   gen_server:call({?Dispatcher_Name, CCM}, {cluster_rengine, 1, {event_arrived, WriteEvent}}),
  receive
    _ -> ?assert(false)
  after 1000
      -> ok
  end,

  subscribe_for_write_events(CCM, standard),
  gen_server:call({?Dispatcher_Name, CCM}, {cluster_rengine, 1, {event_arrived, WriteEvent}}),

  receive
    {ok, standard, _} -> ok
  after 1000
    -> ?assert(false)
  end,

  subscribe_for_write_events(CCM, tree),
  gen_server:call({?Dispatcher_Name, CCM}, {cluster_rengine, 1, {event_arrived, WriteEvent}}),

  receive
    {ok, standard, _} -> ok
  after 1000
    -> ?assert(false)
  end,
  receive
    Ans -> ok
  after 1000
    -> ?assert(false)
  end,

  ok.


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(2, true),
  [CCM | _] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config).

subscribe_for_write_events(Node, ProcessingMethod) ->
  EventHandlerMapFun = fun(#write_event{user_id = UserIdString}) ->
    string_to_integer(UserIdString)
  end,

  EventHandlerDispMapFun = fun (#write_event{user_id = UserId}) ->
    UserIdInt = string_to_integer(UserId),
    UserIdInt div 100
  end,

  EventHandler = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, ProcessingMethod, UserId}
  end,

  EventItem = #event_handler_item{processing_method = ProcessingMethod, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun},
  gen_server:call({?Dispatcher_Name, Node}, {rule_manager, 1, {add_event_handler, {write_event, EventItem}}}).

repeat(N, F) -> for(1, N, F).
for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

count_answers(ToReceive) ->
  Set = sets:new(),
  receive
    {ok, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 1000 ->
    {ToReceive, Set}
  end.

count_answers(0, Set) ->
  {0, Set};
count_answers(ToReceive, Set) ->
  receive
    {'EXIT', _, _} -> count_answers(ToReceive, Set);
    {ok, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 1000 ->
    {ToReceive, Set}
  end.

ccm_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

worker_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.

string_to_integer(SomeString) ->
  {SomeInteger, _} = string:to_integer(SomeString),
  case SomeString of
    error ->
      throw(badarg);
    _ -> SomeInteger
  end.