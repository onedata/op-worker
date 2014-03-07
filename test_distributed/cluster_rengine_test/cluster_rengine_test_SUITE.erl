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

-include("veil_modules/dao/dao.hrl").
-include("logging.hrl").
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("veil_modules/cluster_rengine/cluster_rengine.hrl").

%% API
-export([test_event_subscription/1, test_event_aggregation/1, test_dispatching/1]).

-export([ccm_code1/0, worker_code/0]).

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
%all() -> [test_event_subscription, test_event_aggregation, test_dispatching].
all() -> [test_event_subscription, test_event_aggregation, test_dispatching].

-define(assert_received(ResponsePattern), receive
                                            ResponsePattern -> ok
                                          after 200
                                            -> ?assert(false)
                                          end).

-define(assert_received(ResponsePattern, Timeout), receive
                                            ResponsePattern -> ok
                                          after Timeout
                                            -> ?assert(false)
                                          end).
-define(SH, "DirectIO").

-record(check_quota_event, {ans_pid, user_id}).

test_event_subscription(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  WriteEvent = #write_event{user_id = "1234", ans_pid = self()},

  send_event(WriteEvent, CCM),
  assert_nothing_received(CCM),

  EventHandler = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, standard, self()}
  end,
  subscribe_for_write_events(CCM, standard, EventHandler),
  send_event(WriteEvent, CCM),
  ?assert_received({ok, standard, _}),
  assert_nothing_received(CCM),

  EventHandler2 = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, tree, self()}
  end,
  subscribe_for_write_events(CCM, tree, EventHandler2),
  send_event(WriteEvent, CCM),
  % from my observations it takes about 200ms until disp map fun is registered in cluster_manager
  timer:sleep(900),

  ?assert_received({ok, standard, _}),
  ?assert_received({ok, tree, _}, 3000),
  assert_nothing_received(CCM).


test_event_aggregation(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  EventHandler = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, tree, self()}
  end,
  subscribe_for_write_events(CCM, tree, EventHandler, #processing_config{init_counter = 4}),
  WriteEvent = #write_event{user_id = "1234", ans_pid = self()},

  repeat(3, fun() -> send_event(WriteEvent, CCM) end),
  timer:sleep(900),
  assert_nothing_received(CCM),

  send_event(WriteEvent, CCM),
  ?assert_received({ok, tree, _}),
  assert_nothing_received(CCM),

  repeat(3, fun() -> send_event(WriteEvent, CCM) end),
  assert_nothing_received(CCM),
  send_event(WriteEvent, CCM),
  ?assert_received({ok, tree, _}),
  assert_nothing_received(CCM).

test_dispatching(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  EventHandler = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, tree, self()}
  end,
  subscribe_for_write_events(CCM, tree, EventHandler),

  WriteEvent1 = #write_event{user_id = "1234", ans_pid = self()},
  WriteEvent2 = #write_event{user_id = "1235", ans_pid = self()},
  WriteEvent3 = #write_event{user_id = "1334", ans_pid = self()},
  WriteEvent4 = #write_event{user_id = "1335", ans_pid = self()},
  WriteEvent5 = #write_event{user_id = "1236", ans_pid = self()},
  WriteEvents = [WriteEvent1, WriteEvent2, WriteEvent3, WriteEvent4],
  SendWriteEvent = fun (Event) -> send_event(Event, CCM) end,

  SendWriteEvents = fun() ->
    spawn(fun() ->
      lists:foreach(SendWriteEvent, WriteEvents)
    end)
  end,

  SendWriteEvents(),
  timer:sleep(1000),
  count_answers(8),

  MessagesNum = 300,
  repeat(MessagesNum, SendWriteEvents),
  {Count, SetOfPids} = count_answers(4*MessagesNum),
  ?assertEqual(0, Count),
  ?assertEqual(6, sets:size(SetOfPids)),

  SendWriteEvents(),
  {Count2, SetOfPids2} = count_answers(4),
  ?assertEqual(0, Count2),
  ?assertEqual(4, sets:size(SetOfPids2)),

  % WriteEvent1 and WriteEvent5 should be dispatched to the same process
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent5),
  {Count3, SetOfPids3} = count_answers(2),
  ?assertEqual(0, Count3),
  ?assertEqual(1, sets:size(SetOfPids3)),

  % WriteEvent1 and WriteEvent2 should be dispatched to different processes on the same node
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent2),
  {Count4, SetOfPids4} = count_answers(2),
  ?assertEqual(0, Count4),
  ?assertEqual(2, sets:size(SetOfPids4)),
  NodesFromPids = fun(Pids) -> sets:from_list(lists:map(fun (Pid) -> node(Pid) end, sets:to_list(Pids))) end,
  SetOfNodes = NodesFromPids(SetOfPids4),
  ?assertEqual(1, sets:size(SetOfNodes)),

  % WriteEvent1 and WriteEvent3 should be dispatched to different processes on different nodes
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent3),
  {Count5, SetOfPids5} = count_answers(2),
  ?assertEqual(0, Count5),
  ?assertEqual(2, sets:size(SetOfPids5)),
  SetOfNodes2 = NodesFromPids(SetOfPids5),
  ?assertEqual(2, sets:size(SetOfNodes2)).

test_quota_case(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  Host = "localhost",
  Port = ?config(port, Config),

  %% files_manager call with given user's DN
  FM = fun(M, A, DN) ->
    Me = self(),
    Pid = spawn(CCM, fun() -> put(user_id, DN), Me ! {self(), apply(logical_files_manager, M, A)} end),
    receive
      {Pid, Resp} -> Resp
    end
  end,

  %% Init storage
  {InsertStorageAns, StorageUUID} = rpc:call(CCM, fslogic_storage, insert_storage, [?SH, ?TEST_ROOT]),
  ?assertEqual(ok, InsertStorageAns),

  %% Init users
  AddUser = fun(Login, Teams, Cert) ->
    {ReadFileAns, PemBin} = file:read_file(Cert),
    ?assertEqual(ok, ReadFileAns),
    {ExtractAns, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertEqual(rdnSequence, ExtractAns),
    {ConvertAns, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertEqual(ok, ConvertAns),
    DnList = [DN],

    Name = "user1 user1",
    Email = "user1@email.net",
    {CreateUserAns, #veil_document{uuid = UserID}} = rpc:call(CCM, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
    ?assertEqual(ok, CreateUserAns),
    {DnList, UserID}
  end,

  Login1 = "veilfstestuser",
  Teams1 = ["veilfstestgroup(Grp)"],
  Login2 = "veilfstestuser2",
  Teams2 = ["veilfstestgroup(Grp)"],
  Cert1 = ?COMMON_FILE("peer.pem"),
  {DN1, UserID1} = AddUser(Login1, Teams1, Cert1),
  %% END init users

  %% Init connections
  {ConAns1, Socket1} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}, auto_handshake]),
  ?assertEqual(ok, ConAns1),
  %% END init connections

  FileName = "events_quota_case",
  FileSize = 100,

  AnsCreate = FM(create, [FileName], DN1),
  ?assertEqual(ok, AnsCreate),
  AnsTruncate = FM(truncate, [FileName, FileSize], DN1),
  ?assertEqual(ok, AnsTruncate),
  {AnsGetFileAttr, _} = FM(getfileattr, [FileName], DN1),
  ?assertEqual(ok, AnsGetFileAttr),

  WriteEvent = #write_event{user_id = UserID1, ans_pid = self()},
  repeat(10, fun() -> send_event(WriteEvent, CCM) end),
  assert_nothing_received(CCM),

  EventHandler = fun(#write_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, write_event_processed, self()},
    CheckQuotaNeededEvent = #check_quota_event{user_id = UserId},
    send_event(CheckQuotaNeededEvent, CCM)
  end,

  subscribe_for_write_events(CCM, standard, EventHandler),
  send_event(WriteEvent, CCM),
  ?assert_received({ok, write_event_processed, _}),
  assert_nothing_received(CCM),

  CheckQuotaEventHandler = fun(#check_quota_event{user_id = UserId, ans_pid = AnsPid}) ->
    AnsPid ! {ok, check_quota_processed, self()}
    %if excedded send exceeded
  end,
  subscribe_for_events(check_quota_event, CCM, CheckQuotaEventHandler),
  send_event(WriteEvent, CCM),
  ?assert_received({ok, write_event_processed, _}),
  ?assert_received({ok, check_quota_processed, _}),
  assert_nothing_received(CCM).




%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(2, true),
  [CCM | WorkerNodes] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  Res = lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),

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
  StartAdditionalWorker(CCM, dao),

  nodes_manager:wait_for_cluster_init(length(NodesUp) - 1),
  Res.

end_per_testcase(distributed_test, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns);

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).


subscribe_for_write_events(Node, ProcessingMethod, EventHandler) ->
  subscribe_for_write_events(Node, ProcessingMethod, EventHandler, #processing_config{}).

subscribe_for_write_events(Node, ProcessingMethod, EventHandler, ProcessingConfig) ->
  EventHandlerMapFun = fun(#write_event{user_id = UserIdString}) ->
    string_to_integer(UserIdString)
  end,

  EventHandlerDispMapFun = fun(#write_event{user_id = UserId}) ->
    UserIdInt = string_to_integer(UserId),
    UserIdInt div 100
  end,

  EventItem = #event_handler_item{processing_method = ProcessingMethod, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = ProcessingConfig},
  gen_server:call({?Dispatcher_Name, Node}, {rule_manager, 1, self(), {add_event_handler, {write_event, EventItem}}}),

  receive
    ok -> ok
  after 400 ->
    ?assert(false)
  end,

  timer:sleep(800).

subscribe_for_events(EventType, Node, EventHandler) ->
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},
  gen_server:call({?Dispatcher_Name, Node}, {rule_manager, 1, self(), {add_event_handler, {EventType, EventItem}}}),

  receive
    ok -> ok
  after 400 ->
    ?assert(false)
  end,

  timer:sleep(800).


repeat(N, F) -> for(1, N, F).
for(N, N, F) -> [F()];
for(I, N, F) -> [F() | for(I + 1, N, F)].

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

assert_nothing_received(Node) ->
  receive
    Ans ->
%%       cluster_rengine:log("FAIL: ", Ans),
      gen_server:call({?Dispatcher_Name, Node}, {cluster_rengine, 1, {log, "FAIL:", Ans}}),
      ?assert(false)
  after 1000
    -> ok
  end.

send_event(Event, Node) ->
  gen_server:call({?Dispatcher_Name, Node}, {cluster_rengine, 1, {event_arrived, Event}}).

count_answers(ToReceive) ->
  Set = sets:new(),
  receive
    {ok, _, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 2000 ->
    {ToReceive, Set}
  end.

count_answers(0, Set) ->
  {0, Set};
count_answers(ToReceive, Set) ->
  receive
    {'EXIT', _, _} -> count_answers(ToReceive, Set);
    {ok, _, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 2000 ->
    {ToReceive, Set}
  end.
