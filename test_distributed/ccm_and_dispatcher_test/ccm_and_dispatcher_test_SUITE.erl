%% Copyright
-module(ccm_and_dispatcher_test_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([modules_start_and_ping_test/1]).

-include("env_setter.hrl").
-include("registered_names.hrl").
-include("records.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

all() -> [modules_start_and_ping_test].

modules_start_and_ping_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, 6666}, {ccm_nodes, ['ccm@localhost']}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  StateNum0 = gen_server:call({global, ?CCM}, get_state_num),
  Check1 = (StateNum0 == 1),
  Check1 = true,

  gen_server:cast({global, ?CCM}, get_state_from_db),
  timer:sleep(100),
  State = gen_server:call({global, ?CCM}, get_state),
  Workers = State#cm_state.workers,
  Check2 = (length(Workers) == 1),
  Check2 = true,
  StateNum1 = gen_server:call({global, ?CCM}, get_state_num),
  Check3 = (StateNum1 == 2),
  Check3 = true,

  gen_server:cast({global, ?CCM}, init_cluster),
  State2 = gen_server:call({global, ?CCM}, get_state),
  Workers2 = State2#cm_state.workers,
  Jobs = ?Modules,
  Check4 = (length(Workers2) == length(Jobs)),
  Check4 = true,
  StateNum2 = gen_server:call({global, ?CCM}, get_state_num),
  Check5 = (StateNum2 == 3),
  Check5 = true,

  ProtocolVersion = 1,
  CheckModules = fun(M, Sum) ->
    Ans = gen_server:call(M, {test_call, ProtocolVersion, ping}),
    case Ans of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  Check6 = (PongsNum == length(Jobs)),
  Check6 = true,

  env_setter:stop_app(),
  env_setter:stop_test().