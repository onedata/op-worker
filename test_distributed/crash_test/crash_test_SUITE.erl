-module(crash_test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("env_setter.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").

-export([all/0]).
-export([ccm1_test/1, ccm2_test/1, worker_test/1, tester_test/1]).

all() -> [ccm1_test, ccm2_test, worker_test, tester_test].

ccm1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm}, {dispatcher_port, 5555}, {heart_beat, 1}, {initialization_time, 3}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(5000),
  env_setter:stop_app(),
  env_setter:stop_test().

ccm2_test(_Config) ->
  ok.

worker_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 6666}, {heart_beat, 1}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(5000),
  env_setter:stop_app(),
  env_setter:stop_test().

tester_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  pong = net_adm:ping('ccm1@localhost'),
  timer:sleep(3500),
  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
  Jobs = ?Modules,
  Check1 = (length(Workers) == length(Jobs)),
  Check1 = true,
  env_setter:stop_test().